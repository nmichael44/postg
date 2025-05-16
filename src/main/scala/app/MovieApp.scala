package app

import cats.effect.*
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import cats.Applicative

import scala.annotation.switch
import scala.concurrent.duration.*

import app.serviceslive.{ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive, ServerStateUpdateServiceLive}
import app.AppConfig.{ActorMemCacheConfig, AppConfig, BackendServerConfig, DirectorMemCacheConfig, MemCacheConfig, MovieMemCacheConfig}
import app.HttpWorker.CacheStatus
import app.JobSpecs.{JobKind, JobResult}
import app.JobSpecs.JobKind.{CreateMovie, FetchCompanyData, FetchJsonObject, GetActorDetails, GetDirectorDetails, GetDirectorsDetailsByName, GetFileContent, GetMovie, GetMovieWithCounting, GetMoviesByDirector, ReadTwoFilesInParallel}
import app.JobSpecs.JobResult.{ActorDetailsResult, CompanyDataResult, CreateMovieResult, DirectorDetailsResult, DirectorsDetailsByNameResult, FileContentResult, JsonObjectResult, MovieDetailsResult, MovieWithCountingResult, MoviesByDirectorResult, TwoFilesInParallelResult}
import app.MovieApp.AppMemCaches
import app.MovieDbModel.DirectorPath
import app.Utils as U
import com.comcast.ip4s.{Ipv4Address, Port}
import doobie.util.transactor.Transactor
import fs2.io.net.Network
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.middleware.FollowRedirect
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.dsl.io.*
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger
import pureconfig.ConfigSource
import services.{ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerState, ServerStateUpdateService}

object MovieApp:
  private[app] final case class LiveServerState[F[_]](
      movieRequestCounts: Ref[F, Map[Long, Int]],
      jobQueue: Queue[F, HttpWorker.Job[F]],
  ) extends ServerState[F]

  private[app] object LiveServerState:
    def create[F[_]: Async](backendServer: BackendServerConfig): F[ServerState[F]] = {
      val boundedQueueCapacity = backendServer.getBoundedQueueCapacity
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQueue <- Queue.bounded[F, HttpWorker.Job[F]](boundedQueueCapacity)

      } yield LiveServerState[F](movieReqCounts, jobQueue)
    }

  private[app] enum WebServiceResult(val tag: Int):
    case OkStringRes(s: String) extends WebServiceResult(WebServiceResult.OkStringResTag)
    case OkJsonRes(json: Json) extends WebServiceResult(WebServiceResult.OkJsonResTag)
    case NotFoundRes(s: String) extends WebServiceResult(WebServiceResult.NotFoundResTag)
    case BadRequestRes(e: String) extends WebServiceResult(WebServiceResult.BadRequestResTag)
    case InternalServerErrorRes extends WebServiceResult(WebServiceResult.InternalServerErrorResTag)

  private object WebServiceResult:
    inline val OkStringResTag = 0
    inline val OkJsonResTag = 1
    inline val NotFoundResTag = 2
    inline val BadRequestResTag = 3
    inline val InternalServerErrorResTag = 4

  private[app] final class Render[F[_]: Applicative](dsl: Http4sDsl[F]):
    import app.ImplicitConversions.castAs
    import dsl.*
    import WebServiceResult.*

    def apply(wsr: WebServiceResult): F[Response[F]] = (wsr.tag: @switch) match {
      case OkStringResTag => Ok(wsr.castAs[OkStringRes].s)
      case OkJsonResTag => Ok(wsr.castAs[OkJsonRes].json)
      case NotFoundResTag => NotFound(wsr.castAs[NotFoundRes].s)
      case BadRequestResTag => BadRequest(wsr.castAs[BadRequestRes].e)
      case InternalServerErrorResTag => InternalServerError()
    }

  private def jobHandler[F[_]: { Async as async, Logger as logger }, T <: JobResult](
      msg: String,
      serverState: ServerState[F],
      job: JobKind,
      f: T => WebServiceResult,
  ): F[WebServiceResult] =
    val jobName = job.shortName
    val res: F[Either[Throwable, JobResult]] = for {
      _ <- U.logi(msg)
      deferred <- Deferred[F, Either[Throwable, JobResult]]
      _ <- U.logi(s"Queueing job '$jobName'.")
      _ <- serverState.jobQueue.offer(HttpWorker.Job(job, deferred))
      _ <- U.logi(s"Job '$jobName' queued. Waiting for response.")
      outcome <- deferred.get // Wait for the answer
      _ <- U.logi(s"Job '$jobName': Response received.")
      _ <- outcome match {
        case Right(_) => U.logi(s"Job '$jobName': Successful response.")
        case Left(e) => U.loge(e, s"Job '$jobName' failed. Returning internal server error.")
      }
    } yield outcome

    res.map(mkResponse[F, T](_, f))

  private def mkResponse[F[_]: Async, T](
      resEither: Either[Throwable, JobResult],
      f: T => WebServiceResult,
  ): WebServiceResult =
    resEither.fold(_ => WebServiceResult.InternalServerErrorRes, jr => f(jr.asInstanceOf[T]))

  private def getDirectorsDetailsByName[F[_]: { Async, Logger as logger }](
      req: Request[F],
      serverState: ServerState[F],
      directorPath: DirectorPath,
  ): F[WebServiceResult] =
    ensureOnlyAllowedParams(allowedParamsForGetDirectors, req)
      .getOrElse {
        jobHandler[F, DirectorsDetailsByNameResult](
          "Fetching directors details by name.",
          serverState,
          GetDirectorsDetailsByName(directorPath.firstName, directorPath.lastName),
          dirs => WebServiceResult.OkJsonRes(dirs.directors.asJson),
        )
      }

  private def getDirectorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, DirectorDetailsResult](
      "Fetching directors details.",
      serverState,
      GetDirectorDetails(directorId),
      _.director.fold(WebServiceResult.NotFoundRes("Director not found"))(dir => WebServiceResult.OkJsonRes(dir.asJson)),
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      actorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, ActorDetailsResult](
      "Fetching actor details.",
      serverState,
      GetActorDetails(actorId),
      _.actor.fold(WebServiceResult.NotFoundRes("Actor not found"))(act => WebServiceResult.OkJsonRes(act.asJson)),
    )

  private val firstNameParam: String = "firstName"

  private object firstNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](firstNameParam)

  private val lastNameParam: String = "lastName"

  private object lastNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](lastNameParam)

  private val allowedParamsForGetDirectors: Set[String] = Set(firstNameParam, lastNameParam)

  private object fileNameQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName")

  private object fileName1QueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName1")

  private object fileName2QueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName2")

  private object titleQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("title")

  private object yearQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Int]("year")

  private def getMoviesByDirector[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, MoviesByDirectorResult](
      "Fetching movies by director Id.",
      serverState,
      GetMoviesByDirector(directorId),
      mvs => WebServiceResult.OkJsonRes(mvs.movies.asJson),
    )

  private def getMovie[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      movieId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, MovieDetailsResult](
      "Fetching movie by Id.",
      serverState,
      GetMovie(movieId),
      _.movie.fold(WebServiceResult.NotFoundRes("Movie not found"))(mv => WebServiceResult.OkJsonRes(mv.asJson)),
    )

  private def getMovieWithCounting[F[_]: { Async, Logger as logger }](
      movieId: Long,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, MovieWithCountingResult](
      "Fetching movie by Id with counting.",
      serverState,
      GetMovieWithCounting(movieId),
      _.movie.fold(WebServiceResult.BadRequestRes(s"Movie id: '$movieId' not found!"))(mv =>
        WebServiceResult.OkJsonRes(mv.asJson),
      ),
    )

  private def createMovie[F[_]: { Async, Logger as logger }](
      title: String,
      year: Int,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, CreateMovieResult](
      "Creating new movie.",
      serverState,
      CreateMovie(title, year),
      cmr => WebServiceResult.OkStringRes(cmr.movieId.toString),
    )

  private def getFileContent[F[_]: { Async, Logger as logger }](
      fileName: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, FileContentResult](
      "Getting file content.",
      serverState,
      GetFileContent(fileName),
      fc => WebServiceResult.OkStringRes(fc.content),
    )

  private def readTwoFilesInParallel[F[_]: { Async, Logger as logger }](
      fileName1: String,
      fileName2: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, TwoFilesInParallelResult](
      "Reading two files in parallel.",
      serverState,
      ReadTwoFilesInParallel(fileName1, fileName2),
      tfp => WebServiceResult.OkStringRes(tfp.content),
    )

  private def fetchCompanyData[F[_]: { Async, Logger }](companyName: String, serverState: ServerState[F]): F[WebServiceResult] =
    jobHandler[F, CompanyDataResult](
      "Fetching company data.",
      serverState,
      FetchCompanyData(companyName),
      cd => WebServiceResult.OkStringRes(cd.companyData),
    )

  private def fetchJasonObject[F[_]: { Async, Logger as logger }](serverState: ServerState[F]): F[WebServiceResult] =
    jobHandler[F, JsonObjectResult](
      "Fetching json object.",
      serverState,
      FetchJsonObject(),
      jor => WebServiceResult.OkJsonRes(jor.json),
    )

  private def routesDefinition[F[_]: { Async, Logger }](
      serverState: ServerState[F],
  ): PartialFunction[Request[F], F[WebServiceResult]] =
    case req @ GET -> Root / "getDirectorsByName" :? firstNameOptionalQueryParamDecoderMatcher(
          firstName,
        ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
      getDirectorsDetailsByName(
        req,
        serverState,
        DirectorPath(firstName, lastName),
      )
    case GET -> Root / "getDirector" / LongVar(directorId) =>
      getDirectorDetails(serverState, directorId)
    case GET -> Root / "getActor" / LongVar(actorId) =>
      getActorDetails(serverState, actorId)
    case GET -> Root / "getMoviesByDirector" / LongVar(directorId) =>
      getMoviesByDirector(serverState, directorId)
    case GET -> Root / "getMovie" / LongVar(movieId) =>
      getMovie(serverState, movieId)
    case GET -> Root / "getMovieWithCounting" / LongVar(movieId) =>
      getMovieWithCounting(movieId, serverState)
    case POST -> Root / "createMovie" :? titleQueryParamDecoderMatcher(
          title,
        ) +& yearQueryParamDecoderMatcher(year) =>
      createMovie(title, year, serverState)
    case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) =>
      getFileContent(fileName, serverState)
    case GET -> Root / "readTwoFilesInParallel" :? fileName1QueryParamDecoderMatcher(
          fileName1,
        ) +& fileName2QueryParamDecoderMatcher(fileName2) =>
      readTwoFilesInParallel(fileName1, fileName2, serverState)
    case GET -> Root / "fetchCompanyData" / companyName =>
      fetchCompanyData(companyName, serverState)
    case GET -> Root / "getJsonObject" =>
      fetchJasonObject(serverState)

  private def routes[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      render: Render[F],
  ): PartialFunction[Request[F], F[Response[F]]] =
    routesDefinition(serverState).andThen(_ >>= render.apply)

  private[app] def allRoutesComplete[F[_]: { Async, Logger }](serverState: ServerState[F], render: Render[F]): HttpApp[F] =
    HttpRoutes.of[F](routes[F](serverState, render)).orNotFound

  private def ensureOnlyAllowedParams[F[_]: Applicative as app](
      allowedParams: Set[String],
      req: Request[F],
  ): Option[F[WebServiceResult]] =
    val providedParams = req.multiParams.keySet
    val extraParams = providedParams -- allowedParams
    Option.when(extraParams.nonEmpty)(
      app.pure(
        WebServiceResult.BadRequestRes(
          s"Extra params found in quest: ${extraParams.mkString(", ")}.",
        ),
      ),
    )

  private def getServerHostIPPort(appConfig: AppConfig): (Ipv4Address, Port) =
    val serverConnection = appConfig.getServerConnectionConfig
    val (host, port) = (serverConnection.getHost, serverConnection.getPort)

    (Ipv4Address.fromString(host), Port.fromInt(port)) match {
      case (Some(ipv4Address), Some(port)) => (ipv4Address, port)
      case (None, _) => throw AssertionError(s"Illegal ServerHostIP: '$host'.")
      case (_, None) => throw AssertionError(s"Illegal ServerHostPort: '$port'.")
    }

  private def createCache[F[_]: { Temporal, Logger }, T](
      cacheName: String,
      capacity: Int,
      cleanupDuration: FiniteDuration,
  ): Resource[F, MemCache[F, Long, T]] =
    MemCache.createResource[F, Long, T](cacheName, capacity, cleanupDuration)

  private def createDirectorMemCache[F[_]: { Temporal, Logger }](
      directorMemCacheConfig: DirectorMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Director]] =
    createCache[F, MovieDbModel.Director](
      "Director MemCache",
      directorMemCacheConfig.getCapacity,
      directorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  private def createActorMemCache[F[_]: { Temporal, Logger }](
      actorMemCacheConfig: ActorMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Actor]] =
    createCache[F, MovieDbModel.Actor](
      "Actor MemCache",
      actorMemCacheConfig.getCapacity,
      actorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  private def createMovieMemCache[F[_]: { Temporal, Logger }](
      movieMemCacheConfig: MovieMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Movie]] =
    createCache[F, MovieDbModel.Movie](
      "Movie MemCache",
      movieMemCacheConfig.getCapacity,
      movieMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  final class AppMemCaches[F[_]](
      val directorCache: MemCache[F, Long, MovieDbModel.Director],
      val actorCache: MemCache[F, Long, MovieDbModel.Actor],
      val movieCache: MemCache[F, Long, MovieDbModel.Movie],
  )

  private def createMemCaches[F[_]: { Temporal, Logger }](memCacheConfig: MemCacheConfig): Resource[F, AppMemCaches[F]] =
    (
      createDirectorMemCache(memCacheConfig.getDirectorMemCacheConfig),
      createActorMemCache(memCacheConfig.getActorMemCacheConfig),
      createMovieMemCache(memCacheConfig.getMovieMemCacheConfig),
    ).mapN((d, a, m) => AppMemCaches[F](d, a, m))

  private def createConfigResource[F[_]: { Async as async, Logger }]() =
    Resource.eval[F, AppConfig](
      async.fromEither(
        ConfigSource.default
          .at("app-config")
          .load[AppConfig]
          .left
          .map(pureconfig.error.ConfigReaderException[AppConfig]),
      ),
    )

  // This is the number of redirects Ember will perform when a response
  // specifies that a redirection.
  inline private val MaxRedirects = 5

  private def createServerResource[F[_]: { Async, Network, Logger }](
      serverHostIP: Ipv4Address,
      serverHostPort: Port,
      httpApp: HttpApp[F],
  ): Resource[F, http4s.server.Server] =
    EmberServerBuilder
      .default[F]
      .withHost(serverHostIP)
      .withPort(serverHostPort)
      .withShutdownTimeout(5.seconds)
      .withHttpApp(httpApp)
      .build

  private type CoreResources[F[_]] =
    Resource[
      F,
      (
          AppConfig,
          ServerState[F],
          http4s.client.Client[F],
          Supervisor[F],
          Transactor[F],
          AppMemCaches[F],
      ),
    ]

  private def runHttpApp[F[_]: { Async, Network, Logger }](serverState: ServerState[F], appConfig: AppConfig): F[ExitCode] =
    val render: Render[F] = Render(Http4sDsl[F])
    val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)
    val httpApp: HttpApp[F] = allRoutesComplete[F](serverState, render)

    createServerResource(serverHostIP, serverHostPort, httpApp)
      .use(server => U.logi(s"Server started with base uri: '${server.baseUri.toString}'.") *> Async[F].never)
      .as(ExitCode.Success)

  def run: IO[ExitCode] =
    type F = IO

    Slf4jLogger.create[F] >>= { implicit logger =>
      val coreResources: CoreResources[F] = for {
        appConfig <- createConfigResource[F]()
        appMemCaches <- createMemCaches[F](appConfig.getMemCacheConfig)
        serverState <- Resource.eval(LiveServerState.create[F](appConfig.getBackendServerConfig))
        httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxRedirects))
        supervisor <- Supervisor[F](await = false)
        xa <- DoobieObj.xaResource(appConfig)
      } yield (appConfig, serverState, httpClient, supervisor, xa, appMemCaches)

      coreResources.use { (appConfig, serverState, httpClient, supervisor, xa, appMemCaches) =>
        val externalApiClientService: ExternalApiClientService[F] =
          ExternalApiClientServiceLive.create[F](httpClient)
        val movieRepositoryService: MovieRepositoryService[F] =
          MovieRepositoryServiceLive.create(xa)
        val fileSystemService: FileSystemService[F] = FileSystemServiceLive.create
        val serverStateUpdateService: ServerStateUpdateService[F] =
          ServerStateUpdateServiceLive.create(serverState)

        HttpWorker.startWorkers(
          appConfig.getBackendServerConfig,
          movieRepositoryService,
          externalApiClientService,
          fileSystemService,
          serverStateUpdateService,
          serverState.jobQueue,
          supervisor,
          appMemCaches,
          CacheStatus.CachesEnabled,
        ) *> runHttpApp(serverState, appConfig)
      }
    }
