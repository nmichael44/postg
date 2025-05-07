package app

import cats.effect.*
import cats.effect.kernel.Async
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import cats.Applicative

import scala.annotation.switch
import scala.concurrent.duration.*

import app.serviceslive.{ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive, ServerStateUpdateServiceLive}
import app.AppConfig.AppConfig
import app.JobSpecs.{JobKind, JobResult}
import app.JobSpecs.JobKind.{CreateMovie, FetchCompanyData, FetchJsonObject, GetActorDetails, GetDirectorDetails, GetDirectorsDetailsByName, GetFileContent, GetMovieById, GetMovieByIdWithCounting, GetMoviesByDirectorId, ReadTwoFilesInParallel}
import app.JobSpecs.JobResult.{ActorDetailsResult, CompanyDataResult, CreateMovieResult, DirectorDetailsResult, DirectorsDetailsByNameResult, FileContentResult, JsonObjectResult, MovieByIdResult, MovieByIdWithCountingResult, MoviesByDirectorIdResult, TwoFilesInParallelResult}
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
  private final val BoundedQueueCapacity: Int = 256

  private final case class LiveServerState[F[_]](
      movieRequestCounts: Ref[F, Map[Long, Int]],
      jobQueue: Queue[F, HttpWorker.Job[F]],
  ) extends ServerState[F]

  private object LiveServerState:
    def create[F[_]: Async]: F[ServerState[F]] =
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQueue <- Queue.bounded[F, HttpWorker.Job[F]](BoundedQueueCapacity)
      } yield LiveServerState[F](movieReqCounts, jobQueue)

  private enum WebServiceResult(val tag: Int):
    case OkStringRes(s: String) extends WebServiceResult(WebServiceResult.OkStringResTag)
    case OkJsonRes(json: Json) extends WebServiceResult(WebServiceResult.OkJsonResTag)
    case BadRequestRes(e: String) extends WebServiceResult(WebServiceResult.BadRequestResTag)
    case InternalServerErrorRes extends WebServiceResult(WebServiceResult.InternalServerErrorResTag)

  private object WebServiceResult:
    inline val OkStringResTag = 0
    inline val OkJsonResTag = 1
    inline val BadRequestResTag = 2
    inline val InternalServerErrorResTag = 3

  private final class Render[F[_]: Applicative](dsl: Http4sDsl[F]):
    import app.ImplicitConversions.as
    import dsl.*
    import WebServiceResult.*

    def apply(wsr: WebServiceResult): F[Response[F]] = (wsr.tag: @switch) match {
      case OkStringResTag => Ok(wsr.as[OkStringRes].s)
      case OkJsonResTag => Ok(wsr.as[OkJsonRes].json)
      case BadRequestResTag => BadRequest(wsr.as[BadRequestRes].e)
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
      _.director.fold(WebServiceResult.BadRequestRes(s"Director id: '$directorId' not found!"))(dir =>
        WebServiceResult.OkJsonRes(dir.asJson),
      ),
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      actorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, ActorDetailsResult](
      "Fetching actor details.",
      serverState,
      GetActorDetails(actorId),
      _.actor.fold(WebServiceResult.BadRequestRes(s"Actor id: '$actorId' not found!"))(act =>
        WebServiceResult.OkJsonRes(act.asJson),
      ),
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

  private def getMoviesByDirectorId[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, MoviesByDirectorIdResult](
      "Fetching movies by director Id.",
      serverState,
      GetMoviesByDirectorId(directorId),
      mvs => WebServiceResult.OkJsonRes(mvs.movies.asJson),
    )

  private def getMovieById[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      movieId: Long,
  ): F[WebServiceResult] =

    jobHandler[F, MovieByIdResult](
      "Fetching movie by Id.",
      serverState,
      GetMovieById(movieId),
      _.movie.fold(WebServiceResult.BadRequestRes(s"Movie id: '$movieId' not found!"))(mv =>
        WebServiceResult.OkJsonRes(mv.asJson),
      ),
    )

  private def getMovieByIdWithCounting[F[_]: { Async, Logger as logger }](
      movieId: Long,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, MovieByIdWithCountingResult](
      "Fetching movie by Id with counting.",
      serverState,
      GetMovieByIdWithCounting(movieId),
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
      getMoviesByDirectorId(serverState, directorId)
    case GET -> Root / "getMovieById" / LongVar(movieId) =>
      getMovieById(serverState, movieId)
    case GET -> Root / "getMovieByIdWithCounting" / LongVar(movieId) =>
      getMovieByIdWithCounting(movieId, serverState)
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

  private def allRoutesComplete[F[_]: { Async, Logger }](serverState: ServerState[F], render: Render[F]): HttpApp[F] =
    HttpRoutes
      .of[F](routes[F](serverState, render))
      .orNotFound

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
    val serverConnection = appConfig.getServerConnection
    val (host, port) = (serverConnection.getHost, serverConnection.getPort)

    (Ipv4Address.fromString(host), Port.fromInt(port)) match {
      case (Some(ipv4Address), Some(port)) => (ipv4Address, port)
      case (None, _) => throw AssertionError(s"Illegal ServerHostIP: '$host'.")
      case (_, None) => throw AssertionError(s"Illegal ServerHostPort: '$port'.")
    }

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
      (AppConfig, ServerState[F], http4s.client.Client[F], Supervisor[F], Transactor[F]),
    ]

  def run: IO[ExitCode] =
    type F = IO

    Slf4jLogger.create[F].flatMap { implicit logger =>
      val configResource: Resource[F, AppConfig] =
        Resource.eval(
          IO.fromEither(
            ConfigSource.default
              .at("app-config")
              .load[AppConfig]
              .left
              .map(pureconfig.error.ConfigReaderException[AppConfig]),
          ),
        )
      val coreResources: CoreResources[F] = for {
        appConfig <- configResource
        serverState <- Resource.eval(LiveServerState.create[F])
        httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxRedirects))
        supervisor <- Supervisor[F]
        xa <- DoobieObj.xaResource(appConfig)
      } yield (appConfig, serverState, httpClient, supervisor, xa)

      coreResources.use { (appConfig, serverState, httpClient, supervisor, xa) =>
        val externalApiClientService: ExternalApiClientService[F] =
          ExternalApiClientServiceLive.create[F](httpClient)
        val movieRepositoryService: MovieRepositoryService[F] =
          MovieRepositoryServiceLive.create(xa)
        val fileSystemService: FileSystemService[F] = FileSystemServiceLive.create

        val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)
        val render: Render[F] = Render(Http4sDsl[F])

        for {
          _ <- {
            val serverStateUpdateService: ServerStateUpdateService[F] =
              ServerStateUpdateServiceLive.create(serverState)

            HttpWorker.startWorkers(
              movieRepositoryService,
              externalApiClientService,
              fileSystemService,
              serverStateUpdateService,
              serverState.jobQueue,
              supervisor,
            )
          }
          exitCode <- {
            val httpApp: HttpApp[F] = allRoutesComplete[F](serverState, render)
            createServerResource(serverHostIP, serverHostPort, httpApp)
              .use(server =>
                U.logi(
                  s"Server started with base uri: '${server.baseUri.toString}'.",
                ) *> Async[F].never,
              )
              .as(ExitCode.Success)
          }
        } yield exitCode
      }
    }
