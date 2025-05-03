package app

import cats.effect.*
import cats.effect.kernel.Async
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*

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
import pureconfig.error.ConfigReaderException
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

  private def routeHandler[F[_]: { Async as async, Logger as logger }, T <: JobResult](
      msg: String,
      serverState: ServerState[F],
      job: JobKind,
      f: T => F[Response[F]],
      dsl: Http4sDsl[F],
  ): F[Response[F]] = {
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

    res.flatMap(r => mkResponse[F, T](dsl, r, f))
  }

  private def mkResponse[F[_]: Async, T](
      dsl: Http4sDsl[F],
      resEither: Either[Throwable, JobResult],
      f: T => F[Response[F]],
  ): F[Response[F]] =
    import dsl.*
    resEither.fold(_ => InternalServerError(), x => f(U.castTo[T](x)))

  private def getDirectorsDetailsByName[F[_]: { Async, Logger as logger }](
      req: Request[F],
      serverState: ServerState[F],
      directorPath: DirectorPath,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    ensureOnlyAllowedParams(allowedParamsForGetDirectors, req, dsl)
      .getOrElse {
        routeHandler[F, DirectorsDetailsByNameResult](
          "Fetching directors details by name.",
          serverState,
          GetDirectorsDetailsByName(directorPath.firstName, directorPath.lastName),
          dirs => Ok(dirs.directors.asJson),
          dsl,
        )
      }

  private def getDirectorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, DirectorDetailsResult](
      "Fetching directors details.",
      serverState,
      GetDirectorDetails(directorId),
      _.director.fold(BadRequest(s"Director id: '$directorId' not found!"))(dir => Ok(dir.asJson)),
      dsl,
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      actorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, ActorDetailsResult](
      "Fetching actor details.",
      serverState,
      GetActorDetails(actorId),
      _.actor.fold(BadRequest(s"Actor id: '$actorId' not found!"))(act => Ok(act.asJson)),
      dsl,
    )

  private val firstNameParam: String = "firstName"

  private object firstNameOptionalQueryParamDecoderMatcher
      extends OptionalQueryParamDecoderMatcher[String](firstNameParam)

  private val lastNameParam: String = "lastName"

  private object lastNameOptionalQueryParamDecoderMatcher
      extends OptionalQueryParamDecoderMatcher[String](lastNameParam)

  private val allowedParamsForGetDirectors: Set[String] = Set(firstNameParam, lastNameParam)

  private object fileNameQueryParamDecoderMatcher
      extends QueryParamDecoderMatcher[String]("fileName")

  private object fileName1QueryParamDecoderMatcher
      extends QueryParamDecoderMatcher[String]("fileName1")

  private object fileName2QueryParamDecoderMatcher
      extends QueryParamDecoderMatcher[String]("fileName2")

  private object titleQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("title")

  private object yearQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Int]("year")

  private def getMoviesByDirectorId[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, MoviesByDirectorIdResult](
      "Fetching movies by director Id.",
      serverState,
      GetMoviesByDirectorId(directorId),
      mvs => Ok(mvs.movies.asJson),
      dsl,
    )

  private def getMovieById[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      movieId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, MovieByIdResult](
      "Fetching movie by Id.",
      serverState,
      GetMovieById(movieId),
      _.movie.fold(BadRequest(s"Movie id: '$movieId' not found!"))(mv => Ok(mv.asJson)),
      dsl,
    )

  private def getMovieByIdWithCounting[F[_]: { Async, Logger as logger }](
      movieId: Long,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, MovieByIdWithCountingResult](
      "Fetching movie by Id with counting.",
      serverState,
      GetMovieByIdWithCounting(movieId),
      _.movie.fold(BadRequest(s"Movie id: '$movieId' not found!"))(mv => Ok(mv.asJson)),
      dsl,
    )

  private def createMovie[F[_]: { Async, Logger as logger }](
      title: String,
      year: Int,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, CreateMovieResult](
      "Creating new movie.",
      serverState,
      CreateMovie(title, year),
      cmr => Ok(cmr.movieId.toString),
      dsl,
    )

  private def getFileContent[F[_]: { Async, Logger as logger }](
      fileName: String,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, FileContentResult](
      "Getting file content.",
      serverState,
      GetFileContent(fileName),
      fc => Ok(fc.content),
      dsl,
    )

  private def readTwoFilesInParallel[F[_]: { Async, Logger as logger }](
      fileName1: String,
      fileName2: String,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, TwoFilesInParallelResult](
      "Reading two files in parallel.",
      serverState,
      ReadTwoFilesInParallel(fileName1, fileName2),
      tfp => Ok(tfp.content),
      dsl,
    )

  private def fetchCompanyData[F[_]: { Async, Logger }](
      companyName: String,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, CompanyDataResult](
      "Fetching company data.",
      serverState,
      FetchCompanyData(companyName),
      cd => Ok(cd.companyData),
      dsl,
    )

  private def fetchJasonObject[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    routeHandler[F, JsonObjectResult](
      "Fetching json object.",
      serverState,
      FetchJsonObject(),
      jor => Ok(jor.json),
      dsl,
    )

  private def routes[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): PartialFunction[Request[F], F[Response[F]]] =
    case req @ GET -> Root / "getDirectorsByName" :? firstNameOptionalQueryParamDecoderMatcher(
          firstName,
        ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
      getDirectorsDetailsByName(req, serverState, DirectorPath(firstName, lastName), dsl)
    case GET -> Root / "getDirector" / LongVar(directorId) =>
      getDirectorDetails(serverState, directorId, dsl)
    case GET -> Root / "getActor" / LongVar(actorId) =>
      getActorDetails(serverState, actorId, dsl)
    case GET -> Root / "getMoviesByDirector" / LongVar(directorId) =>
      getMoviesByDirectorId(serverState, directorId, dsl)
    case GET -> Root / "getMovieById" / LongVar(movieId) =>
      getMovieById(serverState, movieId, dsl)
    case GET -> Root / "getMovieByIdWithCounting" / LongVar(movieId) =>
      getMovieByIdWithCounting(movieId, serverState, dsl)
    case POST -> Root / "createMovie" :? titleQueryParamDecoderMatcher(
          title,
        ) +& yearQueryParamDecoderMatcher(year) =>
      createMovie(title, year, serverState, dsl)
    case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) =>
      getFileContent(fileName, serverState, dsl)
    case GET -> Root / "readTwoFilesInParallel" :? fileName1QueryParamDecoderMatcher(
          fileName1,
        ) +& fileName2QueryParamDecoderMatcher(fileName2) =>
      readTwoFilesInParallel(fileName1, fileName2, serverState, dsl)
    case GET -> Root / "fetchCompanyData" / companyName =>
      fetchCompanyData(companyName, serverState, dsl)
    case GET -> Root / "getJsonObject" =>
      fetchJasonObject(serverState, dsl)

  private def allRoutesComplete[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): HttpApp[F] =
    HttpRoutes
      .of[F](routes[F](serverState, dsl))
      .orNotFound

  private def ensureOnlyAllowedParams[F[_]: MonadCancelThrow](
      allowedParams: Set[String],
      req: Request[F],
      dsl: Http4sDsl[F],
  ): Option[F[Response[F]]] =
    import dsl.*

    val providedParams = req.multiParams.keySet
    val extraParams = providedParams -- allowedParams
    Option.when(extraParams.nonEmpty)(
      BadRequest(s"Extra params found in quest: ${extraParams.mkString(", ")}."),
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

  def run: IO[ExitCode] =
    type F = IO

    Slf4jLogger.create[F].flatMap { implicit logger =>
      ConfigSource.default.at("app-config").load[AppConfig] match {
        case Left(failures) => IO.raiseError(ConfigReaderException[AppConfig](failures))
        case Right(appConfig) =>
          val coreResources: Resource[F, (http4s.client.Client[F], Supervisor[F], Transactor[F])] =
            for {
              httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxRedirects))
              supervisor <- Supervisor[F]
              xa <- DoobieObj.xaResource(appConfig)
            } yield (httpClient, supervisor, xa)

          coreResources.use { (httpClient, supervisor, xa) =>
            val externalApiClientService: ExternalApiClientService[F] =
              ExternalApiClientServiceLive.create[F](httpClient)
            val movieRepositoryService: MovieRepositoryService[F] =
              MovieRepositoryServiceLive.create(xa)
            val fileSystemService: FileSystemService[F] = FileSystemServiceLive.create

            val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)
            val dsl: Http4sDsl[F] = Http4sDsl[F]

            for {
              serverState <- LiveServerState.create[F]
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
              httpServer <- {
                val httpApp: HttpApp[F] = allRoutesComplete[F](serverState, dsl)
                createServerResource(serverHostIP, serverHostPort, httpApp)
                  .use(server =>
                    U.logi(
                      s"Server started with base uri: '${server.baseUri.toString}'.",
                    ) *> Async[F].never,
                  )
                  .as(ExitCode.Success)
              }
            } yield httpServer
          }
      }
    }
