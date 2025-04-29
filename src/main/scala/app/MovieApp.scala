package app

import cats.data.NonEmptyVector
import cats.effect.*
import cats.effect.implicits.*
import cats.effect.kernel.Async
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import cats.syntax.parallel.*

import java.nio.file.Paths
import scala.concurrent.duration.*
import scala.io.Source

import app.serviceslive.{ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive}
import app.AppConfig.AppConfig
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
import services.{ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerState}

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
        jobQ <- Queue.bounded[F, HttpWorker.Job[F]](BoundedQueueCapacity)
      } yield LiveServerState[F](movieReqCounts, jobQ)

  // The approach we have taken here is to have the worker fiber build the "recipe" i.e.
  // construct the F[_] program that we are going to execute.  This is in the spirit
  // of keeping the http4s fiber work to a minimum and have the worker do all the work.
  private def enqueueJobAndWaitForResult[F[_]: { Async as async, Logger as logger }](
      jobName: String,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
      programBuilder: () => F[Response[F]],
  ): F[Response[F]] = {
    import dsl.*

    for {
      d <- Deferred[F, Either[Throwable, Response[F]]]
      _ <- U.logi(s"Queueing job '$jobName'.")
      _ <- serverState.jobQueue.offer(HttpWorker.Job(jobName, programBuilder, d))
      _ <- U.logi(s"Job '$jobName' queued. Waiting for response.")
      outcome <- d.get // Wait for the answer
      _ <- U.logi(s"Job '$jobName': Response received.")
      response <- outcome match {
        case Right(resp) =>
          U.logi(s"Job '$jobName': Successful response.") *>
            async.pure(resp)
        case Left(e) =>
          U.loge(e, s"Job '$jobName' failed.  Returning internal server error.") *>
            dsl.InternalServerError()
      }
    } yield response
  }

  private def getDirectorsDetailsByName[F[_]: { Async, Logger as logger }](
      req: Request[F],
      mr: MovieRepositoryService[F],
      serverState: ServerState[F],
      directorPath: DirectorPath,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getDirectorsDetailsByName",
      serverState,
      dsl,
      () =>
        ensureOnlyAllowedParams(allowedParamsForGetDirectors, req, dsl)
          .getOrElse {
            for {
              directorsDetails <- mr.getDirectorsDetails(
                directorPath.firstName,
                directorPath.lastName,
              )
              _ <- U.logi("Fetching directors details")
              response <- Ok(directorsDetails.asJson)
            } yield response
          },
    )

  private def getDirectorDetails[F[_]: { Async, Logger as logger }](
      mr: MovieRepositoryService[F],
      serverState: ServerState[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getDirectorDetails",
      serverState,
      dsl,
      () =>
        for {
          directorDetailsMap <- mr.getDirectorDetails(NonEmptyVector.one(directorId))
          _ <- U.logi("Fetching director details")
          response <- directorDetailsMap
            .get(directorId)
            .map(directorDetails => Ok(directorDetails.asJson))
            .getOrElse(BadRequest(s"Director id: '$directorId' not found!"))
        } yield response,
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      mr: MovieRepositoryService[F],
      serverState: ServerState[F],
      actorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getActorDetails",
      serverState,
      dsl,
      () =>
        for {
          actorDetailsMap <- mr.getActorDetails(NonEmptyVector.one(actorId))
          _ <- U.logi(s"Fetching actor details for ID: $actorId")
          response <- actorDetailsMap
            .get(actorId)
            .map(actorDetails => Ok(actorDetails.asJson))
            .getOrElse(BadRequest(s"Actor id: '$actorId' not found!"))
        } yield response,
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

  private def getMoviesByDirectorIdFromDb[F[_]: MonadCancelThrow](
      mr: MovieRepositoryService[F],
      directorId: Long,
      serverState: ServerState[F],
  ): F[Seq[MovieDbModel.Movie]] =
    mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      .map(m => m.getOrElse(directorId, Seq.empty))

  private def getMoviesByDirectorId[F[_]: { Async, Logger as logger }](
      mr: MovieRepositoryService[F],
      serverState: ServerState[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getMoviesByDirectorId",
      serverState,
      dsl,
      () =>
        for {
          moviesMap <- mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
          _ <- U.logi(s"Fetching movies for director ID: $directorId")
          response <- Ok(moviesMap.getOrElse(directorId, Seq.empty).asJson)
        } yield response,
    )

  private def getMovieById[F[_]: { Async, Logger as logger }](
      mr: MovieRepositoryService[F],
      serverState: ServerState[F],
      movieId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getMovieById",
      serverState,
      dsl,
      () =>
        for {
          movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
          _ <- U.logi(s"Fetching movie details for ID: $movieId")
          response <- movieDetailsMap
            .get(movieId)
            .map(movie => Ok(movie.asJson))
            .getOrElse(BadRequest(s"Movie id: '$movieId' not found!"))
        } yield response,
    )

  private def getMovieByIdWithCounting[F[_]: { Async, Logger as logger }](
      mr: MovieRepositoryService[F],
      movieId: Long,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getMovieByIdWithCounting",
      serverState,
      dsl,
      () =>
        for {
          _ <- U.logi(
            s"Fetching movie details for ID: $movieId and incrementing counter count",
          )
          movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
          response <- movieDetailsMap
            .get(movieId)
            .fold(BadRequest(s"Movie id: '$movieId' not found!")) { movie =>
              for {
                newCountForMovie <- serverState.movieRequestCounts.modify { counts =>
                  val newCounts = counts.updatedWith(movieId)(_.fold(1)(_ + 1).some)
                  (newCounts, newCounts(movieId))
                }
                _ <- U.logi(s"Counter now is $newCountForMovie")
                okResponse <- Ok(movie.asJson)
              } yield okResponse
            }
        } yield response,
    )

  private def getContentOfFileName[F[_]: { Async, Logger as logger }](
      fileName: String,
      fileSystemService: FileSystemService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "getContentOfFileName",
      serverState,
      dsl,
      () =>
        for {
          _ <- U.logi(s"Asked to read file: '$fileName'.")
          res <- fileSystemService
            .readFileContent(fileName)
            .flatMap(Ok(_))
            .handleErrorWith(e => BadRequest(s"Error reading file: ${e.getMessage}"))
        } yield res,
    )

  private def readFileContent[F[_]: { Async }](path: java.nio.file.Path): Resource[F, String] =
    Resource
      .fromAutoCloseable(Async[F].blocking(Source.fromFile(path.toFile)))
      .evalMap(source => Async[F].blocking(source.mkString))

  // An alternative implementation of the function above.
  private def readFileContent2[F[_]: { Async as async }](
      path: java.nio.file.Path,
  ): Resource[F, String] =
    Resource
      .make(async.blocking(Source.fromFile(path.toFile)))(source => async.blocking(source.close()))
      .map(_.mkString)

  private def readTwoFilesInParallel[F[_]: { Async, Logger as logger }](
      fileName1: String,
      fileName2: String,
      fileSystemService: FileSystemService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "readTwoFilesInParallel",
      serverState,
      dsl,
      () =>
        for {
          _ <- U.logi(s"Reading the two files in parallel.")
          _ <- U.logi(s"FileName1 = '$fileName1'")
          _ <- U.logi(s"FileName2 = '$fileName2'")
          res <- fileSystemService
            .readTwoFilesInParallel(fileName1, fileName2)
            .flatMap(Ok(_))
            .handleErrorWith(e => BadRequest(s"Error reading files: ${e.getMessage}"))
        } yield res,
    )

  private def fetchCompanyData[F[_]: { Async, Logger }](
      companyName: String,
      apiClient: ExternalApiClientService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "fetchCompanyData",
      serverState,
      dsl,
      () =>
        for {
          data <- apiClient.fetchCompanyData(companyName)
          res <- Ok(data)
        } yield res,
    )

  private def fetchJasonObject[F[_]: { Async, Logger as logger }](
      apiClient: ExternalApiClientService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    enqueueJobAndWaitForResult(
      "fetchJasonObject",
      serverState,
      dsl,
      () =>
        for {
          _ <- U.logi("Fetching some json object recursively.")
          obj <- apiClient.fetchAsJson[MovieDbModel.Movie](
            Uri.unsafeFromString("http://127.0.0.1:8080/getMovieById/0"),
          )
          res <- Ok(obj.asJson)
        } yield res,
    )

  private def routes[F[_]: { Async, Logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): PartialFunction[Request[F], F[Response[F]]] =
    case req @ GET -> Root / "getDirectorsByName" :? firstNameOptionalQueryParamDecoderMatcher(
          firstName,
        ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
      getDirectorsDetailsByName(req, mr, serverState, DirectorPath(firstName, lastName), dsl)
    case GET -> Root / "getDirector" / LongVar(directorId) =>
      getDirectorDetails(mr, serverState, directorId, dsl)
    case GET -> Root / "getActor" / LongVar(actorId) =>
      getActorDetails(mr, serverState, actorId, dsl)
    case GET -> Root / "getMoviesByDirector" / LongVar(directorId) =>
      getMoviesByDirectorId(mr, serverState, directorId, dsl)
    case GET -> Root / "getMovieById" / LongVar(movieId) =>
      getMovieById(mr, serverState, movieId, dsl)
    case GET -> Root / "getMovieByIdWithCounting" / LongVar(movieId) =>
      getMovieByIdWithCounting(mr, movieId, serverState, dsl)
    case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) =>
      getContentOfFileName(fileName, fileSystemService, serverState, dsl)
    case GET -> Root / "readTwoFilesInParallel" :? fileName1QueryParamDecoderMatcher(
          fileName1,
        ) +& fileName2QueryParamDecoderMatcher(fileName2) =>
      readTwoFilesInParallel(fileName1, fileName2, fileSystemService, serverState, dsl)
    case GET -> Root / "fetchCompanyData" / companyName =>
      fetchCompanyData(companyName, apiClient, serverState, dsl)
    case GET -> Root / "getJsonObject" =>
      fetchJasonObject(apiClient, serverState, dsl)

  private def allRoutesComplete[F[_]: { Async, Logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileService: FileSystemService[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): HttpApp[F] =
    HttpRoutes.of[F](routes[F](mr, apiClient, fileService, serverState, dsl)).orNotFound

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

  inline private val NumberOfWorkers = 32

  private def startWorkers[F[_]: { Async, Logger }](
      numWorkers: Int,
      queue: Queue[F, HttpWorker.Job[F]],
      supervisor: Supervisor[F],
  ): F[Unit] =
    (0 until numWorkers).toVector
      .traverse_(workerId => supervisor.supervise(HttpWorker.worker(workerId, queue)))

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
            val fileService: FileSystemService[F] = FileSystemServiceLive.create
            val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)
            val dsl: Http4sDsl[F] = Http4sDsl[F]

            for {
              serverState <- LiveServerState.create[F]
              _ <- startWorkers(NumberOfWorkers, serverState.jobQueue, supervisor)
              httpApp: HttpApp[F] = allRoutesComplete[F](
                movieRepositoryService,
                externalApiClientService,
                fileService,
                serverState,
                dsl,
              )
              httpServer <- createServerResource(serverHostIP, serverHostPort, httpApp)
                .use(server =>
                  U.logi(
                    s"Server started with base uri: '${server.baseUri.toString}'.",
                  ) *> Async[F].never,
                )
                .as(ExitCode.Success)
            } yield httpServer
          }
      }
    }
