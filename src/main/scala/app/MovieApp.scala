package app

import app.AppConfig.AppConfig
import app.MovieDbModel.DirectorPath
import cats.data.NonEmptyVector
import cats.effect.implicits.*
import cats.effect.std.Queue
import cats.effect.*
import cats.syntax.all.*
import cats.syntax.parallel.*
import com.comcast.ip4s.{Ipv4Address, Port}
import doobie.util.transactor.Transactor
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.middleware.FollowRedirect
import org.http4s.dsl.Http4sDsl
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.dsl.io.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import java.nio.file.Paths
import scala.annotation.unused
import scala.concurrent.duration.*
import scala.io.Source

object MovieApp:
  private sealed trait ServerState[F[_]] {
    val movieRequestCounts: Ref[F, Map[Long, Int]]
    val jobQueue: Queue[F, Job]
  }

  private case class Job(jobName: String)

  private final val BoundedQueueCapacity: Int = 256

  private final case class LiveServerState[F[_]](
      movieRequestCounts: Ref[F, Map[Long, Int]],
      jobQueue: Queue[F, Job],
  ) extends ServerState[F]

  private object LiveServerState:
    def create[F[_]: { Async, Logger }]: F[ServerState[F]] =
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQ <- Queue.bounded[F, Job](BoundedQueueCapacity)
      } yield new LiveServerState[F](movieReqCounts, jobQ)

  private def getDirectorsDetailsByName[F[_]: { MonadCancelThrow, Logger }](
      req: Request[F],
      mr: MovieRepository[F],
      directorPath: DirectorPath,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    ensureOnlyAllowedParams(allowedParamsForGetDirectors, req, dsl)
      .getOrElse {
        for {
          directorsDetails <- mr.getDirectorsDetails(directorPath.firstName, directorPath.lastName)
          _ <- Logger[F].info("Fetching directors details")
          response <- Ok(directorsDetails.asJson): F[Response[F]]
        } yield response
      }

  private def getDirectorDetails[F[_]: { MonadCancelThrow, Logger }](
      mr: MovieRepository[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      directorDetailsMap <- mr.getDirectorDetails(NonEmptyVector.one(directorId))
      _ <- Logger[F].info("Fetching director details")
      response <- directorDetailsMap
        .get(directorId)
        .map(directorDetails => Ok(directorDetails.asJson))
        .getOrElse(BadRequest(s"Director id: '$directorId' not found!"))
    } yield response

  private def getActorDetails[F[_]: { MonadCancelThrow, Logger }](
      mr: MovieRepository[F],
      actorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      actorDetailsMap <- mr.getActorDetails(NonEmptyVector.one(actorId))
      _ <- Logger[F].info(s"Fetching actor details for ID: $actorId")
      response <- actorDetailsMap
        .get(actorId)
        .map(actorDetails => Ok(actorDetails.asJson))
        .getOrElse(BadRequest(s"Actor id: '$actorId' not found!"))
    } yield response

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
      mr: MovieRepository[F],
      directorId: Long,
      serverState: ServerState[F],
  ): F[Seq[MovieDbModel.Movie]] =
    mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      .map(m => m.getOrElse(directorId, Seq.empty))

  private def getMoviesByDirectorId[F[_]: { MonadCancelThrow, Logger }](
      mr: MovieRepository[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      moviesMap <- mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      _ <- Logger[F].info(s"Fetching movies for director ID: $directorId")
      response <- Ok(moviesMap.getOrElse(directorId, Seq.empty).asJson)
    } yield response

  private def getMovieById[F[_]: { MonadCancelThrow, Logger }](
      mr: MovieRepository[F],
      movieId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
      _ <- Logger[F].info(s"Fetching movie details for ID: $movieId")
      response <- movieDetailsMap
        .get(movieId)
        .map(movie => Ok(movie.asJson))
        .getOrElse(BadRequest(s"Movie id: '$movieId' not found!"))
    } yield response

  private def getMovieByIdWithCounting[F[_]: { MonadCancelThrow, Logger }](
      mr: MovieRepository[F],
      movieId: Long,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    val logger = Logger[F]
    for {
      _ <- logger.info(s"Fetching movie details for ID: $movieId and incrementing counter count")
      movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
      response <- movieDetailsMap
        .get(movieId)
        .fold(BadRequest(s"Movie id: '$movieId' not found!")) { movie =>
          for {
            newCountForMovie <- serverState.movieRequestCounts.modify { counts =>
              val newCounts = counts.updatedWith(movieId)(_.fold(1)(_ + 1).some)
              (newCounts, newCounts(movieId))
            }
            _ <- logger.info(s"Counter now is $newCountForMovie")
            okResponse <- Ok(movie.asJson)
          } yield okResponse
        }
    } yield response

  private def getContentOfFileName[F[_]: { Async, Logger }](
      fileName: String,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      _ <- Logger[F].info(s"Asked to read file: '$fileName'.")
      res <- fs2.io.file.Files
        .forAsync[F]
        .readAll(fs2.io.file.Path(fileName)) // Read file as Stream[IO, Byte]
        .through(fs2.text.utf8.decode) // Decode to UTF-8 string
        .compile
        .string
        .flatMap(Ok(_))
        .handleErrorWith(ex => BadRequest(s"Error reading file: ${ex.getMessage}"))
    } yield res

  private def getContentOfFileNameExplicit[F[_]: { Async, Logger }](
      fileName: String,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      _ <- Logger[F].info(s"Asked to read file explicitly: '$fileName'.")
      res <- readFileContent(Paths.get(fileName)).use(c => Ok(Async[F].pure(c)))
    } yield res

  private def readFileContent[F[_]: { Async }](path: java.nio.file.Path): Resource[F, String] =
    Resource
      .fromAutoCloseable(Async[F].blocking(Source.fromFile(path.toFile)))
      .evalMap(source => Async[F].blocking(source.mkString))

  // An alternative implementation of the function above.
  private def readFileContent2[F[_]: { Async }](path: java.nio.file.Path): Resource[F, String] =
    Resource
      .make(
        Async[F].blocking(Source.fromFile(path.toFile)),
      )(source => Async[F].blocking(source.close()))
      .map(_.mkString)

  private def readTwoFilesInParallel[F[_]: { Async, Logger }](
      fileName1: String,
      fileName2: String,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    val logger = Logger[F]
    for {
      _ <- logger.info(s"Reading the two files in parallel.")
      _ <- logger.info(s"FileName1 = '$fileName1'")
      _ <- logger.info(s"FileName2 = '$fileName2'")
      res <- Ok(
        (
          readFileContent(Paths.get(fileName1)).use(Async[F].pure),
          readFileContent(Paths.get(fileName2)).use(Async[F].pure),
        ).parMapN((c1, c2) => c1 + c2),
      )
    } yield res

  private def fetchCompanyData[F[_]: { Async }](
      companyName: String,
      apiClient: ExternalApiClient[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      data <- apiClient.fetchCompanyData(companyName)
      res <- Ok(data)
    } yield res

  private def fetchJasonObject[F[_]: { Async, Logger }](
      apiClient: ExternalApiClient[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      _ <- Logger[F].info("Fetching some json object recursively.")
      obj <- apiClient.fetchAsJson[MovieDbModel.Movie](
        Uri.unsafeFromString("http://127.0.0.1:8080/getMovieById/0"),
      )
      res <- Ok(obj.asJson)
    } yield res

  private def enqueueJob[F[_]: { Async, Logger }](
      jobName: String,
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      success <- serverState.jobQueue.tryOffer(Job(jobName))
      res <-
        if success
        then Ok(s"Task '$jobName' was enqueued properly.")
        else BadRequest(s"Could not enqueue '$jobName'. Queue was full.  Try again later.")
    } yield res

  // Example call:
  // http://127.0.0.1:8080/getDirector/2
  private def allRoutes[F[_]: { Async, Logger }](
      mr: MovieRepository[F],
      apiClient: ExternalApiClient[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): HttpRoutes[F] =
    import dsl.*

    HttpRoutes.of[F] {
      case req @ GET -> Root / "getDirectorsByName" :? firstNameOptionalQueryParamDecoderMatcher(
            firstName,
          ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
        getDirectorsDetailsByName(req, mr, DirectorPath(firstName, lastName), dsl)
      case GET -> Root / "getDirector" / LongVar(directorId) =>
        getDirectorDetails(mr, directorId, dsl)
      case GET -> Root / "getActor" / LongVar(actorId) =>
        getActorDetails(mr, actorId, dsl)
      case GET -> Root / "getMoviesByDirector" / LongVar(directorId) =>
        getMoviesByDirectorId(mr, directorId, dsl)
      case GET -> Root / "getMovieById" / LongVar(movieId) =>
        getMovieById(mr, movieId, dsl)
      case GET -> Root / "getMovieByIdWithCounting" / LongVar(movieId) =>
        getMovieByIdWithCounting(mr, movieId, serverState, dsl)
      case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) =>
        getContentOfFileName(fileName, dsl)
      case GET -> Root / "getFileExplicit" :? fileNameQueryParamDecoderMatcher(fileName) =>
        getContentOfFileNameExplicit(fileName, dsl)
      case GET -> Root / "readTwoFilesInParallel" :? fileName1QueryParamDecoderMatcher(
            fileName1,
          ) +& fileName2QueryParamDecoderMatcher(fileName2) =>
        readTwoFilesInParallel(fileName1, fileName2, dsl)
      case GET -> Root / "fetchCompanyData" / companyName =>
        fetchCompanyData(companyName, apiClient, dsl)
      case GET -> Root / "getJsonObject" =>
        fetchJasonObject(apiClient, dsl)
      case GET -> Root / "enqueueJob" / jobName =>
        enqueueJob(jobName, serverState, dsl)
    }

  private def allRoutesComplete[F[_]: { Async, Logger }](
      mr: MovieRepository[F],
      apiClient: ExternalApiClient[F],
      serverState: ServerState[F],
      dsl: Http4sDsl[F],
  ): HttpApp[F] =
    allRoutes[F](mr, apiClient, serverState, dsl).orNotFound

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
      case (None, _) => throw new AssertionError(s"Illegal ServerHostIP: '$host'.")
      case (_, None) =>
        throw new AssertionError(s"Illegal ServerHostPort: '$port'.")
    }

  private def worker[F[_]: { Temporal, Logger }](workerId: Int, queue: Queue[F, Job]): F[Nothing] =
    val logger = Logger[F]

    val processJob: F[Unit] = for {
      _ <- logger.info(s"Worker '$workerId' waiting for work.")
      job <- queue.take
      _ <- logger.info(s"Worker '$workerId' received job '${job.jobName}'.")
      _ <- Temporal[F].sleep(500.milliseconds)
      _ <- logger.info(s"Worker '$workerId' completed job '${job.jobName}'.")
    } yield ()

    processJob.foreverM

  private final val NumberOfWorkers: Int = 16

  private def startWorkers[F[_]: { Temporal, Logger }](
      numWorkers: Int,
      queue: Queue[F, Job],
  ): F[Unit] =
    (1 to numWorkers).toList
      .parTraverse_(workerId => worker(workerId, queue).start)

  private final val MaxRedirects: Int = 5

  def run(@unused args: List[String]): IO[ExitCode] =
    type F = IO

    ConfigSource.default.at("app-config").load[AppConfig] match {
      case Left(failures) => IO.raiseError(ConfigReaderException[AppConfig](failures))
      case Right(appConfig) =>
        EmberClientBuilder
          .default[F]
          .build
          .map(client => FollowRedirect[F](MaxRedirects)(client))
          .use { httpClient =>
            val externalApiClient = ExternalApiClientImpl.create[F](httpClient)
            DoobieObj
              .xaResource(appConfig)
              .use { (xa: Transactor[F]) =>
                val movieRepository: MovieRepository[F] = MovieRepositoryDb.create(xa)
                val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)

                Slf4jLogger.create[IO].flatMap { implicit logger =>
                  LiveServerState.create[F].flatMap { serverState =>
                    val dsl: Http4sDsl[F] = Http4sDsl[F]
                    val httpRoutes: HttpApp[F] =
                      allRoutesComplete[F](movieRepository, externalApiClient, serverState, dsl)

                    startWorkers(NumberOfWorkers, serverState.jobQueue) *>
                      EmberServerBuilder
                        .default[IO]
                        .withHost(serverHostIP)
                        .withPort(serverHostPort)
                        .withShutdownTimeout(10.seconds)
                        .withHttpApp(httpRoutes)
                        .build
                        .use(_ => Logger[F].info("Server started!") *> Async[F].never)
                        .as(ExitCode.Success)
                  }
                }
              }
          }
    }
