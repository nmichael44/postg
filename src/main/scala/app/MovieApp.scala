package app

import app.MovieDbModel.DirectorPath
import cats.data.NonEmptyList
import cats.effect.{Async, ExitCode, IO, MonadCancelThrow, Resource}
import cats.effect.implicits.parallelForGenSpawn
import cats.syntax.all.*
import cats.syntax.parallel.*

import java.nio.file.Paths
import scala.annotation.unused
import scala.concurrent.duration.*
import scala.io.Source
import com.comcast.ip4s.{ipv4, port}
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.fragments.in
import doobie.util.transactor.Transactor
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.dsl.io.*
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

object MovieApp:
  private def getDirectorsDetailsFromDb[F[_]: MonadCancelThrow](
      xa: Transactor[F],
      directorPath: DirectorPath,
  ): F[Vector[MovieDbModel.Director]] =
    val DirectorPath(firstName, lastName) = directorPath

    sql"select directorId, firstName, lastName, dob from t where ($firstName is null or firstName = $firstName) and ($lastName is null or lastName = $lastName)"
      .query[MovieDbModel.Director]
      .to[Vector]
      .transact(xa)

  private def getDirectorDetailsFromDb[F[_]: MonadCancelThrow](
      xa: Transactor[F],
      directorId: Long,
  ): F[Option[MovieDbModel.Director]] =
    sql"select directorId, firstName, lastName, dob from t where directorId = $directorId"
      .query[MovieDbModel.Director]
      .option
      .transact(xa)

  private def getDirectorsDetailsByName[F[_]: { MonadCancelThrow, Logger }](
      req: Request[F],
      xa: Transactor[F],
      directorPath: DirectorPath,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    ensureOnlyAllowedParams(allowedParamsForGetDirectors, dsl, req)
      .getOrElse {
        for {
          directorsDetails <- getDirectorsDetailsFromDb(xa, directorPath)
          _ <- Logger[F].info("Fetching directors details")
          response <- Ok(directorsDetails.asJson): F[Response[F]]
        } yield response
      }

  private def getDirectorDetails[F[_]: { MonadCancelThrow, Logger }](
      xa: Transactor[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      directorDetailsOpt <- getDirectorDetailsFromDb(xa, directorId)
      _ <- Logger[F].info("Fetching director details")
      response <- directorDetailsOpt.fold(BadRequest(s"Director id: '$directorId' not found!")) {
        directorDetails => Ok(directorDetails.asJson)
      }
    } yield response

  private def getActorDetailsFromDb[F[_]: MonadCancelThrow](
      xa: Transactor[F],
      actorId: Long,
  ): F[Option[MovieDbModel.Actor]] =
    sql"SELECT actorId, firstName, lastName, dob FROM actors WHERE actorId = $actorId"
      .query[MovieDbModel.Actor]
      .option
      .transact(xa)

  private def getActorDetails[F[_]: { MonadCancelThrow, Logger }](
      xa: Transactor[F],
      actorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      actorDetailsOpt <- getActorDetailsFromDb(xa, actorId)
      _ <- Logger[F].info(s"Fetching actor details for ID: $actorId")
      response <- actorDetailsOpt.fold(BadRequest(s"Actor id: '$actorId' not found!")) {
        actorDetails => Ok(actorDetails.asJson)
      }
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
      xa: Transactor[F],
      directorId: Long,
  ): F[Vector[MovieDbModel.Movie]] =
    sql"""SELECT m.movieId, m.title, m.year
          FROM movies m
          JOIN movieDirector md ON m.movieId = md.movieId
          WHERE md.directorId = $directorId
       """
      .query[MovieDbModel.Movie]
      .to[Vector]
      .transact(xa)

  private def getMoviesByDirectorId[F[_]: { MonadCancelThrow, Logger }](
      xa: Transactor[F],
      directorId: Long,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    for {
      movies <- getMoviesByDirectorIdFromDb(xa, directorId)
      _ <- Logger[F].info(s"Fetching movies for director ID: $directorId")
      response <- Ok(movies.asJson)
    } yield response

  private def getMoviesByDirectorNameFromDb[F[_] : MonadCancelThrow](
                                                                       xa: Transactor[F],
                                                                       directorPath: DirectorPath,
                                                                     ): F[String] =
    val DirectorPath(firstName, lastName) = directorPath
    for {
      directors <- getDirectorsDetailsFromDb()
    }

    def createQueryForMovies(directorIds: NonEmptyList[Long]) =
      sql"""select md.directorId, m.movieId, m.title, m.year from movies m, movieDirector md
            where
              md.movieId = m.movieId and
         """ ++ in(fr"md.directorId", directorIds)
      
    val DirectorPath(firstName, lastName) = directorPath

    val queryForDirectors =
      sql"""select directorId, firstName, lastName, dob from directors
            where
             ($firstName is null or d.firstName = $firstName) and
             ($lastName is null or d.lastName = $lastName
           """
      for {
        directors <- queryForDirectors.query[MovieDbModel.Director].to[Vector].tra
      }
      sql"""select coalesce(xx, '[]'::json) from (SELECT json_agg(json_build_object(
       'directorId', d.directorId,
       'firstName', d.firstName,
       'lastName', d.lastName,
       'dob', d.dob,
       'movies', coalesce((
         SELECT json_agg(json_build_object(
           'movieId', m.movieId,
           'title', m.title,
           'year', m.year
         ))
         FROM movies m
         JOIN movieDirector md ON m.movieId = md.movieId
         WHERE md.directorId = d.directorId
       ), '[]'::json)
     )) xx
     FROM directors d
     where ($firstName is null or d.firstName = $firstName) and ($lastName is null or d.lastName = $lastName)) zz
     """

    query
      .query[String]
      .unique
      .transact(xa)

  // Done as an experiment: Have the db build the final json answer...
  private def getMoviesByDirectorNameFromDb2[F[_] : MonadCancelThrow](
                                                                       xa: Transactor[F],
                                                                       directorPath: DirectorPath,
                                                                     ): F[String] =
    val DirectorPath(firstName, lastName) = directorPath

    val query =
      sql"""select coalesce(xx, '[]'::json) from (SELECT json_agg(json_build_object(
       'directorId', d.directorId,
       'firstName', d.firstName,
       'lastName', d.lastName,
       'dob', d.dob,
       'movies', coalesce((
         SELECT json_agg(json_build_object(
           'movieId', m.movieId,
           'title', m.title,
           'year', m.year
         ))
         FROM movies m
         JOIN movieDirector md ON m.movieId = md.movieId
         WHERE md.directorId = d.directorId
       ), '[]'::json)
     )) xx
     FROM directors d
     where ($firstName is null or d.firstName = $firstName) and ($lastName is null or d.lastName = $lastName)) zz
     """

    query
      .query[String]
      .unique
      .transact(xa)

  private def getMoviesByDirectorName[F[_]: { MonadCancelThrow, Logger }](
      req: Request[F],
      xa: Transactor[F],
      directorPath: DirectorPath,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    val r = ensureOnlyAllowedParams(allowedParamsForGetDirectors, dsl, req)
    r.getOrElse {
      for {
        _ <- Logger[F].info(s"Fetching movies per director by name: $directorPath")
        moviesPerDirector <- getMoviesByDirectorNameFromDb(xa, directorPath)
        _ <- Logger[F].info(s"Movies fetched: $moviesPerDirector")
        response <- Ok(moviesPerDirector)
      } yield response
    }

  private def getContentOfFileName[F[_]: { Async, Logger }](
      fileName: String,
      dsl: Http4sDsl[F],
  ): F[Response[F]] =
    import dsl.*

    val filePath = fs2.io.file.Path(fileName)
    for {
      _ <- Logger[F].info(s"Asked to read file: '$fileName'.")
      res <- fs2.io.file.Files
        .forAsync[F]
        .readAll(filePath) // Read file as Stream[IO, Byte]
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

  private def allRoutes[F[_]: { Async, Logger }](
      xa: Transactor[F],
      dsl: Http4sDsl[F],
  ): HttpRoutes[F] =
    import dsl.*

    HttpRoutes.of[F] {
      case req @ GET -> Root / "getDirectorsByName" :? firstNameOptionalQueryParamDecoderMatcher(
            firstName,
          ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
        getDirectorsDetailsByName(req, xa, DirectorPath(firstName, lastName), dsl)
      case GET -> Root / "getDirector" / LongVar(directorId) =>
        getDirectorDetails(xa, directorId, dsl)
      case GET -> Root / "getActor" / LongVar(actorId) =>
        getActorDetails(xa, actorId, dsl)
      case GET -> Root / "getMoviesByDirector" / LongVar(directorId) =>
        getMoviesByDirectorId(xa, directorId, dsl)
      case req @ GET -> Root / "getMoviesByDirectorName" :? firstNameOptionalQueryParamDecoderMatcher(
            firstName,
          ) +& lastNameOptionalQueryParamDecoderMatcher(lastName) =>
        getMoviesByDirectorName(req, xa, DirectorPath(firstName, lastName), dsl)
      case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) =>
        getContentOfFileName(fileName, dsl)
      case GET -> Root / "getFileExplicit" :? fileNameQueryParamDecoderMatcher(fileName) =>
        getContentOfFileNameExplicit(fileName, dsl)
      case GET -> Root / "readTwoFilesInParallel" :? fileName1QueryParamDecoderMatcher(
            fileName1,
          ) +& fileName2QueryParamDecoderMatcher(fileName2) =>
        readTwoFilesInParallel(fileName1, fileName2, dsl)
    }

  private def allRoutesComplete[F[_]: { Async, Logger }](
      xa: Transactor[F],
      dsl: Http4sDsl[F],
  ): HttpApp[F] =
    allRoutes[F](xa, dsl).orNotFound

  private def ensureOnlyAllowedParams[F[_]: MonadCancelThrow](
      allowedParams: Set[String],
      dsl: Http4sDsl[F],
      req: Request[F],
  ): Option[F[Response[F]]] =
    import dsl.*

    val providedParams = req.multiParams.keySet
    val extraParams = providedParams -- allowedParams
    Option.when(extraParams.nonEmpty)(
      BadRequest(s"Extra params found in quest: ${extraParams.mkString(", ")}."),
    )

  // Example call:
  // http://127.0.0.1:8080/getDirector/2
  def run(@unused args: List[String]): IO[ExitCode] =
    type F[A] = IO[A]

    val dsl: Http4sDsl[F] = Http4sDsl[F]

    Utils
      .readDbConfig[F]("Config.cfg")
      .use(config =>
        DoobieObj
          .xaResource(config)
          .use { (xa: Transactor[F]) =>
            Slf4jLogger.create[IO].flatMap { implicit logger =>
              EmberServerBuilder
                .default[IO]
                .withHost(ipv4"0.0.0.0")
                .withPort(port"8080")
                .withShutdownTimeout(10.seconds)
                .withHttpApp(allRoutesComplete[F](xa, dsl))
                .build
                .use(_ => Logger[F].info("Server started!") *> Async[F].never)
                .as(ExitCode.Success)
            }
          },
      )
