package app.serviceslive

import cats.data.NonEmptyVector
import cats.effect.Async
import cats.implicits.*

import app.permissions.Permissions
import app.permissions.Permissions.Permission
import app.services.MovieRepositoryService
import app.services.MovieRepositoryUtils.DBError
import app.MovieDbModel
import app.MovieDbModel.UserDetailsInDb
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.{Get, Read}
import doobie.util.transactor.Transactor
import org.postgresql.util.PSQLException
import MovieRepositoryServiceLive.given

private final class MovieRepositoryServiceLive[F[_]: Async as async] private (xa: Transactor[F])
    extends MovieRepositoryService[F]:
  override def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Vector[MovieDbModel.Director]] =
    sql"""select directorId, firstName, lastName, dob from t
          where
           ($firstName is null or firstName = $firstName)
           and ($lastName is null or lastName = $lastName)"""
      .query[MovieDbModel.Director]
      .to[Vector]
      .transact(xa)

  override def getDirectorDetails(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, MovieDbModel.Director]] =
    val directorIdsVec = directorIds.toVector
    val e = Map.empty[Long, MovieDbModel.Director]

    sql"""select directorId, firstName, lastName, dob from t where directorId = ANY($directorIdsVec)"""
      .query[MovieDbModel.Director]
      .stream
      .fold(e)((m, d) => m.updated(d.directorId, d))
      .compile
      .lastOrError
      .transact(xa)

  override def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]] =
    val actorIdsVec = actorIds.toVector
    val e = Map.empty[Long, MovieDbModel.Actor]

    sql"""SELECT actorId, firstName, lastName, dob FROM actors WHERE actorId = ANY($actorIdsVec)"""
      .query[MovieDbModel.Actor]
      .stream
      .fold(e)((m, a) => m.updated(a.actorId, a))
      .compile
      .lastOrError
      .transact(xa)

  override def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]] =
    val directorIdsVec = directorIds.toVector
    val e = Map.empty[Long, List[MovieDbModel.Movie]]

    sql"""SELECT md.directorId, m.movieId, m.title, m.year
            FROM movies m
            JOIN movieDirector md ON m.movieId = md.movieId
            WHERE md.directorId = ANY($directorIdsVec)"""
      .query[(Long, Long, String, Int)]
      .stream
      .fold(e) { case (m, (directorId, movieId, title, year)) =>
        val movie = MovieDbModel.Movie(movieId, title, year)
        m.updatedWith(directorId) { elemOpt =>
          Some(elemOpt match {
            case Some(movies) => movie :: movies
            case None => List(movie)
          })
        }
      }
      .compile
      .lastOrError
      .transact(xa)

  override def getMovieDetails(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]] =
    val movieIdsVec = movieIds.toVector
    val e = Map.empty[Long, MovieDbModel.Movie]

    sql"""select movieId, title, year from movies where movieId = ANY($movieIdsVec)"""
      .query[MovieDbModel.Movie]
      .stream
      .fold(e)((m, movie) => m.updated(movie.movieId, movie))
      .compile
      .lastOrError
      .transact(xa)

  override def createMovie(title: String, year: Int): F[Long] =
    sql"""insert into movies (title, year) values($title, $year)""".update
      .withUniqueGeneratedKeys[Long]("movieid")
      .transact(xa)

  private val PostgresDuplicateValueSqlState: String = "23505"

  override def createSystemUser(loginName: String, hashedPassword: String): F[Either[DBError, Long]] =
    sql"""insert into systemUsers (loginName, hashedPassword) values($loginName, $hashedPassword)""".update
      .withUniqueGeneratedKeys[Long]("userid")
      .attempt
      .flatMap {
        case Right(userId) => Right(userId).pure
        case Left(e: PSQLException) if e.getSQLState == PostgresDuplicateValueSqlState =>
          Left(DBError.DuplicateLoginName(loginName)).pure
        case Left(e) => doobie.FC.raiseError(e)
      }
      .transact(xa)

  override def fetchSystemUserByLoginName(loginName: String): F[Option[MovieDbModel.UserDetailsInDb]] =
    sql"""select userId, loginName, hashedPassword from systemUsers where loginName = $loginName"""
      .query[MovieDbModel.UserDetailsInDb]
      .option
      .transact(xa)

  override def fetchSystemUserByUserId(userId: Long): F[Option[MovieDbModel.UserDetailsInDb]] =
    sql"""select userId, loginName, hashedPassword from systemUsers where userId = $userId"""
      .query[MovieDbModel.UserDetailsInDb]
      .option
      .transact(xa)

  override def fetchSystemUserPermissions(userId: Long): F[Vector[Permission]] =
    sql"""select p.description from userPermissions up, permissions p where up.userid = $userId and up.permissionId = p.permissionId"""
      .query[Permission]
      .to[Vector]
      .transact(xa)
end MovieRepositoryServiceLive

object MovieRepositoryServiceLive:
  def create[F[_]: Async](xa: Transactor[F]): MovieRepositoryService[F] =
    MovieRepositoryServiceLive[F](xa)

  given Get[Permission] = summon[Get[String]].map(Permissions.fromString)
end MovieRepositoryServiceLive
