package app.serviceslive

import cats.data.NonEmptyVector
import cats.effect.Async

import app.services.MovieRepositoryService
import app.MovieDbModel
import app.MovieDbModel.UserDetailsWithId
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor
import fs2.Stream.*

private final class MovieRepositoryServiceLive[F[_]: Async] private (xa: Transactor[F]) extends MovieRepositoryService[F]:
  override def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Seq[MovieDbModel.Director]] =
    sql"""select directorId, firstName, lastName, dob from t
          where
           ($firstName is null or firstName = $firstName)
           and ($lastName is null or lastName = $lastName)"""
      .query[MovieDbModel.Director]
      .to[Vector]
      .map(identity)
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

  override def createSystemUser(loginName: String, password: String): F[Int] =
    sql"""insert into systemUsers (loginName, hashedPassword) values($loginName, $password)""".update
      .withUniqueGeneratedKeys[Int]("userid")
      .transact(xa)

  override def fetchSystemUserByLoginName(loginName: String): F[Option[MovieDbModel.UserDetailsWithId]] =
    sql"""select userId, loginName, hashedPassword from systemUsers where loginName = $loginName"""
      .query[MovieDbModel.UserDetailsWithId]
      .option
      .transact(xa)

  override def fetchSystemUserByUserId(userId: Int): F[Option[MovieDbModel.UserDetailsWithId]] =
    sql"""select userId, loginName, hashedPassword from systemUsers where userId = $userId"""
      .query[MovieDbModel.UserDetailsWithId]
      .option
      .transact(xa)

object MovieRepositoryServiceLive:
  def create[F[_]: Async](xa: Transactor[F]): MovieRepositoryService[F] =
    MovieRepositoryServiceLive[F](xa)
