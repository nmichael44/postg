package app

import cats.effect.Async
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor

final class MovieRepositoryDb[F[_]: Async](xa: Transactor[F]) extends MovieRepository[F]:
  def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Vector[MovieDbModel.Director]] =
    sql"select directorId, firstName, lastName, dob from t where ($firstName is null or firstName = $firstName) and ($lastName is null or lastName = $lastName)"
      .query[MovieDbModel.Director]
      .to[Vector]
      .transact(xa)

  def getDirectorDetails(directorId: Long): F[Option[MovieDbModel.Director]] =
    sql"select directorId, firstName, lastName, dob from t where directorId = $directorId"
      .query[MovieDbModel.Director]
      .option
      .transact(xa)

  def getActorDetails(actorId: Long): F[Option[MovieDbModel.Actor]] =
    sql"SELECT actorId, firstName, lastName, dob FROM actors WHERE actorId = $actorId"
      .query[MovieDbModel.Actor]
      .option
      .transact(xa)

  def getMoviesByDirectorId(directorId: Long): F[Vector[MovieDbModel.Movie]] =
    sql"""SELECT m.movieId, m.title, m.year
            FROM movies m
            JOIN movieDirector md ON m.movieId = md.movieId
            WHERE md.directorId = $directorId
         """
      .query[MovieDbModel.Movie]
      .to[Vector]
      .transact(xa)

  def getMoviesByDirectorName(firstName: Option[String], lastName: Option[String]): F[String] = ???
