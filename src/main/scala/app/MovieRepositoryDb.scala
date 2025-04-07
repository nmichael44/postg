package app

import cats.data.NonEmptyVector
import cats.effect.Async

import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor
import fs2.Stream.*

final class MovieRepositoryDb[F[_]: Async](xa: Transactor[F]) extends MovieRepository[F]:
  def getDirectorsDetails(
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

  def getDirectorDetails(directorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Director]] =
    val directorIdsVec = directorIds.toVector
    val e = Map.empty[Long, MovieDbModel.Director]

    sql"""select directorId, firstName, lastName, dob from t where directorId = ANY($directorIdsVec)"""
      .query[MovieDbModel.Director]
      .stream
      .fold(e)((m, d) => m.updated(d.directorId, d))
      .compile
      .lastOrError
      .transact(xa)

  def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]] =
    val actorIdsVec = actorIds.toVector
    val e = Map.empty[Long, MovieDbModel.Actor]

    sql"""SELECT actorId, firstName, lastName, dob FROM actors WHERE actorId = ANY($actorIdsVec)"""
      .query[MovieDbModel.Actor]
      .stream
      .fold(e)((m, a) => m.updated(a.actorId, a))
      .compile
      .lastOrError
      .transact(xa)

  def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]] =
    val directorIdsVec = directorIds.toVector
    val e = Map.empty[Long, List[MovieDbModel.Movie]]

    sql"""SELECT md.directorId, m.movieId, m.title, m.year
            FROM movies m
            JOIN movieDirector md ON m.movieId = md.movieId
            WHERE md.directorId = ANY($directorIdsVec)
         """
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

object MovieRepositoryDb:
  def create[F[_]: Async](xa: Transactor[F]): MovieRepository[F] =
    new MovieRepositoryDb[F](xa)
