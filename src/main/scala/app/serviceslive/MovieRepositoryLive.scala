package app.serviceslive

import cats.data.NonEmptyVector
import cats.effect.Async

import app.services.MovieRepositoryService
import app.MovieDbModel
import doobie.implicits.*
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor
import fs2.Stream.*

private final class MovieRepositoryLive[F[_]: Async] private (xa: Transactor[F])
    extends MovieRepositoryService[F]:
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

  override def getMoviesByIds(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]] =
    val movieIdsVec = movieIds.toVector
    val e = Map.empty[Long, MovieDbModel.Movie]

    sql"""select movieId, title, year from movies where movieId = ANY($movieIdsVec)"""
      .query[MovieDbModel.Movie]
      .stream
      .fold(e)((m, movie) => m.updated(movie.movieId, movie))
      .compile
      .lastOrError
      .transact(xa)

object MovieRepositoryLive:
  def create[F[_]: Async](xa: Transactor[F]): MovieRepositoryService[F] =
    MovieRepositoryLive[F](xa)
