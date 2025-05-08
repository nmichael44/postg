package app

import cats.data.NonEmptyVector
import cats.effect.Async

import java.time.LocalDate

import app.services.MovieRepositoryService
import app.ImplicitConversions.*

final class MovieRepositoryInMemory[F[_]: Async] extends MovieRepositoryService[F]:
  private val Directors: Map[Long, MovieDbModel.Director] = Map(
    0L -> MovieDbModel.Director(0L, "Steven", "Spielberg", LocalDate.of(1965, 5, 1)),
    1L -> MovieDbModel.Director(1L, "Neo", "Michael", LocalDate.of(1970, 4, 19)),
    2L -> MovieDbModel.Director(2L, "Neo", "Momonedes", LocalDate.of(2014, 1, 19)),
  )

  private val Actors: Map[Long, MovieDbModel.Actor] = Map(
    0L -> MovieDbModel.Actor(0L, "Neo", "Michael", LocalDate.of(1970, 4, 19)),
  )

  private val Movies: Map[Long, MovieDbModel.Movie] = Map(
    0L -> MovieDbModel.Movie(0L, "Xorkatikes malakies", 1980),
    1L -> MovieDbModel.Movie(1L, "Tsioftes", 2010),
  )

  private val MovieToDirector: Map[Long, Long] = Map(
    0L -> 1L,
    1L -> 1L,
  )

  override def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Seq[MovieDbModel.Director]] =
    Async[F].delay:
      Directors.values.filter { director =>
        firstName.forall(_ == director.firstName) && lastName.forall(_ == director.lastName)
      }.toVector

  override def getDirectorDetails(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, MovieDbModel.Director]] =
    Async[F].delay:
      directorIds.view.flatMap(id => Directors.get(id).map(e => (id, e))).toMap

  override def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]] =
    Async[F].delay:
      actorIds.view.flatMap(id => Actors.get(id).map(e => (id, e))).toMap

  override def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]] =
    Async[F].delay:
      directorIds.view
        .flatMap { directorId =>
          MovieToDirector.iterator.filter(p => p._2 == directorId)
        }
        .toVector
        .groupMap(_._2)(p => Movies(p._1))

  override def getMovieDetails(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]] =
    val e = Map.empty[Long, MovieDbModel.Movie]
    Async[F].delay:
      movieIds.toVector.foldLeft(e) { (m, movieId) =>
        Movies.get(movieId).fold(m)(m.updated(movieId, _))
      }

  override def createMovie(title: String, year: Int): F[Long] =
    ??? // For now
