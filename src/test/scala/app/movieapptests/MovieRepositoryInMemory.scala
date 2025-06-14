package app.movieapptests

import cats.data.NonEmptyVector
import cats.effect.{Async, Ref}
import cats.syntax.all.*

import java.time.LocalDate

import app.services.MovieRepositoryService
import app.services.MovieRepositoryUtils.DBError
import app.JobSpecs.CreateSystemUserError
import app.MovieDbModel

final class MovieRepositoryInMemory[F[_]: Async as async](
    directorsRef: Ref[F, Map[Long, MovieDbModel.Director]],
    actorsRef: Ref[F, Map[Long, MovieDbModel.Actor]],
    moviesRef: Ref[F, Map[Long, MovieDbModel.Movie]],
    movieToDirectorRef: Ref[F, Map[Long, Long]],
) extends MovieRepositoryService[F]:
  override def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Seq[MovieDbModel.Director]] =
    directorsRef.get.map(
      _.values
        .filter { director =>
          firstName.forall(_ == director.firstName) && lastName.forall(_ == director.lastName)
        }
        .toVector,
    )

  override def getDirectorDetails(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, MovieDbModel.Director]] =
    directorsRef.get.map(m => directorIds.toVector.mapFilter(id => m.get(id).map((id, _))).toMap)

  override def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]] =
    actorsRef.get.map(m => actorIds.toVector.mapFilter(id => m.get(id).map((id, _))).toMap)

  override def getMoviesByDirectorId(directorIds: NonEmptyVector[Long]): F[Map[Long, Seq[MovieDbModel.Movie]]] =
    (movieToDirectorRef.get, moviesRef.get).mapN { (movieToDirector, movies) =>
      directorIds.toVector.mapFilter { directorId =>
        val directorMovies: Seq[MovieDbModel.Movie] =
          movieToDirector.view.filter(_._2 == directorId).map(p => movies(p._1)).toVector
        Option.when(directorMovies.nonEmpty)((directorId, directorMovies))
      }.toMap
    }

  override def getMovieDetails(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]] =
    moviesRef.get.map(m => movieIds.toVector.mapFilter(id => m.get(id).map((id, _))).toMap)

  override def createMovie(title: String, year: Int): F[Long] =
    moviesRef.modify { m =>
      val movieId = m.size.toLong
      (m.updated(movieId, MovieDbModel.Movie(movieId, title, year)), movieId)
    }

  override def createSystemUser(loginName: String, hashedPassword: String): F[Either[DBError, Int]] =
    ???

  override def fetchSystemUserByLoginName(loginName: String): F[Option[MovieDbModel.UserDetailsInDb]] =
    ???

  override def fetchSystemUserByUserId(userId: Int): F[Option[MovieDbModel.UserDetailsInDb]] =
    ???

object MovieRepositoryInMemory:
  def createWithDefaultDb[F[_]: Async]: F[MovieRepositoryService[F]] =
    create(Directors, Actors, Movies, MovieToDirector)

  def create[F[_]: Async](
      directors: Map[Long, MovieDbModel.Director],
      actors: Map[Long, MovieDbModel.Actor],
      movies: Map[Long, MovieDbModel.Movie],
      movieToDirector: Map[Long, Long],
  ): F[MovieRepositoryService[F]] =
    for {
      directorsRef <- Ref.of(directors)
      actorsRef <- Ref.of(actors)
      moviesRef <- Ref.of(movies)
      movieToDirectorRef <- Ref.of(movieToDirector)
    } yield MovieRepositoryInMemory[F](directorsRef, actorsRef, moviesRef, movieToDirectorRef)

  private val Directors: Map[Long, MovieDbModel.Director] = Map(
    0L -> MovieDbModel.Director(0L, "Steven", "Spielberg", LocalDate.of(1965, 5, 1)),
    1L -> MovieDbModel.Director(1L, "Neo", "Michael", LocalDate.of(1970, 4, 19)),
    2L -> MovieDbModel.Director(2L, "Neo", "Momonedes", LocalDate.of(2014, 1, 19)),
  )

  private val Actors: Map[Long, MovieDbModel.Actor] = Map(
    0L -> MovieDbModel.Actor(0L, "Neo", "Michael", LocalDate.of(1970, 4, 19)),
  )

  private val Movies: Map[Long, MovieDbModel.Movie] =
    Map(
      0L -> MovieDbModel.Movie(0L, "Xorkatikes malakies", 1980),
      1L -> MovieDbModel.Movie(1L, "Tsioftes", 2010),
    )

  private val MovieToDirector: Map[Long, Long] = Map(
    0L -> 1L,
    1L -> 1L,
  )
