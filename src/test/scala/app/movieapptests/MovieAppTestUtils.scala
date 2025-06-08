package app.movieapptests

import cats.data.NonEmptyVector
import cats.effect.MonadCancelThrow

import scala.util.control.NoStackTrace

import app.services.MovieRepositoryService
import app.MovieDbModel

object MovieAppTestUtils:
  val MovieRepositoryServiceNotImplemented: Exception =
    new Exception("MovieRepositoryService not properly overridden in test") with NoStackTrace

  class MovieRepositoryServiceShunning[F[_]: MonadCancelThrow as mc] extends MovieRepositoryService[F]:
    override def getDirectorsDetails(firstName: Option[String], lastName: Option[String]): F[Seq[MovieDbModel.Director]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def getDirectorDetails(directorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Director]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def getMoviesByDirectorId(directorIds: NonEmptyVector[Long]): F[Map[Long, Seq[MovieDbModel.Movie]]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def getMovieDetails(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def createMovie(title: String, year: Int): F[Long] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def createSystemUser(loginName: String, hashedPassword: String): F[Int] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def fetchSystemUserByLoginName(loginName: String): F[Option[MovieDbModel.UserDetailsInDb]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
    override def fetchSystemUserByUserId(userId: Int): F[Option[MovieDbModel.UserDetailsInDb]] =
      mc.raiseError(MovieRepositoryServiceNotImplemented)
