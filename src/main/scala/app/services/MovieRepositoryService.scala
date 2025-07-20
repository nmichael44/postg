package app.services

import cats.data.NonEmptyVector

import app.permissions.Permissions.Permission
import app.services.MovieRepositoryUtils.DBError
import app.MovieDbModel

trait MovieRepositoryService[F[_]]:
  def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Vector[MovieDbModel.Director]]

  def getDirectorDetails(directorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Director]]

  def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]]

  def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]]

  def getMovieDetails(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]]

  def createMovie(title: String, year: Int): F[Long]

  def createSystemUser(loginName: String, hashedPassword: String): F[Either[DBError, Long]]

  def fetchSystemUserByLoginName(loginName: String): F[Option[MovieDbModel.UserDetailsInDb]]

  def fetchSystemUserByUserId(userId: Long): F[Option[MovieDbModel.UserDetailsInDb]]

  def fetchSystemUserPermissions(userId: Long): F[Vector[Permission]]
