package app.services

import cats.data.NonEmptyVector
import cats.effect.Async

import app.MovieDbModel

trait MovieRepositoryService[F[_]]:
  def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Seq[MovieDbModel.Director]]

  def getDirectorDetails(directorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Director]]

  def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]]

  def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]]

  def getMoviesByIds(movieIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Movie]]
