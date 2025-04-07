package app

import cats.data.NonEmptyVector
import cats.effect.Async

trait MovieRepository[F[_]]:
  def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Seq[MovieDbModel.Director]]

  def getDirectorDetails(directorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Director]]

  def getActorDetails(actorIds: NonEmptyVector[Long]): F[Map[Long, MovieDbModel.Actor]]

  def getMoviesByDirectorId(
      directorIds: NonEmptyVector[Long],
  ): F[Map[Long, Seq[MovieDbModel.Movie]]]
