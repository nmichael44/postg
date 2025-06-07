package app

import java.time.LocalDate

import io.circe.generic.auto.*

object MovieDbModel:
  final case class DirectorPath(firstName: Option[String], lastName: Option[String]):
    override def toString: String =
      s"{ firstName = ${firstName.getOrElse("None")}, lastName = ${lastName.getOrElse("None")} }"

  final case class Actor(actorId: Long, firstName: String, lastName: String, dob: LocalDate):
    override def toString: String = s"Actor($firstName $lastName)"

  final case class Director(
      directorId: Long,
      firstName: String,
      lastName: String,
      dob: LocalDate,
  ):
    override def toString: String = s"Director($firstName $lastName)"

  final case class Movie(
      movieId: Long,
      title: String,
      year: Int,
  ):
    override def toString: String = s"Movie($title)"

  final case class DirectorWithMovies(
      director: MovieDbModel.Director,
      movies: Vector[MovieDbModel.Movie],
  ):
    override def toString: String = s"DirectorWithMovies($director, $movies)"

  final case class UserDetails(loginName: String, password: String)

  final case class UserDetailsWithId(userId: Int, loginName: String, password: String)
