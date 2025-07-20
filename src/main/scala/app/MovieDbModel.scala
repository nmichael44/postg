package app

import java.time.LocalDate

import app.permissions.Permissions.Permission

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

  final case class UserDetailsInDb(userId: Long, loginName: String, hashedPassword: String)

  final case class AuthenticatedUser(userId: Long, permissions: Set[Permission], issuedAt: Long, expiresAt: Long)

  final case class EmailMessage(
      from: String,
      tos: Seq[String],
      ccs: Seq[String],
      bccs: Seq[String],
      subject: String,
      body: String,
  )
end MovieDbModel
