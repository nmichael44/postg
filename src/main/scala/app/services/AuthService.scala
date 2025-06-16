package app.services

import app.MovieDbModel.{AuthenticatedUser, UserDetailsInDb}

trait AuthService[F[_]]:
  def createToken(user: UserDetailsInDb, permissions: Seq[String]): F[String]
  def validateToken(token: String): F[Either[Throwable, AuthenticatedUser]]
