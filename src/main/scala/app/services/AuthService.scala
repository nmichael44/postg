package app.services

import app.permissions.Permissions.Permission
import app.MovieDbModel.{AuthenticatedUser, UserDetailsInDb}

trait AuthService[F[_]]:
  def createToken(user: UserDetailsInDb, permissions: Seq[Permission]): F[String]
  def validateToken(token: String): F[Either[Throwable, AuthenticatedUser]]
end AuthService
