package app.services

import app.AuthUtils.AppToken
import app.MovieDbModel.UserDetailsWithId

trait AuthenticationService[F[_]]:
  def createToken(user: UserDetailsWithId, permissions: Seq[String]): F[String]
  def validateToken(token: String): F[Either[Throwable, AppToken]]
