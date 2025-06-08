package app.services

import app.AuthUtils.AppToken
import app.MovieDbModel.UserDetailsInDb

trait AuthenticationService[F[_]]:
  def createToken(user: UserDetailsInDb, permissions: Seq[String]): F[String]
  def validateToken(token: String): F[Either[Throwable, AppToken]]
