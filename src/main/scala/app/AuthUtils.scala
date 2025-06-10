package app

object AuthUtils:
  case class AuthenticatedUser(userId: Long, permissions: Set[String], issuedAt: Long, expiresAt: Long)
