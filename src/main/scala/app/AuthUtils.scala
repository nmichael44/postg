package app

object AuthUtils:
  final case class AppToken(
      userId: Long,
      permissions: Seq[String],
      expiresAt: Long,
  )

  case class AuthenticatedUser(userId: Long, permissions: Set[String], expiresAt: Long)

  object AuthenticatedUser:
    def create(appToken: AppToken): AuthenticatedUser =
      AuthenticatedUser(appToken.userId, appToken.permissions.toSet, appToken.expiresAt)
