package app

object AuthUtils:
  final case class AppToken(
      userId: Long,
      permissions: Seq[String],
      expiresAt: Long,
  )
