package app

trait PasswordHasher[F[_]]:
  def hashPassword(password: String): F[String]
  def checkPassword(password: String, hash: String): F[Boolean]
