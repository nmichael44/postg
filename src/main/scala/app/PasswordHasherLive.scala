package app

import cats.effect.Sync

import app.PasswordHasherLive.createArgonFunction
import com.password4j.{Argon2Function, Password}
import com.password4j.types.Argon2

private final class PasswordHasherLive[F[_]: Sync as sync] private extends PasswordHasher[F]:
  private val argon2Function: Argon2Function =
    createArgonFunction(
      memory = 65536, // In KB (64MB)
      iterations = 3,
      parallelism = 1,
      outputLength = 32,
      argon2Type = Argon2.ID,
      version = 19,
    )

  inline private final val LengthOfSaltValue = 16

  override def hashPassword(password: String): F[String] =
    sync.blocking:
      Password
        .hash(password)
        .addRandomSalt(LengthOfSaltValue)
        .`with`(argon2Function)
        .getResult

  override def checkPassword(password: String, hashedPassword: String): F[Boolean] =
    sync.blocking:
      Password.check(password, hashedPassword).`with`(argon2Function)

object PasswordHasherLive:
  def create[F[_]: Sync]: PasswordHasher[F] =
    PasswordHasherLive[F]

  private def createArgonFunction(
      memory: Int,
      iterations: Int,
      parallelism: Int,
      outputLength: Int,
      argon2Type: Argon2,
      version: Int,
  ) = Argon2Function.getInstance(memory, iterations, parallelism, outputLength, argon2Type, version)
