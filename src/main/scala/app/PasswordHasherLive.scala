package app

import cats.effect.Sync

import com.password4j.{Argon2Function, Password}
import com.password4j.types.Argon2

final class PasswordHasherLive[F[_]: Sync as sync] extends PasswordHasher[F]:
  private val argon2Function: Argon2Function =
    Argon2Function.getInstance(
      65536,     // memory in KiB (64MB)
      3,         // iterations
      1,         // parallelism
      32,        // output hash length in bytes
      Argon2.ID, // Argon2id
      19,        // version 19
    )

  inline private final val LengthOfSaltValue = 16

  override def hashPassword(password: String): F[String] =
    sync.blocking {
      Password
        .hash(password)
        .addRandomSalt(LengthOfSaltValue)
        .`with`(argon2Function)
        .getResult
    }

  override def checkPassword(password: String, hashedPassword: String): F[Boolean] =
    sync.blocking {
      Password.check(password, hashedPassword).`with`(argon2Function)
    }
