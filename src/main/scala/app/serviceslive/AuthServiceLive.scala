package app.serviceslive

import cats.effect.Sync
import cats.syntax.all.*

import java.time.{Clock, Instant}

import app.permissions.Permissions.Permission
import app.services.AuthService
import app.AppConfig.AuthConfig
import app.ImplicitConversions.*
import app.MovieDbModel.AuthenticatedUser
import app.MovieDbModel.UserDetailsInDb
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import pdi.jwt.{JwtAlgorithm, JwtCirce}
import pdi.jwt.algorithms.JwtHmacAlgorithm

private final class AuthServiceLive[F[_]: Sync as sync] private (authConfig: AuthConfig, clock: Clock) extends AuthService[F]:
  private val JwtEncodingAlgorithm: JwtHmacAlgorithm =
    AuthServiceLive.getHmacAlgorithm(authConfig)

  private val JwtDecodingAlgorithmList: Seq[JwtHmacAlgorithm] = Seq(JwtEncodingAlgorithm)

  override def createToken(user: UserDetailsInDb, permissions: Seq[Permission]): F[String] =
    sync.blocking {
      val userId = user.userId
      val nowEpochSec = Instant.now(clock).getEpochSecond
      val expiryEpochSec = nowEpochSec + authConfig.getExpirationPeriodInSecond

      val issuedAtJson = nowEpochSec.asJson
      val expiresAtJson = expiryEpochSec.asJson

      val claim = Json.obj(
        "iss" -> "neo-app".asJson,
        "sub" -> userId.toString.asJson,
        "iat" -> issuedAtJson,
        "exp" -> expiresAtJson,
        // The fields that will end up in content (see ValidateToken).
        // We replicate some of the fields above to simply the code in validateToken().
        "userId"      -> userId.asJson,
        "issuedAt"    -> issuedAtJson,
        "expiresAt"   -> expiresAtJson,
        "permissions" -> permissions.asJson,
      )

      JwtCirce.encode(claim, authConfig.getSecretKey, JwtEncodingAlgorithm)
    }
  end createToken

  override def validateToken(token: String): F[Either[Throwable, AuthenticatedUser]] = sync.blocking {
    JwtCirce.decode(token, authConfig.getSecretKey, JwtDecodingAlgorithmList).toEither >>= { jwtClaim =>
      decode[AuthenticatedUser](jwtClaim.content)
    }
  }
  end validateToken
end AuthServiceLive

object AuthServiceLive:
  def create[F[_]: Sync](authConfig: AuthConfig, clock: Clock): AuthService[F] =
    AuthServiceLive[F](authConfig, clock)

  private def getHmacAlgorithm(authConfig: AuthConfig): JwtHmacAlgorithm =
    JwtAlgorithm
      .fromString(authConfig.getJwtEncodingAlgorithm)
      .safeAs[JwtHmacAlgorithm]
      .getOrElse(throw AssertionError("We only support Hmac algorithms for token encryption."))
end AuthServiceLive
