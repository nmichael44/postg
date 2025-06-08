package app.serviceslive

import cats.effect.Sync

import java.time.{Clock, Instant}

import app.services.AuthenticationService
import app.AppConfig.AuthConfig
import app.AuthUtils.AppToken
import app.ImplicitConversions.*
import app.MovieDbModel.UserDetailsWithId
import io.circe.*
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import pdi.jwt.{JwtAlgorithm, JwtCirce}
import pdi.jwt.algorithms.JwtHmacAlgorithm

private final class AuthenticationServiceLive[F[_]: Sync as sync] private (authConfig: AuthConfig, clock: Clock)
    extends AuthenticationService[F]:
  private val JwtEncodingAlgorithm: JwtHmacAlgorithm =
    AuthenticationServiceLive.getHmacAlgorithm(authConfig)

  private val JwtDecodingAlgorithmList: Seq[JwtHmacAlgorithm] = Seq(JwtEncodingAlgorithm)

  override def createToken(user: UserDetailsWithId, permissions: Seq[String]): F[String] =
    sync.blocking {
      val epochSec = Instant.now(clock).getEpochSecond

      val claim = Json.obj(
        "iss"         -> "neo-app".asJson,
        "sub"         -> user.userId.asJson,
        "iat"         -> epochSec.asJson,
        "exp"         -> (epochSec + authConfig.getExpirationPeriodInSecond).asJson,
        "permissions" -> permissions.asJson,
      )

      JwtCirce.encode(claim, authConfig.getSecretKey, JwtEncodingAlgorithm)
    }

  override def validateToken(token: String): F[Either[Throwable, AppToken]] =
    sync.blocking {
      for {
        claim <- JwtCirce.decode(token, authConfig.getSecretKey, JwtDecodingAlgorithmList).toEither
        appToken <- decode[AppToken](claim.content)
      } yield appToken
    }

object AuthenticationServiceLive:
  def create[F[_]: Sync](authConfig: AuthConfig, clock: Clock): AuthenticationService[F] =
    AuthenticationServiceLive[F](authConfig, clock)

  private def getHmacAlgorithm(authConfig: AuthConfig): JwtHmacAlgorithm =
    JwtAlgorithm
      .fromString(authConfig.getJwtEncodingAlgorithm)
      .safeAs[JwtHmacAlgorithm]
      .getOrElse(throw AssertionError("We only support Hmac algorithms for token encryption."))
