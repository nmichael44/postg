package app.serviceslive

import cats.effect.Sync

import java.time.{Clock, Instant}

import app.services.AuthService
import app.AppConfig.AuthConfig
import app.AuthUtils.AppToken
import app.ImplicitConversions.*
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

  final case class MyClaim(iss: String, sub: Int, iat: Long, exp: Long, permissions: Seq[String])

  override def createToken(user: UserDetailsInDb, permissions: Seq[String]): F[String] =
    sync.blocking {
      val epochSec = Instant.now(clock).getEpochSecond

      val claim = Json.obj(
        "iss"         -> "neo-app".asJson,
        "sub"         -> user.userId.toString.asJson,
        "iat"         -> epochSec.asJson,
        "exp"         -> (epochSec + authConfig.getExpirationPeriodInSecond).asJson,
        "permissions" -> permissions.asJson,
      )

      JwtCirce.encode(claim, authConfig.getSecretKey, JwtEncodingAlgorithm)
    }

  override def validateToken(token: String): F[Either[Throwable, AppToken]] =
    sync.blocking {
      for {
        jwtClaim <- JwtCirce.decode(token, authConfig.getSecretKey, JwtDecodingAlgorithmList).toEither
        permissions <- decode[Map[String, Seq[String]]](jwtClaim.content)
      } yield AppToken(
        java.lang.Long.parseLong(jwtClaim.subject.get),
        permissions.getOrElse("permissions", throw AssertionError("Bad decoding")),
        jwtClaim.expiration.get,
      )
    }

object AuthServiceLive:
  def create[F[_]: Sync](authConfig: AuthConfig, clock: Clock): AuthService[F] =
    AuthServiceLive[F](authConfig, clock)

  private def getHmacAlgorithm(authConfig: AuthConfig): JwtHmacAlgorithm =
    JwtAlgorithm
      .fromString(authConfig.getJwtEncodingAlgorithm)
      .safeAs[JwtHmacAlgorithm]
      .getOrElse(throw AssertionError("We only support Hmac algorithms for token encryption."))
