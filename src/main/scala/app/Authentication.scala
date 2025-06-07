package app

import cats.effect.kernel.Clock
import cats.effect.Sync

import javax.management.relation.RoleStatus
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

import dev.profunktor.auth.jwt.*
import pdi.jwt.{JwtAlgorithm, JwtClaim}

object Authentication:
  val x = 1
//object Authentication:
//  final case class AppUser(userId: Long, loginName: String)
//
//  enum Role:
//    case FrontOffice
//    case BackOffice
//
//  final case class AppUserClaim(userId: Long, loginName: String, roles: Set[Role])
//
//  final class JwtAuthService[F[_]: Sync](clock: Clock[F]):
//    // Configuration for JWT
//    private val jwtAlgorithm: JwtAlgorithm = JwtAlgorithm.HS256
//
//    // Keep this secret and load from config in production!
//    private val secretKey: String = "Holy-diver-you've-been-down-too-long-in-the-midnight-sea"
//
//    // Define Token Expiration
//    private val tokenLifeTime: FiniteDuration = 2.hours
//
//    // Token Issuer
//    private val tokenIssuer: String = "MovieApp"
//
//    private def buildJwtClaim(userClaim: AppUserClaim): F[JwtClaim] =
//      for {
//        now <- clock.realTime.map(_.toSeconds)
//        expiresAt = now + tokenLifetime.toSeconds
//      } yield JwtClaim(
//        content = UserClaim.encoder(userClaim).noSpaces, // Encode your case class to JSON string
//        expiration = Some(expiresAt),
//        issuedAt = Some(now),
//        issuer = tokenIssuer,
//        // audience = tokenAudience // Uncomment if you use audience
//      )
//
//    // Function to create a JWT claim for a user
//    def makeUserToken[F[_]](user: AppUser): F[String] =
//      for {
//        claim <- JwtClaim(content = s"""{"user": "${user.username}", "id": ${user.id}}""").issuedNow
//          .expiresIn(1.hour.toSeconds) // Set token expiration
//        token <- jwtEncode[IO](claim, secretKey, jwtAlgorithm)
//      } yield token
