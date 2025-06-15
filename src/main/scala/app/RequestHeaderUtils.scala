package app

import cats.data.NonEmptyList

import org.http4s.Request
import org.typelevel.ci.*

object RequestHeaderUtils:
  def getHeaderValue[F[_]](req: Request[F], keyName: CIString): Option[NonEmptyList[String]] =
    // Fetch all matching headers
    req.headers.get(keyName).map(_.map(_.value))

  private val XRequestId: CIString = CIString("X-Request-ID")

  def getXRequestId[F[_]](req: Request[F]): Option[String] =
    getHeaderValue(req, XRequestId).map(_.head)
