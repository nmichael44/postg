package app.serviceslive

import cats.effect.Async

import app.services.ExternalApiClientService
import io.circe.Decoder
import org.http4s.{Method, Request, Uri}
import org.http4s.circe.jsonOf
import org.http4s.client.Client

private final class ExternalApiClientServiceLive[F[_]: Async as async] private (client: Client[F])
    extends ExternalApiClientService[F]:
  override def fetchUri(uri: Uri): F[String] =
    val request: Request[F] = Request[F](Method.GET, uri)
    doRequest(client, request)

  override def fetchCompanyData(companyName: String): F[String] =
    val uri: Uri = Uri.unsafeFromString(s"https://www.$companyName.com")
    val request = Request[F](Method.GET, uri)
    doRequest(client, request)

  def fetchAsJson[A: Decoder](uri: org.http4s.Uri): F[A] =
    client.expect[A](uri)(using jsonOf[F, A])

  private def doRequest(client: Client[F], request: Request[F]): F[String] =
    client.run(request).use { response =>
      if (response.status.isSuccess)
        response.bodyText.compile.string
      else
        async.raiseError(
          RuntimeException(s"External service call failed with status: ${response.status}."),
        )
    }

object ExternalApiClientServiceLive:
  def create[F[_]: Async](client: Client[F]): ExternalApiClientService[F] =
    ExternalApiClientServiceLive[F](client)
