package app

import cats.effect.Async
import app.ExternalApiClientImpl.doRequest
import io.circe.Decoder
import org.http4s.circe.jsonOf
import org.http4s.{Method, Request, Uri}
import org.http4s.client.Client

final class ExternalApiClientImpl[F[_]: Async] private (client: Client[F])
    extends ExternalApiClient[F]:
  override def fetchUri(uri: Uri): F[String] =
    val request: Request[F] = Request[F](Method.GET, uri)
    doRequest(client, request)

  override def fetchCompanyData(companyName: String): F[String] =
    val uri: Uri = Uri.unsafeFromString(s"https://www.$companyName.com")
    val request = Request[F](Method.GET, uri)
    doRequest(client, request)

  def fetchAsJson[A: Decoder](uri: org.http4s.Uri): F[A] =
    client.expect[A](uri)(jsonOf[F, A])

object ExternalApiClientImpl:
  def create[F[_]: Async](client: Client[F]): ExternalApiClient[F] =
    new ExternalApiClientImpl[F](client)

  private def doRequest[F[_]: Async](client: Client[F], request: Request[F]): F[String] =
    client.run(request).use { response =>
      if (response.status.isSuccess)
        response.bodyText.compile.string
      else
        Async[F].raiseError(
          new RuntimeException(s"External service call failed with status: ${response.status}."),
        )
    }
