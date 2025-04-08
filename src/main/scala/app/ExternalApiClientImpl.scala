package app

import cats.effect.Async

import org.http4s.{Method, Request, Uri}
import org.http4s.client.Client

final class ExternalApiClientImpl[F[_]: Async] private (client: Client[F])
    extends ExternalApiClient[F]:
  override def fetchUri(uri: Uri): F[String] =
    val request: Request[F] = Request[F](Method.GET, uri)
    doRequest(request)

  override def fetchCompanyData(companyName: String): F[String] =
    val uri: Uri = Uri.unsafeFromString(s"https://www.$companyName.com")
    val request = Request[F](Method.GET, uri)
    doRequest(request)

  private def doRequest(request: Request[F]): F[String] =
    client.run(request).use { response =>
      if (response.status.isSuccess)
        response.bodyText.compile.string
      else
        Async[F].raiseError(
          new RuntimeException(s"External service call failed with status: ${response.status}."),
        )
    }

object ExternalApiClientImpl:
  def create[F[_]: Async](client: Client[F]): ExternalApiClient[F] =
    new ExternalApiClientImpl[F](client)
