package app

trait ExternalApiClient[F[_]]:
  def fetchUri(uri: org.http4s.Uri): F[String]
  def fetchCompanyData(companyName: String): F[String]
