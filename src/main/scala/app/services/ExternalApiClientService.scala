package app.services

import io.circe.Decoder

trait ExternalApiClientService[F[_]]:
  def fetchUri(uri: org.http4s.Uri): F[String]

  def fetchCompanyData(companyName: String): F[String]

  def fetchAsJson[A: Decoder](uri: org.http4s.Uri): F[A]
