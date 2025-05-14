package app.movieapptests

import cats.effect.Async

import app.services.{ExternalApiClientService, FileSystemService, ServerStateUpdateService}
import org.http4s.Uri

object ServiceStubs:
  final class ExternalApiClientServiceStub[F[_]: Async as async] extends ExternalApiClientService[F]:
    def fetchUri(uri: org.http4s.Uri): F[String] =
      async.raiseError(new NotImplementedError(s"ExternalApiClientServiceStub.fetchUri($uri) not implemented for this test"))

    override def fetchCompanyData(companyName: String): F[String] =
      async.pure(s"Dummy company data for $companyName")

    override def fetchAsJson[A](uri: Uri)(implicit decoder: io.circe.Decoder[A]): F[A] =
      async.raiseError(
        new NotImplementedError(s"ExternalApiClientServiceStub.fetchAsJson($uri) not implemented for this test"),
      )

  final class FileSystemServiceStub[F[_]: Async as async] extends FileSystemService[F]:
    override def readFileContent(fileName: String): F[String] =
      async.pure(s"Dummy file content for $fileName")

    override def readTwoFilesInParallel(fileName1: String, fileName2: String): F[String] =
      async.pure(s"Dummy parallel file content for $fileName1 & $fileName2")

  final class ServerStateUpdateServiceStub[F[_]: Async as async] extends ServerStateUpdateService[F]:
    override def get(movieId: Long): F[Option[Int]] = ???

    override def getAllCounts: F[Map[Long, Int]] = ???

    override def incrementAndGet(movieId: Long): F[Int] =
      async.pure(1) // Return a fake count
