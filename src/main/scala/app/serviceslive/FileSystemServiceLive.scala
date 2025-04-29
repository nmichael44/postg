package app.serviceslive

import cats.effect.{Async, Resource}
import cats.effect.implicits.*
import cats.effect.kernel.Async
import cats.implicits.*

import scala.io.Source

import app.services.FileSystemService

private final class FileSystemServiceLive[F[_]: Async as async] private extends FileSystemService[F]:
  def readFileContent(fileName: String): F[String] =
    readFileContentAux(fileName).use(async.pure)

  def readTwoFilesInParallel(fileName1: String, fileName2: String): F[String] =
    (
      readFileContentAux(fileName1).use(async.pure),
      readFileContentAux(fileName2).use(async.pure),
    ).parMapN((c1, c2) => c1 + c2)

  private def readFileContentAux(path: String): Resource[F, String] =
    Resource
      .fromAutoCloseable(async.blocking(Source.fromFile(path)))
      .evalMap(source => async.blocking(source.mkString))

object FileSystemServiceLive:
  def create[F[_]: Async]: FileSystemService[F] = FileSystemServiceLive[F]
