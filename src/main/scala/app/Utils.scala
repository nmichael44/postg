package app

import cats.data.NonEmptyVector
import cats.effect.{Async, Resource}
import cats.syntax.all.*

import java.nio.file.Paths
import scala.collection.View
import scala.io.Source

object Utils:
  private def parseLine[F[_]: Async](line: String): F[(String, String)] =
    line.split("=", 2).toList match {
      case key :: value :: Nil => Async[F].pure(key, value)
      case _ =>
        Async[F].raiseError(
          new IllegalArgumentException(
            s"Invalid config line: '$line'. Expected 'key=value' format.",
          ),
        )
    }

  private def mkException(field: String): IllegalArgumentException =
    new IllegalArgumentException(s"Missing or invalid field '$field'")

  private final val HostKey = "host"
  private final val PortKey = "port"
  private final val UserKey = "user"
  private final val PasswordKey = "password"

  case class DatabaseConfig(host: String, port: Int, user: String, password: String)

  private def parseDatabaseConfig[F[_]: Async](config: String): F[DatabaseConfig] =
    def parseKey[T](
        configMap: Map[String, String],
        key: String,
        mapperFn: String => Option[T],
        filterFn: T => Boolean,
    ): F[T] =
      configMap.get(key).flatMap(mapperFn).filter(filterFn).liftTo[F](mkException(key))

    def parseHost(configMap: Map[String, String]): F[String] =
      parseKey(configMap, HostKey, Some.apply, _.nonEmpty)

    def parsePort(configMap: Map[String, String]): F[Int] =
      parseKey(configMap, PortKey, _.toIntOption, p => p > 0 && p < 65536)

    def parseUser(configMap: Map[String, String]): F[String] =
      parseKey(configMap, UserKey, Some.apply, _.nonEmpty)

    def parsePassword(configMap: Map[String, String]): F[String] =
      parseKey(configMap, PasswordKey, Some.apply, _.nonEmpty)

    for {
      configMap <- config.linesIterator.toVector.traverse(parseLine[F]).map(_.toMap)
      host <- parseHost(configMap)
      port <- parsePort(configMap)
      user <- parseUser(configMap)
      password <- parsePassword(configMap)
    } yield DatabaseConfig(host, port, user, password)

  def readDbConfig[F[_]: Async](path: String): Resource[F, DatabaseConfig] =
    Resource
      .fromAutoCloseable(Async[F].blocking(Source.fromFile(Paths.get(path).toFile)))
      .evalMap(source => Async[F].blocking(source.mkString))
      .evalMap(parseDatabaseConfig[F])

  // We can't make this a value class because NonEmptyVector already is one.
  implicit class ToView[A](nev: NonEmptyVector[A]) {
    def view: View[A] = nev.toVector.view
  }
