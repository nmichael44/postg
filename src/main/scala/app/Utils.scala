package app

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import java.nio.file.Paths
import scala.io.Source

object Utils:
  private def parseLine[F[_]: Async](line: String): F[(String, String)] =
    line.split("=", 2).toList match {
      case key :: value :: Nil => Async[F].pure(key.toLowerCase, value)
      case _ =>
        Async[F].raiseError(
          new IllegalArgumentException(
            s"Invalid config line: '$line'. Expected 'key=value' format.",
          ),
        )
    }

  private def mkException(field: String): IllegalArgumentException =
    new IllegalArgumentException(s"Missing or invalid field '$field'")

  private final val HostKey = "Host"
  private final val PortKey = "Port"
  private final val UserKey = "User"
  private final val PasswordKey = "Password"
  private final val ServerHostIP = "ServerHostIP"
  private final val ServerHostPort = "ServerHostPort"

  private def isValidPort(port: Int): Boolean = port > 0 && port < 65536

  case class DatabaseConfig(
      host: String,
      port: Int,
      user: String,
      password: String,
      serverHostIP: String,
      serverHostPort: Int,
  )

  private def parseKey[F[_]: Async, T](
      configMap: Map[String, String],
      key: String,
      mapperFn: String => Option[T],
      filterFn: T => Boolean,
  ): F[T] =
    configMap
      .get(key.toLowerCase)
      .flatMap(mapperFn)
      .filter(filterFn)
      .liftTo[F](mkException(key))

  private def parseHost[F[_]: Async](configMap: Map[String, String]): F[String] =
    parseKey(configMap, HostKey, Some.apply, _.nonEmpty)

  private def parsePort[F[_]: Async](configMap: Map[String, String]): F[Int] =
    parseKey(configMap, PortKey, _.toIntOption, isValidPort)

  private def parseUser[F[_]: Async](configMap: Map[String, String]): F[String] =
    parseKey(configMap, UserKey, Some.apply, _.nonEmpty)

  private def parsePassword[F[_]: Async](configMap: Map[String, String]): F[String] =
    parseKey(configMap, PasswordKey, Some.apply, _.nonEmpty)

  private def parseServerHostIP[F[_]: Async](configMap: Map[String, String]): F[String] =
    parseKey(configMap, ServerHostIP, Some.apply, _.nonEmpty)

  private def parseServerHostPort[F[_]: Async](configMap: Map[String, String]): F[Int] =
    parseKey(configMap, ServerHostPort, _.toIntOption, isValidPort)

  private def parseDatabaseConfig[F[_]: Async](config: String): F[DatabaseConfig] =
    for {
      configMap <- config.linesIterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .filter(s => s(0) != '#')
        .toVector
        .traverse(parseLine[F])
        .map(_.toMap)
      host <- parseHost(configMap)
      port <- parsePort(configMap)
      user <- parseUser(configMap)
      password <- parsePassword(configMap)
      serverHostIP <- parseServerHostIP(configMap)
      serverHostPort <- parseServerHostPort(configMap)
    } yield DatabaseConfig(host, port, user, password, serverHostIP, serverHostPort)

  def readDbConfig[F[_]: Async](path: String): Resource[F, DatabaseConfig] =
    Resource
      .fromAutoCloseable(Async[F].blocking(Source.fromFile(Paths.get(path).toFile)))
      .evalMap(source => Async[F].blocking(source.mkString))
      .evalMap(parseDatabaseConfig[F])
