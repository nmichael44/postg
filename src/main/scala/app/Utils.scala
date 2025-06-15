package app

import cats.effect.{Async, Concurrent, Resource}
import cats.syntax.all.*
import cats.FlatMap

import java.nio.file.Paths
import scala.io.Source

import org.typelevel.log4cats.Logger

object Utils:
  private def parseLine[F[_]: Async as async](line: String): F[(String, String)] =
    line.split("=", 2).toList match {
      case key :: value :: Nil => async.pure(key.toLowerCase, value)
      case _ =>
        async.raiseError(
          IllegalArgumentException(
            s"Invalid config line: '$line'. Expected 'key=value' format.",
          ),
        )
    }

  private def mkException(field: String): IllegalArgumentException =
    IllegalArgumentException(s"Missing or invalid field '$field'")

  inline private val HostKey = "Host"
  inline private val PortKey = "Port"
  inline private val UserKey = "User"
  inline private val PasswordKey = "Password"
  inline private val ServerHostIP = "ServerHostIP"
  inline private val ServerHostPort = "ServerHostPort"

  def isValidPort(port: Int): Boolean = port > 0 && port < 65536

  // Not in use anymore... just keep for maybe later extract the validation code and
  // move it to the new implementation.
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

  def readDbConfig[F[_]: Async as async](path: String): Resource[F, DatabaseConfig] =
    Resource
      .fromAutoCloseable(async.blocking(Source.fromFile(Paths.get(path).toFile)))
      .evalMap(source => async.blocking(source.mkString) >>= parseDatabaseConfig[F])

  def logi[F[_]: Logger as logger](fiber: String, s: String): F[Unit] =
    logger.info(s"$fiber :: $s")

  def loge[F[_]: Logger as logger](e: Throwable, fiber: String, s: String): F[Unit] =
    logger.error(e)(s"$fiber :: $s")

  def logi[F[_]: Logger as logger](fiber: String, uuid: String, s: String): F[Unit] =
    logger.info(s"$fiber [$uuid] :: $s")

  def loge[F[_]: Logger as logger](e: Throwable, fiber: String, uuid: String, s: String): F[Unit] =
    logger.error(e)(s"$fiber [$uuid] :: $s")
