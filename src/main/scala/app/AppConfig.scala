package app

import cats.syntax.either.*

import pureconfig.*
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}
import pureconfig.ConvertHelpers.*

object AppConfig:
  final case class DbConnectionConfig(
      private val host: String,
      private val port: Port,
      private val user: String,
      private val password: String,
      private val maxConnections: Int,
      private val minIdleConnections: Int,
  ) derives ConfigReader:
    def getHost: String = host
    def getPort: Int = port.port
    def getUser: String = user
    def getPassword: String = password
    def getMaxConnections: Int = maxConnections
    def getMinIdleConnections: Int = minIdleConnections

  final case class ServerConnectionConfig(
      private val host: String,
      private val port: Port,
  ) derives ConfigReader:
    def getHost: String = host
    def getPort: Int = port.port

  final case class BackendServerConfig(
      private val numberOfWorkers: Int,
      private val boundedQueueCapacity: Int,
      private val actorMemCacheCleanupDurationInMillis: Int,
  ) derives ConfigReader:
    def getNumberOfWorkers: Int = numberOfWorkers
    def getBoundedQueueCapacity: Int = boundedQueueCapacity
    def getActorMemCacheCleanupDurationInMillis: Int = actorMemCacheCleanupDurationInMillis

  final case class AppConfig(
      private val name: String,
      private val dbConnectionConfig: DbConnectionConfig,
      private val serverConnectionConfig: ServerConnectionConfig,
      private val backendServerConfig: BackendServerConfig,
  ) derives ConfigReader:
    def getDbConnectionConfig: DbConnectionConfig = dbConnectionConfig
    def getServerConnectionConfig: ServerConnectionConfig = serverConnectionConfig
    def getBackendServerConfig: BackendServerConfig = backendServerConfig

  final case class Port(port: Int) extends AnyVal

  given ConfigReader[Port] = ConfigReader.fromCursor { cursor =>
    cursor.asInt
      .flatMap(intPort =>
        if Utils.isValidPort(intPort)
        then Port(intPort).asRight
        else
          ConfigReaderFailures(
            ConvertFailure(
              CannotConvert(
                intPort.toString,
                "Port",
                "Port value is outside the valid range (1-65535)",
              ),
              cursor,
            ),
          ).asLeft,
      )
  }
