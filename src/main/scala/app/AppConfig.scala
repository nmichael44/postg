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
      private val keyStoreFile: String,
      private val keyStorePassword: String,
  ) derives ConfigReader:
    def getHost: String = host
    def getPort: Int = port.port
    def getKeyStoreFile: String = keyStoreFile
    def getKeyStorePassword: String = keyStorePassword

  final case class BackendServerConfig(
      private val numberOfWorkers: Int,
      private val boundedQueueCapacity: Int,
  ) derives ConfigReader:
    def getNumberOfWorkers: Int = numberOfWorkers
    def getBoundedQueueCapacity: Int = boundedQueueCapacity

  final case class DirectorMemCacheConfig(
      private val capacity: Int,
      private val cleanupDurationInMillis: Int,
      private val cacheEnabled: Boolean,
  ) derives ConfigReader:
    def getCapacity: Int = capacity
    def getCleanupDurationInMillis: Int = cleanupDurationInMillis
    def getCacheEnabled: Boolean = cacheEnabled

  final case class ActorMemCacheConfig(
      private val capacity: Int,
      private val cleanupDurationInMillis: Int,
      private val cacheEnabled: Boolean,
  ) derives ConfigReader:
    def getCapacity: Int = capacity
    def getCleanupDurationInMillis: Int = cleanupDurationInMillis
    def getCacheEnabled: Boolean = cacheEnabled

  final case class MovieMemCacheConfig(
      private val capacity: Int,
      private val cleanupDurationInMillis: Int,
      private val cacheEnabled: Boolean,
  ) derives ConfigReader:
    def getCapacity: Int = capacity
    def getCleanupDurationInMillis: Int = cleanupDurationInMillis
    def getCacheEnabled: Boolean = cacheEnabled

  final case class MemCacheConfig(
      private val directorMemCacheConfig: DirectorMemCacheConfig,
      private val actorMemCacheConfig: ActorMemCacheConfig,
      private val movieMemCacheConfig: MovieMemCacheConfig,
  ) derives ConfigReader:
    def getDirectorMemCacheConfig: DirectorMemCacheConfig = directorMemCacheConfig
    def getActorMemCacheConfig: ActorMemCacheConfig = actorMemCacheConfig
    def getMovieMemCacheConfig: MovieMemCacheConfig = movieMemCacheConfig

  final case class AuthConfig(
      private val secretKey: String,
      private val expirationPeriodInSeconds: Long,
      private val jwtEncodingAlgorithm: String,
  ):
    def getSecretKey: String = secretKey
    def getExpirationPeriodInSecond: Long = expirationPeriodInSeconds
    def getJwtEncodingAlgorithm: String = jwtEncodingAlgorithm

  final case class AppConfig(
      private val name: String,
      private val dbConnectionConfig: DbConnectionConfig,
      private val serverConnectionConfig: ServerConnectionConfig,
      private val backendServerConfig: BackendServerConfig,
      private val memCacheConfig: MemCacheConfig,
      private val authConfig: AuthConfig,
  ) derives ConfigReader:
    def getDbConnectionConfig: DbConnectionConfig = dbConnectionConfig
    def getServerConnectionConfig: ServerConnectionConfig = serverConnectionConfig
    def getBackendServerConfig: BackendServerConfig = backendServerConfig
    def getMemCacheConfig: MemCacheConfig = memCacheConfig
    def getAuthConfig: AuthConfig = authConfig

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
