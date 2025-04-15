package app

import cats.syntax.either.*

import pureconfig.*
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}
import pureconfig.ConvertHelpers.*

object AppConfig:
  final case class DbConnection(
      private val host: String,
      private val port: Port,
      private val user: String,
      private val password: String,
  ) derives ConfigReader:
    def getHost: String = host
    def getPort: Int = port.port
    def getUser: String = user
    def getPassword: String = password

  final case class ServerConnection(
      private val host: String,
      private val port: Port,
  ) derives ConfigReader:
    def getHost: String = host
    def getPort: Int = port.port

  final case class AppConfig(
      private val name: String,
      private val dbConnection: DbConnection,
      private val serverConnection: ServerConnection,
  ) derives ConfigReader:
    def getDbConnection: DbConnection = dbConnection
    def getServerConnection: ServerConnection = serverConnection

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
