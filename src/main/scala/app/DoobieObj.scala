package app

import cats.*
import cats.data.*
import cats.effect.*
// This is just for testing. Consider using cats.effect.IOApp instead of calling
// unsafe methods directly.
import cats.effect.unsafe.implicits.global
import cats.free.Free
import cats.implicits.*

import scala.concurrent.ExecutionContext

import app.AppConfig.AppConfig
import app.AppConfig.DbConnectionConfig
import com.zaxxer.hikari.HikariConfig
import doobie.*
import doobie.free.connection
import doobie.hikari.HikariTransactor
import doobie.implicits.*
import doobie.util.transactor

object DoobieObj:
  private final val DriverName: String = "org.postgresql.Driver"

  private def createTransactorResource[F[_]: Async](dbConfig: DbConnectionConfig): Resource[F, HikariTransactor[F]] =
    val (host, port) = (dbConfig.getHost, dbConfig.getPort)

    val databaseURL = s"jdbc:postgresql://$host:$port/postgres"
    val user = dbConfig.getUser
    val password = dbConfig.getPassword
    val maxConnections = dbConfig.getMaxConnections
    val minIdleConnections = dbConfig.getMinIdleConnections

    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(DriverName)
    hikariConfig.setJdbcUrl(databaseURL)
    hikariConfig.setUsername(user)
    hikariConfig.setPassword(password)

    // Set the maximum pool size
    hikariConfig.setMaximumPoolSize(maxConnections)
    hikariConfig.setMinimumIdle(minIdleConnections)

    HikariTransactor.fromHikariConfig[F](hikariConfig)

  def xaResource[F[_]: Async](dbConfig: DbConnectionConfig): Resource[F, HikariTransactor[F]] =
    createTransactorResource[F](dbConfig)

  // Transaction example.
  def f15(xa: Transactor[IO]): IO[Int] = {
    val n: Int = 1
    val transaction: Free[connection.ConnectionOp, Int] = for {
      neo1_v <- sql"select m from neo1 where n = $n".query[Int].unique
      neo2_v <- sql"select m from neo2 where n = $neo1_v".query[Int].unique
      ins <- sql"insert into neo3 values($neo2_v)".update.run
    } yield ins

    transaction.transact(xa)
  }
