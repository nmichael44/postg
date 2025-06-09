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

  // A transactor that gets connections from java.sql.DriverManager and executes blocking operations
  // on our synchronous EC. See the chapter on connection handling for more info.
  //  private val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
  //    driver = "org.postgresql.Driver", // JDBC driver classname
  //    url = "jdbc:postgresql://localhost:5432/postgres", // Connect URL
  //    user = "neom", // Database user name
  //    password = "neom11", // Database password
  //    logHandler = None // Don't set up logging for now. See Logging page for how to log events in detail
  //  )

  /*
  def f1(xa: Transactor[IO]): IO[Int] = 42.pure[ConnectionIO].transact(xa)

  def f2(xa: Transactor[IO]): IO[Int] = sql"select 42".query[Int].unique.transact(xa)

  def f3(xa: Transactor[IO]): IO[List[(Int, Double)]] =
    (for {
        a <- sql"select 42".query[Int].unique
        b <- sql"select random()".query[Double].unique
      } yield (a, b))
    .replicateA(5)
    .transact(xa)

  def f4(xa: Transactor[IO]): IO[Vector[(Int, Double)]] =
    sql"select n, d from t"
      .query[(Int, Double)]
      .to[Vector]
      .transact(xa)

  private type T0 = (Option[Int], Option[Double])
  private type T1 = Seq[T0]

  def f5(xa: Transactor[IO]): IO[T1] =
    sql"select n, d from t1".query[T0].to[Vector].transact(xa)

  def f6(xa: Transactor[IO]): IO[T1] =
    sql"select n, d from t1"
      .query[T0]
      .stream
      .take(2)
      .compile
      .toVector
      .transact(xa)

  def f7(xa: Transactor[IO]): IO[T1] =
    val s_1: fragment.Fragment = sql"select n, d from t1"
    val s0: Query0[T0] = s_1.query[T0]
    val s1: Stream[ConnectionIO, T0] = s0.stream
    val s2: Stream[ConnectionIO, T0] = s1.take(2)
    val s3: Stream.CompileOps[ConnectionIO, ConnectionIO, T0] = s2.compile

    val s4: ConnectionIO[Vector[T0]] = s3.toVector
    val s5: IO[Vector[T0]] = s4.transact(xa)

    s5

  final case class ND(n: Option[Int], d: Option[Double])

  def f8(xa: Transactor[IO]): IO[Vector[ND]] =
    sql"select n, d from t1".query[ND].to[Vector].transact(xa)

  def f9(xa: Transactor[IO]): IO[Map[String, Seq[Int]]] =
    sql"select s, n from t2"
      .query[(String, Int)]
      .to[Vector]
      .map(_.groupMap(_._1)(_._2))
      .transact(xa)

  def f10(xa: Transactor[IO]): IO[Map[String, Seq[Int]]] =
    sql"select s, n from t2 order by n desc"
      .query[(String, Int)]
      .stream
      .fold(Map.empty[String, List[Int]]) { case (acc, (s, n)) =>
        acc.updated(s, n +: acc.getOrElse(s, List.empty))
      }
      .compile
      .lastOrError
      .transact(xa)

  final case class C(s0: String, n: Int, s1: String)

  def f11(xa: Transactor[IO]): IO[C] = {
    sql"select s, n, s || n from t2 where s = ${"a"} and n = ${1}"
      .query[C]
      .unique
      .transact(xa)
  }

  def f12(xa: Transactor[IO]): IO[Vector[C]] =
    (fr"""select s, n, s || n from t2 where s = ${"a"} and """ ++ Fragments.in(fr"n", 100, 1, 2, 3, 4, 5, 6, 7, 8,9,10,11))
      .query[C]
      .to[Vector]
      .transact(xa)

  def f13(xa: Transactor[IO]): IO[Int] = {
    val n = 2
    val d = 3.14
    sql"insert into t1 values($n, $d)".update.run.transact(xa)
  }

  def f14(xa: Transactor[IO]): IO[Int] = {
    val n1: Option[Int] = Some(4)
    val n2: Option[Int] = Some(2)
    val transaction = for {
      s2 <- sql"insert into neo1 values($n1)".update.run
      s1 <- sql"insert into neo2 values($n2)".update.run
    } yield s2 + s1

    transaction.transact(xa)
  }
   */
  def f15(xa: Transactor[IO]): IO[Int] = {
    val n: Int = 1
    val transaction: Free[connection.ConnectionOp, Int] = for {
      neo1_v <- sql"select m from neo1 where n = $n".query[Int].unique
      neo2_v <- sql"select m from neo2 where n = $neo1_v".query[Int].unique
      ins <- sql"insert into neo3 values($neo2_v)".update.run
    } yield ins

    transaction.transact(xa)
  }
