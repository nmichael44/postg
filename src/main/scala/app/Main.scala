package app

import cats.*
import cats.effect.*
import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.std.Console
import cats.implicits.*

import scala.concurrent.duration.*

import com.comcast.ip4s.*
import doobie.*
import doobie.implicits.*
import doobie.implicits.toSqlInterpolator
import doobie.util.transactor
import doobie.util.transactor.Transactor
import org.http4s.{HttpApp, HttpRoutes, QueryParamDecoder}
import org.http4s.dsl.*
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.implicits.*
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] = // Refs.run(args
    implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

    MovieApp.run

// VecSummer.run

// Boo.Timing.doIt()
//MovieApp.run(List.empty)
//

// MovieApp.run(args)

//  private def readInt[F[_]: MonadCancelThrow](console: Console[F]): F[Option[Int]] =
//    console.readLine.map(_.toIntOption)
//
//  private def writeGreeting[F[_]: MonadCancelThrow](console: Console[F]): F[Unit] =
//    console.print("Enter an integer: ")
//
//  private def writeIntToDb[F[_]: MonadCancelThrow](xa: Transactor[F], n: Int): F[Unit] = {
//    val transaction = sql"insert into neo3 values($n)".update.run
//
//    transaction.transact(xa).void
//  }
//
//  private def httpRoutes[F[_]: MonadCancelThrow](
//      xa: Transactor[F],
//      serverShutdown: Deferred[F, Unit],
//  ): HttpRoutes[F] =
//    val dsl = Http4sDsl[F]
//    import dsl.*
//
//    HttpRoutes
//      .of[F] {
//        case GET -> Root / "hello" => Ok("Hello morons!")
//        case GET -> Root / "hello" / name => Ok(s"Hello $name!")
//        case GET -> Root / "save" / someIntStr =>
//          val nOpt = someIntStr.toIntOption
//          nOpt.fold(BadRequest("Invalid int")) { n =>
//            val update = sql"INSERT INTO neo3 VALUES ($n)".update.run.transact(xa)
//            update *> Created("Created!")
//          }
//        case GET -> Root / "exit" =>
//          serverShutdown.complete(()).flatMap(_ => Ok("Shutting down..."))
//      }
//
//  private def httpApp[F[_]: MonadCancelThrow](
//      xa: Transactor[F],
//      serverShutdown: Deferred[F, Unit],
//  ): HttpApp[F] =
//    httpRoutes(xa, serverShutdown).orNotFound

//    type F[A] = IO[A]
//
//    DoobieObj.xaResource
//      .use { xa =>
//        Deferred[F, Unit].flatMap { serverShutdown =>
//          EmberServerBuilder
//            .default[IO]
//            .withHost(ipv4"0.0.0.0")
//            .withPort(port"8080")
//            .withShutdownTimeout(10.seconds)
//            .withHttpApp(httpApp[F](xa, serverShutdown))
//            .build
//            .use { _ => // Use the server resource
//              serverShutdown.get // Wait for shutdown concurrently
//            } *> Console[F].println("Server shut down.")
//        }
//      }
//      .as(ExitCode.Success)

//  private def loop[F[_]: Monad: MonadCancelThrow: Console](xa: Transactor[F]): F[ExitCode] =
//    val console = Console[F]
//
//    for {
//      _ <- writeGreeting(console)
//      optInt <- readInt(console)
//      _ <- optInt match {
//        case Some(n) => writeIntToDb(xa, n) >> loop[F](xa).void
//        case None => console.println("Invalid int.  Exiting.")
//      }
//    } yield ExitCode.Success

/*
  val prog: IO[ExitCode] = DoobieObj.xaResource.use(loop)

  prog
 */
