package app

import cats.effect
import cats.effect.{ExitCode, IO}
import cats.effect.kernel.{Concurrent, Ref}
import cats.implicits.*

import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

object Refs:
  private def mkRef[F[_]: Concurrent, A](a: A): F[Ref[F, A]] = Ref.of[F, A](a)

  private def incOne[F[_]](r: Ref[F, Int]): F[Unit] = r.update(_ + 1)

  private def doCalc[F[_]: { Concurrent, Logger as logger }](n: Int): F[Unit] = {
    val fRef: F[Ref[F, Int]] = mkRef[F, Int](n)
    for {
      r <- fRef
      get = r.get
      n0 <- get
      _ <- logger.info(s"r = $n0")
      _ <- incOne(r)
      n1 <- get
      _ <- logger.info(s"r = $n1")
      _ <- r.update(_ * 2)
      n2 <- r.getAndSet(25)
      _ <- logger.info(s"r = $n2")
      n3 <- get
      _ <- logger.info(s"r = $n3")
    } yield ()
  }

  def run(args: List[String]): IO[ExitCode] =
    Slf4jLogger.create[IO].flatMap { implicit logger =>
      doCalc[IO](11).as(ExitCode.Success)
    }
