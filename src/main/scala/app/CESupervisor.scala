package app

import cats.effect.{ExitCode, Temporal}
import cats.effect.kernel.Outcome
import cats.effect.std.Supervisor
import cats.implicits.*

import scala.concurrent.duration.{Duration, DurationInt}

import org.typelevel.log4cats.Logger

object CESupervisor:
  private def job[F[_]: { Temporal as temporal, Logger as logger }](
      fibName: String,
      d: Duration,
  ): F[Int] =
    for {
      _ <- logger.info(s"Starting executing job '$fibName'.'")
      _ <- logger.info(s"Working on job '$fibName'.")
      _ <- temporal.sleep(d)
      _ <- logger.info(s"End executing job '$fibName'.")
    } yield fibName.length

  def run[F[_]: { Temporal as temporal, Logger as logger }]: F[ExitCode] =
    Supervisor[F].use { supervisor =>
      val (job0, job1) = (job("f", 2.seconds), job("fff", 3.seconds))

      for {
        (fib0, fib1) <- (supervisor.supervise(job0), supervisor.supervise(job1)).tupled
        _ <- temporal.sleep(7.seconds)
        n0 <- fib0.join.flatMap {
          case Outcome.Succeeded(fa) => fa
          case Outcome.Canceled() => 0.pure
          case Outcome.Errored(e) => 0.pure
        }
        n1 <- fib1.join.flatMap {
          case Outcome.Succeeded(fa) => fa
          case Outcome.Canceled() => 0.pure
          case Outcome.Errored(e) => 0.pure
        }
        _ <- logger.info(s"n0 = $n0 and n1 = $n1")
      } yield ExitCode.Success
    }
