package app

import cats.effect.{ExitCode, IO}
import cats.effect.kernel.Outcome
import cats.syntax.all.*

import org.typelevel.log4cats.Logger

object VecSummer:
  private final val WorkersNumber = 16

  private def sum(v: Array[Double], startIdx: Int, cnt: Int)(implicit
      logger: Logger[IO],
  ): IO[Double] =
    logger.info(s"sum was called with startIdx: $startIdx and cnt: $cnt") *>
      IO.delay {
        var s: Double = 0.0
        for i <- startIdx until (startIdx + cnt) do s += v(i)
        s
      }

  private def summer(v: Array[Double])(implicit logger: Logger[IO]): IO[Double] =
    val len = v.length

    if len < WorkersNumber
    then sum(v, 0, v.length)
    else
      val elemsPerThread = len / WorkersNumber
      val remainingElems = len % WorkersNumber

      val res: IO[Double] = for {
        fibers <- (0 until WorkersNumber).toList.parTraverse { i =>
          val startIdx = i * elemsPerThread
          val cnt =
            if i == WorkersNumber - 1 then elemsPerThread + remainingElems else elemsPerThread
          sum(v, startIdx, cnt).start
        }
        outcomes <- fibers.parTraverse(_.join)
        results <- outcomes.traverse {
          case Outcome.Succeeded(s) => s
          case _ => IO.raiseError(AssertionError("Invalid outcome"))
        }
      } yield results.sum

      res

  def run(implicit logger: Logger[IO]): IO[ExitCode] =
    for {
      _ <- logger.info("Creating the array")
      v <- IO.delay {
        (0 until 100_000_100).map(_ => 0.5d).toArray
      }
      _ <- logger.info("Starting the summation")
      res <- summer(v)
      _ <- logger.info(s"Sum was: $res")
    } yield ExitCode.Success
