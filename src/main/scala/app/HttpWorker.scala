package app

import cats.effect.{Async, Deferred}
import cats.effect.std.Queue
import cats.syntax.all.*

import scala.concurrent.duration.*

import org.http4s.Response
import org.typelevel.log4cats.Logger

object HttpWorker:
  final class Job[F[_]](
      val jobName: String,
      val f: () => F[Response[F]],
      val deferred: Deferred[F, Either[Throwable, Response[F]]],
  )

  // Do the program building (the recipe) in the F monad so that if that
  // fails, we can catch it below and recover.  The worker fiber must continue its
  // life no matter what.
  private def buildProgram[F[_]: { Async as async, Logger as logger }](
      prompt: String,
      jobName: String,
      deferred: Deferred[F, Either[Throwable, Response[F]]],
      programBuilder: () => F[Response[F]],
  ): F[F[Response[F]]] = for {
    outcome <- async.delay(programBuilder()).attempt
    res <- outcome match {
      case Right(fa) =>
        logger.info(s"$prompt: Building program was successful.") *> async.pure(fa)
      case Left(e) =>
        logger.error(e)(
          s"$prompt: Job '$jobName' failed with errors during program construction.",
        ) *>
          logger.info(s"$prompt: Sending failed results back...") *>
          deferred.complete(Either.left(e)) *>
          async.raiseError(e)
    }
  } yield res

  private def executeProgram[F[_]: { Async as async, Logger as logger }](
      prompt: String,
      jobName: String,
      program: F[Response[F]],
  ): F[Either[Throwable, Response[F]]] = for {
    outcome <- program.attempt
    _ <- outcome match {
      case Right(_) =>
        logger.info(s"$prompt: Completed job '$jobName' successfully.")
      case Left(e) =>
        logger.error(e)(s"$prompt: Job '$jobName' failed with errors during program execution.")
    }
  } yield outcome

  def worker[F[_]: { Async as async, Logger as logger }](
      workerId: Int,
      queue: Queue[F, Job[F]],
  ): F[Nothing] =
    val prompt = s"Worker '$workerId'"

    val processOneJob: F[Unit] = for {
      _ <- logger.info(s"$prompt: Waiting for work.")
      (jobName, programBuilder, deferred) <- queue.take.map(j => (j.jobName, j.f, j.deferred))
      _ <- logger.info(s"$prompt: Starting to work on '$jobName'.")
      // Build the program
      program <- buildProgram(prompt, jobName, deferred, programBuilder)
      // and now execute it
      outcome <- executeProgram(prompt, jobName, program)
      // finally, send the results back to the calling fiber.
      _ <- logger.info(s"$prompt: Sending results of job '$jobName' back...")
      _ <- deferred.complete(outcome)
    } yield ()

    val processOneJobSafely: F[Unit] = processOneJob.handleErrorWith { e =>
      logger.error(e)(s"Worker '$workerId': Unhandled worker error. Restarting...") *>
        async.sleep(1.second)
    }

    processOneJobSafely.foreverM
