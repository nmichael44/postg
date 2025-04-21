package app

import cats.effect.*
import cats.effect.kernel.Async
import cats.effect.syntax.all.*
import cats.syntax.all.*

import org.typelevel.log4cats.Logger

object DeferredProducerConsumer:
  private final case class JobToDo(n: Int)

  private def createProducer[F[_]: Async](d: Deferred[F, JobToDo], job: JobToDo): F[Unit] =
    d.complete(job).void

  private def createConsumer[F[_]](d: Deferred[F, JobToDo]): F[JobToDo] =
    d.get

  def run[F[_]: { Async, Logger }]: F[ExitCode] =
    val logger = Logger[F]
    val jobToSend = JobToDo(11)
    for {
      d <- Deferred[F, JobToDo]
      _producerFiber <- createProducer[F](d, jobToSend).start
      consumerFiber <- createConsumer[F](d).start
      outcome <- consumerFiber.join
      job <- outcome match {
        case Outcome.Succeeded(fa) => fa
        case Outcome.Errored(e) =>
          logger.error(e)("Consumer fiber failed") *> Async[F].raiseError(e)
        case Outcome.Canceled() =>
          // If canceled, log it and reflect the cancellation in the main fiber
          logger.warn("Consumer fiber was canceled") *>
            Async[F].raiseError(
              new AssertionError("Canceled thread"),
            )
      }
      _ <- logger.info(s"Job came back from deferred: $jobToSend.")
      _ <- logger.info(s"Job came back from deferred: $job.")
      _ <- logger.info(System.identityHashCode(jobToSend).toString)
      _ <- logger.info(System.identityHashCode(job).toString)
      _ <- logger.info(jobToSend.hashCode().toString)
      _ <- logger.info(job.hashCode().toString)
      _ <- Logger[F].info((job eq jobToSend).toString)
    } yield ExitCode.Success
