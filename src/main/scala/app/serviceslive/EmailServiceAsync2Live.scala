package app.serviceslive

import cats.data.NonEmptyList
import cats.effect.{Async, Fiber, Resource}
import cats.effect.std.Queue
import cats.effect.syntax.all.*
import cats.implicits.*
import cats.syntax.functor.*

import scala.concurrent.duration.DurationInt

import app.services.EmailService
import app.AppConfig.GMailConfig
import app.Utils as U
import emil.{Mail, MailConfig, Send}
import emil.javamail.internal.JavaMailConnection
import emil.javamail.JavaMailEmil
import org.typelevel.log4cats.Logger
import retry.{retryingOnErrors, ErrorHandler, HandlerDecision, RetryPolicies, RetryPolicy}

final class EmailServiceAsync2Live[F[_]: Async] private (queue: Queue[F, Mail[F]]) extends EmailService[F]:
  override def sendEmail(email: Mail[F]): F[Unit] =
    queue.offer(email)

  override def sendEmail(emails: NonEmptyList[emil.Mail[F]]): F[Unit] =
    emails.toList.traverseVoid(sendEmail)

object EmailServiceAsync2Live:
  def create[F[_]: { Async as async, Logger }](gmailConfig: GMailConfig): Resource[F, EmailService[F]] =
    val emil: JavaMailEmil[F] = JavaMailEmil[F]().asInstanceOf[JavaMailEmil[F]]
    val mailConf: MailConfig =
      MailConfig.gmailSmtp(gmailConfig.getEmailUser, gmailConfig.getEmailUserPassword)

    for {
      queue <- Resource.eval(Queue.bounded[F, Mail[F]](EmailQueueSize))
      _ <- Resource.make(startWorker(queue, emil, mailConf))(stopWorker)
    } yield new EmailServiceAsync2Live[F](queue)

  private def startWorker[F[_]: { Async as async, Logger }](
      queue: Queue[F, Mail[F]],
      emil: JavaMailEmil[F],
      mailConf: MailConfig,
  ): F[Fiber[F, Throwable, Unit]] =
    val retryPolicy: RetryPolicy[F, Throwable] =
      RetryPolicies.capDelay(3.minutes, RetryPolicies.fibonacciBackoff[F](1.seconds))

    val errorHandler: ErrorHandler[F, Unit] = (err, details) =>
      U.loge(err, "RetryFiber", s"Worker lifecycle failed. Restarting... Details: $details")
        .as(HandlerDecision.Continue)

    val emailWorker = EmailWorker(queue, emil, mailConf)
    retryingOnErrors(emailWorker.go)(retryPolicy, errorHandler).start

  private def stopWorker[F[_]: { Async, Logger }](fiber: Fiber[F, Throwable, Unit]) =
    U.logi("MainFiber", "Shutting down email worker.") *> fiber.cancel

  private final val EmailQueueSize: Int = 128

  private final class EmailWorker[F[_]: { Async as async, Logger }](
      queue: Queue[F, Mail[F]],
      emil: JavaMailEmil[F],
      mailConf: MailConfig,
  ):
    private def sendEmail(email: Mail[F], sender: Send[F, JavaMailConnection], connection: JavaMailConnection): F[Unit] =
      sender.sendMails(NonEmptyList.one(email)).run(connection).void

    private val logEmailFound = logi("Email found. Attempting to send.")
    private val logEmailSend = logi("Email sent successfully!")
    private val logWorkerCreatingNewConnection = logi("Worker creating new connection")

    private def onError(email: Mail[F]) = (e: Throwable) =>
      loge(e, "Exception thrown while sending email. Returning email to queue and reestablishing connection.") *>
        queue.offer(email) *> async.raiseError[Unit](e)

    val go: F[Unit] =
      logWorkerCreatingNewConnection *>
        emil.connection(mailConf).use { connection =>
          val sender = emil.sender

          (for {
            email <- queue.take
            _ <- logEmailFound
            _ <- sendEmail(email, sender, connection).handleErrorWith(onError(email))
            _ <- logEmailSend
          } yield ()).foreverM
        }

    private val EmailWorkerName: String = "EmailWorkerFiber"

    private def logi(s: String): F[Unit] =
      U.logi(EmailWorkerName, s)

    private def loge(e: Throwable, s: String): F[Unit] =
      U.loge(e, EmailWorkerName, s)
