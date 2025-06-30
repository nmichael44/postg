package app.serviceslive

import cats.data.NonEmptyList
import cats.effect.{Async, Resource}
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
  def create[F[_]: { Async as async, Logger as logger }](gmailConfig: GMailConfig): Resource[F, EmailService[F]] =
    val emil = JavaMailEmil[F]().asInstanceOf[JavaMailEmil[F]]
    val mailConf: MailConfig =
      MailConfig.gmailSmtp(gmailConfig.getEmailUser, gmailConfig.getEmailUserPassword)

    val retryPolicy: RetryPolicy[F, Throwable] =
      RetryPolicies.capDelay(4.minutes, RetryPolicies.fibonacciBackoff[F](2.seconds))

    val errorHandler: ErrorHandler[F, Unit] =
      (err, details) =>
        loge(err, s"Worker lifecycle failed. Restarting... Details: $details")
          .as(HandlerDecision.Continue)

    for {
      queue <- Resource.eval(Queue.bounded[F, Mail[F]](EmailQueueSize))
      supervisedWorker = retryingOnErrors(workerAction(emil, mailConf, queue))(retryPolicy, errorHandler)
      _ <- Resource.make(supervisedWorker.start)(fiber => U.logi("Main Fiber", "Shutting down email worker.") *> fiber.cancel)
    } yield new EmailServiceAsync2Live[F](queue)

  private def sendEmail[F[_]: { Async, Logger }](
      email: Mail[F],
      sender: Send[F, JavaMailConnection],
      connection: JavaMailConnection,
  ): F[Unit] =
    sender.sendMails(NonEmptyList.one(email)).run(connection).void

  private def workerAction[F[_]: { Async as async, Logger }](
      emil: JavaMailEmil[F],
      mailConf: MailConfig,
      queue: Queue[F, Mail[F]],
  ): F[Unit] = {
    val logEmailFound = logi("Email found. Attempting to send.")
    val logEmailSend = logi("Email sent successfully!")
    val onError = (e: Throwable, email: Mail[F]) =>
      loge(e, "Exception thrown while sending email. Returning email to queue and reestablishing connection.") *>
        queue.offer(email) *> async.raiseError[Unit](e)

    logi("Worker creating new connection") *>
      emil.connection(mailConf).use { connection =>
        val sender = emil.sender
        val processOneEmail = for {
          email <- queue.take
          _ <- logEmailFound
          _ <- sendEmail(email, sender, connection).handleErrorWith(onError(_, email))
          _ <- logEmailSend
        } yield ()

        processOneEmail.foreverM
      }
  }

  private val EmailWorkerName: String = "EmailWorker"

  private val EmailQueueSize: Int = 128

  private def logi[F[_]: Logger](s: String): F[Unit] =
    U.logi(EmailWorkerName, s)

  private def loge[F[_]: Logger](e: Throwable, s: String): F[Unit] =
    U.loge(e, EmailWorkerName, s)
