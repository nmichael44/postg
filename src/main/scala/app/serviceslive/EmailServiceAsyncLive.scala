package app.serviceslive

import cats.{Functor, Monad}
import cats.data.NonEmptyList
import cats.effect.kernel.Fiber
import cats.effect.std.Queue
import cats.effect.syntax.all.*
import cats.effect.Async
import cats.implicits.*
import cats.syntax.functor.*

import scala.concurrent.duration.DurationInt

import app.services.EmailService
import app.AppConfig.GMailConfig
import app.Utils as U
import emil.{Emil, Mail, MailConfig}
import emil.javamail.JavaMailEmil
import org.typelevel.log4cats.Logger

private final class EmailServiceAsyncLive[F[_]: Async] private (queue: Queue[F, Mail[F]]) extends EmailService[F]:
  override def sendEmail(email: Mail[F]): F[Unit] =
    queue.offer(email)
  end sendEmail

  override def sendEmail(emails: NonEmptyList[emil.Mail[F]]): F[Unit] =
    emails.toList.traverseVoid(sendEmail)
  end sendEmail
end EmailServiceAsyncLive

object EmailServiceAsyncLive:
  def create[F[_]: { Async, Logger }](gmailConfig: GMailConfig): F[EmailService[F]] =
    val mailConf: MailConfig =
      MailConfig.gmailSmtp(gmailConfig.getEmailUser, gmailConfig.getEmailUserPassword)

    val myEmil: Emil[F] = JavaMailEmil[F]()
    val mailer = myEmil(mailConf)

    logi("Creating queue...") *>
      Queue.bounded[F, Mail[F]](EmailQueueSize) >>= { queue =>
      logi("Creating worker...")
      createWorker(queue, mailer) >>= { _ =>
        logi("Creating EmailServiceAsyncLive...")
          .as(
            EmailServiceAsyncLive(queue),
          ) <* logi("Email service ready!")
      }
    }
  end create

  private val WorkerSleepDuration: scala.concurrent.duration.Duration = 32.seconds
  private val WorkerBatchSize: Option[Int] = Some(3)
  private val EmailWorkerName: String = "EmailWorker"
  private val EmailQueueSize: Int = 128

  private def sendEmails[F[_]: Functor](emails: NonEmptyList[Mail[F]], mailer: Emil.Run[F, ?]): F[Unit] =
    mailer.send_(emails).void
  end sendEmails

  private def sendUntilEmpty[F[_]: { Monad, Logger }](queue: Queue[F, Mail[F]], mailer: Emil.Run[F, ?]): F[Unit] =
    def loop(): F[Unit] =
      queue.tryTakeN(WorkerBatchSize) >>= {
        case Nil => logi("No emails were found.")
        case m :: ms =>
          logi("Some emails were found.  Sending...") *>
            sendEmails(NonEmptyList(m, ms), mailer) *>
            logi("Batch of emails sent.") >>
            loop()
      }

    loop()
  end sendUntilEmpty

  private def createWorker[F[_]: { Async as async, Logger }](
      queue: Queue[F, Mail[F]],
      mailer: Emil.Run[F, ?],
  ): F[Fiber[F, Throwable, Nothing]] =
    val sendEmailUntilQueueEmpty = sendUntilEmpty(queue, mailer)
    val logGoingToSleep = logi("Going to sleep...")
    val sleepForAWhile = async.sleep(WorkerSleepDuration)
    val logAwakeAndReadyToWork = logi("Awake! Let's check if we have emails to send.")
    val onError = loge(_, "Error while processing emails. Continuing...")

    val processUntilEmpty = for {
      _ <- sendEmailUntilQueueEmpty
      _ <- logGoingToSleep
      _ <- sleepForAWhile
      _ <- logAwakeAndReadyToWork
    } yield ()

    val processWithErrorHandling: F[Unit] = processUntilEmpty.handleErrorWith(onError)

    processWithErrorHandling.foreverM.start
  end createWorker

  private def logi[F[_]: Logger](s: String): F[Unit] =
    U.logi(EmailWorkerName, s)
  end logi

  private def loge[F[_]: Logger](e: Throwable, s: String): F[Unit] =
    U.loge(e, EmailWorkerName, s)
  end loge
end EmailServiceAsyncLive
