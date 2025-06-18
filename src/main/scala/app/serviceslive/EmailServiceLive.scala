package app.serviceslive

import cats.data.NonEmptyList
import cats.effect.Sync
import cats.syntax.functor.*
import cats.Functor

import app.services.EmailService
import app.AppConfig.GMailConfig
import emil.*
import emil.javamail.*

final class EmailServiceLive[F[_]: Functor] private (mailer: Emil.Run[F, ?]) extends EmailService[F]:
  override def sendEmail(email: Mail[F]): F[Unit] =
    sendEmail(NonEmptyList.one(email))

  override def sendEmail(emails: NonEmptyList[Mail[F]]): F[Unit] =
    mailer.send_(emails).void

object EmailServiceLive:
  def create[F[_]: Sync](gmailConfig: GMailConfig): EmailServiceLive[F] =
    val mailConf: MailConfig = MailConfig.gmailSmtp(gmailConfig.getEmailUser, gmailConfig.getEmailUserPassword)

    val myEmil: Emil[F] = JavaMailEmil[F]()
    val mailer = myEmil(mailConf)

    EmailServiceLive(mailer)

//createConfigResource[IO].use { config =>
//  val ms = EmailServiceLive.create[IO](config.getGmailConfig)
//  val email = MailBuilder(
//    From[IO](MailAddress.unsafe(None, "nmichael@gmail.com")),
//    To[IO](MailAddress.unsafe(None, "nmichael@yahoo.com")),
//    Cc[IO](MailAddress.unsafe(None, "nmichael@gmail.com")),
//    Subject[IO]("First email"),
//    TextBody[IO]("Hi there, this is Neo's program saying hello..."),
//  ).build
//
//  ms.sendEmail(NonEmptyList.of(email, email, email)).as(ExitCode.Success)
//}
