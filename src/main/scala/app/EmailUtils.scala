package app

import cats.data.{Validated, ValidatedNec}
import cats.implicits.*
import cats.Applicative

import com.sanctionco.jmail.{EmailValidationResult, JMail}
import emil.builder.*

object EmailUtils:
  private def toMailAddresses(xs: Seq[String]): Seq[emil.MailAddress] =
    xs.map(emil.MailAddress.unsafe(None, _))
  end toMailAddresses

  def createMail[F[_]: Applicative](
      from: String,
      tos: Seq[String],
      ccs: Seq[String],
      bccs: Seq[String],
      subject: String,
      body: String,
  ): emil.Mail[F] =
    emil.builder
      .MailBuilder(
        From[F](emil.MailAddress.unsafe(None, from)),
        Tos[F](toMailAddresses(tos)),
        Ccs[F](toMailAddresses(ccs)),
        Bccs[F](toMailAddresses(bccs)),
        Subject[F](subject),
        TextBody[F](body),
      )
      .build
  end createMail

  def validateEmail(kind: String, email: String): ValidatedNec[String, Unit] =
    val result: EmailValidationResult = JMail.validate(email)
    if result.isSuccess then ().validNec
    else
      val reason = result.getFailureReason
      s"$kind email '$email' was invalid. Reason: $reason".invalidNec
  end validateEmail

  def validateEmails(kind: String, emails: Seq[String]): ValidatedNec[String, Unit] =
    emails.traverse(validateEmail(kind, _)).as(())
  end validateEmails
end EmailUtils
