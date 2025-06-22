package app

import cats.data.{NonEmptyVector, Validated, ValidatedNec}
import cats.implicits.*

import com.sanctionco.jmail.{EmailValidationResult, JMail}

object EmailValidator:
  def validateEmail(kind: String, email: String): ValidatedNec[String, Unit] =
    val result: EmailValidationResult = JMail.validate(email)
    if result.isSuccess then ().validNec
    else
      val reason = result.getFailureReason
      s"$kind email '$email' was invalid. Reason: $reason".invalidNec

  def validateEmails(kind: String, emails: Seq[String]): ValidatedNec[String, Unit] =
    emails.traverse(validateEmail(kind, _)).as(())
