package app.services

import cats.data.NonEmptyList

trait EmailService[F[_]]:
  def sendEmail(email: emil.Mail[F]): F[Unit]
  def sendEmail(emails: NonEmptyList[emil.Mail[F]]): F[Unit]
end EmailService
