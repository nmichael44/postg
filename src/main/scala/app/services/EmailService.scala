package app.services

import cats.data.NonEmptyList

import emil.*

trait EmailService[F[_]]:
  def sendEmail(email: Mail[F]): F[Unit]
  def sendEmail(emails: NonEmptyList[Mail[F]]): F[Unit]
