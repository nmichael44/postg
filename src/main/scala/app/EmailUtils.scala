package app

import cats.Applicative

import emil.builder.*

object EmailUtils:
  private def toMailAddresses(xs: Seq[String]): Seq[emil.MailAddress] =
    xs.map(emil.MailAddress.unsafe(None, _))

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
