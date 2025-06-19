package app

import cats.data.{Chain, NonEmptyChain, NonEmptyVector, Validated, ValidatedNec, ValidatedNel}
import cats.implicits.*

import app.{Utils => U}

object PasswordValidator:
  inline private final val PasswordMinLen = 8

  private def hasCharWithProperty(pred: Char => Boolean, password: String): Boolean =
    password.exists(pred)

  private def isLongEnough(password: String): Boolean =
    password.length >= PasswordMinLen

  private def hasUpperCase(password: String): Boolean =
    hasCharWithProperty(_.isUpper, password)

  private def hasLowerCase(password: String): Boolean =
    hasCharWithProperty(_.isLower, password)

  private def hasDigit(password: String): Boolean =
    hasCharWithProperty(_.isDigit, password)

  private def hasSpecialChar(password: String): Boolean =
    hasCharWithProperty(c => !c.isLetterOrDigit, password)

  private type ValidatedNec[E, A] = Validated[NonEmptyChain[E], A]

  extension [A](a: A)
    private def validNec[E]: ValidatedNec[E, A] = Validated.Valid(a)
    private def invalidNec[B]: ValidatedNec[A, B] = Validated.Invalid(NonEmptyChain.one(a))

  extension (t: Boolean)
    private def valid[A, B](a: A, b: B): ValidatedNec[B, A] =
      if t then a.validNec else b.invalidNec

  private val ErrorStringForValidateLength: String =
    s"Password must be at least $PasswordMinLen characters."

  private def validateLength(password: String): ValidatedNec[String, Unit] =
    isLongEnough(password).valid((), ErrorStringForValidateLength)

  private def validateUpperCase(password: String): ValidatedNec[String, Unit] =
    hasUpperCase(password).valid((), "Password must have uppercase characters.")

  private def validateLowerCase(password: String): ValidatedNec[String, Unit] =
    hasLowerCase(password).valid((), "Password must have lowercase characters.")

  private def validateDigit(password: String): ValidatedNec[String, Unit] =
    hasDigit(password).valid((), "Password must have at least one digit.")

  private def validateSpecialChar(password: String): ValidatedNec[String, Unit] =
    hasSpecialChar(password).valid((), "Password must have at least one special character.")

  def isPasswordGoodEnough(password: String): Validated[NonEmptyVector[String], String] =
    (
      validateLength(password),
      validateUpperCase(password),
      validateLowerCase(password),
      validateDigit(password),
      validateSpecialChar(password),
    ).mapN(U.const5(password))
      .leftMap(_.toNonEmptyVector)
