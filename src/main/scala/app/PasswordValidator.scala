package app

import cats.data.ValidatedNel
import cats.implicits.*

object PasswordValidator:
  private final val PasswordMinLen: Int = 8

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

  private def validateLength(password: String): ValidatedNel[String, Unit] =
    if isLongEnough(password) then ().validNel else "Password must be at least 8 characters.".invalidNel

  private def validateUpperCase(password: String): ValidatedNel[String, Unit] =
    if hasUpperCase(password) then ().validNel else "Password must have uppercase characters.".invalidNel

  private def validateLowerCase(password: String): ValidatedNel[String, Unit] =
    if hasLowerCase(password) then ().validNel else "Password must have lowercase characters.".invalidNel

  private def validateDigit(password: String): ValidatedNel[String, Unit] =
    if hasDigit(password) then ().validNel else "Password must have at least one digit.".invalidNel

  private def validateSpecialChar(password: String): ValidatedNel[String, Unit] =
    if hasSpecialChar(password) then ().validNel else "Password must have at least one special character.".invalidNel

  def isPasswordGoodEnough(password: String): ValidatedNel[String, String] =
    (
      validateLength(password),
      validateUpperCase(password),
      validateLowerCase(password),
      validateDigit(password),
      validateSpecialChar(password),
    ).mapN((_, _, _, _, _) => password)
