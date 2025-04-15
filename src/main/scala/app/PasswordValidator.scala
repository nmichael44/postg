package app

object PasswordValidator:
  private final val PasswordMinLen: Int = 8

  private def hasCharWithProperty(pred: Char => Boolean, password: String): Boolean =
    password.exists(pred)

  private def isLongEnough(password: String) = password.length >= PasswordMinLen
  private def hasUpperCase(password: String) = hasCharWithProperty(_.isUpper, password)
  private def hasLowerCase(password: String) = hasCharWithProperty(_.isLower, password)
  private def hasDigit(password: String) = hasCharWithProperty(_.isDigit, password)
  private def hasSpecialChar(password: String) =
    hasCharWithProperty(c => !c.isLetterOrDigit, password)

  // Returns a list of reasons why the password was not good enough.
  // If the sequence is empty then the password was good.
  def isPasswordGoodEnough(password: String): Seq[String] =
    val vb = Vector.newBuilder[String]
    if !isLongEnough(password) then
      vb.addOne(s"Password must be at least $PasswordMinLen characters.")
    if !hasUpperCase(password) then vb.addOne("Password must have uppercase characters.")
    if !hasLowerCase(password) then vb.addOne("Password must have lowercase characters.")
    if !hasDigit(password) then vb.addOne("Password must have at least one digits.")
    if !hasSpecialChar(password) then
      vb.addOne("Password must have at least one special character.")

    vb.result()
