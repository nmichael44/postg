package app

import GADT.Sty.{SBoolean, SInt, SOption}

object GADT:
  enum Sty[T]:
    case SInt extends Sty[Int]
    case SBoolean extends Sty[Boolean]
    case SOption[T1](inner: Sty[T1]) extends Sty[Option[T1]]

  private def zero[T](s: Sty[T]): T =
    s match
      case SInt => 0
      case SBoolean => false
      case SOption(_) => None

  private val xi: Int = zero(SInt)
  private val xb: Boolean = zero(SBoolean)
  private val xoi: Option[Int] = zero(SOption(SInt))
  private val xob: Option[Boolean] = zero(SOption(SBoolean))

  println(xi)
  println(xb)
  println(xoi)
  println(xob)
