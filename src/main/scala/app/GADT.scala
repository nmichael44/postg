package app

import GADT.Sty.{SBoolean, SInt, SOption}

object GADT:
  enum Sty[T]:
    case SInt extends Sty[Int]
    case SBoolean extends Sty[Boolean]
    case SOption[T1](inner: Sty[T1]) extends Sty[Option[T1]]

  object Sty:
    // This manually-defined 'given' instance is the key to the solution.
    // It tells the compiler that any `Sty[A]` can be compared with any `Sty[B]`,
    // without placing any constraints on the inner types A and B.
    // This satisfies the compiler's check before GADT pattern matching refines the types,
    // resolving the conflict with global strict equality.
    given [A, B]: CanEqual[Sty[A], Sty[B]] = CanEqual.derived

  private def zero[T](s: Sty[T]): T =
    s match
      case SInt => 0
      case SBoolean => false
      case SOption(n) =>
        n match {
          case SInt => Some(0)
          case SBoolean => Some(false)
          case SOption(_) => None
        }

  private val xi: Int = zero(SInt)
  private val xb: Boolean = zero(SBoolean)
  private val xoi: Option[Int] = zero(SOption(SInt))
  private val xob: Option[Boolean] = zero(SOption(SBoolean))

  println(xi)
  println(xb)
  println(xoi)
  println(xob)
