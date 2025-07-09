package app

final class MyPair[A](val first: A, val second: A):

  // This method is only available if A is a subtype of String.
  def concatenate(implicit ev: A <:< String): String =
    first + second

  // This method is only available if A is a subtype of Int.
  def sum(implicit ev: A <:< Int): Int =
    first + second

  // This method is only available if A is an Int.
  def product(implicit ev: A =:= Int): Int =
    first * second

object MyPair:
  val z0 = MyPair(1, 2)
  val s0 = z0.sum

  val z1 = MyPair("a", "b")
  val s1 = z1.concatenate

  val z2 = MyPair(3, 4)
  val s2 = z2.product

  println(s0 + s1.length + s2)
