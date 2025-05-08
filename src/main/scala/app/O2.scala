package app

object O2 {
  private case class C(x: Int)

  def g(): Unit = {
    println("Entering g")
    lazy val lc = {
      println("Building c")
      val c = C(10)
      println("Done building c")
      c
    }

    println("Calling f")
    f(lc)
    println("Leaving g")
  }

  private def f(c: => C): Unit = {
    println("Entering f")
    println(c)
    println("Leaving f")
  }

  def sum(n: Int): Int = (1 to n).sum
  def product(n: Int): Int = (1 to n).product
  def sumSquared(n: Int): Int = (1 to n).map(x => x * x).sum
  def sumSquared2(n: Int): Int = (1 to n).fold(0)((x, acc) => acc + x * x)

  def f(n: Int, f: => Int): Int = n + f

  def g(m: Int): Int = f(m, 1)

  trait StringAble[A] {
    def str(a: A): String
  }

  given intStringAble: StringAble[Int] with
    def str(a: Int): String = java.lang.Integer.toString(a)

  given doubleStringAble: StringAble[Double] with
    def str(a: Double): String = java.lang.Double.toString(a)

  given stringStringAble: StringAble[String] with
    def str(s: String): String = s

  private def toMyString1[A: StringAble](a: A): String =
    summon[StringAble[A]].str(a)

  private def toMyString2[A](a: A)(using stringer: StringAble[A]): String =
    stringer.str(a)

  def f1(i: Int, d: Double, s: String): String =
    toMyString1(i) + toMyString1(d) + toMyString1(s)

  def f2(i: Int, d: Double, s: String): String =
    toMyString2(i) + toMyString2(d) + toMyString2(s)
}
