package app

object JitTest:
  def f1(p: (Int, Int, Int, Int)): Int =
    val (x0, x1, x2, x3) = p

    x0 * 2 + x1 * 3 + x2 * 5 + x3 * 7

  def f2(x0: Int, x1: Int, x2: Int, x3: Int): Int =
    val (y0, y1, y2, y3) = (x0 * 2, x1 * 3, x2 * 5, x3 * 7)

    y0 + y1 + y2 + y3

  def f3(p: (Int, Int, Int, Int)): Int =
    val x0 = p._1
    val x1 = p._2
    val x2 = p._3
    val x3 = p._4

    x0 * 2 + x1 * 3 + x2 * 5 + x3 * 7

  def f4(x0: Int, x1: Int, x2: Int, x3: Int): Int =
    val y0 = x0 * 2
    val y1 = x1 * 3
    val y2 = x2 * 5
    val y3 = x3 * 7

    y0 + y1 + y2 + y3
end JitTest
