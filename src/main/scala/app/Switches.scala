package app

object Switches:
  // Bug report:
  // https://github.com/scala/scala3/issues/23034
  class Instr(val tag: Int)

  private final val A0Tag: Int = 0
  private case object A0 extends Instr(A0Tag)

  private final val A1Tag: Int = 1
  private case object A1 extends Instr(A1Tag)

  private final val A2Tag: Int = 2
  private case class A2(n: Int) extends Instr(A2Tag)

  private final val A3Tag: Int = 3
  private case class A3(n: Int, m: Int) extends Instr(A3Tag)

  private final val A4Tag: Int = 4
  private case class A4(s: String) extends Instr(A4Tag)

  def eval(i: Instr): Int =
    i.tag match {
      case A0Tag /* A0 */ => 0
      case A1Tag /* A1 */ => 1
      case A2Tag /* A2(n) */ =>
        i.asInstanceOf[A2].n
      case A3Tag /* A3(n, m) */ =>
        val a3 = i.asInstanceOf[A3]
        a3.n + a3.m
      case A4Tag /* A4(s) */ =>
        i.asInstanceOf[A4].s.length
    }
