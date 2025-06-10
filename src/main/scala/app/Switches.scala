package app

object Switches:
  // Bug report:
  // https://github.com/scala/scala3/issues/23034
  // Not a bug, apparently.
  class Instr(val tag: Int)

  inline private val A0Tag = 0
  private case object A0 extends Instr(A0Tag)

  inline private val A1Tag = 1
  private case object A1 extends Instr(A1Tag)

  inline private val A2Tag = 2
  private case class A2(n: Int) extends Instr(A2Tag)

  inline private val A3Tag = 3
  private case class A3(n: Int, m: Int) extends Instr(A3Tag)

  inline private val A4Tag = 4
  private case class A4(s: String) extends Instr(A4Tag)

  def eval(i: Instr): Int =
    i.tag match {
      case A0Tag => 0
      case A1Tag => 1
      case A2Tag => i.asInstanceOf[A2].n
      case A3Tag => val a3 = i.asInstanceOf[A3]; a3.n + a3.m
      case A4Tag => i.asInstanceOf[A4].s.length
    }

  private val DispatchMap: Map[Class[?], (instr: Any) => Int] = Map(
    classOf[A0.type] -> (i => 0),
    classOf[A1.type] -> (i => 1),
    classOf[A2]      -> (i => i.asInstanceOf[A2].n),
    classOf[A3]      -> (i => { val a3 = asInstanceOf[A3]; a3.n + a3.m }),
    classOf[A4]      -> (i => i.asInstanceOf[A4].s.length),
  )

  def eval2(i: Instr): Int =
    DispatchMap.get(i.getClass) match {
      case Some(f) => f(i)
      case None => throw AssertionError("Unimplemented instruction.")
    }

  enum Omega derives CanEqual {
    case O0
    case O1
    case O2
    case O3
    case O4
  }

  // Dispatch in Scala is a disaster.
  def evalO(omega: Omega): Int =
    import scala.annotation.switch
    omega /*: @switch*/ match {
      case Omega.O0 => 0
      case Omega.O1 => 1
      case Omega.O2 => 2
      case Omega.O3 => 3
      case Omega.O4 => 4
    }
