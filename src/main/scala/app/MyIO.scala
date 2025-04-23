package app

case class MyIO[A](run: () => Option[A]):
  def map[B](f: A => B): MyIO[B] =
    MyIO(() =>
      run() match {
        case Some(a) => Some(f(a))
        case None => None
      },
    )

  def flatMap[B](f: A => MyIO[B]): MyIO[B] =
    MyIO(() =>
      run() match {
        case Some(a) => f(a).run()
        case None => None
      },
    )

  def withFilter(p: A => Boolean): MyIO[A] =
    MyIO(() =>
      run() match {
        case res @ Some(a) => if p(a) then res else None
        case None => None
      },
    )

object MyIO:
  def pure[A](a: A): MyIO[A] = MyIO(() => Some(a))

  def delay[A](a: => A): MyIO[A] = MyIO(() =>
    import scala.util.control.NonFatal
    try
      Some(a)
    catch {
      case NonFatal(_) => None
      case t: Throwable => throw t
    },
  )

  private def g(n: Int): Int =
    if n == 0 then 1 else n * g(n - 1)

  private def isEven(n: Int): Boolean = (n & 1) == 0
  private def isOdd(n: Int): Boolean = !isEven(n)

  def f(n: Int): MyIO[Int] =
    for {
      k <- MyIO.delay(g(n))
      if isOdd(k)
      j <- MyIO.delay(g(n + 1))
      if isEven(j)
    } yield k + j

  def h(n: Int): MyIO[Int] =
    for {
      k <- MyIO.delay(g(n))
      j <- MyIO.delay(g(n + 1))
      m <- MyIO.delay(g(n + 1))
    } yield k + j + m
