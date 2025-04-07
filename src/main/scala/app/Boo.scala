package app

import cats.effect.IO

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object Boo:
  case class MyIO[A](unsafeRun: () => A) {
    def map[B](f: A => B): MyIO[B] =
      MyIO(() => f(unsafeRun()))

    def flatMap[B](f: A => MyIO[B]): MyIO[B] =
      MyIO(() => f(unsafeRun()).unsafeRun())
  }

  private object MyIO {
    def putStr(s: => String): MyIO[Unit] =
      MyIO(() => println(s))
  }

  object Printing extends App {
    private val hello: MyIO[Unit] = MyIO.putStr("hello!")
    private val world: MyIO[Unit] = MyIO.putStr("world!")
    private val helloWorld: MyIO[Unit] = for {
      _ <- hello
      _ <- world
    } yield ()

    helloWorld.unsafeRun()
  }

  object Timing extends App {
    private val clock: MyIO[Long] =
      MyIO(() => System.currentTimeMillis)

    def time[A](action: MyIO[A]): MyIO[(FiniteDuration, A)] =
      for {
        t0 <- clock
        a <- action
        t1 <- clock
      } yield (FiniteDuration(t1 - t0, TimeUnit.MILLISECONDS), a)

    def doIt(): Unit =
      val timedHello: MyIO[(FiniteDuration, Unit)] = Timing.time(MyIO.putStr("hello"))
      timedHello.unsafeRun() match {
        case (duration, _) => println(s"'hello' took $duration")
      }
  }

  val x: IO[Int] = IO(12)
  val y: IO[Unit] = IO(println(123))
  val z: IO[Unit] = IO.println(456)
  val omega: IO[Int] = IO.pure(789)

  val e0: IO[Int] = IO(throw new RuntimeException("oh no!"))
  val e1: IO[Int] = IO.raiseError(new RuntimeException("oh no!"))

  import cats.syntax.all._

  val x1: IO[String] = x.map(_.toString)
  val zz: IO[(String, Int)] = (IO(2), IO("5")).mapN((i, s) => (i.toString, Integer.parseInt(s)))
  val zz2: IO[String] = for {
    i <- IO(2).debug()
    j <- IO(4 + i)
  } yield (j + 1).toString
