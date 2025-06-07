package app

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

  object Printing /* extends App */ {
    private val hello: MyIO[Unit] = MyIO.putStr("hello!")
    private val world: MyIO[Unit] = MyIO.putStr("world!")
    private val helloWorld: MyIO[Unit] = for {
      _ <- hello
      _ <- world
    } yield ()

    helloWorld.unsafeRun()
  }

  object Timing /* extends App */ {
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
