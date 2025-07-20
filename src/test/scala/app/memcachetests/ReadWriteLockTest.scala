package app.memcachetests

import cats.effect.*
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.locks.ReadWriteLock
import app.locks.ReadWriteLockCE

final class ReadWriteLockTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  def withLock[A](test: ReadWriteLock[IO] => IO[A]): IO[A] =
    ReadWriteLockCE.create[IO].flatMap(test)

  "ReadWriteLock" - {

    "should allow multiple readers to acquire concurrently" in withLock { lock =>
      for {
        ref <- Ref[IO].of(0)
        r = lock.read.use(_ => IO.sleep(200.millis) >> ref.update(_ + 1))
        _ <- List.fill(10)(r).parSequence
        result <- ref.get
      } yield result shouldBe 10
    }

    "should ensure writer has exclusive access" in withLock { lock =>
      for {
        ref <- Ref[IO].of(0)
        writer = lock.write.use(_ => ref.update(_ + 1))
        reader = lock.read.use(_ => IO.sleep(100.millis))
        _ <- reader.start
        _ <- IO.sleep(50.millis)
        _ <- writer.start.flatMap(_.joinWithNever)
        result <- ref.get
      } yield result shouldBe 1
    }

    "should block readers if a writer is pending" in withLock { lock =>
      for {
        ref <- Ref[IO].of(List.empty[String])
        writerStarted <- Deferred[IO, Unit]
        readerAcquired <- Deferred[IO, Unit]

        writer = lock.write.use(_ => writerStarted.complete(()) >> IO.sleep(300.millis) >> ref.update(_ :+ "writer done"))

        reader = lock.read
          .use(_ =>
            println("About to write deferred")
            readerAcquired.complete(()) >> ref.update(_ :+ "reader acquired"),
          )

        _ <- writer.start
        _ <- writerStarted.get
        _ <- IO.sleep(50.millis)
        readerFiber <- reader.start
        _ <- IO.sleep(50.millis)
        readAcquired <- readerAcquired.tryGet
        _ <- readerFiber.joinWithNever
        result <- ref.get
      } yield {
        readAcquired shouldBe None
        (result should contain).inOrder("writer done", "reader acquired")
      }
    }

    "should respect FIFO order (readers after writer go after)" in withLock { lock =>
      for {
        ref <- Ref[IO].of(List.empty[String])
        writerDone <- Deferred[IO, Unit]
        reader1Done <- Deferred[IO, Unit]

        // Writer goes first
        writer = lock.write.use(_ => ref.update(_ :+ "writer") >> IO.sleep(100.millis) >> writerDone.complete(()))

        // reader1 waits for the writer, then runs and signals reader2
        reader1 = lock.read.use(_ => writerDone.get >> ref.update(_ :+ "reader1") >> reader1Done.complete(()))

        // reader2 waits for reader1 to finish
        reader2 = lock.read.use(_ => reader1Done.get >> ref.update(_ :+ "reader2"))

        // Start all in order
        _ <- writer.start
        _ <- IO.sleep(50.millis)
        _ <- reader1.start
        _ <- reader2.start
        _ <- IO.sleep(500.millis)
        result <- ref.get
      } yield result shouldBe List("writer", "reader1", "reader2")
    }

    "should not starve readers queued behind a writer" in withLock { lock =>
      for {
        ref <- Ref[IO].of(List.empty[String])
        latch <- Deferred[IO, Unit]

        writer = lock.write.use(_ => IO.sleep(100.millis) >> ref.update(_ :+ "writer") >> latch.complete(()))

        reader = lock.read.use(_ => latch.get >> ref.update(_ :+ "reader"))

        _ <- writer.start
        _ <- IO.sleep(50.millis)
        _ <- reader.start
        _ <- IO.sleep(300.millis)
        result <- ref.get
      } yield result shouldBe List("writer", "reader")
    }

    "should not deadlock under many queued readers and writers" in withLock { lock =>
      val ops: List[IO[Unit]] = List.tabulate(10) { i =>
        if (i % 2 == 0)
          lock.read.use(_ => IO.sleep(20.millis))
        else
          lock.write.use(_ => IO.sleep(20.millis))
      }

      ops.parSequence.void.timeout(3.seconds)
    }
  }
}
