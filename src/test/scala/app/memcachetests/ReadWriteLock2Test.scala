package app.memcachetests

import cats.effect.{IO, Ref}
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.syntax.all.*

import scala.concurrent.duration.*

import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.matchers.should.Matchers

import app.locks.ReadWriteLockCE

final class ReadWriteLock2Test extends AsyncFunSuite with AsyncIOSpec with Matchers {
  // Helper to track the state of a critical section for testing
  case class CriticalSection(readers: Int, writers: Int):
    def read: CriticalSection = {
      assert(writers == 0, "A reader entered while a writer was present.")
      this.copy(readers = readers + 1)
    }
    def unread: CriticalSection = this.copy(readers = readers - 1)
    def write: CriticalSection = {
      assert(writers == 0, "A writer entered while another writer was present.")
      assert(readers == 0, "A writer entered while readers were present.")
      this.copy(writers = writers + 1)
    }
    def unwrite: CriticalSection = this.copy(writers = writers - 1)

  given CanEqual[CriticalSection, CriticalSection] = CanEqual.derived

  object CriticalSection {
    def empty: CriticalSection = CriticalSection(0, 0)
  }

  // --- Basic Tests ---

  test("a read lock can be acquired when no locks are held") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      _ <- lock.read.use(_ => IO.unit)
    } yield succeed
  }

  test("a write lock can be acquired when no locks are held") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      _ <- lock.write.use(_ => IO.unit)
    } yield succeed
  }

  test("multiple read locks can be acquired simultaneously") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      section <- Ref.of[IO, CriticalSection](CriticalSection.empty)
      reader = lock.read.use { _ =>
        section.update(_.read) *> IO.sleep(50.millis) *> section.update(_.unread)
      }
      _ <- List.fill(5)(reader).parSequence_
      s <- section.get
    } yield s shouldBe CriticalSection.empty
  }

  // --- Exclusivity & Wake-up Logic Tests ---

  test("a write lock prevents a read lock from being acquired") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      writeLatch <- IO.deferred[Unit]   // To signal writer has the lock
      readFinished <- IO.deferred[Unit] // To signal reader has finished "trying"

      writerFiber <- lock.write.use { _ =>
        writeLatch.complete(()) *> readFinished.get
      }.start

      _ <- writeLatch.get // Wait until writer has the lock

      readerFiber <- lock.read.use(_ => IO.unit).start
      result <- IO.race(readerFiber.join, IO.sleep(100.millis))

      _ <- readFinished.complete(()) // Allow writer to finish
      _ <- writerFiber.join
    } yield result.isRight shouldBe true // Right means the sleep won, so reader was blocked
  }

  test("a read lock prevents a write lock from being acquired") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      readLatch <- IO.deferred[Unit]
      writeFinished <- IO.deferred[Unit]

      readerFiber <- lock.read.use { _ =>
        readLatch.complete(()) *> writeFinished.get
      }.start

      _ <- readLatch.get

      writerFiber <- lock.write.use(_ => IO.unit).start
      result <- IO.race(writerFiber.join, IO.sleep(100.millis))

      _ <- writeFinished.complete(())
      _ <- readerFiber.join
    } yield result.isRight shouldBe true // Right means the sleep won, so writer was blocked
  }

  test("releasing the last read lock wakes up a waiting writer") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      log <- Ref.of[IO, List[String]](List.empty)
      r1Latch <- IO.deferred[Unit]

      r1 = lock.read.use(_ => log.update("r1 in" :: _) *> r1Latch.get *> log.update("r1 out" :: _))
      w1 = lock.write.use(_ => log.update("w1 in" :: _) *> log.update("w1 out" :: _))
      r2 = lock.read.use(_ => log.update("r2 in" :: _) *> log.update("r2 out" :: _))

      r1Fiber <- r1.start
      _ <- IO.sleep(20.millis) // ensure r1 gets lock
      w1Fiber <- w1.start
      _ <- IO.sleep(20.millis) // ensure w1 is waiting
      r2Fiber <- r2.start
      _ <- IO.sleep(20.millis) // ensure r2 is waiting

      _ <- r1Latch.complete(()) // Release r1

      _ <- w1Fiber.join
      _ <- r2Fiber.join
      _ <- r1Fiber.join

      finalLog <- log.get.map(_.reverse)
    } yield {
      val w1InIndex = finalLog.indexOf("w1 in")
      val r2InIndex = finalLog.indexOf("r2 in")
      w1InIndex should be > -1
      r2InIndex should be > -1
      // Crucial check: writer should get the lock before the second reader
      w1InIndex should be < r2InIndex
    }
  }

  test("releasing a write lock wakes up all waiting readers") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      log <- Ref.of[IO, List[String]](List.empty)
      w1Latch <- IO.deferred[Unit]

      w1 = lock.write.use(_ => log.update("w1 in" :: _) *> w1Latch.get *> log.update("w1 out" :: _))
      reader = lock.read.use(_ => log.update("reader in" :: _))

      w1Fiber <- w1.start
      _ <- IO.sleep(20.millis) // ensure w1 gets lock

      readerFibers <- List.fill(5)(reader.start).sequence

      _ <- w1Latch.complete(()) // Release writer

      _ <- readerFibers.traverse_(_.join)
      _ <- w1Fiber.join

      finalLog <- log.get
    } yield finalLog.count(_ == "reader in") shouldBe 5
  }

  test("a waiting writer acquires the lock after the current writer releases it") {
    for {
      lock <- ReadWriteLockCE.create[IO]
      log <- Ref.of[IO, List[String]](List.empty)
      w1Latch <- IO.deferred[Unit]

      // Writer 1 acquires the lock, logs, then waits on a latch
      w1 = lock.write.use { _ =>
        log.update("w1 in" :: _) *> w1Latch.get *> log.update("w1 out" :: _)
      }

      // Writer 2 will attempt to acquire and should block
      w2 = lock.write.use { _ =>
        log.update("w2 in" :: _) *> log.update("w2 out" :: _)
      }

      w1Fiber <- w1.start
      _ <- IO.sleep(20.millis) // Ensure w1 acquires the lock

      w2Fiber <- w2.start
      _ <- IO.sleep(20.millis) // Ensure w2 is in the waiting queue

      // At this point, log should be ["w1 in"]
      // Now, release w1
      _ <- w1Latch.complete(())

      // Wait for both to complete
      _ <- w1Fiber.join
      _ <- w2Fiber.join

      finalLog <- log.get.map(_.reverse)
    } yield
      // The only correct sequence is for w1 to finish before w2 starts
      finalLog shouldBe List("w1 in", "w1 out", "w2 in", "w2 out")
  }
}
