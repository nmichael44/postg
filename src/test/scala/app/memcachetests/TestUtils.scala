package app.memcachetests

import app.MemCache
import cats.effect.{IO, Resource}
import org.scalatest.Assertion
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger

import java.time.Instant
import scala.concurrent.duration.*

object TestUtils:
  // An implicit noop logger.
  implicit val testLogger: Logger[IO] = NoOpLogger[IO]

  extension (leftAssertion: Assertion) {
    /** Sequencing operator for Assertions mostly to avoid intellij warnings. */
    def ~&>(rightAssertion: => Assertion): Assertion =
      rightAssertion
  }

  def createCache[K: Ordering, V](
      name: String = "test-cache",
      capacity: Int = 10,
      cleanupDuration: FiniteDuration = 1.hour,
  ): Resource[IO, MemCache[IO, K, V]] =
    require(capacity > 0, "Capacity must be positive.")
    MemCache.createResource[IO, K, V](name, capacity, cleanupDuration)

  def hasExpired(expiry: Instant, now: Instant): Boolean =
    !now.isBefore(expiry)
