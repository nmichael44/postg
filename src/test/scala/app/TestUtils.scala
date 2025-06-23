package app

import cats.effect.{IO, Resource}

import java.time.Instant
import scala.concurrent.duration.*

import org.scalatest.Assertion

import org.typelevel.log4cats.noop.NoOpLogger
//import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

object TestUtils:
  // An implicit noop logger.
  implicit val testLogger: Logger[IO] = NoOpLogger[IO]
  // implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  extension (leftAssertion: Assertion) {
    /** Sequencing operator for Assertions mostly to avoid intellij warnings. */
    def ~&>(rightAssertion: Assertion): Assertion = rightAssertion
  }

  def createCache[K: Ordering, V](
      name: String = "test-cache",
      capacity: Int = 10,
      cleanupDuration: FiniteDuration = 1.hour,
      timeTickDuration: FiniteDuration = 4.seconds,
  ): Resource[IO, MemCache[IO, K, V]] =
    require(capacity > 0, "Capacity must be positive.")
    MemCache.createResource[IO, K, V](name, capacity, cleanupDuration, timeTickDuration)

  def hasExpired(expiry: Instant, now: Instant): Boolean =
    !now.isBefore(expiry)
