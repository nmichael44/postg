package app

import cats.effect.IO

import org.scalatest.Assertion

import org.typelevel.log4cats.noop.NoOpLogger // Using a No-Op Logger
import org.typelevel.log4cats.Logger

object TestUtils {
  // An implicit noop logger.
  implicit val testLogger: Logger[IO] = NoOpLogger[IO]

  extension (leftAssertion: Assertion) {
    /** Sequencing operator for Assertions mostly to avoid intellij warnings. */
    def ~&>(rightAssertion: => Assertion): Assertion =
      rightAssertion
  }
}
