package app.memcachetests

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.TestUtils.*

final class MemCacheExpiryTest extends AsyncFreeSpec with AsyncIOSpec with Matchers:
  "MemCache: Basic Expiry Logic (via get)" - {
    "E1: should return None for an item whose expiry duration has passed" in {
      val k = "expiredKey"
      val v = 42
      val expiryDuration = 10.seconds // Java Duration for put
      val sleepDuration = 13.seconds  // Scala Duration for IO.sleep, longer than expiry

      createCache[String, Int]().use { cache =>
        for {
          _ <- cache.put(k, v, expiryDuration)
          getBeforeSleep <- cache.get(k) // Should be Some(value)
          _ <- IO.sleep(sleepDuration)
          getAfterSleep <- cache.get(k) // Should be None
        } yield
          // Using the new operator to sequence assertions
          (getBeforeSleep shouldBe Some(v)) ~&> (getAfterSleep shouldBe None)
      }
    }

    "E2: should return Some for an item whose expiry duration has not passed" in {
      val key = "activeKey"
      val value = 43
      val expiryDuration = 10.seconds
      val sleepDuration = 5.seconds // Shorter than expiry

      createCache[String, Int]().use { cache =>
        for {
          _ <- cache.put(key, value, expiryDuration)
          _ <- IO.sleep(sleepDuration)
          getResult <- cache.get(key)
        } yield getResult shouldBe Some(value)
      }
    }

    "E3: should respect the new expiry when an item is overwritten" - {
      "scenario: extend expiry" in {
        val k = "expiryUpdateKey"
        val v0 = 100
        val v1 = 200
        val initialExpiry = 10.seconds
        val extendedExpiry = 10.seconds

        createCache[String, Int]().use { cache =>
          for {
            // Put with initial short expiry
            _ <- cache.put(k, v0, initialExpiry)
            // Sleep for a bit, but less than initial expiry
            _ <- IO.sleep(7.seconds)
            get1 <- cache.get(k) // Should still be there

            // Overwrite with a new value and longer expiry
            // The new expiry time will be calculated from *this* moment.
            _ <- cache.put(k, v1, extendedExpiry)

            // Sleep past the original expiry time, but not past the new extended one
            _ <- IO.sleep(4.seconds)
            get2 <- cache.get(k)
            _ <- IO.sleep(7.seconds)
            get3 <- cache.get(k)
          } yield (get1 shouldBe Some(v0)) ~&>
            (get2 shouldBe Some(v1)) ~&>
            (get3 shouldBe None)
        }
      }

      "scenario: shorten expiry (and it expires)" in {
        val k = "shortenExpiryKey"
        val v = 300
        val initialExpiry = 15.seconds
        val shortenedExpiry = 10.seconds // New expiry from now

        createCache[String, Int]().use { cache =>
          for {
            // Put with initial long expiry
            _ <- cache.put(k, v, initialExpiry)
            _ <- IO.sleep(1.second) // Small sleep
            get1 <- cache.get(k)    // Should be there

            // Overwrite with a new value and shorter expiry
            _ <- cache.put(k, 301, shortenedExpiry)

            // Sleep past the new shortened expiry
            _ <- IO.sleep(12.seconds)
            get2 <- cache.get(k)
          } yield (get1 shouldBe Some(v)) ~&> (get2 shouldBe None) // Should be gone due to new shorter expiry
        }
      }

      "scenario: change from timed to no expiry" in {
        val k = "removeExpiryKey"
        val v0 = 400
        val v1 = 401
        val initialExpiry = 10.seconds

        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put(k, v0, initialExpiry)
            _ <- IO.sleep(1.second)
            get1 <- cache.get(k) // Should be there

            // Overwrite, this time with no expiry
            _ <- cache.put(k, v1)

            // Sleep past the original expiry time
            _ <- IO.sleep(14.seconds)
            get2 <- cache.get(k)
          } yield (get1 shouldBe Some(v0)) ~&> (get2 shouldBe Some(v1)) // Should still be present as expiry was removed
        }
      }

      "scenario: change from no expiry to timed (and it expires)" in {
        val k = "addExpiryKey"
        val (v0, v1) = (500, 501)
        val newExpiry = 10.seconds

        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put(k, v0) // No expiry initially
            _ <- IO.sleep(1.second)
            get1 <- cache.get(k) // Should be there

            _ <- cache.put(k, v1, newExpiry)
            _ <- IO.sleep(1.second)
            get2 <- cache.get(k)

            // Sleep past the new expiry time
            _ <- IO.sleep(11.seconds)
            get3 <- cache.get(k)
          } yield (get1 shouldBe Some(v0)) ~&> (get2 shouldBe Some(v1)) ~&> (get3 shouldBe None)
        }
      }
    }
  }
