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
      val key = "expiredKey"
      val value = 42
      val expiryDuration = 100.millis // Java Duration for put
      val sleepDuration = 200.millis  // Scala Duration for IO.sleep, longer than expiry

      createCache[String, Int]().use { cache =>
        for {
          _ <- cache.put(key, value, expiryDuration)
          getBeforeSleep <- cache.get(key) // Should be Some(value)
          _ <- IO.sleep(sleepDuration)
          getAfterSleep <- cache.get(key) // Should be None
        } yield
          // Using the new operator to sequence assertions
          (getBeforeSleep shouldBe Some(value)) ~&> (getAfterSleep shouldBe None)
      }
    }

    "E2: should return Some for an item whose expiry duration has not passed" in {
      val key = "activeKey"
      val value = 43
      val expiryDuration = 200.millis
      val sleepDuration = 100.millis // Shorter than expiry

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
        val key = "expiryUpdateKey"
        val value1 = 100
        val initialExpiry = 100.millis
        val extendedExpiry = 300.millis // New expiry from now

        createCache[String, Int]().use { cache =>
          for {
            // Put with initial short expiry
            _ <- cache.put(key, value1, initialExpiry)
            // Sleep for a bit, but less than initial expiry
            _ <- IO.sleep(50.millis)
            get1 <- cache.get(key) // Should still be there

            // Overwrite with a new value and longer expiry
            // The new expiry time will be calculated from *this* moment.
            _ <- cache.put(key, 200, extendedExpiry)

            // Sleep past the original expiry time, but not past the new extended one
            // Total sleep from initial put: 50ms (above) + 100ms (here) = 150ms
            // Original expiry was 100ms from initial put.
            // New expiry is 300ms from the *second* put.
            _ <- IO.sleep(100.millis)
            get2 <- cache.get(key)
          } yield (get1 shouldBe Some(value1)) ~&> (get2 shouldBe Some(200)) // Should still be present due to new expiry
        }
      }

      "scenario: shorten expiry (and it expires)" in {
        val key = "shortenExpiryKey"
        val value1 = 300
        val initialExpiry = 500.millis
        val shortenedExpiry = 50.millis // New expiry from now

        createCache[String, Int]().use { cache =>
          for {
            // Put with initial long expiry
            _ <- cache.put(key, value1, initialExpiry)
            _ <- IO.sleep(10.millis) // Small sleep
            get1 <- cache.get(key)   // Should be there

            // Overwrite with a new value and shorter expiry
            _ <- cache.put(key, 301, shortenedExpiry)

            // Sleep past the new shortened expiry
            _ <- IO.sleep(100.millis) // Total sleep from 2nd put: 100ms. New expiry was 50ms.
            get2 <- cache.get(key)
          } yield (get1 shouldBe Some(value1)) ~&> (get2 shouldBe None) // Should be gone due to new shorter expiry
        }
      }

      "scenario: change from timed to no expiry" in {
        val key = "removeExpiryKey"
        val value1 = 400
        val initialExpiry = 100.millis

        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put(key, value1, initialExpiry)
            _ <- IO.sleep(50.millis)
            get1 <- cache.get(key) // Should be there

            // Overwrite, this time with no expiry
            _ <- cache.put(key, 401)

            // Sleep past the original expiry time
            _ <- IO.sleep(100.millis) // Total sleep from initial put: 50ms + 100ms = 150ms
            get2 <- cache.get(key)
          } yield (get1 shouldBe Some(value1)) ~&> (get2 shouldBe Some(401)) // Should still be present as expiry was removed
        }
      }

      "scenario: change from no expiry to timed (and it expires)" in {
        val key = "addExpiryKey"
        val value1 = 500
        val newExpiry = 100.millis

        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put(key, value1) // No expiry initially
            _ <- IO.sleep(50.millis)    // Irrelevant sleep, just to show passage of time
            get1 <- cache.get(key)      // Should be there

            // Overwrite, this time with an expiry
            _ <- cache.put(key, 501, newExpiry)

            // Sleep past the new expiry time
            _ <- IO.sleep(150.millis)
            get2 <- cache.get(key)
          } yield (get1 shouldBe Some(value1)) ~&> (get2 shouldBe None) // Should be expired
        }
      }
    }
  }
