package app // Or your preferred test package structure

import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import scala.concurrent.duration._

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

final class MemCacheWorkerTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestUtils.* // Import logger and extension method

  // Helper to create a MemCache instance for worker tests
  // It's crucial to set a short cleanupDuration for these tests.
  def createCacheForWorkerTest[K: Ordering, V](
      name: String = "worker-test-cache",
      capacity: Int = 10,
      cleanupDuration: FiniteDuration, // Explicitly require cleanupDuration
  ): Resource[IO, MemCache[IO, K, V]] =
    require(capacity > 0, "Capacity must be positive.")
    require(cleanupDuration > Duration.Zero && cleanupDuration < 1.minute, "Cleanup duration should be short for tests.")

    MemCache.createResource[IO, K, V](name, capacity, cleanupDuration)

  "MemCache: Background Cleanup Worker Logic" - {
    // W1: Worker removes an expired item after cleanup interval.
    "W1: should remove an item that expires before the worker runs" in {
      val cleanupInterval = 100.millis
      val itemExpiry = java.time.Duration.ofMillis(50) // Expires before first cleanup
      val key = "itemToClean"
      val value = 1

      createCacheForWorkerTest[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(key, value, itemExpiry)
          // Item in the cache will expire at T+50ms. The worker will first run around T+100ms.

          getBeforeWorker <- cache.get(key) // Should be Some(1), expiry check in get is not enough for this test

          // Sleep a bit longer than the cleanupInterval to ensure the worker has run at least once.
          // Add a small buffer to account for scheduling.
          _ <- IO.sleep(cleanupInterval + 10.millis)

          getAfterWorker <- cache.get(key) // Should be None, cleaned by worker
        } yield (getBeforeWorker shouldBe Some(value)) ~&> (getAfterWorker shouldBe None)
      }
    }

    // W2: Worker does not remove a non-expired item.
    "W2: should not remove an item that has not expired by the time worker runs" in {
      val cleanupInterval = 100.millis
      val itemExpiry = java.time.Duration.ofMillis(500) // Expires well after first cleanup
      val key = "itemToKeep"
      val value = 2

      createCacheForWorkerTest[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(key, value, itemExpiry)
          // Item in the cache will expire at T+500ms.
          // Worker will first run around T+100ms.

          // Sleep a bit longer than the cleanupInterval.
          _ <- IO.sleep(cleanupInterval + 10.millis)

          getAfterWorker <- cache.get(key) // Should still be Some(2)
        } yield getAfterWorker shouldBe Some(value)
      }
    }

    "W2b: should not remove an item with no expiry" in {
      val cleanupInterval = 100.millis
      val key = "itemWithNoExpiry"
      val value = 3

      createCacheForWorkerTest[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(key, value) // No expiry
          // Worker will first run around T+100ms.

          // Sleep a bit longer than the cleanupInterval.
          _ <- IO.sleep(cleanupInterval + 10.millis)

          getAfterWorker <- cache.get(key) // Should still be Some(3)
        } yield getAfterWorker shouldBe Some(value)
      }
    }

    // W3: Worker correctly handles multiple expired items within one cleanup cycle.
    "W3: should remove multiple items that expire before the worker runs" in {
      val cleanupInterval = 150.millis
      val key1 = "multiExpire1"
      val expiry1 = java.time.Duration.ofMillis(50)
      val key2 = "multiExpire2"
      val expiry2 = java.time.Duration.ofMillis(100)
      val key3 = "multiKeep" // This one should not expire quickly
      val expiry3 = java.time.Duration.ofMillis(1000)

      createCacheForWorkerTest[String, Int](capacity = 5, cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(key1, 1, expiry1) // Expires at T+50
          _ <- cache.put(key2, 2, expiry2) // Expires at T+100
          _ <- cache.put(key3, 3, expiry3) // Expires at T+1000
          // Worker runs around T+150ms. By then, key1 and key2 should be expired.

          // Sleep past the cleanup interval
          _ <- IO.sleep(cleanupInterval + 10.millis)

          get1 <- cache.get(key1)
          get2 <- cache.get(key2)
          get3 <- cache.get(key3)
        } yield (get1 shouldBe None) ~&>
          (get2 shouldBe None) ~&>
          (get3 shouldBe Some(3))
      }
    }

    // W3b: Worker handles a mix of expired and non-expired items correctly
    "W3b: should only remove expired items, leaving non-expired and no-expiry items" in {
      val cleanupInterval = 100.millis
      val kExp1 = "exp1"
      val expDur1 = java.time.Duration.ofMillis(20)
      val kExp2 = "exp2"
      val expDur2 = java.time.Duration.ofMillis(50)

      val kNoExp = "noExp"
      val kLongExp = "longExp"
      val longExpDur = java.time.Duration.ofMillis(500)

      createCacheForWorkerTest[String, Int](capacity = 10, cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(kExp1, 1, expDur1)
          _ <- cache.put(kNoExp, 2)
          _ <- cache.put(kExp2, 3, expDur2)
          _ <- cache.put(kLongExp, 4, longExpDur)

          // Sleep past the cleanup interval, allowing exp1 and exp2 to be cleaned
          _ <- IO.sleep(cleanupInterval + 10.millis)

          getExp1 <- cache.get(kExp1)
          getNoExp <- cache.get(kNoExp)
          getExp2 <- cache.get(kExp2)
          getLongExp <- cache.get(kLongExp)

        } yield (getExp1 shouldBe None) ~&>
          (getNoExp shouldBe Some(2)) ~&>
          (getExp2 shouldBe None) ~&>
          (getLongExp shouldBe Some(4))
      }
    }
  }
}
