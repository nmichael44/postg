package app.memcachetests

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.TestUtils.*

final class MemCacheWorkerTest extends AsyncFreeSpec with AsyncIOSpec with Matchers:
  "MemCache: Background Cleanup Worker Logic" - {
    "W1: should remove an item that expires before the worker runs" in {
      val cleanupInterval = 30.seconds
      val itemExpiry = 10.seconds // Expires before first cleanup
      val k = "itemToClean"
      val v = 1

      createCache[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(k, v, itemExpiry)

          getBeforeWorker <- cache.get(k) // Should be Some(1), expiry check in get is not enough for this test

          // Sleep a bit longer than the cleanupInterval to ensure the worker has run at least once.
          // Add a small buffer to account for scheduling.
          _ <- IO.sleep(cleanupInterval + 1.second)

          getAfterWorker <- cache.get(k) // Should be None, cleaned by worker
        } yield (getBeforeWorker shouldBe Some(v)) ~&> (getAfterWorker shouldBe None)
      }
    }

    "W2: should not remove an item that has not expired by the time worker runs" in {
      val cleanupInterval = 30.seconds
      val itemExpiry = cleanupInterval + 2.second // Expires well after first cleanup
      val (k, v) = ("itemToKeep", 2)

      createCache[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(k, v, itemExpiry)
          // Sleep a bit longer than the cleanupInterval.
          _ <- IO.sleep(cleanupInterval + 1.second)

          getAfterWorker <- cache.get(k) // Should still be Some(2)
          _ <- IO.sleep(2.second)
          getAfterExpiry <- cache.get(k) // Should not be returned
        } yield (getAfterWorker shouldBe Some(v)) ~&> (getAfterExpiry shouldBe None)
      }
    }

    "W2b: should not remove an item with no expiry" in {
      val cleanupInterval = 30.seconds
      val (k, v) = ("itemWithNoExpiry", 3)

      createCache[String, Int](cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(k, v)

          // Sleep a bit longer than the cleanupInterval.
          _ <- IO.sleep(cleanupInterval + 1.second)

          getAfterWorker <- cache.get(k) // Should still be Some(3)
        } yield getAfterWorker shouldBe Some(v)
      }
    }
  }
