package app // Or your preferred test package structure

import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import scala.concurrent.duration._

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class MemCacheTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestUtils.* // Import logger and extension method

  // Helper to create a MemCache instance for tests
  def createCache[K: Ordering, V](
      name: String = "test-cache",
      capacity: Int = 10,
      cleanupDuration: FiniteDuration = 1.hour, // Background cleanup, not directly tested here
  ): Resource[IO, MemCache[IO, K, V]] =
    MemCache.createResource[IO, K, V](name, capacity, cleanupDuration)

  "MemCache: Core get and put operations" - {
    // P1: Basic `put` and `get`
    "P1: Basic put and get" - {
      "should retrieve a value after putting it (no expiry)" in
        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put("key1", 100)
            result <- cache.get("key1")
          } yield result shouldBe Some(100)
        }

      "should return None for a non-existent key" in
        createCache[String, Int]().use { cache =>
          cache.get("nonExistentKey").asserting(_ shouldBe None)
        }
    }

    // P2: `put` with overwrite
    "P2: Put with overwrite" - {
      "should return the new value after overwriting an existing key" in
        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put("key1", 100)
            get1 <- cache.get("key1")
            _ <- cache.put("key1", 200) // Overwrite
            get2 <- cache.get("key1")
          } yield
            // Using the new operator for sequencing assertions
            (get1 shouldBe Some(100)) ~&> (get2 shouldBe Some(200))
        }
    }

    // P3: `put` with expiry
    "P3: Put with expiry" - {
      "should retrieve a value immediately after putting it with an expiry" in {
        val expiryDuration = java.time.Duration.ofSeconds(60)

        createCache[String, Int]().use { cache =>
          for {
            _ <- cache.put("key1", 300, expiryDuration)
            result <- cache.get("key1")
          } yield result shouldBe Some(300)
        }
      }

      "should allow putting the same key first with expiry, then without" in {
        val expiryDuration = java.time.Duration.ofSeconds(60)

        createCache[String, Int](capacity = 2).use { cache =>
          for {
            _ <- cache.put("key1", 100, expiryDuration)
            getExp <- cache.get("key1")
            _ <- cache.put("key1", 101) // Overwrite, no expiry
            getNoExp <- cache.get("key1")
          } yield (getExp shouldBe Some(100)) ~&> (getNoExp shouldBe Some(101))
        }
      }

      "should allow putting the same key first without expiry, then with" in {
        val expiryDuration = java.time.Duration.ofSeconds(60)

        createCache[String, Int](capacity = 2).use { cache =>
          for {
            _ <- cache.put("key1", 200)
            getNoExp <- cache.get("key1")
            _ <- cache.put("key1", 201, expiryDuration) // Overwrite, with expiry
            getExp <- cache.get("key1")
          } yield (getNoExp shouldBe Some(200)) ~&> (getExp shouldBe Some(201))
        }
      }
    }
  }
}
