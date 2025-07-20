package app.memcachetests

import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.MemCaches.MemCache
import app.TestUtils.*

final class MemCacheCapacityTest extends AsyncFreeSpec with AsyncIOSpec with Matchers:
  private def createCacheWithCapacity[K: Ordering, V](
      capacity: Int,
      name: String = "capacity-test-cache",
      cleanupDuration: FiniteDuration = 1.hour,
      timeTickDuration: FiniteDuration = 4.seconds,
  ): Resource[IO, MemCache[IO, K, V]] =
    require(capacity > 0, "Capacity must be positive for these tests")
    MemCache.createResource[IO, K, V](name, capacity, cleanupDuration, timeTickDuration)

  "MemCache: Basic Capacity and LRU Eviction" - {
    "C1: should evict an item when capacity is exceeded" - {
      "evicts the least recently used item (by insertion order if no gets)" in {
        val capacity = 2
        createCacheWithCapacity[String, Int](capacity).use { cache =>
          for {
            _ <- cache.put("key1", 1) // LRU candidate after key2, key3
            _ <- cache.put("key2", 2)
            // At this point, cache is full: {"key1":1, "key2":2}
            // LRU order (oldest to newest by put): key1, key2

            _ <- cache.put("key3", 3) // This should evict "key1"
            // Cache should now be: {"key2":2, "key3":3}

            get1 <- cache.get("key1")
            get2 <- cache.get("key2")
            get3 <- cache.get("key3")
          } yield (get1 shouldBe None) ~&> // key1 was evicted
            (get2 shouldBe Some(2)) ~&>
            (get3 shouldBe Some(3))
        }
      }
    }

    "C2: should evict the least recently used (LRU) item" in {
      val capacity = 3
      createCacheWithCapacity[String, Int](capacity).use { cache =>
        for {
          _ <- cache.put("key1", 1) // seq 0
          _ <- cache.put("key2", 2) // seq 1
          _ <- cache.put("key3", 3) // seq 2
          // Cache: {"k1":1 (0), "k2":2 (1), "k3":3 (2)}
          // LRU order (seq): k1, k2, k3

          _ <- cache.get("key1") // Access key1, making it MRU. seq becomes 3
          // Cache: {"k1":1 (3), "k2":2 (1), "k3":3 (2)}
          // LRU order (seq): k2, k3, k1

          _ <- cache.put("key4", 4) // This should evict "key2" (oldest seq 1)
          // Cache should now be: {"k1":1 (3), "k3":3 (2), "k4":4 (4)}

          getKey1 <- cache.get("key1")
          getKey2 <- cache.get("key2") // Should be evicted
          getKey3 <- cache.get("key3")
          getKey4 <- cache.get("key4")
        } yield (getKey1 shouldBe Some(1)) ~&>
          (getKey2 shouldBe None) ~&>
          (getKey3 shouldBe Some(3)) ~&>
          (getKey4 shouldBe Some(4))
      }
    }

    "C3: get operation should update the LRU status of an item (making it MRU)" in {
      val capacity = 2
      createCacheWithCapacity[String, Int](capacity).use { cache =>
        for {
          _ <- cache.put("keyA", 10) // seq 0
          _ <- cache.put("keyB", 20) // seq 1
          // Cache: {"keyA":10 (0), "keyB":20 (1)}
          // LRU order (seq): keyA, keyB

          _ <- cache.get("keyA") // Access "keyA", its seq counter should update (to 2)
          // Cache: {"keyA":10 (2), "keyB":20 (1)}
          // LRU order (seq): keyB, keyA

          _ <- cache.put("keyC", 30) // This should evict "keyB" (oldest seq 1)
          // Cache: {"keyA":10 (2), "keyC":30 (3)}

          getA <- cache.get("keyA")
          getB <- cache.get("keyB") // Should be evicted
          getC <- cache.get("keyC")
        } yield (getA shouldBe Some(10)) ~&>
          (getB shouldBe None) ~&>
          (getC shouldBe Some(30))
      }
    }

    "Edge case: capacity 1" - {
      "should correctly handle capacity of 1" in
        createCacheWithCapacity[String, Int](capacity = 1).use { cache =>
          for {
            _ <- cache.put("one", 1)
            get1 <- cache.get("one")
            _ <- cache.put("two", 2) // "one" should be evicted
            getOneAfterEvict <- cache.get("one")
            getTwo <- cache.get("two")
          } yield (get1 shouldBe Some(1)) ~&>
            (getOneAfterEvict shouldBe None) ~&>
            (getTwo shouldBe Some(2))
        }

      "get should refresh item in capacity 1 cache before next put" in
        createCacheWithCapacity[String, Int](capacity = 1).use { cache =>
          for {
            _ <- cache.put("one", 1)
            _ <- cache.get("one")    // Refresh "one"
            _ <- cache.put("two", 2) // "one" should still be evicted as get doesn't change capacity logic, only LRU order
            getOneAfterEvict <- cache.get("one")
            getTwo <- cache.get("two")
          } yield (getOneAfterEvict shouldBe None) ~&> // Eviction is based on capacity limit first
            (getTwo shouldBe Some(2))
        }
    }

    "Overwriting a key should not evict another key if capacity is not exceeded" in
      createCacheWithCapacity[String, Int](capacity = 2).use { cache =>
        for {
          _ <- cache.put("k1", 1)
          _ <- cache.put("k2", 2)
          // Cache: k1, k2
          _ <- cache.put("k1", 11) // Overwrite k1
          // Cache should still be: k1 (new value), k2. No eviction.
          getK1 <- cache.get("k1")
          getK2 <- cache.get("k2")
        } yield (getK1 shouldBe Some(11)) ~&>
          (getK2 shouldBe Some(2))
      }

    "Overwriting a key after a get should not evict another key if capacity is not exceeded" in
      createCacheWithCapacity[String, Int](capacity = 2).use { cache =>
        for {
          _ <- cache.put("k1", 1) // k1 (seq 0)
          _ <- cache.put("k2", 2) // k2 (seq 1)
          // Cache: k1 (seq 0, LRU), k2 (seq 1, MRU)

          _ <- cache.get("k1") // Access k1. k1 becomes MRU (seq 2). k2 (seq 1) is now LRU.
          // Cache: k1 (seq 2, MRU), k2 (seq 1, LRU)

          _ <- cache.put("k1", 11) // Overwrite k1. k1 becomes MRU again (seq 3). k2 (seq 1) remains LRU.
          // Cache should still be: k1 (new value, seq 3), k2 (seq 1). No eviction.

          getK1 <- cache.get("k1")
          getK2 <- cache.get("k2")
        } yield (getK1 shouldBe Some(11)) ~&>
          (getK2 shouldBe Some(2))
      }
  }
