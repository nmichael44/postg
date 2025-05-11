package app.memcachetests

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO
import cats.implicits.*

import java.time.Instant
import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.memcachetests.TestUtils.*

final class MemCacheInternalsTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  "MemCache: Internal State Verification" - {
    "should reflect correct internal state after puts and gets" in {
      val cacheCapacity = 3
      val (k1, v1) = ("key1", 100)
      val (k2, v2) = ("key2", 200)
      val (k3, v3) = ("key3", 300)
      val k2ExpiryDuration = java.time.Duration.ofSeconds(3600) // 1 hour

      createCache[String, Int](capacity = cacheCapacity).use { cache =>
        for {
          nowBeforePuts <- IO.realTimeInstant

          _ <- cache.put(k1, v1) // seqCounter becomes 1 (k1 -> seq 0)
          _ <- cache.put(k2, v2, k2ExpiryDuration) // seqCounter becomes 2 (k2 -> seq 1)
          _ <- cache.put(k3, v3) // seqCounter becomes 3 (k3 -> seq 2)
          _ <- cache.get(k1) // k1 accessed, its seq should update to 3. seqCounter becomes 4.

          internalState <- cache.getInternalCacheState()
          (mainMap, expirySet, lruMap, currentSeqCounter) = internalState

          nowAfterOps <- IO.realTimeInstant
        } yield
          // --- Assertions on mainMap ---
          (mainMap.size shouldBe 3) ~&>
            (mainMap.contains(k1) shouldBe true) ~&>
            (mainMap(k1).v shouldBe v1) ~&>
            (mainMap(k1).expiryOpt shouldBe None) ~&>
            (mainMap(k1).seqCount shouldBe 3) ~&>
            (mainMap.contains(k2) shouldBe true) ~&>
            (mainMap(k2).v shouldBe v2) ~&>
            (mainMap(k2).expiryOpt.isDefined shouldBe true) ~&> {
              val k2ExpectedExpiry = nowBeforePuts.plus(k2ExpiryDuration)
              (mainMap(k2).expiryOpt.get.getEpochSecond shouldBe k2ExpectedExpiry.getEpochSecond) ~&>
                (mainMap(k2).seqCount shouldBe 1)
            } ~&> {
              (mainMap.contains(k3) shouldBe true) ~&>
                (mainMap(k3).v shouldBe v3) ~&>
                (mainMap(k3).expiryOpt shouldBe None) ~&>
                (mainMap(k3).seqCount shouldBe 2)
            } ~&>
            // --- Assertions on expirySet ---
            {
              (expirySet.size shouldBe 1) ~&>
                (expirySet.nonEmpty shouldBe true) ~&>
                (expirySet.headOption.map(_._2) shouldBe Some(k2)) ~&>
                (hasExpired(expirySet.head._1, nowAfterOps) shouldBe false)
            } ~&>
            // --- Assertions on lruMap ---
            {
              (lruMap.size shouldBe 3) ~&> {
                val lruOrder = lruMap.toList.sortBy(_._1).map(_._2)
                lruOrder shouldBe List(k2, k3, k1)
              }
            } ~&>
            // --- Assertion on seqCounter ---
            (currentSeqCounter shouldBe 4) ~&>
            succeed
      }
    }

    "should reflect correct internal state after LRU eviction due to capacity" in {
      val cacheCapacity = 2
      val (k1, v1) = ("key1", 100) // Will be put first, seq 0
      val (k2, v2) = ("key2", 200) // Will be put second, seq 1
      val (k3, v3) = ("key3", 300) // Will be put third, seq 2, causing eviction

      createCache[String, Int](capacity = cacheCapacity).use { cache =>
        for {
          _ <- cache.put(k1, v1) // k1 -> seq 0
          _ <- cache.put(k2, v2) // k2 -> seq 1, globalSeq -> 2
          // Cache: k1 (0), k2 (1). LRU: k1
          _ <- cache.put(k3, v3) // k3 -> seq 2, globalSeq -> 3. k1 should be evicted.
          // Cache: k2 (1), k3 (2). LRU: k2

          internalState <- cache.getInternalCacheState()
          (mainMap, expirySet, lruMap, currentSeqCounter) = internalState
        } yield
          // --- Assertions on mainMap ---
          (mainMap.size shouldBe cacheCapacity) ~&> // Should be 2
            (mainMap.contains(k1) shouldBe false) ~&> // k1 evicted
            (mainMap.contains(k2) shouldBe true) ~&>
            (mainMap(k2).v shouldBe v2) ~&>
            (mainMap(k2).seqCount shouldBe 1) ~&> // k2's original seqCount
            (mainMap.contains(k3) shouldBe true) ~&>
            (mainMap(k3).v shouldBe v3) ~&>
            (mainMap(k3).seqCount shouldBe 2) ~&> // k3's seqCount
            // --- Assertions on expirySet ---
            (expirySet.isEmpty shouldBe true) ~&> // No items had expiry
            // --- Assertions on lruMap ---
            // Expected LRU order (least to most recent seqCount): k2 (1), k3 (2)
            (lruMap.size shouldBe cacheCapacity) ~&> { // Should be 2
              val lruOrder = lruMap.toList.sortBy(_._1).map(_._2)
              lruOrder shouldBe List(k2, k3)
            } ~&>
            // --- Assertion on seqCounter ---
            // put k1 (0->1), put k2 (1->2), put k3 (2->3)
            (currentSeqCounter shouldBe 3) ~&>
            succeed
      }
    }

    "should reflect correct internal state after background worker cleanup" in {
      val cacheCapacity = 5
      val cleanupInterval = 100.millis // Short cleanup for test

      val (kExp1, vExp1) = ("expKey1", 1)
      val exp1Duration = java.time.Duration.ofMillis(20) // Expires quickly

      val (kExp2, vExp2) = ("expKey2", 2)
      val exp2Duration = java.time.Duration.ofMillis(50) // Expires quickly

      val (kNoExp, vNoExp) = ("noExpiryKey", 3)

      val (kLongExp, vLongExp) = ("longExpiryKey", 4)
      val longExpDuration = java.time.Duration.ofMillis(500) // Expires after worker run

      createCache[String, Int](capacity = cacheCapacity, cleanupDuration = cleanupInterval).use { cache =>
        for {
          _ <- cache.put(kExp1, vExp1, exp1Duration) // seq 0
          _ <- cache.put(kNoExp, vNoExp) // seq 1
          _ <- cache.put(kExp2, vExp2, exp2Duration) // seq 2
          _ <- cache.put(kLongExp, vLongExp, longExpDuration) // seq 3
          // global seqCounter is now 4

          // Sleep longer than cleanupInterval to ensure worker runs
          _ <- IO.sleep(cleanupInterval + 75.millis) // e.g., 175ms sleep

          internalState <- cache.getInternalCacheState()
          (mainMap, expirySet, lruMap, currentSeqCounter) = internalState

          nowForExpiryCheck <- IO.realTimeInstant

        } yield
          // --- Assertions on mainMap ---
          // kExp1 and kExp2 should be cleaned by the worker
          (mainMap.size shouldBe 2) ~&>
            (mainMap.contains(kExp1) shouldBe false) ~&>
            (mainMap.contains(kExp2) shouldBe false) ~&>
            (mainMap.contains(kNoExp) shouldBe true) ~&>
            (mainMap(kNoExp).v shouldBe vNoExp) ~&>
            (mainMap(kNoExp).seqCount shouldBe 1) ~&> // Original seqCount
            (mainMap.contains(kLongExp) shouldBe true) ~&>
            (mainMap(kLongExp).v shouldBe vLongExp) ~&>
            (mainMap(kLongExp).seqCount shouldBe 3) ~&> // Original seqCount
            // --- Assertions on expirySet ---
            // Only kLongExp should remain in expirySet
            (expirySet.size shouldBe 1) ~&>
            (expirySet.headOption.map(_._2) shouldBe Some(kLongExp)) ~&>
            // Verify kLongExp is not yet expired by our current time
            (hasExpired(expirySet.head._1, nowForExpiryCheck) shouldBe false) ~&>
            // --- Assertions on lruMap ---
            // Should contain kNoExp and kLongExp
            (lruMap.size shouldBe 2) ~&> {
              val lruOrderKeys = lruMap.toList.sortBy(_._1).map(_._2)
              lruOrderKeys should contain theSameElementsAs List(kNoExp, kLongExp)
              // Check specific order if important: kNoExp (seq 1), kLongExp (seq 3)
              lruOrderKeys shouldBe List(kNoExp, kLongExp)
            } ~&>
            // --- Assertion on seqCounter ---
            // Worker cleanup does not change the main seqCounter
            (currentSeqCounter shouldBe 4) ~&>
            succeed
      }
    }

    "should maintain consistency between mainMap and lruMap sizes after various operations" in {
      val cacheCapacity = 3
      val (k1, v1) = ("key1", 100)
      val (k2, v2) = ("key2", 200)
      val (k3, v3) = ("key3", 300)
      val (k4, v4) = ("key4", 400)
      val expirySoon = java.time.Duration.ofMillis(50)
      val expiryLater = java.time.Duration.ofSeconds(3600)

      createCache[String, Int](capacity = cacheCapacity).use { cache =>
        for {
          _ <- cache.put(k1, v1)
          _ <- cache.put(k2, v2, expirySoon)
          _ <- cache.put(k3, v3, expiryLater)
          _ <- cache.get(k1)
          _ <- cache.put(k4, v4)
          _ <- IO.sleep(100.millis)
          internalState <- cache.getInternalCacheState()
          (mainMap, _, lruMap, _) = internalState
        } yield (mainMap.size shouldBe lruMap.size) ~&>
          (mainMap.keySet shouldBe lruMap.values.toSet) ~&>
          succeed
      }
    }

    "should maintain correct state with large capacity after worker cleanup" in {
      val cacheCapacity = 100
      val cleanupInterval = 100.millis
      val shortExpiry = java.time.Duration.ofMillis(50)
      val longExpiry = java.time.Duration.ofSeconds(3600)

      // Generate test data
      val (n1, n2, n3) = (40, 30, 20) // Number of items for each category
      val (m1, m2) = (10, 5) // Number of items to 'get' for LRU update

      val shortExpiryPairs = (1 to n1).map(i => (s"Short$i", i)).toVector
      val longExpiryPairs = (1 to n2).map(i => (s"Long$i", i * 100)).toVector
      val noExpiryPairs = (1 to n3).map(i => (s"NoExp$i", i * 10_000)).toVector

      createCache[String, Int](capacity = cacheCapacity, cleanupDuration = cleanupInterval).use { cache =>
        for {
          // Insert items with different expiry times.
          _ <- shortExpiryPairs.traverse { case (k, v) => cache.put(k, v, shortExpiry) }
          _ <- longExpiryPairs.traverse { case (k, v) => cache.put(k, v, longExpiry) }
          _ <- noExpiryPairs.traverse { case (k, v) => cache.put(k, v) }

          // Access some items to update their LRU status.
          _ <- longExpiryPairs.take(m1).traverse { case (k, _) => cache.get(k) }
          _ <- noExpiryPairs.take(m2).traverse { case (k, _) => cache.get(k) }

          // Wait for the cleanup worker to run.
          _ <- IO.sleep(cleanupInterval + 50.millis)

          internalState <- cache.getInternalCacheState()
          (mainMap, expirySet, lruMap, currentSeqCounter) = internalState
        } yield
          // Short expiry items should be cleaned up
          (shortExpiryPairs.forall(p => !mainMap.contains(p._1)) shouldBe true) ~&>
            // Long expiry and no expiry items should remain
            (longExpiryPairs.forall(p => mainMap.contains(p._1)) shouldBe true) ~&>
            (noExpiryPairs.forall(p => mainMap.contains(p._1)) shouldBe true) ~&>
            // Maps should be consistent
            (mainMap.size shouldBe (n2 + n3)) ~&> // Expected remaining items
            (lruMap.size shouldBe (n2 + n3)) ~&>
            (mainMap.keySet shouldBe lruMap.values.toSet) ~&>
            // Expiry set should only contain long expiry items
            (expirySet.size shouldBe longExpiryPairs.length) ~&>
            (currentSeqCounter shouldBe (n1 + n2 + n3 + m1 + m2))
          succeed
      }
    }

    "should maintain correct state with large capacity and parallel operations after worker cleanup" in {
      val cacheCapacity = 100
      val cleanupInterval = 100.millis
      val shortExpiry = java.time.Duration.ofMillis(50)
      val longExpiry = java.time.Duration.ofSeconds(3600)

      // Generate test data
      val (n1, n2, n3) = (40, 30, 20) // Number of items for each category
      val (m1, m2) = (10, 5) // Number of items to 'get' for LRU update

      val shortExpiryPairs = (1 to n1).map(i => (s"ShortPar$i", i)).toVector // Changed key prefix for uniqueness
      val longExpiryPairs = (1 to n2).map(i => (s"LongPar$i", i * 100)).toVector
      val noExpiryPairs = (1 to n3).map(i => (s"NoExpPar$i", i * 10_000)).toVector

      createCache[String, Int](capacity = cacheCapacity, cleanupDuration = cleanupInterval).use { cache =>
        for {
          // Insert items with different expiry times in parallel.
          _ <- shortExpiryPairs.parTraverse_ { case (k, v) => cache.put(k, v, shortExpiry) }
          _ <- longExpiryPairs.parTraverse_ { case (k, v) => cache.put(k, v, longExpiry) }
          _ <- noExpiryPairs.parTraverse_ { case (k, v) => cache.put(k, v) }

          // Access some items to update their LRU status in parallel.
          _ <- longExpiryPairs.take(m1).parTraverse_ { case (k, _) => cache.get(k) }
          _ <- noExpiryPairs.take(m2).parTraverse_ { case (k, _) => cache.get(k) }

          // Wait for the cleanup worker to run.
          _ <- IO.sleep(cleanupInterval + 50.millis)

          internalState <- cache.getInternalCacheState()
          (mainMap, expirySet, lruMap, currentSeqCounter) = internalState
        } yield
          // Short expiry items should be cleaned up
          (shortExpiryPairs.forall(p => !mainMap.contains(p._1)) shouldBe true) ~&>
            // Long expiry and no expiry items should remain
            (longExpiryPairs.forall(p => mainMap.contains(p._1)) shouldBe true) ~&>
            (noExpiryPairs.forall(p => mainMap.contains(p._1)) shouldBe true) ~&>
            // Check sizes explicitly
            (mainMap.size shouldBe (n2 + n3)) ~&> // Expected remaining items
            (lruMap.size shouldBe (n2 + n3)) ~&>
            // Maps should be consistent
            (mainMap.keySet shouldBe lruMap.values.toSet) ~&>
            // Expiry set should only contain long expiry items
            (expirySet.size shouldBe longExpiryPairs.length) ~&>
            // Assertion on seqCounter
            (currentSeqCounter shouldBe (n1 + n2 + n3 + m1 + m2)) ~&>
            succeed
      }
    }
  }
}
