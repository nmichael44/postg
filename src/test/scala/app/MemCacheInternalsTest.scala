package app

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO

import java.time.Instant
import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import TestUtils.*

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
  }
}
