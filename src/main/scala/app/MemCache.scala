package app

import cats.{FlatMap, Functor}
import cats.effect.{ExitCode, Temporal}
import cats.effect.kernel.{Fiber, Ref, Resource}
import cats.effect.syntax.all.*
import cats.implicits.*

import java.time.Instant
import scala.collection.immutable.{TreeMap, TreeSet}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import app.MemCache.{hasExpired, CacheElem, CacheState}
import org.typelevel.log4cats.Logger

final class MemCache[F[_]: { Temporal, Logger as logger }, K: Ordering, V] private (
    memCacheName: String,
    capacity: Int,
    r: Ref[F, CacheState[K, V]],
    cleanupFiber: Fiber[F, Throwable, Nothing],
):
  def get(k: K): F[Option[V]] =
    (Temporal[F].realTimeInstant) >>= { now =>
      r.modify { case cacheState0 @ CacheState(m0, s0, lruMap0, seqCounter0) =>
        m0.get(k) match {
          case Some(CacheElem(v, expiryOpt, seqCount)) =>
            expiryOpt match {
              // Item has an expiry, AND it is currently expired.
              case Some(expiry) if hasExpired(expiry, now) =>
                (cacheState0, None)
              // This case covers two cases:
              //   1. Item has an expiry, AND it is NOT currently expired.
              //   2. Item has NO expiry (expiryOpt is None).
              case _ =>
                val m1 = m0.updated(k, CacheElem(v, expiryOpt, seqCounter0))
                val s1 = s0
                val lruMap1 = (lruMap0 - seqCount).updated(seqCounter0, k)
                val seqCounter1 = seqCounter0 + 1
                (CacheState(m1, s1, lruMap1, seqCounter1), v.some)
            }
          case None =>
            (cacheState0, None)
        }
      }
    }

  def put(k: K, v: V): F[Unit] =
    putAux(k, v, None)

  def put(k: K, v: V, duration: java.time.Duration): F[Unit] =
    putAux(k, v, duration.some)

  private def evictIfNecessary(m0: TreeMap[K, CacheElem[V]], s0: TreeSet[(Instant, K)], lru0: TreeMap[Long, K]) =
    if m0.size == capacity
    then
      val (_, minK) = lru0.min
      val CacheElem(_, expiryOpt, seqCounter) = m0(minK)
      val m1 = m0 - minK
      val s1 = expiryOpt.fold(s0)(expiry => s0 - ((expiry, minK)))
      val lru1 = lru0 - seqCounter

      (m1, s1, lru1)
    else (m0, s0, lru0)

  private def putAux(k: K, v: V, durationOpt: Option[java.time.Duration]): F[Unit] =
    Temporal[F].realTimeInstant >>= { now =>
      r.update { case CacheState(m, s, lruMap, seqCounter0) =>
        val existingEntryOpt: Option[CacheElem[V]] = m.get(k)

        val (m0, s0, lruMap0) = if existingEntryOpt.isDefined then (m, s, lruMap) else evictIfNecessary(m, s, lruMap)

        val newExpiryOpt: Option[Instant] = durationOpt.map(now.plus)
        val m1 = m0.updated(k, CacheElem(v, newExpiryOpt, seqCounter0))

        val s1Aux = existingEntryOpt.flatMap(_._2).fold(s0)(currExpiry => s0 - ((currExpiry, k)))
        val s1 = newExpiryOpt.fold(s1Aux)(newExpiry => s1Aux + ((newExpiry, k)))
        val lruMap1 = existingEntryOpt
          .fold(lruMap0) { case CacheElem(_, _, seqCount) => lruMap0 - seqCount }
          .updated(seqCounter0, k)
        val seqCounter1 = seqCounter0 + 1

        CacheState(m1, s1, lruMap1, seqCounter1)
      }
    }

  private def stopCleanupFiber(): F[Unit] =
    logger.info(s"Stopping mem cache '$memCacheName' worker...") *>
      cleanupFiber.cancel *>
      logger.info(s"Mem cache '$memCacheName' worker stopped.")

  // This function is to be used for testing only.
  def getInternalCacheState: F[(TreeMap[K, CacheElem[V]], TreeSet[(Instant, K)], TreeMap[Long, K], Long)] =
    r.get.map { case CacheState(m, s, lruMap, seqCounter) => (m, s, lruMap, seqCounter) }

object MemCache:
  private final case class CacheState[K, V](
      mainMap: TreeMap[K, CacheElem[V]],
      expirySet: TreeSet[(Instant, K)],
      lruMap: TreeMap[Long, K],
      seqCounter: Long,
  )

  // This is not private because we use it in unit tests.
  final case class CacheElem[V](
      v: V,
      expiryOpt: Option[Instant],
      seqCount: Long,
  )

  private def hasExpired(expiry: Instant, now: Instant): Boolean =
    !now.isBefore(expiry)

  private def create[F[_]: { Temporal, Logger as logger }, K: Ordering, V](
      memCacheName: String,
      capacity: Int,
      cleanupDuration: Duration,
  ): F[MemCache[F, K, V]] =
    for {
      r <- Ref.of(
        CacheState(TreeMap.empty[K, CacheElem[V]], TreeSet.empty[(Instant, K)], TreeMap.empty[Long, K], 0L),
      )
      cleanupFiber <- startWorker(memCacheName, r, cleanupDuration)
    } yield MemCache(memCacheName, capacity, r, cleanupFiber)

  def createResource[F[_]: { Temporal, Logger }, K: Ordering, V](
      memCacheName: String,
      capacity: Int,
      cleanupDuration: Duration,
  ): Resource[F, MemCache[F, K, V]] =
    assert(capacity > 0, "Capacity must be greater than 0.")
    Resource.make(create(memCacheName, capacity, cleanupDuration))(_.stopCleanupFiber())

  private def getSize[F[_]: Functor, K, V](r: Ref[F, CacheState[K, V]]): F[(Int, Int)] =
    r.get.map(cs => (cs.mainMap.size, cs.expirySet.size))

  private def reportSize[F[_]: { FlatMap, Logger as logger }, K, V](
      memCacheName: String,
      r: Ref[F, CacheState[K, V]],
      when: String,
  ): F[Unit] =
    getSize(r) >>= { (mSiz, tSiz) => logger.info(s"Sizes of cache '$memCacheName' $when worker touched it: ($mSiz, $tSiz).") }

  private def worker[F[_]: { Temporal as temporal, Logger as logger }, K: Ordering, V](
      memCacheName: String,
      r: Ref[F, CacheState[K, V]],
      cleanupInterval: Duration,
  ): F[Nothing] =
    (for {
      _ <- logger.info(s"Cleanup worker for '$memCacheName', going to sleep until it's time to work...")
      _ <- temporal.sleep(cleanupInterval)
      _ <- logger.info(s"Cleanup worker for '$memCacheName', is awake and going to work...")
      _ <- reportSize(memCacheName, r, "before")
      now <- temporal.realTimeInstant
      _ <- r.update { case CacheState(m0, s0, lruMap0, seqCounter0) =>
        val expiredEntries = s0.view.takeWhile((expiry, _) => hasExpired(expiry, now)).toVector
        val expiredKeys = expiredEntries.view.map(_._2).toVector
        val expiredSeqs = expiredKeys.view.map(m0(_)._3)

        val m1 = m0 -- expiredKeys
        val s1 = s0 -- expiredEntries
        val lruMap1 = lruMap0 -- expiredSeqs
        val seqCounter1 = seqCounter0

        CacheState(m1, s1, lruMap1, seqCounter1)
      }
      _ <- reportSize(memCacheName, r, "after")
    } yield ()).handleErrorWith { e =>
      // We don't go paranoid and start worrying about errors being thrown from the logger...
      logger.error(e)(
        s"MemCache cleanup worker for '$memCacheName', encountered an error during a cycle.  Worker will continue to run.",
      )
    }.foreverM

  private def startWorker[F[_]: { Temporal, Logger as logger }, K: Ordering, V](
      memCacheName: String,
      r: Ref[F, CacheState[K, V]],
      cleanupInterval: Duration,
  ): F[Fiber[F, Throwable, Nothing]] = for {
    _ <- logger.info(s"Starting mem cache worker for '$memCacheName'...")
    cleanupFiber <- worker(memCacheName, r, cleanupInterval).start
    _ <- logger.info(s"Worker started for '$memCacheName'.")
    _ <- logger.info(s"Fiber is '$cleanupFiber'.")
  } yield cleanupFiber

  def run[F[_]: { Temporal as temporal, Logger as logger }]: F[ExitCode] =
    MemCache.createResource[F, String, Int]("crazy mem cache", 4, 5.seconds).use { cache =>
      for {
        _ <- logger.info("Putting key 'a' with no timeout.")
        _ <- cache.put("a", 1)
        _ <- logger.info("Getting key 'a' immediately.")
        getA1 <- cache.get("a")
        _ <- logger.info(s"Result for 'a' after put (no timeout): $getA1") // Should be Some(1)

        _ <- logger.info("Putting key 'b' with no timeout.")
        _ <- cache.put("b", 2)

        timeoutDuration1 = java.time.Duration.ofSeconds(2)
        _ <- logger.info(s"Putting key 'c' with timeout of $timeoutDuration1.")
        _ <- cache.put("c", 3, timeoutDuration1)

        timeoutDuration2 = java.time.Duration.ofSeconds(3)
        _ <- logger.info(s"Putting key 'c' with timeout of $timeoutDuration2.")
        _ <- cache.put("d", 4, timeoutDuration2)

        _ <- logger.info("Getting key 'b' immediately after put.")
        getB1 <- cache.get("b")
        _ <- logger.info(s"Result for 'b' immediately after put: $getB1") // Should be Some(2)

        _ <- logger.info("Getting key 'c' again.")
        getC1 <- cache.get("c")
        _ <- logger.info(s"Result for 'c' (no timeout): $getC1") // Should be Some(3)

        _ <- logger.info("Getting key 'd' again.")
        getD1 <- cache.get("d")
        _ <- logger.info(s"Result for 'd' (no timeout): $getD1") // Should be Some(4)

        sleepDuration = 6.seconds // Sleep longer than the 2s timeout for 'b'
        _ <- logger.info(s"Sleeping for $sleepDuration to let 'c' expire...")
        _ <- temporal.sleep(sleepDuration) // Use Temporal[IO].sleep with java.time.Duration

        _ <- logger.info("Getting key 'c' after sleep (should be expired).")
        getC2 <- cache.get("c")
        _ <- logger.info(s"Result for 'b' after sleep: $getC2") // Should be None

        _ <- logger.info("Getting key 'd' after sleep (should be expired).")
        getD2 <- cache.get("d")
        _ <- logger.info(s"Result for 'd' after sleep: $getD2") // Should be None

        _ <- logger.info("Getting key 'a' again (should still be present).")
        getA2 <- cache.get("a")
        _ <- logger.info(s"Result for 'a' second time: $getA2") // Should still be Some(1)

        _ <- logger.info("Getting key 'b' again (should still be present).")
        getB2 <- cache.get("b")
        _ <- logger.info(s"Result for 'b' second time: $getB2") // Should still be Some(3)

        // Note: The background worker is running and will eventually clean up 'b'.
        // The get check confirms it's treated as expired immediately based on time.

        _ <- logger.info(
          "MemCache test finished. The background worker will continue until the app exits.",
        )
        _ <- temporal.sleep(11.seconds)
        // In a production scenario, using Resource would be better to ensure the
        // background fiber is cancelled when the cache is no longer needed.
        // IOApp.Simple handles the lifecycle of fibers started within 'run' implicitly.
      } yield ExitCode.Success
    }
