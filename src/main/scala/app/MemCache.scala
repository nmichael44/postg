package app

import cats.{FlatMap, Functor}
import cats.effect.{ExitCode, Temporal}
import cats.effect.kernel.{Fiber, Ref}
import cats.effect.syntax.all.*
import cats.implicits.*

import java.time.Instant
import scala.collection.immutable.TreeMap
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import org.typelevel.log4cats.Logger

final class MemCache[F[_]: { Temporal, Logger }, K: Ordering, V](
    r: Ref[F, TreeMap[K, (V, Option[Instant])]],
    cleanupFiber: Fiber[F, Throwable, Nothing],
):
  def get(k: K): F[Option[V]] =
    Temporal[F].realTimeInstant.flatMap { now =>
      r.get.map(m =>
        m.get(k) match {
          case Some(v, Some(expiry)) => Option.when(now.isBefore(expiry))(v)
          case Some(v, None) => v.some
          case _ => None
        },
      )
    }

  def put(k: K, v: V): F[Unit] =
    putAux(k, v, None)

  def put(k: K, v: V, duration: java.time.Duration): F[Unit] =
    putAux(k, v, duration.some)

  private def putAux(k: K, v: V, durationOpt: Option[java.time.Duration]): F[Unit] =
    Temporal[F].realTimeInstant.flatMap { now =>
      r.modify(m => (m.updated(k, (v, durationOpt.map(now.plus))), ()))
    }

object MemCache:
  def create[F[_]: { Temporal, Logger }, K: Ordering, V]: F[MemCache[F, K, V]] = {
    val logger = Logger[F]

    for {
      r <- Ref.of(TreeMap.empty[K, (V, Option[Instant])])
      cleanupFiber <- startWorker(r, logger)
    } yield new MemCache(r, cleanupFiber)
  }

  private val CleanupInterval: Duration = 1.minutes

  private def getSize[F[_]: Functor, K, V](r: Ref[F, TreeMap[K, (V, Option[Instant])]]): F[Int] =
    r.get.map(_.size)

  private def reportSize[F[_]: FlatMap, K, V](
      r: Ref[F, TreeMap[K, (V, Option[Instant])]],
      when: String,
      logger: Logger[F],
  ): F[Unit] =
    getSize(r).>>=(siz => logger.info(s"Size of cache $when worker touched it: $siz"))

  private def worker[F[_]: Temporal, K: Ordering, V](
      r: Ref[F, TreeMap[K, (V, Option[Instant])]],
      logger: Logger[F],
  ): F[Nothing] =
    val temporal = Temporal[F]

    (for {
      _ <- logger.info("Cleanup worker going to sleep until it's time to work...")
      _ <- temporal.sleep(CleanupInterval)
      _ <- logger.info("Cleanup worker is awake and going to work...")
      _ <- reportSize(r, "before", logger)
      now <- temporal.realTimeInstant
      _ <- r.update { m =>
        m.filter {
          case (_, (_, Some(expiry))) => now.isBefore(expiry)
          case _ => true
        }
      }
      _ <- reportSize(r, "after", logger)
    } yield ()).foreverM

  private def startWorker[F[_]: Temporal, K: Ordering, V](
      r: Ref[F, TreeMap[K, (V, Option[Instant])]],
      logger: Logger[F],
  ): F[Fiber[F, Throwable, Nothing]] = for {
    _ <- logger.info("Starting mem cache worker...")
    cleanupFiber <- worker(r, logger).start
    _ <- logger.info("Worker is running...")
    _ <- logger.info(s"Fiber is '${cleanupFiber.toString}'.")
  } yield cleanupFiber

  def run[F[_]: { Temporal, Logger }]: F[ExitCode] = {
    val logger = Logger[F]
    val temporal = Temporal[F]
    for {
      cache <- MemCache.create[F, String, Int]
      _ <- logger.info("Putting key 'a' with no timeout.")
      _ <- cache.put("a", 1)
      _ <- logger.info("Getting key 'a' immediately.")
      getA1 <- cache.get("a")
      _ <- logger.info(s"Result for 'a' after put (no timeout): $getA1") // Should be Some(1)

      _ <- logger.info("Putting key 'c' with no timeout.")
      _ <- cache.put("c", 3)

      timeoutDuration = java.time.Duration.ofSeconds(2)
      _ <- logger.info(s"Putting key 'b' with timeout of $timeoutDuration.")
      _ <- cache.put("b", 2, timeoutDuration)

      _ <- logger.info("Getting key 'b' immediately after put.")
      getB1 <- cache.get("b")
      _ <- logger.info(s"Result for 'b' immediately after put: $getB1") // Should be Some(2)

      _ <- logger.info("Getting key 'c' again.")
      getC1 <- cache.get("c")
      _ <- logger.info(s"Result for 'c' (no timeout): $getC1") // Should be Some(3)

      sleepDuration = 11.seconds // Sleep longer than the 2s timeout for 'b'
      _ <- logger.info(s"Sleeping for $sleepDuration to let 'b' expire...")
      _ <- temporal.sleep(sleepDuration) // Use Temporal[IO].sleep with java.time.Duration

      _ <- logger.info("Getting key 'b' after sleep (should be expired).")
      getB2 <- cache.get("b")
      _ <- logger.info(s"Result for 'b' after sleep: $getB2") // Should be None

      _ <- logger.info("Getting key 'a' again (should still be present).")
      getA2 <- cache.get("a")
      _ <- logger.info(s"Result for 'a' second time: $getA2") // Should still be Some(1)

      _ <- logger.info("Getting key 'c' again (should still be present).")
      getC2 <- cache.get("c")
      _ <- logger.info(s"Result for 'c' second time: $getC2") // Should still be Some(3)

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
