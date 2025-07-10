package app

import cats.effect.{Async, Resource}
import cats.effect.std.Queue
import cats.implicits.*

import java.util.{SplittableRandom, UUID}
import java.util.random.RandomGenerator

import app.UUIDGenerator.{makeV4UUID, RandomnessSource}

final class UUIDGenerator[F[_]: Async] private (queue: Queue[F, RandomnessSource[F]]):
  private val withItemFromQueue: Resource[F, RandomnessSource[F]] =
    Resource.make(queue.take)(queue.offer)

  private val generateUUID: F[UUID] = withItemFromQueue.use { rndSrc =>
    for {
      msb <- rndSrc.nextLong()
      lsb <- rndSrc.nextLong()
    } yield makeV4UUID(msb, lsb)
  }

  val generateUUIDAsString: F[String] = generateUUID.map(_.toString)

object UUIDGenerator:
  private final class RandomnessSource[F[_]: Async as async](rng: RandomGenerator):
    def nextLong(): F[Long] = async.delay(rng.nextLong())

  private def makeV4UUID(msb: Long, lsb: Long): UUID = UUID(
    (msb & 0xffffffffffff0fffL) | 0x0000000000040000L,
    (lsb & 0x3fffffffffffffffL) | 0x8000000000000000L,
  )

  // How many random number generators are available for use.  If that there more than
  // LevelOfParallelism requests at the same time, the next fiber will wait until one
  // becomes available.
  inline private val LevelOfParallelism = 4

  private def populateQueue[F[_]: Async as async](queue: Queue[F, RandomnessSource[F]], seedOpt: Option[Long]): F[Unit] =
    for {
      nanos <- seedOpt.map(async.pure).getOrElse(async.monotonic.map(_.toNanos))
      masterRng = SplittableRandom(nanos)
      _ <- queue.offer(RandomnessSource[F](masterRng))
      _ <- async.replicateA_(LevelOfParallelism - 1, async.defer(queue.offer(RandomnessSource[F](masterRng.split()))))
    } yield ()

  private def createImpl[F[_]: Async as async](seedOpt: Option[Long]): Resource[F, UUIDGenerator[F]] =
    Resource.eval {
      Queue.bounded[F, RandomnessSource[F]](LevelOfParallelism) >>= (queue =>
        populateQueue(queue, seedOpt) *> async.pure(UUIDGenerator[F](queue))
      )
    }

  def create[F[_]: Async]: Resource[F, UUIDGenerator[F]] =
    createImpl(None)

  def create[F[_]: Async](seed: Long): Resource[F, UUIDGenerator[F]] =
    createImpl(Some(seed))
