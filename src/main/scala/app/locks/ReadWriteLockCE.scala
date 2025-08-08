package app.locks

import cats.effect.{Async, Deferred, Resource}
import cats.effect.Ref
import cats.implicits.*

import scala.collection.immutable.Queue as ScalaQueue

import app.locks.ReadWriteLock

object ReadWriteLockCE:
  private enum Request[F[_]]:
    case Read(d: Deferred[F, Unit]) extends Request[F]
    case Write(d: Deferred[F, Unit]) extends Request[F]

    def getReaderDeferred: Deferred[F, Unit] =
      this match {
        case Read(d) => d
        case Write(_) => throw AssertionError("Should never happen. Found a Write when a Read was expected.")
      }
    end getReaderDeferred

    def getWriterDeferred: Deferred[F, Unit] =
      this match {
        case Read(_) => throw AssertionError("Should never happen. Found a Read when a Write was expected.")
        case Write(d) => d
      }
    end getWriterDeferred

    def isRead: Boolean =
      this.isInstanceOf[Read[?]]
    end isRead
  end Request

  private case class State[F[_]](
      activeReaders: Int,
      activeWriter: Boolean,
      waitingWriters: Int,
      waiting: ScalaQueue[Request[F]],
  )
  end State

  private object State:
    def empty[F[_]]: State[F] = State(0, false, 0, ScalaQueue.empty)
  end State

  def create[F[_]: Async as async]: F[ReadWriteLock[F]] =
    Ref.of[F, State[F]](State.empty).map { state =>
      new ReadWriteLock[F] {
        private val unitF: F[Unit] = async.unit

        private val acquireRead: F[Unit] =
          Deferred[F, Unit] >>= { d =>
            state.modify {
              case s @ State(activeReaders, false, 0, _) => (s.copy(activeReaders = activeReaders + 1), unitF)
              case s => (s.copy(waiting = s.waiting.enqueue(Request.Read(d))), d.get)
            }.flatten
          }
        end acquireRead

        private val releaseRead: F[Unit] =
          state.modify {
            case State(1, _, waitingWriters, waiting) =>
              waiting.dequeueOption match {
                case None => (State.empty, unitF)
                // w must be a writer
                case Some((w, rest)) => (State(0, true, waitingWriters - 1, rest), w.getWriterDeferred.complete(()).void)
              }
            case s @ State(activeReaders, _, _, _) => (s.copy(activeReaders = activeReaders - 1), unitF)
          }.flatten
        end releaseRead

        private val acquireWrite: F[Unit] =
          Deferred[F, Unit] >>= { (d: Deferred[F, Unit]) =>
            state.modify {
              case s @ State(0, false, _, _) => (s.copy(activeWriter = true), unitF)
              case s => (s.copy(waitingWriters = s.waitingWriters + 1, waiting = s.waiting.enqueue(Request.Write(d))), d.get)
            }.flatten
          }
        end acquireWrite

        private val releaseWrite: F[Unit] =
          state.modify { case State(_, _, waitingWriters, waiting) =>
            waiting.dequeueOption match {
              case None => (State.empty, unitF)
              case Some((Request.Write(d), rest)) => (State(0, true, waitingWriters - 1, rest), d.complete(()).void)
              case Some(_) =>
                val readers = waiting.view.takeWhile(_.isRead).toVector
                val numActiveReaders = readers.length
                val rest = waiting.drop(numActiveReaders)
                val wakeReaders: F[Unit] = readers.traverseVoid(_.getReaderDeferred.complete(()))
                (State(numActiveReaders, false, waitingWriters, rest), wakeReaders)
            }
          }.flatten
        end releaseWrite

        val read: Resource[F, Unit] = Resource.make(acquireRead)(_ => releaseRead)
        val write: Resource[F, Unit] = Resource.make(acquireWrite)(_ => releaseWrite)
      }
    }
  end create
end ReadWriteLockCE
