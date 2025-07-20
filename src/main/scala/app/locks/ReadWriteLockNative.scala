package app.locks

import cats.effect.{Async, Resource}
import cats.implicits.*

import java.util.concurrent.locks.ReentrantReadWriteLock as NativeReentrantReadWriteLock

import app.locks.ReadWriteLock

object ReadWriteLockNative:
  def create[F[_]: Async as async]: F[ReadWriteLock[F]] = {
    val fair = true
    async.delay(NativeReentrantReadWriteLock(fair)).map { nativeLock =>
      new ReadWriteLock[F] {
        private val readLock = nativeLock.readLock()
        private val writeLock = nativeLock.writeLock()

        private val acquireRead: F[Unit] = async.blocking(readLock.lock())

        private val releaseRead: F[Unit] = async.blocking(readLock.unlock())

        private val acquireWrite: F[Unit] = async.blocking(writeLock.lock())

        private val releaseWrite: F[Unit] = async.blocking(writeLock.unlock())

        val read: Resource[F, Unit] = Resource.make(acquireRead)(_ => releaseRead)
        val write: Resource[F, Unit] = Resource.make(acquireWrite)(_ => releaseWrite)
      }
    }
  }
