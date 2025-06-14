package app

import cats.effect.{IO, IOLocal}

trait FLocal[F[_], A] {
  def get: F[A]
  def set(a: A): F[Unit]
}

object FLocal {
  def ioLocal[A](initial: A): IO[FLocal[IO, A]] =
    IOLocal(initial).map { local =>
      new FLocal[IO, A] {
        def get: IO[A] = local.get
        def set(a: A): IO[Unit] = local.set(a)
      }
    }
}
