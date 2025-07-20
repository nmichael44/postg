package app.locks

import cats.effect.Resource

trait ReadWriteLock[F[_]]:
  def read: Resource[F, Unit]
  def write: Resource[F, Unit]
