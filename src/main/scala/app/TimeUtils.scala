package app

import cats.effect.*
import cats.syntax.all.*

import java.time.Instant

object TimeUtils:
  def nowInstant[F[_]: Async as async]: F[Instant] =
    async.realTime.map(d => Instant.EPOCH.plusNanos(d.toNanos))
