package app.services

import cats.effect.std.Queue
import cats.effect.Ref

import app.HttpWorker

trait ServerState[F[_]]:
  val movieRequestCounts: Ref[F, Map[Long, Int]]
  val jobQueue: Queue[F, HttpWorker.Job[F]]
