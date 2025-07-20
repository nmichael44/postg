package app.serviceslive

import cats.syntax.all.*
import cats.Functor

import app.services.ServerState
import app.services.ServerStateUpdateService

private final class ServerStateUpdateServiceLive[F[_]: Functor] private (
    serverState: ServerState[F],
) extends ServerStateUpdateService[F]:
  def incrementAndGet(movieId: Long): F[Int] =
    serverState.movieRequestCounts.modify { counts =>
      val newCounts = counts.updatedWith(movieId)(_.fold(1)(_ + 1).some)
      (newCounts, newCounts(movieId))
    }
  end incrementAndGet

  def get(movieId: Long): F[Option[Int]] =
    getAllCounts.map(_.get(movieId))
  end get

  def getAllCounts: F[Map[Long, Int]] =
    serverState.movieRequestCounts.get
  end getAllCounts
end ServerStateUpdateServiceLive

object ServerStateUpdateServiceLive:
  def create[F[_]: Functor](serverState: ServerState[F]): ServerStateUpdateService[F] =
    ServerStateUpdateServiceLive[F](serverState)
  end create
end ServerStateUpdateServiceLive
