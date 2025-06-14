package app

import cats.effect.*
import cats.effect.kernel.Ref
import cats.syntax.all.*

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    MovieApp.run
