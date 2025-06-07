package app

import cats.effect.*

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =
    MovieApp.run
