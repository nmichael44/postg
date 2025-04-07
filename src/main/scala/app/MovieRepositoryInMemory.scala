package app

class MovieRepositoryInMemory[F[_]] extends MovieRepository[F]:
  def getDirectorsDetails(
      firstName: Option[String],
      lastName: Option[String],
  ): F[Vector[MovieDbModel.Director]] = ???

  def getDirectorDetails(directorId: Long): F[Option[MovieDbModel.Director]] = ???

  def getActorDetails(actorId: Long): F[Option[MovieDbModel.Actor]] = ???

  def getMoviesByDirectorId(directorId: Long): F[Vector[MovieDbModel.Movie]] = ???

  def getMoviesByDirectorName(
      firstName: Option[String],
      lastName: Option[String],
  ): F[String] = ???
