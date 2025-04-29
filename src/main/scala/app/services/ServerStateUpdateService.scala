package app.services

trait ServerStateUpdateService[F[_]]:
  /**
   * Increments the view/access count for a specific movie ID and returns the new count.
   *
   * @param movieId
   *   The ID of the movie.
   * @return
   *   An F containing the updated count for the movie.
   */
  def incrementAndGet(movieId: Long): F[Int]

  /**
   * Gets the current view/access count for a specific movie ID.
   *
   * @param movieId
   *   The ID of the movie.
   * @return
   *   An F containing the current count for the movie, or None if the movie has not been counted.
   */
  def get(movieId: Long): F[Option[Int]]

  /**
   * Gets the current view/access counts for all movies.
   *
   * @return
   *   An F containing a Map from movie ID to count.
   */
  def getAllCounts: F[Map[Long, Int]]
