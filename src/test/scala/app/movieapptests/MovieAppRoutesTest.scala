package app.movieapptests

import cats.data.NonEmptyVector
import cats.effect.*
import cats.effect.testing.scalatest.{AssertingSyntax, AsyncIOSpec}

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.{JobSpecs, MovieApp, MovieDbModel}
import app.services.{MovieRepositoryService, ServerState}
import app.AppConfig.BackendServerConfig
import app.JobSpecs.{JobKind, JobResult}
import io.circe.literal.*
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

final class MovieAppRoutesTest extends AsyncFreeSpec with AsyncIOSpec with Matchers with AssertingSyntax {

  // --- Test Dependencies ---
  // Using Slf4jLogger, ensure you have a logging backend configured (like logback-classic)
  // or switch to NoOpLogger if console output is not desired during tests.
  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  // Backend config needed for LiveServerState queue size
  val testBackendConfig: BackendServerConfig = BackendServerConfig(numberOfWorkers = 1, boundedQueueCapacity = 10)

  // --- Mock Services ---
  // Mocks the MovieRepositoryService interface
  def mockMovieRepository(expectedDirectorId: Long, moviesToReturn: Seq[MovieDbModel.Movie]): MovieRepositoryService[IO] =
    new MovieRepositoryService[IO] {
      // Methods not used in this specific test path can be left unimplemented (???)
      // or return default values / errors if needed for other tests.
      def getDirectorsDetails(firstName: Option[String], lastName: Option[String]): IO[Seq[MovieDbModel.Director]] = ???
      def getDirectorDetails(directorIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Director]] = ???
      def getActorDetails(actorIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Actor]] = ???

      // Implementation for the method used by the route under test
      def getMoviesByDirectorId(directorIds: NonEmptyVector[Long]): IO[Map[Long, Seq[MovieDbModel.Movie]]] =
        // Use .exists for NonEmptyVector check
        if (directorIds.exists(_ == expectedDirectorId))
          IO.pure(Map(expectedDirectorId -> moviesToReturn))
        else
          IO.pure(Map.empty)

      def getMovieDetails(movieIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Movie]] = ???
      def createMovie(title: String, year: Int): IO[Long] = ???
    }

  // --- Test Setup Resource ---
  // Manages the lifecycle of stateful resources needed for the test
  def testResources(mockRepo: MovieRepositoryService[IO]): Resource[IO, (ServerState[IO], HttpApp[IO])] = for {
    // Create the ServerState (includes the Job Queue) using the real implementation
    // Requires MovieApp.LiveServerState.create to be accessible (e.g., package-private)
    serverState <- Resource.eval(MovieApp.LiveServerState.create[IO](testBackendConfig))

    // Create the rendering component
    // Requires MovieApp.Render to be accessible (e.g., package-private)
    renderInstance = MovieApp.Render[IO](Http4sDsl[IO])

    // Build the HttpApp using the actual routing logic, passing the created state and renderer
    // Requires MovieApp.allRoutesComplete to be accessible (e.g., package-private)
    httpApp = MovieApp.allRoutesComplete[IO](serverState, renderInstance)

  } yield (serverState, httpApp) // Provide the state and app to the test case

  // --- Test Data ---
  val testDirectorId = 1L
  // Create Movie instances according to the actual definition (id, title, year)
  private val movie1 = MovieDbModel.Movie(101L, "Movie A", 2001)
  private val movie2 = MovieDbModel.Movie(102L, "Movie B", 2005)
  private val expectedMovies = Vector(movie1, movie2) // The sequence returned by the mock service

  // Define the expected JSON structure based on the actual Movie definition
  val expectedJson: Json = json"""
    [
      { "movieId": 101, "title": "Movie A", "year": 2001 },
      { "movieId": 102, "title": "Movie B", "year": 2005 }
    ]
  """

  // --- Test Cases ---
  "MovieApp Routes" - {
    "GET /getMoviesByDirector/{directorId}" - {

      // Test case for successful retrieval
      "should return OK status and list of movies as JSON for a valid director ID" in {
        // Arrange: Create the mock repository with expected data
        val mockRepo = mockMovieRepository(testDirectorId, expectedMovies)

        // Use the resource setup
        testResources(mockRepo).use { case (serverState, httpApp) =>
          // Arrange: Create the HTTP request
          val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorId"))

          // Arrange: Define the background worker simulation logic
          val simulateWorker: IO[Unit] = (for {
            // Dequeue the job placed by the route handler
            job <- serverState.jobQueue.take
            _ <- logger.info(s"Test worker picked up job: ${job.job.shortName}")
            // Execute the job using the mock repository
            result <- (job.job match {
              case JobKind.GetMoviesByDirector(dirId) =>
                mockRepo
                  .getMoviesByDirectorId(NonEmptyVector.one(dirId))
                  .map(moviesMap => JobResult.MoviesByDirectorResult(moviesMap.getOrElse(dirId, Seq.empty)))
              case other =>
                IO.raiseError[JobResult](new Exception(s"Test worker received unexpected job type: $other"))
            }).attempt // Use .attempt to capture success or failure
            // Complete the Deferred to unblock the route handler
            _ <- job.deferred.complete(result)
            _ <- logger.info(s"Test worker completed deferred for job: ${job.job.shortName}")
          } yield ()).onError(e => logger.error(e)("Worker simulation failed")) // Log errors in simulation

          // Act: Run the HTTP request and worker simulation concurrently
          for {
            responseFiber <- httpApp.run(request).start // Start request processing (blocks on Deferred)
            workerFiber <- simulateWorker.start // Start worker simulation (completes Deferred)

            // Assert: Wait for request fiber to finish and get outcome
            outcome <- responseFiber.join
            // Assert: Extract Response or raise error if fiber failed/canceled
            response <- outcome.embedError
            // Assert: Wait for worker fiber to finish (ensures simulation completed cleanly)
            _ <- workerFiber.join

            // Assert: Check response status
            _ <- IO(response.status shouldBe Status.Ok)
            // Assert: Check response body against expected JSON
            responseJson <- response.as[Json]
            _ <- IO(responseJson shouldBe expectedJson)
          } yield succeed // Indicate test success
        }
      }

      // Test case for a director with no associated movies
      "should return OK status and empty list JSON if director has no movies" in {
        val directorIdWithNoMovies = 2L
        // Arrange: Mock repo returns an empty sequence for this director
        val mockRepo = mockMovieRepository(directorIdWithNoMovies, Seq.empty)

        testResources(mockRepo).use { case (serverState, httpApp) =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$directorIdWithNoMovies"))
          val simulateWorker: IO[Unit] = (for {
            job <- serverState.jobQueue.take
            _ <- logger.info(s"Test worker (empty) picked up job: ${job.job.shortName}")
            result <- (job.job match {
              case JobKind.GetMoviesByDirector(dirId) =>
                mockRepo
                  .getMoviesByDirectorId(NonEmptyVector.one(dirId))
                  .map(moviesMap => JobResult.MoviesByDirectorResult(moviesMap.getOrElse(dirId, Seq.empty)))
              case other =>
                IO.raiseError[JobResult](new Exception(s"Test worker (empty) received unexpected job type: $other"))
            }).attempt
            _ <- job.deferred.complete(result)
            _ <- logger.info(s"Test worker (empty) completed deferred for job: ${job.job.shortName}")
          } yield ()).onError(e => logger.error(e)("Worker simulation failed"))

          // Act & Assert
          for {
            responseFiber <- httpApp.run(request).start
            workerFiber <- simulateWorker.start
            outcome <- responseFiber.join
            response <- outcome.embedError
            _ <- workerFiber.join
            _ <- IO(response.status shouldBe Status.Ok)
            responseJson <- response.as[Json]
            _ <- IO(responseJson shouldBe json"[]") // Expect empty JSON array
          } yield succeed
        }
      }

      // Test case for when the underlying service fails
      "should return InternalServerError if the repository service fails" in {
        // Arrange: Create a mock repository that always fails for the relevant method
        val failingRepo: MovieRepositoryService[IO] = new MovieRepositoryService[IO] {
          def getDirectorsDetails(firstName: Option[String], lastName: Option[String]): IO[Seq[MovieDbModel.Director]] =
            IO.raiseError(new Exception("DB error"))
          def getDirectorDetails(directorIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Director]] =
            IO.raiseError(new Exception("DB error"))
          def getActorDetails(actorIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Actor]] =
            IO.raiseError(new Exception("DB error"))
          // This method will be called and raise an error
          def getMoviesByDirectorId(directorIds: NonEmptyVector[Long]): IO[Map[Long, Seq[MovieDbModel.Movie]]] =
            IO.raiseError(new RuntimeException("Simulated DB Failure"))
          def getMovieDetails(movieIds: NonEmptyVector[Long]): IO[Map[Long, MovieDbModel.Movie]] =
            IO.raiseError(new Exception("DB error"))
          def createMovie(title: String, year: Int): IO[Long] = IO.raiseError(new Exception("DB error"))
        }

        testResources(failingRepo).use { case (serverState, httpApp) =>
          val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorId"))
          // Arrange: Worker simulation now handles the error from the mock repo
          val simulateWorkerWithError: IO[Unit] = (for {
            job <- serverState.jobQueue.take
            _ <- logger.info(s"Test worker (error) picked up job: ${job.job.shortName}")
            // Calling the failing mock repo method here, captured by .attempt
            result <- (job.job match {
              case JobKind.GetMoviesByDirector(dirId) =>
                failingRepo
                  .getMoviesByDirectorId(NonEmptyVector.one(dirId))
                  .map(moviesMap => JobResult.MoviesByDirectorResult(moviesMap.getOrElse(dirId, Seq.empty)))
              case other =>
                IO.raiseError[JobResult](new Exception(s"Test worker (error) received unexpected job type: $other"))
            }).attempt // Result will be Left(RuntimeException("Simulated DB Failure"))
            // Complete the deferred with the failure
            _ <- job.deferred.complete(result)
            _ <- logger.info(s"Test worker (error) completed deferred for job: ${job.job.shortName} with result: $result")
          } yield ()).onError(e => logger.error(e)("Worker simulation (error) failed itself"))

          // Act & Assert
          for {
            responseFiber <- httpApp.run(request).start
            workerFiber <- simulateWorkerWithError.start
            outcome <- responseFiber.join
            // Route handler receives Left(...) from deferred.get, maps it to InternalServerError
            response <- outcome.embedError
            _ <- workerFiber.join
            // Assert: Check for InternalServerError status
            _ <- IO(response.status shouldBe Status.InternalServerError)
            // Optionally log the body, often empty or generic for 500 errors
            bodyText <- response.bodyText.compile.string
            _ <- IO(logger.info(s"InternalServerError body: '$bodyText'"))
          } yield succeed
        }
      }
    }
  }
}
