package app.movieapptests

import cats.effect.*
import cats.effect.std.Supervisor
import cats.effect.testing.scalatest.{AssertingSyntax, AsyncIOSpec}

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.{HttpWorker, MemCache, MovieApp, MovieDbModel}
import app.movieapptests.ServiceStubs.{ExternalApiClientServiceStub, FileSystemServiceStub, ServerStateUpdateServiceStub}
import app.services.MovieRepositoryService
import app.AppConfig.BackendServerConfig
import io.circe.literal.*
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

final class MovieAppRoutesTest extends AsyncFreeSpec with AsyncIOSpec with Matchers with AssertingSyntax:
  private implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  private val testBackendConfig: BackendServerConfig =
    BackendServerConfig(numberOfWorkers = 1, boundedQueueCapacity = 4)

  private type CachesType[F[_]] =
    (MemCache[F, Long, MovieDbModel.Director], MemCache[F, Long, MovieDbModel.Actor], MemCache[F, Long, MovieDbModel.Movie])

  // Real MemCache instances (required by HttpWorker, but their effect isn't the focus here).
  private def createCaches[F[_]: { Temporal, Logger }]: Resource[F, CachesType[F]] =
    val workerCleanupDuration = 10.minutes

    for {
      directorCache <- MemCache.createResource[F, Long, MovieDbModel.Director]("testDirectorCache", 5, workerCleanupDuration)
      actorCache <- MemCache.createResource[F, Long, MovieDbModel.Actor]("testActorCache", 5, workerCleanupDuration)
      movieCache <- MemCache.createResource[F, Long, MovieDbModel.Movie]("testMovieCache", 5, workerCleanupDuration)
    } yield (directorCache, actorCache, movieCache)

  // --- Test Setup Resource ---
  // This resource now sets up and starts the actual HttpWorkers
  private def testResources(movieRepository: MovieRepositoryService[IO]): Resource[IO, HttpApp[IO]] =
    for {
      supervisor <- Supervisor[IO]
      serverState <- Resource.eval(MovieApp.LiveServerState.create[IO](testBackendConfig))
      // Stubs for other services
      apiClientStub = new ExternalApiClientServiceStub[IO]
      fileSystemStub = new FileSystemServiceStub[IO]
      serverUpdateStub = new ServerStateUpdateServiceStub[IO]

      (directorCache, actorCache, movieCache) <- createCaches[IO]

      // Start the actual HttpWorkers.
      // HttpWorker.startWorkers itself is F[Unit]; it launches background fibers via the supervisor.
      // These workers will consume jobs from serverState.jobQueue.
      _ <- Resource.eval(
        HttpWorker.startWorkers[IO](
          backendServer = testBackendConfig,
          mr = movieRepository,
          apiClient = apiClientStub,
          fileSystemService = fileSystemStub,
          serverStateUpdateService = serverUpdateStub,
          queue = serverState.jobQueue,
          supervisor = supervisor,
          directorMemCache = directorCache,
          actorMemCache = actorCache,
          movieMemCache = movieCache,
        ),
      )

      // Create the HttpApp (routes)
      renderInstance = MovieApp.Render[IO](Http4sDsl[IO])
      httpApp = MovieApp.allRoutesComplete[IO](serverState, renderInstance)
    } yield httpApp

  // --- Test Cases ---
  "MovieApp Routes with real HttpWorker" - {
    "GET /getMoviesByDirector/{directorId}" - {
      "should return OK and movies from MovieRepositoryInMemory for a valid director ID" in {
        val movieRepoInMemory = new MovieRepositoryInMemory[IO]

        // DirectorId 1L is "Neo Michael" in MovieRepositoryInMemory
        val testDirectorIdFromMemory = 1L

        testResources(movieRepoInMemory).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorIdFromMemory"))

          // Act: Make the request. The real worker will process it.
          // The timeout here is a safety net for the test.
          for {
            response <- httpApp.run(request).timeout(5.seconds) // Add a timeout for safety

            // Assert
            _ <- IO(response.status shouldBe Status.Ok)
            responseJson <- response.as[Json]
            _ <- IO(
              responseJson shouldBe
                json"""[
                         { "movieId": 0, "title": "Xorkatikes malakies", "year": 1980 },
                         { "movieId": 1, "title": "Tsioftes", "year": 2010 }
                       ]
                    """,
            )
          } yield succeed
        }
      }

      "should return OK and empty list if director has no movies in MovieRepositoryInMemory" in {
        val movieRepoInMemory = new MovieRepositoryInMemory[IO]
        val testDirectorIdWithNoMovies = 2L

        testResources(movieRepoInMemory).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorIdWithNoMovies"))

          for {
            response <- httpApp.run(request).timeout(5.seconds)
            _ <- IO(response.status shouldBe Status.Ok)
            responseJson <- response.as[Json]
            _ <- IO(responseJson shouldBe json"[]")
          } yield succeed
        }
      }

      "should return InternalServerError if the repository service (shunning) fails" in {
        // Use the Shunning repository which makes getMoviesByDirectorId fail
        val shunningMovieRepo = MovieAppTestUtils.MovieRepositoryServiceShunning[IO]

        testResources(shunningMovieRepo).use { httpApp =>
          // Any director ID will do, as the repo call will fail
          val anyDirectorId = 0L
          val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$anyDirectorId"))

          for {
            response <- httpApp.run(request).timeout(5.seconds)
            _ <- IO(response.status shouldBe Status.InternalServerError)
            bodyText <- response.bodyText.compile.string
            _ <- IO(logger.info(s"InternalServerError body (shunning repo): '$bodyText'"))
          } yield succeed
        }
      }
    }
  }
