package app.movieapptests

import cats.effect.*
import cats.effect.std.Supervisor
import cats.effect.testing.scalatest.{AssertingSyntax, AsyncIOSpec}
import cats.syntax.all.*

import scala.concurrent.duration.*

import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import app.{HttpWorker, MemCache, MovieApp, MovieDbModel}
import app.movieapptests.ServiceStubs.{ExternalApiClientServiceStub, FileSystemServiceStub, ServerStateUpdateServiceStub}
import app.AppConfig.BackendServerConfig
import app.HttpWorker.CacheStatus
import app.MovieApp.AppMemCaches
import app.TestUtils.*
import io.circe.literal.*
import io.circe.Json
import org.http4s.*
import org.http4s.circe.*
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.Logger

final class MovieAppRoutesTest extends AsyncFreeSpec with AsyncIOSpec with Matchers with AssertingSyntax:
  private val testBackendConfig: BackendServerConfig =
    BackendServerConfig(numberOfWorkers = 1, boundedQueueCapacity = 4)

  private def createCaches[F[_]: { Temporal, Logger }]: Resource[F, AppMemCaches[F]] =
    val workerCleanupDuration = 10.minutes
    (
      MemCache.createResource[F, Long, MovieDbModel.Director]("testDirectorCache", 1, workerCleanupDuration),
      MemCache.createResource[F, Long, MovieDbModel.Actor]("testActorCache", 1, workerCleanupDuration),
      MemCache.createResource[F, Long, MovieDbModel.Movie]("testMovieCache", 1, workerCleanupDuration),
    ).mapN((d, a, m) => AppMemCaches[F](d, a, m))

  // --- Test Setup Resource ---
  // This resource now sets up and starts the actual HttpWorkers
  private def testResources(withDb: Boolean): Resource[IO, HttpApp[IO]] =
    for {
      supervisor <- Supervisor[IO](await = false)
      serverState <- Resource.eval(MovieApp.LiveServerState.create[IO](testBackendConfig))
      // Stubs for other services
      (apiClientStub, fileSystemStub, serverUpdateStub) =
        (ExternalApiClientServiceStub[IO], FileSystemServiceStub[IO], ServerStateUpdateServiceStub[IO])

      appMemCaches <- createCaches[IO]

      // Start the actual HttpWorkers.
      // HttpWorker.startWorkers itself is F[Unit]; it launches background fibers via the supervisor.
      // These workers will consume jobs from serverState.jobQueue.
      movieRepository <- Resource.eval(
        if withDb then MovieRepositoryInMemory.createWithDefaultDb[IO]
        else IO.pure(MovieAppTestUtils.MovieRepositoryServiceShunning[IO]),
      )
      _ <- Resource.eval(
        HttpWorker.startWorkers[IO](
          backendServer = testBackendConfig,
          mr = movieRepository,
          apiClient = apiClientStub,
          fileSystemService = fileSystemStub,
          serverStateUpdateService = serverUpdateStub,
          queue = serverState.jobQueue,
          supervisor = supervisor,
          appMemCaches,
          CacheStatus.CachesDisabled,
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
        // DirectorId 1 is "Neo Michael" in MovieRepositoryInMemory
        val testDirectorIdFromMemory = 1L

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorIdFromMemory"))

          // Make the request. The real worker will process it.
          for {
            response <- httpApp.run(request)
            responseJson <- response.as[Json]
          } yield (response.status shouldBe Status.Ok) ~&> (responseJson shouldBe
            json"""[
                     { "movieId": 0, "title": "Xorkatikes malakies", "year": 1980 },
                     { "movieId": 1, "title": "Tsioftes", "year": 2010 }
                   ]
                   """)
        }
      }
      "should return OK and empty list if director has no movies in MovieRepositoryInMemory" in {
        val testDirectorIdWithNoMovies = 2L

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$testDirectorIdWithNoMovies"))

          for {
            response <- httpApp.run(request)
            responseJson <- response.as[Json]
          } yield (response.status shouldBe Status.Ok) ~&>
            (responseJson shouldBe json"[]")
        }
      }

      "should return InternalServerError if the repository service (shunning) fails" in
        testResources(false).use { httpApp =>
          // Any director ID will do, as the repo call will fail
          val anyDirectorId = 0L
          val request = Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getMoviesByDirector/$anyDirectorId"))

          for {
            response <- httpApp.run(request).timeout(5.seconds)
            _ <- IO(response.status shouldBe Status.InternalServerError)
            bodyText <- response.bodyText.compile.string
            _ <- IO(testLogger.info(s"InternalServerError body (shunning repo): '$bodyText'"))
          } yield succeed
        }
    }

    "Tests for GetDirectorDetails" - {
      "should return OK and director details for a valid director id" in {
        val directorId = 1L

        val expectedJson =
          json"""{
                 "directorId": $directorId,
                 "firstName": "Neo",
                 "lastName": "Michael",
                 "dob": "1970-04-19"
               }"""

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getDirector/$directorId"))

          for {
            response <- httpApp.run(request)
            responseJson <- response.as[Json]
          } yield (response.status shouldBe Status.Ok) ~&>
            (responseJson shouldBe expectedJson)
        }
      }

      "should return NotFound for a non-existent director id" in {
        val nonExistentDirectorId = 11L

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getDirector/$nonExistentDirectorId"))

          for {
            response <- httpApp.run(request)
            bodyText <- response.bodyText.compile.string
          } yield (response.status shouldBe Status.NotFound) ~&>
            (bodyText should equal("Director not found"))
        }
      }
    }

    "Tests for GetActorDetails" - {
      "should return OK and actor details for a valid actor id" in {
        val actorId = 0L

        val expectedJson =
          json"""{
                 "actorId": $actorId,
                 "firstName": "Neo",
                 "lastName": "Michael",
                 "dob": "1970-04-19"
               }"""

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getActor/$actorId"))

          for {
            response <- httpApp.run(request)
            responseJson <- response.as[Json]
          } yield (response.status shouldBe Status.Ok) ~&>
            (responseJson shouldBe expectedJson)
        }
      }

      "should return NotFound for a non-existent actor id" in {
        val nonExistentActorId = 11L

        testResources(true).use { httpApp =>
          val request =
            Request[IO](method = Method.GET, uri = Uri.unsafeFromString(s"/getActor/$nonExistentActorId"))

          for {
            response <- httpApp.run(request)
            bodyText <- response.bodyText.compile.string
          } yield (response.status shouldBe Status.NotFound) ~&>
            (bodyText should equal("Actor not found"))
        }
      }
    }
  }
