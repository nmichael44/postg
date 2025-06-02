package app

import cats.data.NonEmptyVector
import cats.effect.{Async, Deferred}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*

import scala.annotation.switch
import scala.concurrent.duration.*

import app.services.{ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerStateUpdateService}
import app.AppConfig.BackendServerConfig
import app.ImplicitConversions.*
import app.JobSpecs.{JobKind, JobResult}
import app.MovieApp.AppMemCaches
import app.Utils as U
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s.Uri
import org.typelevel.log4cats.Logger

object HttpWorker:
  final class Job[F[_]](
      val job: JobKind,
      val deferred: Deferred[F, Either[Throwable, JobResult]],
  )

  enum CacheStatus:
    case CachesEnabled
    case CachesDisabled

  private final class JobExecutor[F[_]: { Async as async, Logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverStateUpdateService: ServerStateUpdateService[F],
      appMemCaches: AppMemCaches[F],
      cacheStatus: CacheStatus,
  ):
    private val cacheEnabled = cacheStatus == CacheStatus.CachesEnabled
    private val directorMemCache: MemCache[F, Long, MovieDbModel.Director] = appMemCaches.directorCache
    private val actorMemCache: MemCache[F, Long, MovieDbModel.Actor] = appMemCaches.actorCache
    private val movieMemCache: MemCache[F, Long, MovieDbModel.Movie] = appMemCaches.movieCache

    private def getDirectorsDetailsByName(j: JobKind.GetDirectorsDetailsByName): F[JobResult] =
      val (firstName, lastName) = (j.firstName, j.lastName)

      U.logi("Fetching directors details by name") *>
        mr.getDirectorsDetails(firstName, lastName)
          .map(JobResult.DirectorsDetailsByNameResult.apply)

    private def getDetailsWithCache[T](
        itemName: String,
        id: Long,
        cachingDuration: FiniteDuration,
        cache: MemCache[F, Long, T],
        f: NonEmptyVector[Long] => F[Map[Long, T]],
        toJobResult: Option[T] => JobResult,
    ): F[JobResult] =
      for {
        _ <- U.logi(s"Fetching $itemName details for ID: $id")
        cashedItemOpt <- if cacheEnabled then cache.get(id) else async.pure(None)
        itemOpt <- cashedItemOpt match {
          case Some(item) =>
            U.logi(s"$itemName details for ID: $id found in cache.").as(Some(item))
          case None =>
            U.logi(s"$itemName details for ID: $id not found in cache. Fetching from DB.") *>
              f(NonEmptyVector.one(id)) >>= { itemDetailsMap =>
              itemDetailsMap.get(id) match {
                case Some(item) =>
                  U.logi(s"$itemName details for ID: $id found in DB. Putting in cache.") *>
                    (if cacheEnabled
                     then cache.put(id, item, cachingDuration)
                     else async.pure(())).as(Some(item))
                case None =>
                  U.logi(s"$itemName details for ID: $id not found in DB.").as(None)
              }
            }
        }
      } yield toJobResult(itemOpt)

    private val DirectorCachingDuration: FiniteDuration = 2.minutes

    private def getDirectorDetails(j: JobKind.GetDirectorDetails): F[JobResult] =
      getDetailsWithCache(
        "Director",
        j.directorId,
        DirectorCachingDuration,
        directorMemCache,
        mr.getDirectorDetails,
        JobResult.DirectorDetailsResult.apply,
      )

    private def getMoviesByDirector(j: JobKind.GetMoviesByDirector): F[JobResult] =
      val directorId = j.directorId
      for {
        _ <- U.logi(s"Fetching movies for director ID: $directorId")
        moviesMap <- mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      } yield JobResult.MoviesByDirectorResult(moviesMap.getOrElse(directorId, Seq.empty))

    private val ActorCachingDuration: FiniteDuration = 2.minutes

    private def getActorDetails(j: JobKind.GetActorDetails): F[JobResult] =
      getDetailsWithCache(
        "Actor",
        j.actorId,
        ActorCachingDuration,
        actorMemCache,
        mr.getActorDetails,
        JobResult.ActorDetailsResult.apply,
      )

    private val MovieCachingDuration: FiniteDuration = 2.minutes

    private def getMovie(j: JobKind.GetMovie): F[JobResult] =
      getDetailsWithCache(
        "Movie",
        j.movieId,
        MovieCachingDuration,
        movieMemCache,
        mr.getMovieDetails,
        JobResult.MovieDetailsResult.apply,
      )

      val movieId = j.movieId
      for {
        _ <- U.logi(s"Fetching movie details for ID: $movieId")
        movieDetailsMap <- mr.getMovieDetails(NonEmptyVector.one(movieId))
      } yield JobResult.MovieDetailsResult(movieDetailsMap.get(movieId))

    private def getMovieWithCounting(j: JobKind.GetMovieWithCounting): F[JobResult] =
      val movieId = j.movieId
      for {
        _ <- U.logi(s"Fetching movie details for ID $movieId with counting.")
        movieDetailsMap <- mr.getMovieDetails(NonEmptyVector.one(movieId))
        _ <- movieDetailsMap.nonEmpty.whenA(
          serverStateUpdateService.incrementAndGet(movieId) >>=
            (newCounter => U.logi(s"Counter now is $newCounter")),
        )
      } yield JobResult.MovieWithCountingResult(movieDetailsMap.get(movieId))

    private def createMovie(j: JobKind.CreateMovie): F[JobResult] =
      val (title, year) = (j.title, j.year)
      for {
        _ <- U.logi(s"Creating movie with title: '$title' and year: '$year'.")
        movieId <- mr.createMovie(title, year)
      } yield JobResult.CreateMovieResult(movieId)

    private def getFileContent(j: JobKind.GetFileContent): F[JobResult] =
      val fileName = j.fileName
      for {
        _ <- U.logi(s"Asked to read file: '$fileName'.")
        res <- fileSystemService.readFileContent(fileName)
      } yield JobResult.FileContentResult(res)

    private def readTwoFilesInParallel(j: JobKind.ReadTwoFilesInParallel): F[JobResult] =
      val (fileName1, fileName2) = (j.fileName1, j.fileName2)
      for {
        _ <- U.logi(s"Reading the two files in parallel.")
        _ <- U.logi(s"FileName1 = '$fileName1'")
        _ <- U.logi(s"FileName2 = '$fileName2'")
        res <- fileSystemService
          .readTwoFilesInParallel(fileName1, fileName2)
      } yield JobResult.TwoFilesInParallelResult(res)

    private def fetchCompanyData(j: JobKind.FetchCompanyData): F[JobResult] =
      val companyName = j.companyName
      apiClient
        .fetchCompanyData(companyName)
        .map(JobResult.CompanyDataResult.apply)

    private def fetchJsonObject(j: JobKind.FetchJsonObject): F[JobResult] =
      for {
        _ <- U.logi("Fetching some json object recursively.")
        obj <- apiClient
          .fetchAsJson[MovieDbModel.Movie](
            Uri.unsafeFromString("http://127.0.0.1:8080/getMovie/0"),
          )
          .map(_.asJson)
      } yield JobResult.JsonObjectResult(obj)

    def executeJob(job: JobKind): F[JobResult] =
      (job.tag: @switch) match
        case JobKind.GetDirectorsDetailsByNameTag => getDirectorsDetailsByName(job.castAs[JobKind.GetDirectorsDetailsByName])
        case JobKind.GetDirectorDetailsTag => getDirectorDetails(job.castAs[JobKind.GetDirectorDetails])
        case JobKind.GetActorDetailsTag => getActorDetails(job.castAs[JobKind.GetActorDetails])
        case JobKind.GetMoviesByDirectorTag => getMoviesByDirector(job.castAs[JobKind.GetMoviesByDirector])
        case JobKind.GetMovieTag => getMovie(job.castAs[JobKind.GetMovie])
        case JobKind.GetMovieWithCountingTag => getMovieWithCounting(job.castAs[JobKind.GetMovieWithCounting])
        case JobKind.CreateMovieTag => createMovie(job.castAs[JobKind.CreateMovie])
        case JobKind.GetFileContentTag => getFileContent(job.castAs[JobKind.GetFileContent])
        case JobKind.ReadTwoFilesInParallelTag => readTwoFilesInParallel(job.castAs[JobKind.ReadTwoFilesInParallel])
        case JobKind.FetchCompanyDataTag => fetchCompanyData(job.castAs[JobKind.FetchCompanyData])
        case JobKind.FetchJsonObjectTag => fetchJsonObject(job.castAs[JobKind.FetchJsonObject])

  private def worker[F[_]: { Async as async, Logger as logger }](
      workerId: Int,
      queue: Queue[F, Job[F]],
      jobExecutor: JobExecutor[F],
  ): F[Nothing] =
    val prompt = s"Worker '$workerId'"

    val processOneJob: F[Unit] = for {
      _ <- logger.info(s"$prompt: Waiting for work.")
      (job, deferred) <- queue.take.map(j => (j.job, j.deferred))
      _ <- logger.info(s"$prompt: Starting to work on ${job.shortName}...")
      outcome <- jobExecutor.executeJob(job).attempt
      // Finally, send the results back to the calling fiber.
      _ <- logger.info(s"$prompt: Sending results back...")
      _ <- deferred.complete(outcome)
    } yield ()

    val processOneJobSafely: F[Unit] = processOneJob.handleErrorWith { e =>
      logger.error(e)(s"$prompt: Unhandled worker error. Restarting...") *>
        async.sleep(1.second)
    }

    processOneJobSafely.foreverM

  def startWorkers[F[_]: { Async, Logger }](
      backendServer: BackendServerConfig,
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverStateUpdateService: ServerStateUpdateService[F],
      queue: Queue[F, HttpWorker.Job[F]],
      supervisor: Supervisor[F],
      appMemCaches: AppMemCaches[F],
      cacheStatus: CacheStatus,
  ): F[Unit] =
    val jobExecutor: JobExecutor[F] =
      JobExecutor(mr, apiClient, fileSystemService, serverStateUpdateService, appMemCaches, cacheStatus)

    val numberOfWorkers = backendServer.getNumberOfWorkers
    Vector
      .from(0 until numberOfWorkers)
      .traverseVoid(workerId => supervisor.supervise(worker(workerId, queue, jobExecutor)))
