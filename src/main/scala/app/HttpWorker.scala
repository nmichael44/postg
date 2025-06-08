package app

import cats.data.NonEmptyVector
import cats.effect.{Async, Deferred}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*

import scala.annotation.switch
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

import app.services.{ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerStateUpdateService}
import app.AppConfig.BackendServerConfig
import app.ImplicitConversions.*
import app.JobSpecs.{FetchSystemUserError, JobKind, JobResult}
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

    private def getDirectorsDetailsByName(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetDirectorsDetailsByName]
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

    private def getDirectorDetails(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetDirectorDetails]
      getDetailsWithCache(
        "Director",
        j.directorId,
        DirectorCachingDuration,
        directorMemCache,
        mr.getDirectorDetails,
        JobResult.DirectorDetailsResult.apply,
      )

    private def getMoviesByDirector(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetMoviesByDirector]
      val directorId = j.directorId
      for {
        _ <- U.logi(s"Fetching movies for director ID: $directorId")
        moviesMap <- mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      } yield JobResult.MoviesByDirectorResult(moviesMap.getOrElse(directorId, Seq.empty))

    private val ActorCachingDuration: FiniteDuration = 2.minutes

    private def getActorDetails(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetActorDetails]
      getDetailsWithCache(
        "Actor",
        j.actorId,
        ActorCachingDuration,
        actorMemCache,
        mr.getActorDetails,
        JobResult.ActorDetailsResult.apply,
      )

    private val MovieCachingDuration: FiniteDuration = 2.minutes

    private def getMovie(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetMovie]
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

    private def getMovieWithCounting(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetMovieWithCounting]
      val movieId = j.movieId
      for {
        _ <- U.logi(s"Fetching movie details for ID $movieId with counting.")
        movieDetailsMap <- mr.getMovieDetails(NonEmptyVector.one(movieId))
        _ <- movieDetailsMap.nonEmpty.whenA(
          serverStateUpdateService.incrementAndGet(movieId) >>=
            (newCounter => U.logi(s"Counter now is $newCounter")),
        )
      } yield JobResult.MovieWithCountingResult(movieDetailsMap.get(movieId))

    private def createMovie(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.CreateMovie]
      val (title, year) = (j.title, j.year)
      for {
        _ <- U.logi(s"Creating movie with title: '$title' and year: '$year'.")
        movieId <- mr.createMovie(title, year)
      } yield JobResult.CreateMovieResult(movieId)

    private def getFileContent(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetFileContent]
      val fileName = j.fileName
      for {
        _ <- U.logi(s"Asked to read file: '$fileName'.")
        res <- fileSystemService.readFileContent(fileName)
      } yield JobResult.FileContentResult(res)

    private def readTwoFilesInParallel(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.ReadTwoFilesInParallel]
      val (fileName1, fileName2) = (j.fileName1, j.fileName2)
      for {
        _ <- U.logi(s"Reading the two files in parallel.")
        _ <- U.logi(s"FileName1 = '$fileName1'")
        _ <- U.logi(s"FileName2 = '$fileName2'")
        res <- fileSystemService
          .readTwoFilesInParallel(fileName1, fileName2)
      } yield JobResult.TwoFilesInParallelResult(res)

    private def fetchCompanyData(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.FetchCompanyData]
      val companyName = j.companyName
      apiClient
        .fetchCompanyData(companyName)
        .map(JobResult.CompanyDataResult.apply)

    private def fetchJsonObject(jk: JobKind): F[JobResult] =
      for {
        _ <- U.logi("Fetching some json object recursively.")
        obj <- apiClient
          .fetchAsJson[MovieDbModel.Movie](
            Uri.unsafeFromString("http://127.0.0.1:8080/getMovie/0"),
          )
          .map(_.asJson)
      } yield JobResult.JsonObjectResult(obj)

    private def createSystemUser(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.CreateSystemUser]
      val (loginName, password) = (j.loginName, j.password)

      for {
        _ <- U.logi("Creating system user.")
        userId <- mr.createSystemUser(loginName, password)
      } yield JobResult.CreateSystemUserResult(userId)

    private def fetchSystemUserByLoginName(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.FetchSystemUserByLoginName]
      val loginName = j.loginName

      for {
        _ <- U.logi("Fetching system user by loginName.")
        res <- mr.fetchSystemUserByLoginName(loginName).map(_.toRight(FetchSystemUserError.NotFound))
      } yield JobResult.FetchSystemUserByLoginNameResult(res)

    private def fetchSystemUserByUserId(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.FetchSystemUserByUserId]
      val userIdStr = j.userIdStr

      for {
        _ <- U.logi("Fetching system user by userId.")
        res <- userIdStr.toIntOption.fold(async.pure(Left(FetchSystemUserError.BadInput))) { userId =>
          mr.fetchSystemUserByUserId(userId).map(_.toRight(FetchSystemUserError.NotFound))
        }
      } yield JobResult.FetchSystemUserByUserIdResult(res)

    private def processLoginRequest(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.LoginRequest]
      val ud = j.userDetails
      val (loginName, password) = (ud.loginName, ud.password)

      ???

    private val JobHandlersMap: Map[Class[? <: JobKind], JobKind => F[JobResult]] = Map(
      classOf[JobKind.GetDirectorsDetailsByName]  -> getDirectorsDetailsByName,
      classOf[JobKind.GetDirectorDetails]         -> getDirectorDetails,
      classOf[JobKind.GetActorDetails]            -> getActorDetails,
      classOf[JobKind.GetMoviesByDirector]        -> getMoviesByDirector,
      classOf[JobKind.GetMovie]                   -> getMovie,
      classOf[JobKind.GetMovieWithCounting]       -> getMovieWithCounting,
      classOf[JobKind.CreateMovie]                -> createMovie,
      classOf[JobKind.GetFileContent]             -> getFileContent,
      classOf[JobKind.ReadTwoFilesInParallel]     -> readTwoFilesInParallel,
      classOf[JobKind.FetchCompanyData]           -> fetchCompanyData,
      classOf[JobKind.FetchJsonObject]            -> fetchJsonObject,
      classOf[JobKind.CreateSystemUser]           -> createSystemUser,
      classOf[JobKind.FetchSystemUserByLoginName] -> fetchSystemUserByLoginName,
      classOf[JobKind.FetchSystemUserByUserId]    -> fetchSystemUserByUserId,
      classOf[JobKind.LoginRequest]               -> processLoginRequest,
    )

    private def misingJobImplementationException(job: JobKind): Exception =
      new Exception(s"JobHandlersMap does not contain an implementation for class '${job.shortName}'.") with NoStackTrace

    def executeJob(job: JobKind): F[JobResult] =
      JobHandlersMap
        .get(job.getClass)
        .map(_(job))
        .getOrElse(async.raiseError(misingJobImplementationException(job)))

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
