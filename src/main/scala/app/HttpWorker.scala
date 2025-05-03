package app

import cats.data.NonEmptyVector
import cats.effect.{Async, Deferred}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*

import scala.annotation.switch
import scala.concurrent.duration.*

import app.services.{ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerStateUpdateService}
import app.ImplicitConversions.*
import app.JobSpecs.{JobKind, JobResult}
import app.Utils as U
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s.{Response, Uri}
import org.typelevel.log4cats.Logger

object HttpWorker:
  final class Job[F[_]](
      val job: JobKind,
      val deferred: Deferred[F, Either[Throwable, JobResult]],
  )

  private final class JobExecutor[F[_]: { Async as async, Logger as logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverStateUpdateService: ServerStateUpdateService[F],
  ):
    private def getDirectorsDetailsByName(j: JobKind.GetDirectorsDetailsByName): F[JobResult] =
      val (firstName, lastName) = (j.firstName, j.lastName)

      for {
        _ <- U.logi("Fetching directors details by name")
        directorsDetails <- mr.getDirectorsDetails(firstName, lastName)
      } yield JobResult.DirectorsDetailsByNameResult(directorsDetails)

    private def getDirectorDetails(
        j: JobKind.GetDirectorDetails,
    ): F[JobResult] =
      val directorId = j.directorId
      for {
        _ <- U.logi("Fetching director details by id")
        directorDetailsMap <- mr.getDirectorDetails(NonEmptyVector.one(directorId))
      } yield JobResult.DirectorDetailsResult(directorDetailsMap.get(directorId))

    private def getActorDetails(j: JobKind.GetActorDetails): F[JobResult] =
      val actorId = j.actorId
      for {
        _ <- U.logi(s"Fetching actor details for ID: $actorId")
        actorDetailsMap <- mr.getActorDetails(NonEmptyVector.one(actorId))
      } yield JobResult.ActorDetailsResult(actorDetailsMap.get(actorId))

    private def getMoviesByDirectorId(
        j: JobKind.GetMoviesByDirectorId,
    ): F[JobResult] =
      val directorId = j.directorId
      for {
        _ <- U.logi(s"Fetching movies for director ID: $directorId")
        moviesMap <- mr.getMoviesByDirectorId(NonEmptyVector.one(directorId))
      } yield JobResult.MoviesByDirectorIdResult(moviesMap.getOrElse(directorId, Seq.empty))

    private def getMovieById(j: JobKind.GetMovieById): F[JobResult] =
      val movieId = j.movieId
      for {
        _ <- U.logi(s"Fetching movie details for ID: $movieId")
        movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
      } yield JobResult.MovieByIdResult(movieDetailsMap.get(movieId))

    private def getMovieByIdWithCounting(j: JobKind.GetMovieByIdWithCounting): F[JobResult] =
      val movieId = j.movieId
      for {
        _ <- U.logi(s"Fetching movie details for ID $movieId with counting.")
        movieDetailsMap <- mr.getMoviesByIds(NonEmptyVector.one(movieId))
        _ <- movieDetailsMap.nonEmpty.whenA(
          serverStateUpdateService.incrementAndGet(movieId) >>=
            (newCounter => U.logi(s"Counter now is $newCounter")),
        )
      } yield JobResult.MovieByIdWithCountingResult(movieDetailsMap.get(movieId))

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
            Uri.unsafeFromString("http://127.0.0.1:8080/getMovieById/0"),
          )
          .map(_.asJson)
      } yield JobResult.JsonObjectResult(obj)

    def executeJob(job: JobKind): F[JobResult] =
      (job.tag: @switch) match
        case JobKind.GetDirectorsDetailsByNameTag =>
          getDirectorsDetailsByName(U.castTo[JobKind.GetDirectorsDetailsByName](job))
        case JobKind.GetDirectorDetailsTag =>
          getDirectorDetails(U.castTo[JobKind.GetDirectorDetails](job))
        case JobKind.GetActorDetailsTag => getActorDetails(U.castTo[JobKind.GetActorDetails](job))
        case JobKind.GetMoviesByDirectorIdTag =>
          getMoviesByDirectorId(U.castTo[JobKind.GetMoviesByDirectorId](job))
        case JobKind.GetMovieByIdTag =>
          getMovieById(U.castTo[JobKind.GetMovieById](job))
        case JobKind.GetMovieByIdWithCountingTag =>
          getMovieByIdWithCounting(U.castTo[JobKind.GetMovieByIdWithCounting](job))
        case JobKind.CreateMovieTag =>
          createMovie(U.castTo[JobKind.CreateMovie](job))
        case JobKind.GetFileContentTag =>
          getFileContent(U.castTo[JobKind.GetFileContent](job))
        case JobKind.ReadTwoFilesInParallelTag =>
          readTwoFilesInParallel(U.castTo[JobKind.ReadTwoFilesInParallel](job))
        case JobKind.FetchCompanyDataTag =>
          fetchCompanyData(U.castTo[JobKind.FetchCompanyData](job))
        case JobKind.FetchJsonObjectTag =>
          fetchJsonObject(U.castTo[JobKind.FetchJsonObject](job))

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

  inline private val NumberOfWorkers = 32

  def startWorkers[F[_]: { Async, Logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverStateUpdateService: ServerStateUpdateService[F],
      queue: Queue[F, HttpWorker.Job[F]],
      supervisor: Supervisor[F],
  ): F[Unit] = {
    val jobExecutor: JobExecutor[F] =
      JobExecutor(mr, apiClient, fileSystemService, serverStateUpdateService)

    (0 until NumberOfWorkers).toVector
      .traverse_(workerId => supervisor.supervise(worker(workerId, queue, jobExecutor)))
  }
