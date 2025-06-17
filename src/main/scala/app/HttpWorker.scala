package app

import cats.data.{EitherT, NonEmptyVector}
import cats.effect.{Async, Deferred}
import cats.effect.std.Queue
import cats.syntax.all.*

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

import app.services.{AuthService, ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerStateUpdateService}
import app.services.MovieRepositoryUtils.DBError
import app.ImplicitConversions.*
import app.JobSpecs.{CreateSystemUserError, FetchSystemUserError, JobKind, JobResult, LoginRequestError}
import app.MovieApp.{AppDependencies, MemCaches}
import app.Utils as U
import org.typelevel.log4cats.Logger

object HttpWorker:
  final class Job[F[_]](
      val job: JobKind,
      val deferred: Deferred[F, Either[Throwable, JobResult]],
      val uuid: String,
  )

  private final class JobExecutor[F[_]: { Async as async, Logger }](
      mr: MovieRepositoryService[F],
      apiClient: ExternalApiClientService[F],
      fileSystemService: FileSystemService[F],
      serverStateUpdateService: ServerStateUpdateService[F],
      passwordHasherService: PasswordHasher[F],
      authService: AuthService[F],
      memCaches: MemCaches[F],
      val uuidScope: TraceIdScope[F, Option[String]],
  ):
    private val directorMemCache = memCaches.directorCache
    private val actorMemCache = memCaches.actorCache
    private val movieMemCache = memCaches.movieCache

    private val WorkerFiberName = "Worker"

    def logi(s: String): F[Unit] =
      uuidScope.get >>= (uuidOpt => uuidOpt.fold(U.logi(WorkerFiberName, s))(U.logi(WorkerFiberName, _, s)))

    def loge(e: Throwable, s: String): F[Unit] =
      uuidScope.get >>= (uuidOpt => uuidOpt.fold(U.loge(e, WorkerFiberName, s))(U.loge(e, WorkerFiberName, _, s)))

    private def getDirectorsDetailsByName(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetDirectorsDetailsByName]
      val (firstName, lastName) = (j.firstName, j.lastName)

      logi("Fetching directors details by name") *>
        mr.getDirectorsDetails(firstName, lastName)
          .map(JobResult.DirectorsDetailsByNameResult.apply)

    private def getDetailsWithCache[T](
        itemName: String,
        id: Long,
        cachingDuration: FiniteDuration,
        cacheDetails: (Boolean, MemCache[F, Long, T]),
        f: NonEmptyVector[Long] => F[Map[Long, T]],
        toJobResult: Option[T] => JobResult,
    ): F[JobResult] =
      val (cacheEnabled, cache) = cacheDetails
      for {
        _ <- logi(s"Fetching $itemName details for ID: $id")
        cashedItemOpt <- if cacheEnabled then cache.get(id) else async.pure(None)
        itemOpt <- cashedItemOpt match {
          case Some(item) =>
            logi(s"$itemName details for ID: $id found in cache.").as(Some(item))
          case None =>
            cacheEnabled.whenA(logi(s"$itemName details for ID: $id not found in cache.")) *>
              logi(s"$itemName details for ID: $id Fetching from DB.") *>
              f(NonEmptyVector.one(id)) >>= { itemDetailsMap =>
              itemDetailsMap.get(id) match {
                case Some(item) =>
                  logi(s"$itemName details for ID: $id found in DB.") *>
                    cacheEnabled
                      .whenA(logi(s"Putting $itemName for ID: $id in cache.") *> cache.put(id, item, cachingDuration))
                      .as(Some(item))
                case None =>
                  logi(s"$itemName details for ID: $id not found in DB.").as(None)
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
        _ <- logi(s"Fetching movies for director ID: $directorId")
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
        _ <- logi(s"Fetching movie details for ID: $movieId")
        movieDetailsMap <- mr.getMovieDetails(NonEmptyVector.one(movieId))
      } yield JobResult.MovieDetailsResult(movieDetailsMap.get(movieId))

    private def getMovieWithCounting(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.GetMovieWithCounting]
      val movieId = j.movieId
      for {
        _ <- logi(s"Fetching movie details for ID $movieId with counting.")
        movieDetailsMap <- mr.getMovieDetails(NonEmptyVector.one(movieId))
        _ <- movieDetailsMap.nonEmpty.whenA(
          serverStateUpdateService.incrementAndGet(movieId) >>=
            (newCounter => logi(s"Counter now is $newCounter")),
        )
      } yield JobResult.MovieWithCountingResult(movieDetailsMap.get(movieId))

    private def createMovie(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.CreateMovie]
      val (title, year) = (j.title, j.year)
      for {
        _ <- logi(s"Creating movie with title: '$title' and year: '$year'.")
        movieId <- mr.createMovie(title, year)
      } yield JobResult.CreateMovieResult(movieId)

    private def validatePassword(password: String): EitherT[F, CreateSystemUserError, String] =
      PasswordValidator
        .isPasswordGoodEnough(password)
        .toEither
        .leftMap(CreateSystemUserError.BadPassword.apply)
        .toEitherT

    private def createSystemUser(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.CreateSystemUser]
      val userDetails = j.userDetails
      val (loginName, password) = (userDetails.loginName, userDetails.password)

      val res: EitherT[F, CreateSystemUserError, Int] = for {
        _ <- logi("Creating system user.").lift
        _ <- logi("Checking password validity.").lift

        validatedPassword <- validatePassword(password)

        _ <- logi(s"Password is valid. Creating system user '$loginName'.").lift
        hashedPassword <- passwordHasherService.hashPassword(validatedPassword).lift

        userId <- mr
          .createSystemUser(loginName, hashedPassword)
          .toEitherT
          .leftMap { case DBError.DuplicateLoginName(nm) => CreateSystemUserError.DuplicateLoginNameInDB(nm) }
      } yield userId

      res.value.map(JobResult.CreateSystemUserResult.apply)

    private def fetchSystemUserByLoginName(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.FetchSystemUserByLoginName]
      val loginName = j.loginName

      for {
        _ <- logi("Fetching system user by loginName.")
        res <- mr.fetchSystemUserByLoginName(loginName).map(_.toRight(FetchSystemUserError.NotFound))
      } yield JobResult.FetchSystemUserByLoginNameResult(res)

    private def fetchSystemUserByUserId(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.FetchSystemUserByUserId]
      val userIdStr = j.userIdStr

      for {
        _ <- logi("Fetching system user by userId.")
        res <- userIdStr.toIntOption.fold(async.pure(Left(FetchSystemUserError.BadInput))) { userId =>
          mr.fetchSystemUserByUserId(userId).map(_.toRight(FetchSystemUserError.NotFound))
        }
      } yield JobResult.FetchSystemUserByUserIdResult(res)

    private def processLoginRequest(jk: JobKind): F[JobResult] =
      val j = jk.asInstanceOf[JobKind.LoginRequest]
      val ud = j.userDetails
      val (loginName, password) = (ud.loginName, ud.password)

      val res: EitherT[F, LoginRequestError, String] = for {
        userDetails <- mr.fetchSystemUserByLoginName(loginName).toEitherT(LoginRequestError.InvalidLoginPassword)
        _ <- passwordHasherService
          .checkPassword(password, userDetails.hashedPassword)
          .lift
          .ensure(LoginRequestError.InvalidLoginPassword)(identity)
          .biSemiflatTap(_ => logi("Login failed. Invalid password!"), _ => logi("Login was successful!"))

        token <- authService.createToken(userDetails, List("silly", "permissions", "for", "now", "!")).lift
      } yield token

      res.value.map(JobResult.LoginRequestResult.apply)

    private val JobHandlersMap: Map[Class[? <: JobKind], JobKind => F[JobResult]] = Map(
      classOf[JobKind.GetDirectorsDetailsByName]  -> getDirectorsDetailsByName,
      classOf[JobKind.GetDirectorDetails]         -> getDirectorDetails,
      classOf[JobKind.GetActorDetails]            -> getActorDetails,
      classOf[JobKind.GetMoviesByDirector]        -> getMoviesByDirector,
      classOf[JobKind.GetMovie]                   -> getMovie,
      classOf[JobKind.GetMovieWithCounting]       -> getMovieWithCounting,
      classOf[JobKind.CreateMovie]                -> createMovie,
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

  private def createWorker[F[_]: { Async as async, Logger as logger }](
      queue: Queue[F, Job[F]],
      jobExecutor: JobExecutor[F],
  ): F[Nothing] =
    val processOneJob: F[Unit] = for {
      _ <- jobExecutor.logi("Waiting for work.")
      (job, deferred, uuid) <- queue.take.map(j => (j.job, j.deferred, j.uuid))
      _ <- jobExecutor.uuidScope.scope(Some(uuid)).use { _ =>
        val jobExecution = for {
          _ <- jobExecutor.logi(s"Starting to work on ${job.shortName}...")
          outcome <- jobExecutor.executeJob(job).attempt
          _ <- jobExecutor.logi("Done. Sending results back...")
          _ <- deferred.complete(outcome)
        } yield ()

        // Inner Handler: Catches errors for a specific job.
        // It logs with the trace ID and allows the worker to continue immediately.
        jobExecution.handleErrorWith { e =>
          jobExecutor.loge(e, "Error while processing job. The job will be dropped.")
        }
      }
    } yield ()

    // Outer handler.
    // This keeps the worker fiber from dying in case something happened outside the scope of the
    // inner handler (for example, while waiting on the queue).
    val processSafely = processOneJob.handleErrorWith { e =>
      jobExecutor.loge(e, "A non-recoverable error occurred in the worker loop. Restarting....") *>
        async.sleep(1.second)
    }

    processSafely.foreverM

  def startWorkers[F[_]: { Async, Logger }](deps: AppDependencies[F]): F[Unit] =
    val serverState = deps.serverState

    val jobExecutor: JobExecutor[F] =
      JobExecutor(
        deps.movieRepositoryService,
        deps.externalApiClientService,
        deps.fileSystemService,
        deps.serverStateUpdateService,
        deps.passwordHasherService,
        deps.authService,
        deps.memCaches,
        deps.uuidScope,
      )

    val numberOfWorkers = deps.appConfig.getBackendServerConfig.getNumberOfWorkers
    val worker = createWorker(serverState.jobQueue, jobExecutor)
    val supervisor = deps.supervisor
    Vector
      .from(0 until numberOfWorkers)
      .traverseVoid(_ => supervisor.supervise(worker))
