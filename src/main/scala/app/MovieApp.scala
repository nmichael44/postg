package app

import cats.data.{EitherT, Kleisli, OptionT}
import cats.effect.*
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import cats.Applicative

import java.time.Clock
import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

import app.serviceslive.{AuthServiceLive, ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive, ServerStateUpdateServiceLive}
import app.AppConfig.{ActorMemCacheConfig, AppConfig, BackendServerConfig, DirectorMemCacheConfig, MemCacheConfig, MovieMemCacheConfig}
import app.JobSpecs.{FetchSystemUserError, JobKind, JobResult}
import app.JobSpecs.JobKind.{CreateMovie, CreateSystemUser, FetchCompanyData, FetchJsonObject, FetchSystemUserByLoginName, FetchSystemUserByUserId, GetActorDetails, GetDirectorDetails, GetDirectorsDetailsByName, GetFileContent, GetMovie, GetMovieWithCounting, GetMoviesByDirector, LoginRequest, ReadTwoFilesInParallel}
import app.JobSpecs.JobResult.{ActorDetailsResult, CompanyDataResult, CreateMovieResult, CreateSystemUserResult, DirectorDetailsResult, DirectorsDetailsByNameResult, FetchSystemUserByLoginNameResult, FetchSystemUserByUserIdResult, FileContentResult, JsonObjectResult, LoginRequestResult, MovieDetailsResult, MovieWithCountingResult, MoviesByDirectorResult, TwoFilesInParallelResult}
import app.MovieDbModel.DirectorPath
import app.Utils as U
import com.comcast.ip4s.{Ipv4Address, Port}
import doobie.util.transactor.Transactor
import fs2.io.net.Network
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.middleware.FollowRedirect
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.dsl.io.*
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.{`WWW-Authenticate`, Authorization}
import org.http4s.implicits.*
import org.http4s.server.{AuthMiddleware, Router}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.{Logger, LoggerName}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import services.{AuthService, ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerState, ServerStateUpdateService}
import AuthUtils.AuthenticatedUser

object MovieApp:
  private[app] final case class LiveServerState[F[_]](
      movieRequestCounts: Ref[F, Map[Long, Int]],
      jobQueue: Queue[F, HttpWorker.Job[F]],
  ) extends ServerState[F]

  private[app] object LiveServerState:
    def create[F[_]: Async](backendServer: BackendServerConfig): F[ServerState[F]] =
      val boundedQueueCapacity = backendServer.getBoundedQueueCapacity
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQueue <- Queue.bounded[F, HttpWorker.Job[F]](boundedQueueCapacity)

      } yield LiveServerState[F](movieReqCounts, jobQueue)

  private[app] enum WebServiceResult:
    case OkStringRes(s: String)
    case OkJsonRes(json: Json)
    case NotFoundRes(s: String)
    case BadRequestRes(e: String)
    case UnauthorizedRes(e: String)
    case InternalServerErrorRes()

  private[app] final class Render[F[_]: Async as async](dsl: Http4sDsl[F]):
    import dsl.*
    import WebServiceResult.*

    private val NotImplemented: Exception =
      new Exception("MovieRepositoryService not properly overridden in test") with NoStackTrace

    private val ErrorChallenge: Challenge = Challenge(
      scheme = "Bearer",
      realm = "neo_token_service",
      params = Map("error" -> "invalid_grant", "error_description" -> "Invalid username or password"),
    )

    private val ResultHandlerMap: Map[Class[? <: WebServiceResult], WebServiceResult => F[Response[F]]] = Map(
      classOf[OkStringRes]   -> { wsr => Ok(wsr.asInstanceOf[OkStringRes].s) },
      classOf[OkJsonRes]     -> { wsr => Ok(wsr.asInstanceOf[OkJsonRes].json) },
      classOf[NotFoundRes]   -> { wsr => NotFound(wsr.asInstanceOf[NotFoundRes].s) },
      classOf[BadRequestRes] -> { wsr => BadRequest(wsr.asInstanceOf[BadRequestRes].e) },
      classOf[UnauthorizedRes] -> { wsr =>
        Unauthorized(`WWW-Authenticate`(ErrorChallenge), wsr.asInstanceOf[UnauthorizedRes].e)
      },
      classOf[InternalServerErrorRes] -> { _ => InternalServerError() },
    )

    def apply(wsr: WebServiceResult): F[Response[F]] =
      ResultHandlerMap
        .get(wsr.getClass)
        .map(_(wsr))
        .getOrElse(async.raiseError(NotImplemented))

  private def jobHandler[F[_]: { Async as async, Logger }, T <: JobResult](
      msg: String,
      serverState: ServerState[F],
      job: JobKind,
      f: T => WebServiceResult,
  ): F[WebServiceResult] =
    val jobName = job.shortName
    val prompt = s"Job '$jobName'"
    val res: F[Either[Throwable, JobResult]] = for {
      _ <- U.logi(msg)
      deferred <- Deferred[F, Either[Throwable, JobResult]]
      _ <- U.logi(s"$jobName: being queued.")
      _ <- serverState.jobQueue.offer(HttpWorker.Job(job, deferred))
      _ <- U.logi(s"$prompt: queued. Waiting for response.")
      outcome <- deferred.get // Wait for the answer
      _ <- U.logi(s"$prompt: Response received.")
      _ <- outcome match {
        case Right(_) => U.logi(s"$prompt: Successful response.")
        case Left(e) => U.loge(e, s"$prompt: Failed with exception.")
      }
    } yield outcome

    res.map(mkResponse[F, T](_, f))

  private def mkResponse[F[_]: Async, T](
      resEither: Either[Throwable, JobResult],
      f: T => WebServiceResult,
  ): WebServiceResult =
    resEither.fold(_ => WebServiceResult.InternalServerErrorRes(), jr => f(jr.asInstanceOf[T]))

  private def getDirectorsDetailsByName[F[_]: { Async, Logger as logger }](
      ctxReq: ContextRequest[F, AuthenticatedUser],
      serverState: ServerState[F],
      directorPath: DirectorPath,
  ): F[WebServiceResult] =
    ensureOnlyAllowedParams(allowedParamsForGetDirectors, ctxReq)
      .getOrElse {
        jobHandler[F, DirectorsDetailsByNameResult](
          "Fetching directors details by name.",
          serverState,
          GetDirectorsDetailsByName(directorPath.firstName, directorPath.lastName),
          dirs => WebServiceResult.OkJsonRes(dirs.asJson),
        )
      }

  private def getDirectorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, DirectorDetailsResult](
      "Fetching directors details.",
      serverState,
      GetDirectorDetails(directorId),
      _.director.fold(WebServiceResult.NotFoundRes("Director not found"))(dir => WebServiceResult.OkJsonRes(dir.asJson)),
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      actorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, ActorDetailsResult](
      "Fetching actor details.",
      serverState,
      GetActorDetails(actorId),
      _.actor.fold(WebServiceResult.NotFoundRes("Actor not found"))(act => WebServiceResult.OkJsonRes(act.asJson)),
    )

  private val firstNameParam: String = "firstName"

  private object firstNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](firstNameParam)

  private val lastNameParam: String = "lastName"

  private object lastNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](lastNameParam)

  private val allowedParamsForGetDirectors: Set[String] = Set(firstNameParam, lastNameParam)

  private object fileNameQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName")

  private object fileName1QueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName1")

  private object fileName2QueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("fileName2")

  private object titleQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("title")

  private object yearQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Int]("year")

  private def getMoviesByDirector[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, MoviesByDirectorResult](
      "Fetching movies by director Id.",
      serverState,
      GetMoviesByDirector(directorId),
      mvs => WebServiceResult.OkJsonRes(mvs.asJson),
    )

  private def getMovie[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      movieId: Long,
  ): F[WebServiceResult] =
    jobHandler[F, MovieDetailsResult](
      "Fetching movie by Id.",
      serverState,
      GetMovie(movieId),
      _.movie.fold(WebServiceResult.NotFoundRes("Movie not found"))(mv => WebServiceResult.OkJsonRes(mv.asJson)),
    )

  private def getMovieWithCounting[F[_]: { Async, Logger as logger }](
      movieId: Long,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, MovieWithCountingResult](
      "Fetching movie by Id with counting.",
      serverState,
      GetMovieWithCounting(movieId),
      _.movie.fold(WebServiceResult.BadRequestRes(s"Movie id: '$movieId' not found!"))(mv =>
        WebServiceResult.OkJsonRes(mv.asJson),
      ),
    )

  private def createMovie[F[_]: { Async, Logger as logger }](
      title: String,
      year: Int,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, CreateMovieResult](
      "Creating new movie.",
      serverState,
      CreateMovie(title, year),
      cmr => WebServiceResult.OkJsonRes(cmr.asJson),
    )

  private def getFileContent[F[_]: { Async, Logger as logger }](
      fileName: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, FileContentResult](
      "Getting file content.",
      serverState,
      GetFileContent(fileName),
      fc => WebServiceResult.OkStringRes(fc.content),
    )

  private def readTwoFilesInParallel[F[_]: { Async, Logger as logger }](
      fileName1: String,
      fileName2: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, TwoFilesInParallelResult](
      "Reading two files in parallel.",
      serverState,
      ReadTwoFilesInParallel(fileName1, fileName2),
      tfp => WebServiceResult.OkStringRes(tfp.content),
    )

  private def fetchCompanyData[F[_]: { Async, Logger }](companyName: String, serverState: ServerState[F]): F[WebServiceResult] =
    jobHandler[F, CompanyDataResult](
      "Fetching company data.",
      serverState,
      FetchCompanyData(companyName),
      cd => WebServiceResult.OkStringRes(cd.companyData),
    )

  private def fetchJasonObject[F[_]: { Async, Logger as logger }](serverState: ServerState[F]): F[WebServiceResult] =
    jobHandler[F, JsonObjectResult](
      "Fetching json object.",
      serverState,
      FetchJsonObject(),
      jor => WebServiceResult.OkJsonRes(jor.json),
    )

  private given [F[_]: Async]: EntityDecoder[F, MovieDbModel.UserDetails] =
    jsonOf[F, MovieDbModel.UserDetails]

  private def createSystemUser[F[_]: { Async, Logger }](
      req: Request[F],
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    req.as[MovieDbModel.UserDetails] >>= { userDetails =>
      jobHandler[F, CreateSystemUserResult](
        "Creating system user.",
        serverState,
        CreateSystemUser(userDetails),
        csur => WebServiceResult.OkJsonRes(csur.asJson),
      )
    }

  private def createSystemUser[F[_]: { Async, Logger }](
      ctxReq: ContextRequest[F, AuthenticatedUser],
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    createSystemUser(ctxReq.req, serverState)

  def fetchSystemUserByLoginName[F[_]: { Async, Logger }](
      loginName: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, FetchSystemUserByLoginNameResult](
      "Fetching system user by loginName.",
      serverState,
      FetchSystemUserByLoginName(loginName),
      { case FetchSystemUserByLoginNameResult(res) =>
        res match {
          case Left(_) => WebServiceResult.NotFoundRes(s"The given loginName '$loginName' was not found.")
          case Right(r) => WebServiceResult.OkJsonRes(r.asJson)
        }
      },
    )

  private def fetchSystemUserByUserId[F[_]: { Async, Logger }](
      userIdStr: String,
      serverState: ServerState[F],
  ): F[WebServiceResult] =
    jobHandler[F, FetchSystemUserByUserIdResult](
      "Fetching system user by UserId.",
      serverState,
      FetchSystemUserByUserId(userIdStr),
      { case FetchSystemUserByUserIdResult(res) =>
        res match {
          case Left(FetchSystemUserError.NotFound) =>
            WebServiceResult.NotFoundRes(s"The given userId '$userIdStr' was not found.")
          case Left(FetchSystemUserError.BadInput) =>
            WebServiceResult.BadRequestRes(s"The given userId '$userIdStr' was not a valid integer.")
          case Right(r) => WebServiceResult.OkJsonRes(r.asJson)
        }
      },
    )

  private def processLoginRequest[F[_]: { Async, Logger }](req: Request[F], serverState: ServerState[F]): F[WebServiceResult] =
    req.as[MovieDbModel.UserDetails] >>= { userDetails =>
      jobHandler[F, LoginRequestResult](
        "Processing login request.",
        serverState,
        LoginRequest(userDetails),
        { case LoginRequestResult(res) =>
          res match {
            case Left(_) => WebServiceResult.UnauthorizedRes("Invalid loginName/password specified.")
            case Right(token) => WebServiceResult.OkJsonRes(Json.obj("token" -> token.asJson))
          }
        },
      )
    }

  given CanEqual[CIString, CIString] = CanEqual.derived

  private def authMiddleware[F[_]: Async](
      authService: AuthService[F],
      dsl: Http4sDsl[F],
  ): AuthMiddleware[F, AuthenticatedUser] =
    import dsl.*

    val authUser: Kleisli[F, Request[F], Either[String, AuthenticatedUser]] = Kleisli { request =>
      val eitherToken: Either[String, String] =
        request.headers.get[Authorization] match {
          case Some(Authorization(Credentials.Token(AuthScheme.Bearer, token))) => Right(token)
          case _ => Left("Bearer token in Authorization header not found.")
        }

      (for {
        tokenStr <- EitherT.fromEither(eitherToken)
        authUser <- EitherT(authService.validateToken(tokenStr).map(_.left.map(_.getMessage)))
      } yield authUser).value
    }

    val onFailure: AuthedRoutes[String, F] = Kleisli.liftF(OptionT.liftF(Forbidden("Invalid token!")))

    AuthMiddleware(authUser, onFailure)

  private type PF[T, R] = PartialFunction[T, R]
  private type ReqToWsr[F[_]] = PF[Request[F], F[WebServiceResult]]
  private type CtxReqToWsr[F[_]] = PF[ContextRequest[F, AuthenticatedUser], F[WebServiceResult]]

  given CanEqual[Method, Method] = CanEqual.derived
  given CanEqual[Uri.Path, Uri.Path] = CanEqual.derived

  private def publicRoutes[F[_]: { Async, Logger }](serverState: ServerState[F]): ReqToWsr[F] =
    case req @ POST -> Root / "login" =>
      processLoginRequest(req, serverState)
    // This should be removed after we are done testing.
    case req @ POST -> Root / "createSystemUser" =>
      createSystemUser(req, serverState)

  private def authedRoutes[F[_]: { Async, Logger }](serverState: ServerState[F]): CtxReqToWsr[F] =
    case ctxReq @ GET -> Root / "getDirectorsByName" :?
        firstNameOptionalQueryParamDecoderMatcher(firstName) +&
        lastNameOptionalQueryParamDecoderMatcher(lastName) as _ =>
      getDirectorsDetailsByName(ctxReq, serverState, DirectorPath(firstName, lastName))
    case GET -> Root / "getDirector" / LongVar(directorId) as _ =>
      getDirectorDetails(serverState, directorId)
    case GET -> Root / "getActor" / LongVar(actorId) as _ =>
      getActorDetails(serverState, actorId)
    case GET -> Root / "getMoviesByDirector" / LongVar(directorId) as _ =>
      getMoviesByDirector(serverState, directorId)
    case GET -> Root / "getMovie" / LongVar(movieId) as _ =>
      getMovie(serverState, movieId)
    case GET -> Root / "getMovieWithCounting" / LongVar(movieId) as _ =>
      getMovieWithCounting(movieId, serverState)
    case POST -> Root / "createMovie" :?
        titleQueryParamDecoderMatcher(title) +&
        yearQueryParamDecoderMatcher(year) as _ =>
      createMovie(title, year, serverState)
    case GET -> Root / "getFile" :? fileNameQueryParamDecoderMatcher(fileName) as _ =>
      getFileContent(fileName, serverState)
    case GET -> Root / "readTwoFilesInParallel" :?
        fileName1QueryParamDecoderMatcher(fileName1) +&
        fileName2QueryParamDecoderMatcher(fileName2) as _ =>
      readTwoFilesInParallel(fileName1, fileName2, serverState)
    case GET -> Root / "fetchCompanyData" / companyName as _ =>
      fetchCompanyData(companyName, serverState)
    case GET -> Root / "getJsonObject" as _ =>
      fetchJasonObject(serverState)
    case ctxReq @ POST -> Root / "createSystemUser" as _ =>
      createSystemUser(ctxReq, serverState)
    case GET -> Root / "fetchSystemUserByLoginName" / loginName as _ =>
      fetchSystemUserByLoginName(loginName, serverState)
    case GET -> Root / "fetchSystemUserByUserId" / userIdStr as _ =>
      fetchSystemUserByUserId(userIdStr, serverState)

  private def publicRoutesPath[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      render: Render[F],
  ): (String, HttpRoutes[F]) =
    "/" -> HttpRoutes.of[F](publicRoutes(serverState).andThen(_ >>= render.apply))

  private def apiRoutesPath[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      authService: AuthService[F],
      dsl: Http4sDsl[F],
      render: Render[F],
  ): (String, HttpRoutes[F]) =
    val authRoutes: AuthedRoutes[AuthenticatedUser, F] =
      AuthedRoutes.of[AuthenticatedUser, F](authedRoutes(serverState).andThen(_ >>= render.apply))

    "/api" -> authMiddleware(authService, dsl)(authRoutes)

  private def allRoutes[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      authService: AuthService[F],
      dsl: Http4sDsl[F],
      render: Render[F],
  ): HttpApp[F] =
    Router[F](
      publicRoutesPath(serverState, render),
      apiRoutesPath(serverState, authService, dsl, render),
    ).orNotFound

  private def ensureOnlyAllowedParams[F[_]: Applicative as app](
      allowedParams: Set[String],
      ctxReq: ContextRequest[F, AuthenticatedUser],
  ): Option[F[WebServiceResult]] =
    val providedParams = ctxReq.req.multiParams.keySet
    val extraParams = providedParams -- allowedParams

    Option.when(extraParams.nonEmpty)(
      app.pure(
        WebServiceResult.BadRequestRes(
          s"Extra params found in quest: ${extraParams.mkString(", ")}.",
        ),
      ),
    )

  private def getServerHostIPPort(appConfig: AppConfig): (Ipv4Address, Port) =
    val serverConnection = appConfig.getServerConnectionConfig
    val (host, port) = (serverConnection.getHost, serverConnection.getPort)

    (Ipv4Address.fromString(host), Port.fromInt(port)) match {
      case (Some(ipv4Address), Some(port)) => (ipv4Address, port)
      case (None, _) => throw AssertionError(s"Illegal ServerHostIP: '$host'.")
      case (_, None) => throw AssertionError(s"Illegal ServerHostPort: '$port'.")
    }

  private def createCache[F[_]: { Temporal, Logger }, T](
      cacheName: String,
      capacity: Int,
      cleanupDuration: FiniteDuration,
  ): Resource[F, MemCache[F, Long, T]] =
    MemCache.createResource[F, Long, T](cacheName, capacity, cleanupDuration)

  private def createDirectorMemCache[F[_]: { Temporal, Logger }](
      directorMemCacheConfig: DirectorMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Director]] =
    createCache[F, MovieDbModel.Director](
      "Director MemCache",
      directorMemCacheConfig.getCapacity,
      directorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  private def createActorMemCache[F[_]: { Temporal, Logger }](
      actorMemCacheConfig: ActorMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Actor]] =
    createCache[F, MovieDbModel.Actor](
      "Actor MemCache",
      actorMemCacheConfig.getCapacity,
      actorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  private def createMovieMemCache[F[_]: { Temporal, Logger }](
      movieMemCacheConfig: MovieMemCacheConfig,
  ): Resource[F, MemCache[F, Long, MovieDbModel.Movie]] =
    createCache[F, MovieDbModel.Movie](
      "Movie MemCache",
      movieMemCacheConfig.getCapacity,
      movieMemCacheConfig.getCleanupDurationInMillis.milliseconds,
    )

  final class AppMemCaches[F[_]](
      val directorCache: (Boolean, MemCache[F, Long, MovieDbModel.Director]),
      val actorCache: (Boolean, MemCache[F, Long, MovieDbModel.Actor]),
      val movieCache: (Boolean, MemCache[F, Long, MovieDbModel.Movie]),
  )

  private def createMemCaches[F[_]: { Temporal, Logger }](mcc: MemCacheConfig): Resource[F, AppMemCaches[F]] =
    (
      createDirectorMemCache(mcc.getDirectorMemCacheConfig).tupleLeft(mcc.getDirectorMemCacheConfig.getCacheEnabled),
      createActorMemCache(mcc.getActorMemCacheConfig).tupleLeft(mcc.getActorMemCacheConfig.getCacheEnabled),
      createMovieMemCache(mcc.getMovieMemCacheConfig).tupleLeft(mcc.getMovieMemCacheConfig.getCacheEnabled),
    ).mapN((d, a, m) => AppMemCaches[F](d, a, m))

  private def createConfigResource[F[_]: { Async as async, Logger }]() =
    Resource.eval[F, AppConfig](
      async.fromEither(
        ConfigSource.default
          .at("app-config")
          .load[AppConfig]
          .left
          .map(pureconfig.error.ConfigReaderException[AppConfig]),
      ),
    )

  private def createServerResource[F[_]: { Async, Network, Logger }](
      serverHostIP: Ipv4Address,
      serverHostPort: Port,
      httpApp: HttpApp[F],
  ): Resource[F, http4s.server.Server] =
    EmberServerBuilder
      .default[F]
      .withHost(serverHostIP)
      .withPort(serverHostPort)
      .withShutdownTimeout(5.seconds)
      .withHttpApp(httpApp)
      .build

  private type CoreResources[F[_]] =
    Resource[
      F,
      (
          AppConfig,
          ServerState[F],
          http4s.client.Client[F],
          Supervisor[F],
          Transactor[F],
          AppMemCaches[F],
      ),
    ]

  private def runHttpApp[F[_]: { Async, Network, Logger }](
      serverState: ServerState[F],
      authService: AuthService[F],
      appConfig: AppConfig,
  ): F[ExitCode] =
    val dsl: Http4sDsl[F] = Http4sDsl[F]
    val render: Render[F] = Render(dsl)
    val (serverHostIP, serverHostPort) = getServerHostIPPort(appConfig)
    val httpApp: HttpApp[F] = allRoutes[F](serverState, authService, dsl, render)

    createServerResource(serverHostIP, serverHostPort, httpApp)
      .use(server => U.logi(s"Server started with base uri: '${server.baseUri.toString}'.") *> Async[F].never)
      .as(ExitCode.Success)

  // This is the number of redirects Ember will perform when a response
  // specifies that it needs a redirection.
  inline private val MaxHttpClientRedirects = 5

  def run: IO[ExitCode] =
    type F = IO

    implicit val MovieAppLoggerName: LoggerName = LoggerName("MovieAppLogger")

    Slf4jLogger.create[F] >>= { implicit logger =>
      val coreResources: CoreResources[F] = for {
        appConfig <- createConfigResource[F]()
        appMemCaches <- createMemCaches[F](appConfig.getMemCacheConfig)
        serverState <- Resource.eval(LiveServerState.create[F](appConfig.getBackendServerConfig))
        httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxHttpClientRedirects))
        supervisor <- Supervisor[F](await = false)
        xa <- DoobieObj.xaResource[F](appConfig.getDbConnectionConfig)
      } yield (appConfig, serverState, httpClient, supervisor, xa, appMemCaches)

      coreResources.use { (appConfig, serverState, httpClient, supervisor, xa, appMemCaches) =>
        val externalApiClientService: ExternalApiClientService[F] = ExternalApiClientServiceLive.create[F](httpClient)
        val movieRepositoryService: MovieRepositoryService[F] = MovieRepositoryServiceLive.create[F](xa)
        val fileSystemService: FileSystemService[F] = FileSystemServiceLive.create[F]
        val serverStateUpdateService: ServerStateUpdateService[F] = ServerStateUpdateServiceLive.create[F](serverState)
        val passwordHasherService: PasswordHasher[F] = PasswordHasherLive.create[F]

        val authService: AuthService[F] = AuthServiceLive.create[F](appConfig.getAuthConfig, Clock.systemUTC())

        HttpWorker.startWorkers[F](
          appConfig.getBackendServerConfig,
          movieRepositoryService,
          externalApiClientService,
          fileSystemService,
          serverStateUpdateService,
          passwordHasherService,
          authService,
          serverState.jobQueue,
          supervisor,
          appMemCaches,
        ) *> runHttpApp[F](serverState, authService, appConfig)
      }
    }
