package app

import cats.data.{EitherT, Kleisli, OptionT}
import cats.effect.*
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.syntax.all.*
import cats.Applicative

import scala.concurrent.duration.*
import scala.util.control.NoStackTrace

import app.serviceslive.{AuthServiceLive, ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive, ServerStateUpdateServiceLive}
import app.AppConfig.{ActorMemCacheConfig, AppConfig, BackendServerConfig, DirectorMemCacheConfig, MemCacheConfig, MovieMemCacheConfig}
import app.JobSpecs.{CreateSystemUserError, FetchSystemUserError, JobKind, JobResult}
import app.JobSpecs.JobKind.{CreateMovie, CreateSystemUser, FetchSystemUserByLoginName, FetchSystemUserByUserId, GetActorDetails, GetDirectorDetails, GetDirectorsDetailsByName, GetMovie, GetMovieWithCounting, GetMoviesByDirector, LoginRequest}
import app.JobSpecs.JobResult.{ActorDetailsResult, CreateMovieResult, CreateSystemUserResult, DirectorDetailsResult, DirectorsDetailsByNameResult, FetchSystemUserByLoginNameResult, FetchSystemUserByUserIdResult, LoginRequestResult, MovieDetailsResult, MovieWithCountingResult, MoviesByDirectorResult}
import app.MovieDbModel.DirectorPath
import app.Utils as U
import com.comcast.ip4s.{Ipv4Address, Port}
import fs2.io.net.Network
import io.circe.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import org.http4s
import org.http4s.*
import org.http4s.circe.*
import org.http4s.client.middleware.FollowRedirect
import org.http4s.client.Client
import org.http4s.dsl.impl.OptionalQueryParamDecoderMatcher
import org.http4s.dsl.io.*
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.ember.server.EmberServerBuilder
import org.http4s.headers.{`WWW-Authenticate`, Authorization}
import org.http4s.implicits.*
import org.http4s.server.{AuthMiddleware, Router}
import org.typelevel.ci.CIString
import org.typelevel.log4cats.{Logger, LoggerName, StructuredLogger}
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
    def create[F[_]: Async as async](backendServer: BackendServerConfig): F[ServerState[F]] =
      val boundedQueueCapacity = backendServer.getBoundedQueueCapacity
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQueue <- Queue.bounded[F, HttpWorker.Job[F]](boundedQueueCapacity)
      } yield LiveServerState[F](movieReqCounts, jobQueue)

  private[app] enum WebServiceResult:
    case OkJsonRes(json: Json)
    case NotFoundRes(s: String)
    case ConflictRes(s: String)
    case BadRequestRes(e: String)
    case UnauthorizedRes(e: String)
    case InternalServerErrorRes()

  private[app] final class Render[F[_]: Async as async](dsl: Http4sDsl[F]):
    import dsl.*
    import org.http4s.circe.CirceEntityEncoder.*
    import WebServiceResult.*

    private val NotImplemented: Exception =
      new Exception("MovieRepositoryService not properly overridden in test") with NoStackTrace

    private val ErrorChallenge: Challenge = Challenge(
      scheme = "Bearer",
      realm = "neo_token_service",
      params = Map("error" -> "invalid_grant", "error_description" -> "Invalid username or password"),
    )

    private final case class ApiError(message: String, errorCode: String, timestamp: java.time.Instant)

    private object ApiError:
      def apply(message: String): F[ApiError] =
        apply(message, "")

      def apply(message: String, errorCode: String): F[ApiError] =
        TimeUtils.nowInstant.map(ApiError(message, errorCode, _))

    private def okJsonToResponse(wsr: WebServiceResult): F[Response[F]] =
      Ok(wsr.asInstanceOf[OkJsonRes].json)

    private def noFoundToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[NotFoundRes].s, "NOTFOUND") >>= (apiErr => NotFound(apiErr))

    private def conflictToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[ConflictRes].s, "CONFLICT") >>= (apiErr => Conflict(apiErr))

    private def badRequestToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[BadRequestRes].e, "BADREQUEST") >>= (apiErr => BadRequest(apiErr))

    private def unauthorizedToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[BadRequestRes].e, "UNAUTHORIZED") >>= { apiErr =>
        Unauthorized(`WWW-Authenticate`(ErrorChallenge), apiErr)
      }

    private def internalServerErrorToResponse(wsr: WebServiceResult): F[Response[F]] =
      InternalServerError()

    private val ResultHandlerMap: Map[Class[? <: WebServiceResult], WebServiceResult => F[Response[F]]] = Map(
      classOf[OkJsonRes]              -> okJsonToResponse,
      classOf[NotFoundRes]            -> noFoundToResponse,
      classOf[ConflictRes]            -> conflictToResponse,
      classOf[BadRequestRes]          -> badRequestToResponse,
      classOf[UnauthorizedRes]        -> unauthorizedToResponse,
      classOf[InternalServerErrorRes] -> internalServerErrorToResponse,
    )

    def apply(wsr: WebServiceResult): F[Response[F]] =
      ResultHandlerMap
        .get(wsr.getClass)
        .map(_(wsr))
        .getOrElse(async.raiseError(NotImplemented))

  private val FiberName = "http4sFiber"

  private def logi[F[_]: Logger](s: String): F[Unit] =
    U.logi(FiberName, s)

  private def loge[F[_]: Logger](e: Throwable, uuid: String, s: String): F[Unit] =
    U.loge(e, FiberName, uuid, s)

  private def logi[F[_]: Logger](uuid: String, s: String): F[Unit] =
    U.logi(FiberName, uuid, s)

  private def jobHandler[F[_]: { Async as async, Logger }, T <: JobResult](
      req: Request[F],
      serverState: ServerState[F],
      uuidGen: UUIDGenerator[F],
      job: JobKind,
      f: T => WebServiceResult,
  ): F[WebServiceResult] =
    val res: F[Either[Throwable, JobResult]] = for {
      _ <- logi("Finding XRequestId header.")
      uuid <- RequestHeaderUtils
        .getXRequestId(req)
        .fold(logi("... not found -- generating.") *> uuidGen.generateUUIDAsString) { headerUuid =>
          logi("... found!") *> async.pure(headerUuid)
        }
      _ <- logi(uuid, "Processing request.")
      deferred <- Deferred[F, Either[Throwable, JobResult]]
      _ <- logi(uuid, "Request being queued.")
      _ <- serverState.jobQueue.offer(HttpWorker.Job(job, deferred, uuid))
      _ <- logi(uuid, "Waiting for response.")
      outcome <- deferred.get // Wait for the answer
      _ <- logi(uuid, "Response received.")
      _ <- outcome match {
        case Right(_) => logi(uuid, "Successful response.")
        case Left(e) => loge(e, uuid, "Failed with exception.")
      }
    } yield outcome

    res.map(mkResponse[F, T](_, f))

  private def mkResponse[F[_]: Async, T](
      resEither: Either[Throwable, JobResult],
      f: T => WebServiceResult,
  ): WebServiceResult =
    resEither.fold(_ => WebServiceResult.InternalServerErrorRes(), jr => f(jr.asInstanceOf[T]))

  private def getDirectorsDetailsByName[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorPath: DirectorPath,
  ): F[WebServiceResult] = {
    val req: Request[F] = ctxReq.req
    ensureOnlyAllowedParams(allowedParamsForGetDirectors, req)
      .getOrElse {
        jobHandler[F, DirectorsDetailsByNameResult](
          req,
          serverState,
          uuidGen,
          GetDirectorsDetailsByName(directorPath.firstName, directorPath.lastName),
          dirs => WebServiceResult.OkJsonRes(dirs.asJson),
        )
      }
  }

  private def getDirectorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorId: Long,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, DirectorDetailsResult](
      req,
      serverState,
      uuidGen,
      GetDirectorDetails(directorId),
      _.director.fold(WebServiceResult.NotFoundRes("Director not found"))(dir => WebServiceResult.OkJsonRes(dir.asJson)),
    )

  private def getActorDetails[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      actorId: Long,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, ActorDetailsResult](
      req,
      serverState,
      uuidGen,
      GetActorDetails(actorId),
      _.actor.fold(WebServiceResult.NotFoundRes("Actor not found"))(act => WebServiceResult.OkJsonRes(act.asJson)),
    )

  private val firstNameParam: String = "firstName"

  private object firstNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](firstNameParam)

  private val lastNameParam: String = "lastName"

  private object lastNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](lastNameParam)

  private val allowedParamsForGetDirectors: Set[String] = Set(firstNameParam, lastNameParam)

  private object titleQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("title")

  private object yearQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Int]("year")

  private def getMoviesByDirector[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorId: Long,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, MoviesByDirectorResult](
      req,
      serverState,
      uuidGen,
      GetMoviesByDirector(directorId),
      mvs => WebServiceResult.OkJsonRes(mvs.asJson),
    )

  private def getMovie[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      movieId: Long,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, MovieDetailsResult](
      req,
      serverState,
      uuidGen,
      GetMovie(movieId),
      _.movie.fold(WebServiceResult.NotFoundRes("Movie not found"))(mv => WebServiceResult.OkJsonRes(mv.asJson)),
    )

  private def getMovieWithCounting[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      movieId: Long,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, MovieWithCountingResult](
      req,
      serverState,
      uuidGen,
      GetMovieWithCounting(movieId),
      _.movie.fold(WebServiceResult.BadRequestRes(s"Movie id: '$movieId' not found!"))(mv =>
        WebServiceResult.OkJsonRes(mv.asJson),
      ),
    )

  private def createMovie[F[_]: { Async, Logger as logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      title: String,
      year: Int,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, CreateMovieResult](
      req,
      serverState,
      uuidGen,
      CreateMovie(title, year),
      cmr => WebServiceResult.OkJsonRes(cmr.asJson),
    )

  private given [F[_]: Async]: EntityDecoder[F, MovieDbModel.UserDetails] =
    jsonOf[F, MovieDbModel.UserDetails]

  private def createSystemUser[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      req: Request[F],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    req.as[MovieDbModel.UserDetails] >>= { userDetails =>
      jobHandler[F, CreateSystemUserResult](
        req,
        serverState,
        uuidGen,
        CreateSystemUser(userDetails),
        { case CreateSystemUserResult(res) =>
          res match {
            case Left(CreateSystemUserError.DuplicateLoginNameInDB(loginName)) =>
              WebServiceResult.ConflictRes(s"The given loginName '$loginName' was already present in the database.")
            case Left(CreateSystemUserError.BadPassword(errorList)) =>
              val errorStr = errorList.toVector.mkString("\"", "\", \"", "\"")
              WebServiceResult.BadRequestRes(s"Invalid password. Errors: [$errorStr]")
            case Right(userId) =>
              WebServiceResult.OkJsonRes(Json.obj("userId" -> userId.asJson))
          }
        },
      )
    }

  private def createSystemUser[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    createSystemUser(serverState, req, uuidGen)

  def fetchSystemUserByLoginName[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      loginName: String,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, FetchSystemUserByLoginNameResult](
      req,
      serverState,
      uuidGen,
      FetchSystemUserByLoginName(loginName),
      { case FetchSystemUserByLoginNameResult(res) =>
        res match {
          case Left(_) => WebServiceResult.NotFoundRes(s"The given loginName '$loginName' was not found.")
          case Right(r) => WebServiceResult.OkJsonRes(r.asJson)
        }
      },
    )

  private def fetchSystemUserByUserId[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      userIdStr: String,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    jobHandler[F, FetchSystemUserByUserIdResult](
      req,
      serverState,
      uuidGen,
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

  private def processLoginRequest[F[_]: { Async, Logger }](
      serverState: ServerState[F],
      req: Request[F],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    req.as[MovieDbModel.UserDetails] >>= { userDetails =>
      jobHandler[F, LoginRequestResult](
        req,
        serverState,
        uuidGen,
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

  private def createAuthMiddleware[F[_]: Async](
      authService: AuthService[F],
      dsl: Http4sDsl[F],
  ): AuthMiddleware[F, AuthenticatedUser] =
    import dsl.*

    val reqToAuthUser: Kleisli[F, Request[F], Either[String, AuthenticatedUser]] = Kleisli { request =>
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

    AuthMiddleware(reqToAuthUser, onFailure)

  private type PF[T, R] = PartialFunction[T, R]
  private type ReqToWsr[F[_]] = PF[Request[F], F[WebServiceResult]]
  private type CtxReqToWsr[F[_]] = PF[ContextRequest[F, AuthenticatedUser], F[WebServiceResult]]

  given CanEqual[Method, Method] = CanEqual.derived
  given CanEqual[Uri.Path, Uri.Path] = CanEqual.derived

  private def publicRoutes[F[_]: { Async, Logger }](deps: AppDependencies[F]): ReqToWsr[F] =
    val serverState = deps.serverState
    val uuidGen = deps.uuidGen
    {
      case req @ POST -> Root / "login" =>
        processLoginRequest(serverState, req, uuidGen)
      // This should be removed after we are done testing.
      case req @ POST -> Root / "createSystemUser" =>
        createSystemUser(serverState, req, uuidGen)
    }

  private def authedRoutes[F[_]: { Async, Logger }](deps: AppDependencies[F]): CtxReqToWsr[F] =
    val serverState = deps.serverState
    val uuidGen = deps.uuidGen
    {
      case ctxReq @ GET -> Root / "getDirectorsByName" :?
          firstNameOptionalQueryParamDecoderMatcher(firstName) +&
          lastNameOptionalQueryParamDecoderMatcher(lastName) as _ =>
        getDirectorsDetailsByName(serverState, ctxReq, uuidGen, DirectorPath(firstName, lastName))
      case ctxReq @ GET -> Root / "getDirector" / LongVar(directorId) as _ =>
        getDirectorDetails(serverState, ctxReq, uuidGen, directorId)
      case ctxReq @ GET -> Root / "getActor" / LongVar(actorId) as _ =>
        getActorDetails(serverState, ctxReq, uuidGen, actorId)
      case ctxReq @ GET -> Root / "getMoviesByDirector" / LongVar(directorId) as _ =>
        getMoviesByDirector(serverState, ctxReq, uuidGen, directorId)
      case ctxReq @ GET -> Root / "getMovie" / LongVar(movieId) as _ =>
        getMovie(serverState, ctxReq, uuidGen, movieId)
      case ctxReq @ GET -> Root / "getMovieWithCounting" / LongVar(movieId) as _ =>
        getMovieWithCounting(serverState, ctxReq, uuidGen, movieId)
      case ctxReq @ POST -> Root / "createMovie" :?
          titleQueryParamDecoderMatcher(title) +&
          yearQueryParamDecoderMatcher(year) as _ =>
        createMovie(serverState, ctxReq, uuidGen, title, year)
      case ctxReq @ POST -> Root / "createSystemUser" as _ =>
        createSystemUser(serverState, ctxReq, uuidGen)
      case ctxReq @ GET -> Root / "fetchSystemUserByLoginName" / loginName as _ =>
        fetchSystemUserByLoginName(serverState, ctxReq, uuidGen, loginName)
      case ctxReq @ GET -> Root / "fetchSystemUserByUserId" / userIdStr as _ =>
        fetchSystemUserByUserId(serverState, ctxReq, uuidGen, userIdStr)
    }
  private def publicRoutesPath[F[_]: { Async, Logger }](deps: AppDependencies[F], render: Render[F]): (String, HttpRoutes[F]) =
    "/" -> HttpRoutes.of[F](publicRoutes(deps).andThen(_ >>= render.apply))

  private def apiRoutesPath[F[_]: { Async, Logger }](
      deps: AppDependencies[F],
      dsl: Http4sDsl[F],
      render: Render[F],
  ): (String, HttpRoutes[F]) =
    val authRoutes: AuthedRoutes[AuthenticatedUser, F] =
      AuthedRoutes.of[AuthenticatedUser, F](authedRoutes(deps).andThen(_ >>= render.apply))

    "/api" -> createAuthMiddleware(deps.authService, dsl)(authRoutes)

  private def allRoutes[F[_]: { Async, Logger }](deps: AppDependencies[F], dsl: Http4sDsl[F], render: Render[F]): HttpApp[F] =
    Router[F](publicRoutesPath(deps, render), apiRoutesPath(deps, dsl, render)).orNotFound

  private def ensureOnlyAllowedParams[F[_]: Applicative as app](
      allowedParams: Set[String],
      req: Request[F],
  ): Option[F[WebServiceResult]] =
    val providedParams = req.multiParams.keySet
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

  final class MemCaches[F[_]](
      val directorCache: (Boolean, MemCache[F, Long, MovieDbModel.Director]),
      val actorCache: (Boolean, MemCache[F, Long, MovieDbModel.Actor]),
      val movieCache: (Boolean, MemCache[F, Long, MovieDbModel.Movie]),
  )

  private def createMemCaches[F[_]: { Temporal, Logger }](mcc: MemCacheConfig): Resource[F, MemCaches[F]] =
    (
      createDirectorMemCache(mcc.getDirectorMemCacheConfig).tupleLeft(mcc.getDirectorMemCacheConfig.getCacheEnabled),
      createActorMemCache(mcc.getActorMemCacheConfig).tupleLeft(mcc.getActorMemCacheConfig.getCacheEnabled),
      createMovieMemCache(mcc.getMovieMemCacheConfig).tupleLeft(mcc.getMovieMemCacheConfig.getCacheEnabled),
    ).mapN((d, a, m) => MemCaches[F](d, a, m))

  private def createConfigResource[F[_]: { Async as async }]() =
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

  private def runHttpApp[F[_]: { Async, Network, Logger }](deps: AppDependencies[F]): F[ExitCode] =
    val dsl: Http4sDsl[F] = Http4sDsl[F]
    val render: Render[F] = Render(dsl)
    val (serverHostIP, serverHostPort) = getServerHostIPPort(deps.appConfig)
    val httpApp: HttpApp[F] = allRoutes[F](deps, dsl, render)

    createServerResource(serverHostIP, serverHostPort, httpApp)
      .use(server => U.logi("MainFiber", s"Server started with base uri: '${server.baseUri.toString}'.") *> Async[F].never)
      .as(ExitCode.Success)

  // This is the number of redirects Ember will perform when a response
  // specifies that it needs a redirection.
  inline private val MaxHttpClientRedirects = 5

  final class AppDependencies[F[_]](
      val appConfig: AppConfig,
      val serverState: ServerState[F],
      val memCaches: MovieApp.MemCaches[F],
      val supervisor: Supervisor[F],
      val uuidGen: UUIDGenerator[F],
      val uuidScope: TraceIdScope[F, Option[String]],

      // The other services
      val externalApiClientService: ExternalApiClientService[F],
      val movieRepositoryService: MovieRepositoryService[F],
      val fileSystemService: FileSystemService[F],
      val serverStateUpdateService: ServerStateUpdateService[F],
      val passwordHasherService: PasswordHasher[F],
      val authService: AuthService[F],
  )

  def run: IO[ExitCode] =
    type F = IO

    implicit val MovieAppLoggerName: LoggerName = LoggerName("MovieAppLogger")

    Slf4jLogger.create[F] >>= { implicit logger =>
      val appDeps: Resource[F, AppDependencies[F]] = for {
        appConfig <- createConfigResource[F]()
        memCaches <- createMemCaches[F](appConfig.getMemCacheConfig)
        serverState <- Resource.eval[F, ServerState[F]](LiveServerState.create[F](appConfig.getBackendServerConfig))
        httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxHttpClientRedirects))
        supervisor <- Supervisor[F](await = false)
        xa <- DoobieObj.xaResource[F](appConfig.getDbConnectionConfig)
        uuidGen <- UUIDGenerator.create[F]
        uuidScope <- Resource.eval[F, TraceIdScope[F, Option[String]]](TraceIdScope.fromIOLocal[Option[String]](None))
      } yield {
        val externalApiClientService: ExternalApiClientService[F] = ExternalApiClientServiceLive.create[F](httpClient)
        val movieRepositoryService: MovieRepositoryService[F] = MovieRepositoryServiceLive.create[F](xa)
        val fileSystemService: FileSystemService[F] = FileSystemServiceLive.create[F]
        val serverStateUpdateService: ServerStateUpdateService[F] = ServerStateUpdateServiceLive.create[F](serverState)
        val passwordHasherService: PasswordHasher[F] = PasswordHasherLive.create[F]
        val authService: AuthService[F] = AuthServiceLive.create[F](appConfig.getAuthConfig, java.time.Clock.systemUTC())

        AppDependencies(
          appConfig,
          serverState,
          memCaches,
          supervisor,
          uuidGen,
          uuidScope,
          externalApiClientService,
          movieRepositoryService,
          fileSystemService,
          serverStateUpdateService,
          passwordHasherService,
          authService,
        )
      }

      appDeps.use(deps => HttpWorker.startWorkers[F](deps) *> runHttpApp[F](deps))
    }
