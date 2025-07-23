package app

import cats.data.{EitherT, Kleisli, NonEmptyVector, OptionT}
import cats.effect.*
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.{Queue, Supervisor}
import cats.effect.std.Env
import cats.syntax.all.*
import cats.Applicative

import scala.annotation.unused
import scala.concurrent.duration.*

import app.permissions.Permissions.{CompiledPermissionAlgebra, Permission, PermissionAlgebra}
import app.serviceslive.{AuthServiceLive, EmailServiceAsync2Live, ExternalApiClientServiceLive, FileSystemServiceLive, MovieRepositoryServiceLive, ServerStateUpdateServiceLive}
import app.AppConfig.{ActorMemCacheConfig, AppConfig, BackendServerConfig, DirectorMemCacheConfig, MemCacheConfig, MovieMemCacheConfig, ServerConnectionConfig}
import app.ImplicitConversions.*
import app.JobSpecs.{CreateSystemUserError, FetchSystemUserError, JobKind, JobResult}
import app.JobSpecs.JobKind.{CreateMovie, CreateSystemUser, FetchSystemUserByLoginName, FetchSystemUserByUserId, GetActorDetails, GetDirectorDetails, GetDirectorsDetailsByName, GetMovie, GetMovieWithCounting, GetMoviesByDirector, LoginRequest, SendEmail}
import app.JobSpecs.JobResult.{ActorDetailsResult, CreateMovieResult, CreateSystemUserResult, DirectorDetailsResult, DirectorsDetailsByNameResult, FetchSystemUserByLoginNameResult, FetchSystemUserByUserIdResult, LoginRequestResult, MovieDetailsResult, MovieWithCountingResult, MoviesByDirectorResult, SendEmailResult}
import app.MemCaches.MemCache
import app.MovieDbModel.DirectorPath
import app.Utils as U
import com.comcast.ip4s.{Ipv4Address, Port}
import fs2.io.net.tls.*
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
import org.typelevel.log4cats.{Logger, LoggerName}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import services.{AuthService, EmailService, ExternalApiClientService, FileSystemService, MovieRepositoryService, ServerState, ServerStateUpdateService}
import MovieApp.{AppDependencies, Render, WebServiceResult}
import MovieDbModel.AuthenticatedUser

private final class MovieApp[F[_]: { Async as async, Logger as logger }] private (
    deps: AppDependencies[F],
    dsl: Http4sDsl[F],
    render: Render[F],
):
  private val FiberName = "http4sFiber"

  private def logi(s: String): F[Unit] =
    U.logi(FiberName, s)
  end logi

  private def loge(e: Throwable, uuid: String, s: String): F[Unit] =
    U.loge(e, FiberName, uuid, s)
  end loge

  private def logi(uuid: String, s: String): F[Unit] =
    U.logi(FiberName, uuid, s)
  end logi

  private val DeferredF: F[Deferred[F, Either[Throwable, JobResult]]] =
    Deferred[F, Either[Throwable, JobResult]]

  private val logFindingXRequestIdHeader: F[Unit] = logi("Finding XRequestId header.")
  private val logNotFound: F[Unit] = logi("... not found -- generating.")
  private val logFound: F[Unit] = logi("... found!")

  private def getUUIDForRequest(req: Request[F], uuidGen: UUIDGenerator[F]): F[String] =
    RequestHeaderUtils
      .getXRequestId(req)
      .fold(logNotFound *> uuidGen.generateUUIDAsString) { headerUuid =>
        logFound *> async.pure(headerUuid)
      }
  end getUUIDForRequest

  extension (user: AuthenticatedUser)
    inline private def hasPermissions(jobPermissionAlgebra: CompiledPermissionAlgebra): Boolean =
      jobPermissionAlgebra.isSatisfiedBy(user.permissions)
    end hasPermissions

  private def reportUnauthorizedUser(user: AuthenticatedUser, uuid: String, jobName: String): F[WebServiceResult] =
    val userId = user.userId

    logi(uuid, s"Authorization failure for user with id: $userId")
      .as(WebServiceResult.UnauthorizedRes(s"User ($userId) is not authorized to execute job '$jobName'."))
  end reportUnauthorizedUser

  private def jobHandler[T <: JobResult](
      ctxReq: ContextRequest[F, AuthenticatedUser],
      jobPermissionAlgebra: CompiledPermissionAlgebra,
      serverState: ServerState[F],
      uuidGen: UUIDGenerator[F],
      job: JobKind,
      f: T => WebServiceResult,
  ): F[WebServiceResult] =
    val req: Request[F] = ctxReq.req
    val user: AuthenticatedUser = ctxReq.context

    for {
      _ <- logFindingXRequestIdHeader
      uuid <- getUUIDForRequest(req, uuidGen)
      _ <- logi(uuid, "Processing request.")
      res <-
        if user.hasPermissions(jobPermissionAlgebra) then
          for {
            deferred <- DeferredF
            _ <- logi(uuid, "Permission validated. Request being queued.")
            _ <- serverState.jobQueue.offer(HttpWorker.Job(job, deferred, uuid))
            _ <- logi(uuid, "Waiting for response.")
            outcome <- deferred.get // Wait for the answer
            _ <- logi(uuid, "Response received.")
            _ <- outcome match {
              case Right(_) => logi(uuid, "Successful response.")
              case Left(e) => loge(e, uuid, "Failed with exception.")
            }
          } yield mkResponse(outcome, f)
        else reportUnauthorizedUser(user, uuid, job.shortName)
    } yield res
  end jobHandler

  private def jobHandler[T <: JobResult](
      req: Request[F],
      serverState: ServerState[F],
      uuidGen: UUIDGenerator[F],
      job: JobKind,
      f: T => WebServiceResult,
  ): F[WebServiceResult] = for {
    _ <- logFindingXRequestIdHeader
    uuid <- getUUIDForRequest(req, uuidGen)
    _ <- logi(uuid, "Processing request.")
    deferred <- DeferredF
    _ <- logi(uuid, "Request being queued.")
    _ <- serverState.jobQueue.offer(HttpWorker.Job(job, deferred, uuid))
    _ <- logi(uuid, "Waiting for response.")
    outcome <- deferred.get // Wait for the answer
    _ <- logi(uuid, "Response received.")
    _ <- outcome match {
      case Right(_) => logi(uuid, "Successful response.")
      case Left(e) => loge(e, uuid, "Failed with exception.")
    }
  } yield mkResponse(outcome, f)
  end jobHandler

  private def mkResponse[T](
      resEither: Either[Throwable, JobResult],
      f: T => WebServiceResult,
  ): WebServiceResult =
    resEither.fold(_ => WebServiceResult.InternalServerErrorRes(), jr => f(jr.asInstanceOf[T]))
  end mkResponse

  private val GetDirectorDetailsByNamePermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanReadDirectors),
          PermissionAlgebra.Has(Permission.CanReadAnything),
        ),
      )
      .compile

  private def getDirectorsDetailsByName(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorPath: DirectorPath,
  ): F[WebServiceResult] =
    MovieApp
      .ensureOnlyAllowedParams(allowedParamsForGetDirectors, ctxReq.req)
      .getOrElse {
        jobHandler[DirectorsDetailsByNameResult](
          ctxReq,
          GetDirectorDetailsByNamePermissionsAlg,
          serverState,
          uuidGen,
          GetDirectorsDetailsByName(directorPath.firstName, directorPath.lastName),
          dirs => WebServiceResult.OkJsonRes(dirs.asJson),
        )
      }
  end getDirectorsDetailsByName

  private val DirectorNotFound: WebServiceResult = WebServiceResult.NotFoundRes("Director not found")

  private val GetDirectorDetailsPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanReadDirectors),
          PermissionAlgebra.Has(Permission.CanReadAnything),
        ),
      )
      .compile

  private def getDirectorDetails(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[DirectorDetailsResult](
      ctxReq,
      GetDirectorDetailsPermissionsAlg,
      serverState,
      uuidGen,
      GetDirectorDetails(directorId),
      _.director.fold(DirectorNotFound)(dir => WebServiceResult.OkJsonRes(dir.asJson)),
    )
  end getDirectorDetails

  private val ActorNotFound: WebServiceResult = WebServiceResult.NotFoundRes("Actor not found")

  private val GetActorDetailsPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanReadActors),
          PermissionAlgebra.Has(Permission.CanReadAnything),
        ),
      )
      .compile

  private def getActorDetails(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      actorId: Long,
  ): F[WebServiceResult] =
    jobHandler[ActorDetailsResult](
      ctxReq,
      GetActorDetailsPermissionsAlg,
      serverState,
      uuidGen,
      GetActorDetails(actorId),
      _.actor.fold(ActorNotFound)(act => WebServiceResult.OkJsonRes(act.asJson)),
    )
  end getActorDetails

  private val firstNameParam: String = "firstName"

  private object firstNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](firstNameParam)

  private val lastNameParam: String = "lastName"

  private object lastNameOptionalQueryParamDecoderMatcher extends OptionalQueryParamDecoderMatcher[String](lastNameParam)

  private val allowedParamsForGetDirectors: Set[String] = Set(firstNameParam, lastNameParam)

  private object titleQueryParamDecoderMatcher extends QueryParamDecoderMatcher[String]("title")

  private object yearQueryParamDecoderMatcher extends QueryParamDecoderMatcher[Int]("year")

  private val GetMoviesByDirectorPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanReadAnything),
          PermissionAlgebra.And(
            NonEmptyVector
              .of(PermissionAlgebra.Has(Permission.CanReadDirectors), PermissionAlgebra.Has(Permission.CanReadMovies)),
          ),
        ),
      )
      .compile

  private def getMoviesByDirector(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      directorId: Long,
  ): F[WebServiceResult] =
    jobHandler[MoviesByDirectorResult](
      ctxReq,
      GetMoviesByDirectorPermissionsAlg,
      serverState,
      uuidGen,
      GetMoviesByDirector(directorId),
      mvs => WebServiceResult.OkJsonRes(mvs.asJson),
    )
  end getMoviesByDirector

  private val MovieNotFound: WebServiceResult = WebServiceResult.NotFoundRes("Movie not found")

  private val GetMovieDetailsPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanReadMovies),
          PermissionAlgebra.Has(Permission.CanReadAnything),
        ),
      )
      .compile

  private def getMovie(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      movieId: Long,
  ): F[WebServiceResult] =
    jobHandler[MovieDetailsResult](
      ctxReq,
      GetMovieDetailsPermissionsAlg,
      serverState,
      uuidGen,
      GetMovie(movieId),
      _.movie.fold(MovieNotFound)(mv => WebServiceResult.OkJsonRes(mv.asJson)),
    )
  end getMovie

  private def getMovieWithCounting(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      movieId: Long,
  ): F[WebServiceResult] =
    jobHandler[MovieWithCountingResult](
      ctxReq,
      GetMovieDetailsPermissionsAlg,
      serverState,
      uuidGen,
      GetMovieWithCounting(movieId),
      _.movie.fold(WebServiceResult.BadRequestRes(s"Movie id: '$movieId' not found!"))(mv =>
        WebServiceResult.OkJsonRes(mv.asJson),
      ),
    )
  end getMovieWithCounting

  private val CreateMoviePermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra
      .Or(
        NonEmptyVector.of(
          PermissionAlgebra.Has(Permission.CanWriteMovies),
          PermissionAlgebra.Has(Permission.CanWriteAnything),
        ),
      )
      .compile

  private def createMovie(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      title: String,
      year: Int,
  ): F[WebServiceResult] =
    jobHandler[CreateMovieResult](
      ctxReq,
      CreateMoviePermissionsAlg,
      serverState,
      uuidGen,
      CreateMovie(title, year),
      cmr => WebServiceResult.OkJsonRes(cmr.asJson),
    )
  end createMovie

  private given EntityDecoder[F, MovieDbModel.UserDetails] =
    jsonOf[F, MovieDbModel.UserDetails]

  private val CreateSystemUserPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra.Has(Permission.CanCreateSystemUser).compile

  private def createSystemUser(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    ctxReq.req.as[MovieDbModel.UserDetails].attempt >>= {
      case Left(_) => async.pure(WebServiceResult.BadRequestRes("Invalid request body"))
      case Right(userDetails) =>
        jobHandler[CreateSystemUserResult](
          ctxReq,
          CreateSystemUserPermissionsAlg,
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
  end createSystemUser

  private val FetchSystemUserPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra.Has(Permission.CanFetchSystemUser).compile

  def fetchSystemUserByLoginName(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      loginName: String,
  ): F[WebServiceResult] =
    jobHandler[FetchSystemUserByLoginNameResult](
      ctxReq,
      FetchSystemUserPermissionsAlg,
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
  end fetchSystemUserByLoginName

  private def fetchSystemUserByUserId(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
      userIdStr: String,
  ): F[WebServiceResult] =
    jobHandler[FetchSystemUserByUserIdResult](
      ctxReq,
      FetchSystemUserPermissionsAlg,
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
  end fetchSystemUserByUserId

  private given EntityDecoder[F, MovieDbModel.EmailMessage] =
    jsonOf[F, MovieDbModel.EmailMessage]

  private val SendEmailPermissionsAlg: CompiledPermissionAlgebra =
    PermissionAlgebra.Has(Permission.CanSendEmail).compile

  private val BadRequestEmail: F[WebServiceResult] =
    async.pure(WebServiceResult.BadRequestRes("Invalid request body"))

  private def sendEmail(
      serverState: ServerState[F],
      ctxReq: ContextRequest[F, AuthenticatedUser],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    ctxReq.req.as[MovieDbModel.EmailMessage].attempt >>= {
      case Left(_) => BadRequestEmail
      case Right(msg) =>
        jobHandler[SendEmailResult](
          ctxReq,
          SendEmailPermissionsAlg,
          serverState,
          uuidGen,
          SendEmail(msg),
          { case SendEmailResult(res) =>
            res match {
              case Left(errors) => WebServiceResult.BadRequestRes(s"Errors: ${errors.toString}")
              case Right(str) => WebServiceResult.OkJsonRes(Json.obj("status" -> str.asJson))
            }
          },
        )
    }
  end sendEmail

  private def processLoginRequest(
      serverState: ServerState[F],
      req: Request[F],
      uuidGen: UUIDGenerator[F],
  ): F[WebServiceResult] =
    req.as[MovieDbModel.UserDetails].attempt >>= {
      case Left(_) => async.pure(WebServiceResult.BadRequestRes("Invalid request body"))
      case Right(userDetails) =>
        jobHandler[LoginRequestResult](
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
  end processLoginRequest

  given CanEqual[CIString, CIString] = CanEqual.derived

  private def createAuthMiddleware(authService: AuthService[F]): AuthMiddleware[F, AuthenticatedUser] =
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
  end createAuthMiddleware

  private type PF[T, R] = PartialFunction[T, R]
  private type ReqToWsr[G[_]] = PF[Request[G], G[WebServiceResult]]
  private type CtxReqToWsr[G[_]] = PF[ContextRequest[G, AuthenticatedUser], G[WebServiceResult]]

  given CanEqual[Method, Method] = CanEqual.derived
  given CanEqual[Uri.Path, Uri.Path] = CanEqual.derived

  private val publicRoutes: ReqToWsr[F] =
    val serverState = deps.serverState
    val uuidGen = deps.uuidGen
    { case req @ POST -> Root / "login" => processLoginRequest(serverState, req, uuidGen) }

  private val authedRoutes: CtxReqToWsr[F] =
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
      case ctxReq @ POST -> Root / "sendEmail" as _ =>
        sendEmail(serverState, ctxReq, uuidGen)
    }

  private val publicRoutesPath: (String, HttpRoutes[F]) =
    "/" -> HttpRoutes.of[F](publicRoutes.andThen(_ >>= render.apply))

  private val apiRoutesPath: (String, HttpRoutes[F]) =
    val authRoutes: AuthedRoutes[AuthenticatedUser, F] =
      AuthedRoutes.of[AuthenticatedUser, F](authedRoutes.andThen(_ >>= render.apply))

    "/api" -> createAuthMiddleware(deps.authService)(authRoutes)

  val allRoutes: HttpApp[F] =
    Router[F](publicRoutesPath, apiRoutesPath).orNotFound
end MovieApp

object MovieApp:
  private def getServerHostIPPort[F[_]: Async as async](
      serverConnectionConfig: ServerConnectionConfig,
  ): F[(Ipv4Address, Port)] =
    val (host, port) = (serverConnectionConfig.getHost, serverConnectionConfig.getPort)

    (Ipv4Address.fromString(host), Port.fromInt(port)) match {
      case (Some(ipv4Address), Some(port)) => async.pure((ipv4Address, port))
      case (None, _) => async.raiseError(AssertionError(s"Illegal ServerHostIP: '$host'."))
      case (_, None) => async.raiseError(AssertionError(s"Illegal ServerHostPort: '$port'."))
    }
  end getServerHostIPPort

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
  end ensureOnlyAllowedParams

  private def createCache[F[_]: { Temporal, Logger }, T](
      cacheName: String,
      capacity: Int,
      cleanupDuration: FiniteDuration,
      timeTickDuration: FiniteDuration,
      enabled: Boolean,
  ): Resource[F, Option[MemCache[F, Long, T]]] =
    Option.when(enabled)(MemCache.createResource[F, Long, T](cacheName, capacity, cleanupDuration, timeTickDuration)).sequence
  end createCache

  private def createDirectorMemCache[F[_]: { Temporal, Logger }](
      directorMemCacheConfig: DirectorMemCacheConfig,
  ): Resource[F, Option[MemCache[F, Long, MovieDbModel.Director]]] =
    createCache[F, MovieDbModel.Director](
      "Director MemCache",
      directorMemCacheConfig.getCapacity,
      directorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
      directorMemCacheConfig.getTimeTickDurationInMillis.milliseconds,
      directorMemCacheConfig.getCacheEnabled,
    )
  end createDirectorMemCache

  private def createActorMemCache[F[_]: { Temporal, Logger }](
      actorMemCacheConfig: ActorMemCacheConfig,
  ): Resource[F, Option[MemCache[F, Long, MovieDbModel.Actor]]] =
    createCache[F, MovieDbModel.Actor](
      "Actor MemCache",
      actorMemCacheConfig.getCapacity,
      actorMemCacheConfig.getCleanupDurationInMillis.milliseconds,
      actorMemCacheConfig.getTimeTickDurationInMillis.milliseconds,
      actorMemCacheConfig.getCacheEnabled,
    )
  end createActorMemCache

  private def createMovieMemCache[F[_]: { Temporal, Logger }](
      movieMemCacheConfig: MovieMemCacheConfig,
  ): Resource[F, Option[MemCache[F, Long, MovieDbModel.Movie]]] =
    createCache[F, MovieDbModel.Movie](
      "Movie MemCache",
      movieMemCacheConfig.getCapacity,
      movieMemCacheConfig.getCleanupDurationInMillis.milliseconds,
      movieMemCacheConfig.getTimeTickDurationInMillis.milliseconds,
      movieMemCacheConfig.getCacheEnabled,
    )
  end createMovieMemCache

  final class MemCaches[F[_]] private (
      val directorCacheOpt: Option[MemCache[F, Long, MovieDbModel.Director]],
      val actorCacheOpt: Option[MemCache[F, Long, MovieDbModel.Actor]],
      val movieCacheOpt: Option[MemCache[F, Long, MovieDbModel.Movie]],
  )

  object MemCaches:
    def create[F[_]](
        directorCacheOpt: Option[MemCache[F, Long, MovieDbModel.Director]],
        actorCacheOpt: Option[MemCache[F, Long, MovieDbModel.Actor]],
        movieCacheOpt: Option[MemCache[F, Long, MovieDbModel.Movie]],
    ): MemCaches[F] =
      MemCaches(directorCacheOpt, actorCacheOpt, movieCacheOpt)
    end create

  private def createMemCaches[F[_]: { Temporal, Logger }](mcc: MemCacheConfig): Resource[F, MemCaches[F]] =
    (
      createDirectorMemCache(mcc.getDirectorMemCacheConfig),
      createActorMemCache(mcc.getActorMemCacheConfig),
      createMovieMemCache(mcc.getMovieMemCacheConfig),
    ).mapN(MemCaches.create)
  end createMemCaches

  private val MainFiberName: String = "MainFiber"

  private val AppEnvs: Set[String] = Set("dev", "prod")

  private def createConfigResource[F[_]: { Async as async, Env as env, Logger }]: Resource[F, AppConfig] =
    val loadConfig = for {
      appEnvOpt <- env.get("APP_ENV")
      env = appEnvOpt.getOrElse("dev")
      _ <- (!AppEnvs.contains(env)).whenA(
        async.raiseError(AssertionError(s"Bad configuration environment: '$env'.")),
      )
      config <- async.fromEither(
        ConfigSource
          .resources(s"application-$env.conf")
          .withFallback(ConfigSource.resources("application.conf"))
          .at("app-config")
          .load[AppConfig]
          .left
          .map(pureconfig.error.ConfigReaderException[AppConfig]),
      )
      _ <- U.logi(MainFiberName, config.toString)
    } yield config

    Resource.eval(loadConfig)
  end createConfigResource

  private def createServerResource[F[_]: { Async, Network }](
      serverHostIP: Ipv4Address,
      serverHostPort: Port,
      keyStoreFile: String,
      keyStorePassword: String,
      httpApp: HttpApp[F],
  ): Resource[F, http4s.server.Server] =
    val keyStoreFileNio = java.nio.file.Paths.get(keyStoreFile)
    val keyStorePasswordArray = keyStorePassword.toCharArray

    Resource.eval(
      TLSContext.Builder
        .forAsync[F]
        .fromKeyStoreFile(keyStoreFileNio, keyStorePasswordArray, keyStorePasswordArray),
    ) >>= { tlsContext =>
      EmberServerBuilder
        .default[F]
        .withHost(serverHostIP)
        .withPort(serverHostPort)
        .withShutdownTimeout(5.seconds)
        .withHttpApp(httpApp)
        .withTLS(tlsContext)
        .build
    }
  end createServerResource

  private def createHttpApp[F[_]: { Async, Logger }](deps: AppDependencies[F]): HttpApp[F] =
    val dsl: Http4sDsl[F] = Http4sDsl[F]
    val render: Render[F] = Render(dsl)

    MovieApp(deps, dsl, render).allRoutes
  end createHttpApp

  private def createServer[F[_]: { Async as async, Network, Logger }](deps: AppDependencies[F]): F[ExitCode] =
    val serverConnectionConfig = deps.appConfig.getServerConnectionConfig
    val keyStoreFile = serverConnectionConfig.getKeystoreFile
    val keyStorePassword = serverConnectionConfig.getKeystorePassword
    val httpApp = createHttpApp[F](deps)

    getServerHostIPPort[F](serverConnectionConfig) >>= { (serverHostIP, serverHostPort) =>
      createServerResource(serverHostIP, serverHostPort, keyStoreFile, keyStorePassword, httpApp)
        .use(server => U.logi(MainFiberName, s"Server started with base uri: '${server.baseUri.toString}'.") *> async.never)
        .as(ExitCode.Success)
    }
  end createServer

  // This is the number of redirects Ember will perform when a response
  // specifies that it needs a redirection.
  inline private val MaxHttpClientRedirects = 5

  final class AppDependencies[F[_]](
      val appConfig: AppConfig,
      val serverState: ServerState[F],
      val memCaches: MemCaches[F],
      val supervisor: Supervisor[F],
      val uuidGen: UUIDGenerator[F],
      val uuidScope: TraceIdScope[F, Option[String]],

      // The services
      val externalApiClientService: ExternalApiClientService[F],
      val movieRepositoryService: MovieRepositoryService[F],
      val fileSystemService: FileSystemService[F],
      val serverStateUpdateService: ServerStateUpdateService[F],
      val passwordHasherService: PasswordHasher[F],
      val authService: AuthService[F],
      val emailService: EmailService[F],
  )

  private[app] final case class ServerStateLive[F[_]](
      movieRequestCounts: Ref[F, Map[Long, Int]],
      jobQueue: Queue[F, HttpWorker.Job[F]],
  ) extends ServerState[F]

  private[app] object ServerStateLive:
    def create[F[_]: Async as async](backendServer: BackendServerConfig): F[ServerState[F]] =
      val boundedQueueCapacity = backendServer.getBoundedQueueCapacity
      for {
        movieReqCounts <- Ref.of[F, Map[Long, Int]](Map.empty)
        jobQueue <- Queue.bounded[F, HttpWorker.Job[F]](boundedQueueCapacity)
      } yield ServerStateLive[F](movieReqCounts, jobQueue)
    end create
  end ServerStateLive

  private[app] enum WebServiceResult:
    case OkJsonRes(json: Json)
    case NotFoundRes(s: String)
    case ConflictRes(s: String)
    case BadRequestRes(e: String)
    case UnauthorizedRes(e: String)
    case InternalServerErrorRes()
  end WebServiceResult

  private[app] final class Render[F[_]: Async as async](dsl: Http4sDsl[F]):
    import dsl.*
    import org.http4s.circe.CirceEntityEncoder.*
    import WebServiceResult.*

    private val ErrorChallenge: Challenge = Challenge(
      scheme = "Bearer",
      realm = "neo_token_service",
      params = Map("error" -> "invalid_grant", "error_description" -> "Invalid username or password"),
    )

    private final case class ApiError(message: String, errorCode: String, timestamp: java.time.Instant)

    private object ApiError:
      def apply(message: String, errorCode: String): F[ApiError] =
        TimeUtils.nowInstant.map(ApiError(message, errorCode, _))
      end apply
    end ApiError

    private def okJsonToResponse(wsr: WebServiceResult): F[Response[F]] =
      Ok(wsr.asInstanceOf[OkJsonRes].json)
    end okJsonToResponse

    private def noFoundToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[NotFoundRes].s, "NOTFOUND") >>= (apiErr => NotFound(apiErr))
    end noFoundToResponse

    private def conflictToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[ConflictRes].s, "CONFLICT") >>= (apiErr => Conflict(apiErr))
    end conflictToResponse

    private def badRequestToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[BadRequestRes].e, "BADREQUEST") >>= (apiErr => BadRequest(apiErr))
    end badRequestToResponse

    private def unauthorizedToResponse(wsr: WebServiceResult): F[Response[F]] =
      ApiError(wsr.asInstanceOf[UnauthorizedRes].e, "UNAUTHORIZED") >>= { apiErr =>
        Unauthorized(`WWW-Authenticate`(ErrorChallenge), apiErr)
      }
    end unauthorizedToResponse

    private def internalServerErrorToResponse(@unused wsr: WebServiceResult): F[Response[F]] =
      InternalServerError()
    end internalServerErrorToResponse

    private val ResultHandlerMap: Map[Class[? <: WebServiceResult], WebServiceResult => F[Response[F]]] = Map(
      classOf[OkJsonRes]              -> okJsonToResponse,
      classOf[NotFoundRes]            -> noFoundToResponse,
      classOf[ConflictRes]            -> conflictToResponse,
      classOf[BadRequestRes]          -> badRequestToResponse,
      classOf[UnauthorizedRes]        -> unauthorizedToResponse,
      classOf[InternalServerErrorRes] -> internalServerErrorToResponse,
    )

    private def notImplemented(c: Class[?]): Exception =
      Exception(s"Renderer not registered (in ResultHandlerMap) for class '$c'.")

    def apply(wsr: WebServiceResult): F[Response[F]] =
      val c = wsr.getClass
      ResultHandlerMap
        .get(c)
        .map(_(wsr))
        .getOrElse(async.raiseError(notImplemented(c)))
    end apply
  end Render

  private def createLogger[F[_]: Async as async]: F[Logger[F]] =
    val movieAppLoggerName = LoggerName("MovieAppLogger")

    Slf4jLogger.create[F](using async, movieAppLoggerName).widen[Logger[F]]
  end createLogger

  val run: IO[ExitCode] =
    type F = IO

    // We declare these two implicits here, to avoid the numerous calls to fetch them for each function below.
    implicit val async: Async[F] = IO.asyncForIO
    implicit val env: Env[F] = IO.envForIO // env is actually used -- intellij claims it is not, but it's a bug in intellij

    createLogger[F] >>= { implicit logger =>
      val appDeps: Resource[F, AppDependencies[F]] = for {
        appConfig <- createConfigResource[F]
        memCaches <- createMemCaches[F](appConfig.getMemCacheConfig)
        serverState <- Resource.eval[F, ServerState[F]](ServerStateLive.create[F](appConfig.getBackendServerConfig))
        httpClient <- EmberClientBuilder.default[F].build.map(FollowRedirect[F](MaxHttpClientRedirects))
        supervisor <- Supervisor[F](await = false)
        xa <- DoobieObj.xaResource[F](appConfig.getDbConnectionConfig)
        uuidGen <- UUIDGenerator.create[F]
        uuidScope <- Resource.eval[F, TraceIdScope[F, Option[String]]](TraceIdScope.fromIOLocal[Option[String]](None))
        emailService <- EmailServiceAsync2Live.create[F](appConfig.getGmailConfig)
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
          emailService,
        )
      }

      appDeps.use(deps => HttpWorker.createWorkers[F](deps) *> createServer[F](deps))
    }
end MovieApp
