package app

import app.MovieDbModel.UserDetails
import io.circe.Json

object JobSpecs:
  enum JobKind(val shortName: String):
    case GetDirectorsDetailsByName(firstName: Option[String], lastName: Option[String])
        extends JobKind("GetDirectorsDetailsByName")
    case GetDirectorDetails(directorId: Long) extends JobKind("GetDirectorDetails")
    case GetActorDetails(actorId: Long) extends JobKind("GetActorDetails")
    case GetMoviesByDirector(directorId: Long) extends JobKind("GetMoviesByDirectorId")
    case GetMovie(movieId: Long) extends JobKind("GetMovie")
    case GetMovieWithCounting(movieId: Long) extends JobKind("GetMovieWithCounting")
    case CreateMovie(title: String, year: Int) extends JobKind("CreateMovie")
    case CreateSystemUser(userDetails: UserDetails) extends JobKind("CreateSystemUser")
    case FetchSystemUserByLoginName(loginName: String) extends JobKind("FetchSystemUserByLoginName")
    case FetchSystemUserByUserId(userIdStr: String) extends JobKind("FetchSystemUserByUserId")
    case LoginRequest(userDetails: UserDetails) extends JobKind("LoginRequest")

  enum FetchSystemUserError derives CanEqual:
    case NotFound
    case BadInput

  enum LoginRequestError:
    case InvalidLoginPassword

  enum DBError:
    case DuplicateLoginName(loginName: String)

  enum JobResult:
    case DirectorsDetailsByNameResult(directors: Seq[MovieDbModel.Director])
    case DirectorDetailsResult(director: Option[MovieDbModel.Director])
    case ActorDetailsResult(actor: Option[MovieDbModel.Actor])
    case MoviesByDirectorResult(movies: Seq[MovieDbModel.Movie])
    case MovieDetailsResult(movie: Option[MovieDbModel.Movie])
    case MovieWithCountingResult(movie: Option[MovieDbModel.Movie])
    case CreateMovieResult(movieId: Long)
    case CreateSystemUserResult(res: Either[DBError, Int])
    case FetchSystemUserByLoginNameResult(res: Either[FetchSystemUserError, MovieDbModel.UserDetailsInDb])
    case FetchSystemUserByUserIdResult(res: Either[FetchSystemUserError, MovieDbModel.UserDetailsInDb])
    case LoginRequestResult(res: Either[LoginRequestError, String])
