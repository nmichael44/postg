package app

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
    case GetFileContent(fileName: String) extends JobKind("GetFileContent")
    case ReadTwoFilesInParallel(fileName1: String, fileName2: String)
        extends JobKind("ReadTwoFilesInParallel")
    case FetchCompanyData(companyName: String) extends JobKind("FetchCompanyData")
    case FetchJsonObject() extends JobKind("FetchJsonObject")

  enum JobResult:
    case DirectorsDetailsByNameResult(directors: Seq[MovieDbModel.Director])
    case DirectorDetailsResult(director: Option[MovieDbModel.Director])
    case ActorDetailsResult(actor: Option[MovieDbModel.Actor])
    case MoviesByDirectorResult(movies: Seq[MovieDbModel.Movie])
    case MovieDetailsResult(movie: Option[MovieDbModel.Movie])
    case MovieWithCountingResult(movie: Option[MovieDbModel.Movie])
    case CreateMovieResult(movieId: Long)
    case FileContentResult(content: String)
    case TwoFilesInParallelResult(content: String)
    case CompanyDataResult(companyData: String)
    case JsonObjectResult(json: Json)
