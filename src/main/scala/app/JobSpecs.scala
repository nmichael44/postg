package app

import scala.annotation.switch

import io.circe.Json

object JobSpecs:
  enum JobKind(val tag: Int, val shortName: String):
    case GetDirectorsDetailsByName(firstName: Option[String], lastName: Option[String])
        extends JobKind(JobKind.GetDirectorsDetailsByNameTag, "GetDirectorsDetailsByName")
    case GetDirectorDetails(directorId: Long) extends JobKind(JobKind.GetDirectorDetailsTag, "GetDirectorDetails")
    case GetActorDetails(actorId: Long) extends JobKind(JobKind.GetActorDetailsTag, "GetActorDetails")
    case GetMoviesByDirectorId(directorId: Long) extends JobKind(JobKind.GetMoviesByDirectorIdTag, "GetMoviesByDirectorId")
    case GetMovieById(movieId: Long) extends JobKind(JobKind.GetMovieByIdTag, "GetMovieById")
    case GetMovieByIdWithCounting(movieId: Long)
        extends JobKind(JobKind.GetMovieByIdWithCountingTag, "GetMovieByIdWithCounting")
    case CreateMovie(title: String, year: Int) extends JobKind(JobKind.CreateMovieTag, "CreateMovie")
    case GetFileContent(fileName: String) extends JobKind(JobKind.GetFileContentTag, "GetFileContent")
    case ReadTwoFilesInParallel(fileName1: String, fileName2: String)
        extends JobKind(JobKind.ReadTwoFilesInParallelTag, "ReadTwoFilesInParallel")
    case FetchCompanyData(companyName: String) extends JobKind(JobKind.FetchCompanyDataTag, "FetchCompanyData")
    case FetchJsonObject() extends JobKind(JobKind.FetchJsonObjectTag, "FetchJsonObject")

  object JobKind:
    inline val GetDirectorsDetailsByNameTag = 0
    inline val GetDirectorDetailsTag = 1
    inline val GetActorDetailsTag = 2
    inline val GetMoviesByDirectorIdTag = 3
    inline val GetMovieByIdTag = 4
    inline val GetMovieByIdWithCountingTag = 5
    inline val CreateMovieTag = 6
    inline val GetFileContentTag = 7
    inline val ReadTwoFilesInParallelTag = 8
    inline val FetchCompanyDataTag = 9
    inline val FetchJsonObjectTag = 10

  enum JobResult:
    case DirectorsDetailsByNameResult(directors: Seq[MovieDbModel.Director])
    case DirectorDetailsResult(director: Option[MovieDbModel.Director])
    case ActorDetailsResult(actor: Option[MovieDbModel.Actor])
    case MoviesByDirectorIdResult(movies: Seq[MovieDbModel.Movie])
    case MovieByIdResult(movie: Option[MovieDbModel.Movie])
    case MovieByIdWithCountingResult(movie: Option[MovieDbModel.Movie])
    case CreateMovieResult(movieId: Long)
    case FileContentResult(content: String)
    case TwoFilesInParallelResult(content: String)
    case CompanyDataResult(companyData: String)
    case JsonObjectResult(json: Json)
