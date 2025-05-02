package app

import scala.annotation.switch

import io.circe.Json

object JobSpecs:
  enum JobKind(val tag: Int):
    case GetDirectorsDetailsByName(firstName: Option[String], lastName: Option[String])
        extends JobKind(JobKind.GetDirectorsDetailsByNameTag)
    case GetDirectorDetails(directorId: Long) extends JobKind(JobKind.GetDirectorDetailsTag)
    case GetActorDetails(actorId: Long) extends JobKind(JobKind.GetActorDetailsTag)
    case GetMoviesByDirectorId(directorId: Long) extends JobKind(JobKind.GetMoviesByDirectorIdTag)
    case GetMovieById(movieId: Long) extends JobKind(JobKind.GetMovieByIdTag)
    case GetMovieByIdWithCounting(movieId: Long)
        extends JobKind(JobKind.GetMovieByIdWithCountingTag)
    case GetFileContent(fileName: String) extends JobKind(JobKind.GetFileContentTag)
    case ReadTwoFilesInParallel(fileName1: String, fileName2: String)
        extends JobKind(JobKind.ReadTwoFilesInParallelTag)
    case FetchCompanyData(companyName: String) extends JobKind(JobKind.FetchCompanyDataTag)
    case FetchJsonObject() extends JobKind(JobKind.FetchJsonObjectTag)

    def shortName: String =
      (tag: @switch) match
        case JobKind.GetDirectorsDetailsByNameTag => "GetDirectorsDetailsByName"
        case JobKind.GetDirectorDetailsTag => "GetDirectorDetails"
        case JobKind.GetActorDetailsTag => "GetActorDetails"
        case JobKind.GetMoviesByDirectorIdTag => "GetMoviesByDirectorId"
        case JobKind.GetMovieByIdTag => "GetMovieById"
        case JobKind.GetMovieByIdWithCountingTag => "GetMovieByIdWithCounting"
        case JobKind.GetFileContentTag => "GetFileContent"
        case JobKind.ReadTwoFilesInParallelTag => "ReadTwoFilesInParallel"
        case JobKind.FetchCompanyDataTag => "FetchCompanyData"
        case JobKind.FetchJsonObjectTag => "FetchJsonObject"

  object JobKind:
    inline val GetDirectorsDetailsByNameTag = 0
    inline val GetDirectorDetailsTag = 1
    inline val GetActorDetailsTag = 2
    inline val GetMoviesByDirectorIdTag = 3
    inline val GetMovieByIdTag = 4
    inline val GetMovieByIdWithCountingTag = 5
    inline val GetFileContentTag = 6
    inline val ReadTwoFilesInParallelTag = 7
    inline val FetchCompanyDataTag = 8
    inline val FetchJsonObjectTag = 9

  enum JobResult:
    case DirectorsDetailsByNameResult(directors: Seq[MovieDbModel.Director])
    case DirectorDetailsResult(director: Option[MovieDbModel.Director])
    case ActorDetailsResult(actor: Option[MovieDbModel.Actor])
    case MoviesByDirectorIdResult(movies: Seq[MovieDbModel.Movie])
    case MovieByIdResult(movie: Option[MovieDbModel.Movie])
    case MovieByIdWithCountingResult(movie: Option[MovieDbModel.Movie])
    case FileContentResult(content: String)
    case TwoFilesInParallelResult(content: String)
    case CompanyDataResult(companyData: String)
    case JsonObjectResult(json: Json)
