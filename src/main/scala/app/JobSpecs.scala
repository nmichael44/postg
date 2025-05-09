package app

import io.circe.Json

object JobSpecs:
  enum JobKind(val tag: Int, val shortName: String):
    case GetDirectorsDetailsByName(firstName: Option[String], lastName: Option[String])
        extends JobKind(JobKind.GetDirectorsDetailsByNameTag, "GetDirectorsDetailsByName")
    case GetDirectorDetails(directorId: Long) extends JobKind(JobKind.GetDirectorDetailsTag, "GetDirectorDetails")
    case GetActorDetails(actorId: Long) extends JobKind(JobKind.GetActorDetailsTag, "GetActorDetails")
    case GetMoviesByDirector(directorId: Long) extends JobKind(JobKind.GetMoviesByDirectorTag, "GetMoviesByDirectorId")
    case GetMovie(movieId: Long) extends JobKind(JobKind.GetMovieTag, "GetMovie")
    case GetMovieWithCounting(movieId: Long) extends JobKind(JobKind.GetMovieWithCountingTag, "GetMovieWithCounting")
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
    inline val GetMoviesByDirectorTag = 3
    inline val GetMovieTag = 4
    inline val GetMovieWithCountingTag = 5
    inline val CreateMovieTag = 6
    inline val GetFileContentTag = 7
    inline val ReadTwoFilesInParallelTag = 8
    inline val FetchCompanyDataTag = 9
    inline val FetchJsonObjectTag = 10

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
