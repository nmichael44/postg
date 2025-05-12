package app.memcachetests

import cats.effect.{IO, Resource}
import cats.effect.kernel.MonadCancelThrow
import cats.syntax.option.*

import java.time.LocalDate
import scala.concurrent.ExecutionContext

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import doobie.*
import doobie.implicits.*
import org.http4s.dsl.Http4sDsl
import org.http4s.Request
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.Logger

final class MovieAppTest extends AsyncFlatSpec with Matchers with Http4sDsl[IO]
//  implicit val logger: Logger[IO] = Slf4jLogger.getLogger[IO]
//
//  private val dummyDirector = MovieDbModel.Director(1L, "John", "Doe", LocalDate.of(1970, 1, 1))
//  private val dummyActor = MovieDbModel.Actor(1L, "Jane", "Doe", LocalDate.of(1980, 5, 10))
//  private val dummyMovie = MovieDbModel.Movie(1L, "Test Movie", 2023)
//
//  "getDirectorsDetailsFromDb" should "return a vector of directors when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] =
//        if (fa.sql.contains("firstName = $firstName")) IO.pure(Vector(dummyDirector))
//        else IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp
//      .getDirectorsDetailsFromDb(mockXa, MovieApp.DirectorPath(Some("John"), Some("Doe")))
//      .map { result =>
//        result shouldBe Vector(dummyDirector)
//      }
//  }
//
//  it should "return an empty vector when no directors are found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp
//      .getDirectorsDetailsFromDb(mockXa, MovieApp.DirectorPath(Some("NonExistent"), None))
//      .map { result =>
//        result shouldBe Vector.empty
//      }
//  }
//
//  "getDirectorDetailsFromDb" should "return Some(Director) when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] =
//        if (fa.sql.contains("directorId = $directorId")) IO.pure(Some(dummyDirector))
//        else IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getDirectorDetailsFromDb(mockXa, 1L).map { result =>
//      result shouldBe Some(dummyDirector)
//    }
//  }
//
//  it should "return None when not found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getDirectorDetailsFromDb(mockXa, 99L).map { result =>
//      result shouldBe None
//    }
//  }
//
//  "getDirectorsDetailsByName" should "return Ok with directors details when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector(dummyDirector))
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request = Request[IO](uri = uri"/getDirectorsByName?firstName=John&lastName=Doe")
//    MovieApp
//      .getDirectorsDetailsByName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("John"), Some("Doe")),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.Ok
//        response.as[String].map { body =>
//          body should include("\"directorId\":1")
//          body should include("\"firstName\":\"John\"")
//          body should include("\"lastName\":\"Doe\"")
//        }
//      }
//  }
//
//  it should "return Ok with empty array when no directors are found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request = Request[IO](uri = uri"/getDirectorsByName?firstName=NonExistent")
//    MovieApp
//      .getDirectorsDetailsByName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("NonExistent"), None),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.Ok
//        response.as[String].map { body =>
//          body shouldBe "[]"
//        }
//      }
//  }
//
//  it should "return BadRequest for extra query parameters" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request =
//      Request[IO](uri = uri"/getDirectorsByName?firstName=John&lastName=Doe&extra=param")
//    MovieApp
//      .getDirectorsDetailsByName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("John"), Some("Doe")),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.BadRequest
//        response.as[String].map { body =>
//          body should include("Extra params found in quest: extra")
//        }
//      }
//  }
//
//  "getDirectorDetails" should "return Ok with director details when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Some(dummyDirector))
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getDirectorDetails(mockXa, 1L, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body should include("\"directorId\":1")
//        body should include("\"firstName\":\"John\"")
//        body should include("\"lastName\":\"Doe\"")
//      }
//    }
//  }
//
//  it should "return BadRequest when director is not found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getDirectorDetails(mockXa, 99L, this).flatMap { response =>
//      response.status shouldBe Status.BadRequest
//      response.as[String].map { body =>
//        body should include("Director id: '99' not found!")
//      }
//    }
//  }
//
//  "getActorDetailsFromDb" should "return Some(Actor) when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] =
//        if (fa.sql.contains("actorId = $actorId")) IO.pure(Some(dummyActor))
//        else IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getActorDetailsFromDb(mockXa, 1L).map { result =>
//      result shouldBe Some(dummyActor)
//    }
//  }
//
//  it should "return None when actor is not found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getActorDetailsFromDb(mockXa, 99L).map { result =>
//      result shouldBe None
//    }
//  }
//
//  "getActorDetails" should "return Ok with actor details when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Some(dummyActor))
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getActorDetails(mockXa, 1L, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body should include("\"actorId\":1")
//        body should include("\"firstName\":\"Jane\"")
//        body should include("\"lastName\":\"Doe\"")
//      }
//    }
//  }
//
//  it should "return BadRequest when actor is not found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(None)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getActorDetails(mockXa, 99L, this).flatMap { response =>
//      response.status shouldBe Status.BadRequest
//      response.as[String].map { body =>
//        body should include("Actor id: '99' not found!")
//      }
//    }
//  }
//
//  "getMoviesByDirectorIdFromDb" should "return a vector of movies when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] =
//        if (fa.sql.contains("WHERE md.directorId = $directorId")) IO.pure(Vector(dummyMovie))
//        else IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getMoviesByDirectorIdFromDb(mockXa, 1L).map { result =>
//      result shouldBe Vector(dummyMovie)
//    }
//  }
//
//  it should "return an empty vector when no movies are found for the director" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getMoviesByDirectorIdFromDb(mockXa, 99L).map { result =>
//      result shouldBe Vector.empty
//    }
//  }
//
//  "getMoviesByDirectorId" should "return Ok with movies when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector(dummyMovie))
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getMoviesByDirectorId(mockXa, 1L, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body should include("\"movieId\":1")
//        body should include("\"title\":\"Test Movie\"")
//        body should include("\"year\":2023")
//      }
//    }
//  }
//
//  it should "return Ok with empty array when no movies found for director" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(Vector.empty)
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp.getMoviesByDirectorId(mockXa, 99L, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body shouldBe "[]"
//      }
//    }
//  }
//
//  "getMoviesByDirectorNameFromDb" should "return a JSON string with director and movies when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(
//        """[{"directorId":1,"firstName":"John","lastName":"Doe","dob":"1970-01-01","movies":[{"movieId":1,"title":"Test Movie","year":2023}]}]""",
//      )
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp
//      .getMoviesByDirectorNameFromDb(mockXa, MovieApp.DirectorPath(Some("John"), Some("Doe")))
//      .map { result =>
//        result should include("\"directorId\":1")
//        result should include("\"firstName\":\"John\"")
//        result should include("\"lastName\":\"Doe\"")
//        result should include("\"movies\":")
//        result should include("\"movieId\":1")
//        result should include("\"title\":\"Test Movie\"")
//        result should include("\"year\":2023")
//      }
//  }
//
//  it should "return an empty JSON array string when no directors are found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure("[]")
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    MovieApp
//      .getMoviesByDirectorNameFromDb(mockXa, MovieApp.DirectorPath(Some("NonExistent"), None))
//      .map { result =>
//        result shouldBe "[]"
//      }
//  }
//
//  "getMoviesByDirectorName" should "return Ok with director and movies in JSON when found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure(
//        """[{"directorId":1,"firstName":"John","lastName":"Doe","dob":"1970-01-01","movies":[{"movieId":1,"title":"Test Movie","year":2023}]}]""",
//      )
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request = Request[IO](uri = uri"/getMoviesByDirectorName?firstName=John&lastName=Doe")
//    MovieApp
//      .getMoviesByDirectorName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("John"), Some("Doe")),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.Ok
//        response.as[String].map { body =>
//          body should include("\"directorId\":1")
//          body should include("\"firstName\":\"John\"")
//          body should include("\"lastName\":\"Doe\"")
//          body should include("\"movies\":")
//          body should include("\"movieId\":1")
//          body should include("\"title\":\"Test Movie\"")
//          body should include("\"year\":2023")
//        }
//      }
//  }
//
//  it should "return Ok with empty array when no directors found" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure("[]")
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request = Request[IO](uri = uri"/getMoviesByDirectorName?firstName=NonExistent")
//    MovieApp
//      .getMoviesByDirectorName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("NonExistent"), None),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.Ok
//        response.as[String].map { body =>
//          body shouldBe "[]"
//        }
//      }
//  }
//
//  it should "return BadRequest for extra query parameters in getMoviesByDirectorName" in {
//    val mockXa = Transactor[IO] {
//      override def rawTransact[A](fa: IO[A]): IO[A] = fa
//      override def rawTransact[A](fa: IO[A], executionContext: ExecutionContext): IO[A] = fa
//      override def transact[A](fa: ConnectionIO[A]): IO[A] = IO.pure("[]")
//      override def connect(implicit ev: MonadCancelThrow[IO]): Resource[IO, java.sql.Connection] =
//        Resource.pure(null)
//    }
//    val request =
//      Request[IO](uri = uri"/getMoviesByDirectorName?firstName=John&lastName=Doe&extra=param")
//    MovieApp
//      .getMoviesByDirectorName(
//        request,
//        mockXa,
//        MovieApp.DirectorPath(Some("John"), Some("Doe")),
//        this,
//      )
//      .flatMap { response =>
//        response.status shouldBe Status.BadRequest
//        response.as[String].map { body =>
//          body should include("Extra params found in quest: extra")
//        }
//      }
//  }
//
//  // Tests for file reading functionalities
//  "getContentOfFileName" should "return Ok with file content when file exists" ignore { // Needs a way to mock the file system
//    val fileName = "test.txt"
//    val fileContent = "This is a test file."
//    // In a real test, you would create a temporary file with this content.
//    // For now, this test is ignored as it requires mocking the file system.
//    val request = Request[IO](uri = uri"/getFile?fileName=" + fileName)
//    MovieApp.getContentOfFileName(fileName, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        // In a real test, you would assert that body == fileContent
//        body shouldBe "" // Placeholder assertion
//      }
//    }
//  }
//
//  it should "return BadRequest when file does not exist" ignore { // Needs a way to mock the file system
//    val fileName = "nonexistent.txt"
//    val request = Request[IO](uri = uri"/getFile?fileName=" + fileName)
//    MovieApp.getContentOfFileName(fileName, this).flatMap { response =>
//      response.status shouldBe Status.BadRequest
//      response.as[String].map { body =>
//        body should include(s"Error reading file: $fileName")
//      }
//    }
//  }
//
//  "getContentOfFileNameExplicit" should "return Ok with file content when file exists" ignore { // Needs a way to mock the file system
//    val fileName = "test.txt"
//    val fileContent = "This is a test file."
//    val request = Request[IO](uri = uri"/getFileExplicit?fileName=" + fileName)
//    MovieApp.getContentOfFileNameExplicit(fileName, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body shouldBe "" // Placeholder assertion
//      }
//    }
//  }
//
//  it should "return BadRequest when file does not exist for explicit reading" ignore { // Needs a way to mock the file system
//    val fileName = "nonexistent.txt"
//    val request = Request[IO](uri = uri"/getFileExplicit?fileName=" + fileName)
//    MovieApp.getContentOfFileNameExplicit(fileName, this).flatMap { response =>
//      response.status shouldBe Status.BadRequest
//      response.as[String].map { body =>
//        body should include(s"Error reading file: nonexistent.txt")
//      }
//    }
//  }
//
//  "readTwoFilesInParallel" should "return Ok with concatenated content of two files" ignore { // Needs a way to mock the file system
//    val fileName1 = "file1.txt"
//    val fileName2 = "file2.txt"
//    val content1 = "Content of file 1."
//    val content2 = "Content of file 2."
//    val expectedContent = content1 + content2
//    val request = Request[IO](uri =
//      uri"/readTwoFilesInParallel?fileName1=" + fileName1 + "&fileName2=" + fileName2,
//    )
//    MovieApp.readTwoFilesInParallel(fileName1, fileName2, this).flatMap { response =>
//      response.status shouldBe Status.Ok
//      response.as[String].map { body =>
//        body shouldBe "" // Placeholder assertion
//      }
//    }
//  }
