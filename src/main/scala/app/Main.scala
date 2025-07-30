package app

import cats.effect.*
import cats.effect.{Deferred, IO}
import cats.effect.{IO, IOApp}
import cats.implicits.*

object Main extends IOApp:
  private val program: IO[ExitCode] = MovieApp.run

  override def run(args: List[String]): IO[ExitCode] = program

  //  private def consumer(d: Deferred[IO, Int]): IO[Int] = d.get
//
//  private val program: IO[ExitCode] = {
//    val x = UUIDGenerator.create[IO]
//
//    x.use { uuidGen =>
//      val getUUID = uuidGen.generateUUIDAsString
//      for {
//        uuids <- Vector.fill(7000)(getUUID).parSequence
//        _ <- IO.whenA(uuids.hasDuplicates)(IO.raiseError(new AssertionError("Duplicates")))
//        _ <- uuids.traverseVoid(IO.println)
//      } yield ExitCode.Success
//    }
//  }
//
//  extension [A](v: Vector[A]) {
//    def hasDuplicates: Boolean = {
//      val setSize = v.toSet.size
//      println(v.size)
//      println(setSize)
//      v.size != setSize
//    }
//  }
//
//  override def run(args: List[String]): IO[ExitCode] = program
//
//    println("Warming up the JIT compiler...")
//    // Run the method thousands of times to trigger C2 compilation
//    var k = 0
//    for (i <- 1 to 2_000_001)
////      val p = (i, i + 1, i + 2, i + 3)
//      k += JitTest.gg(i, i + 1) + JitTest.hh(i, i + 1)
//    println(k)
////    println("Warm-up complete. The assembly code should have been printed.")
//    // ReturningThis
//    // Refs.run(args)
//    IO.pure(ExitCode.Success)
end Main
