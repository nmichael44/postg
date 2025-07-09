package app

import cats.effect.*

object Main extends IOApp:
  override def run(args: List[String]): IO[ExitCode] =

//    println("Warming up the JIT compiler...")
//    // Run the method thousands of times to trigger C2 compilation
//    var k = 0
//    for (i <- 1 to 2_000_001) {
//      val p = (i, i + 1, i + 2, i + 3)
//      k += JitTest.f1(p) + JitTest.f2(i, i + 1, i + 2, i + 3) +
//        JitTest.f3(p) + JitTest.f4(i, i + 1, i + 2, i + 3)
//    }
//    println(k)
//    println("Warm-up complete. The assembly code should have been printed.")
    MovieApp.run
    // ReturningThis
    // Refs.run(args)
//    IO.pure(ExitCode.Success)
