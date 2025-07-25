package app

import java.lang.Math as math
import scala.reflect.ClassTag

// A simple dynamic programming problem from a google interview (on you tube).
// I had to do it...
object SquareLand:
  private val land: Array[Array[Int]] = Array(
    Array(1, 1, 1, 0, 0, 1),
    Array(1, 1, 1, 0, 1, 1),
    Array(1, 1, 1, 1, 0, 0),
    Array(1, 1, 1, 1, 1, 1),
    Array(1, 1, 0, 0, 1, 1),
    Array(0, 1, 1, 0, 1, 1),
  )

  private def findGoodSquare(v: Array[Array[Int]]): (Int, Int, Int, Array[Array[Int]]) =
    val rows = land.length
    val cols = land(0).length

    def mk2dArray[T: ClassTag](i: Int, j: Int, z: T): Array[Array[T]] = Array.fill(i, j)(z)

    def onEdge(i: Int, j: Int): Boolean = i == rows - 1 || j == cols - 1

    def min3(i: Int, j: Int, k: Int): Int = math.min(i, math.min(j, k))

    val sqSizes = mk2dArray(rows, cols, 0)

    var maxI: Int = -1
    var maxJ: Int = -1
    var maxSquare: Int = 0

    (rows - 1 to 0 by -1).foreach { i =>
      (cols - 1 to 0 by -1).foreach { j =>
        val elem = v(i)(j)
        if elem == 1 then
          if onEdge(i, j) then sqSizes(i)(j) = 1
          else
            val m = min3(sqSizes(i + 1)(j), sqSizes(i)(j + 1), sqSizes(i + 1)(j + 1))
            val newMaxSize = m + 1
            sqSizes(i)(j) = newMaxSize

            if (newMaxSize > maxSquare) {
              maxI = i
              maxJ = j
              maxSquare = newMaxSize
            } else ()
      }
    }

    (maxI, maxJ, maxSquare, sqSizes)

  private def printArray(v: Array[Array[Int]]): Unit =
    val rows = land.length
    val cols = land(0).length

    (0 until rows).foreach { i =>
      (0 until cols - 1).foreach { j =>
        printf(s"%d, ", v(i)(j))
      }
      printf(s"%d\n", v(i)(cols - 1))
    }

  def doIt(): Unit =
    printArray(land)
    println()
    val (i, j, siz, v) = findGoodSquare(land)
    println(s"i = $i, j = $j, siz = $siz\n")
    printArray(v)
    println()
end SquareLand
