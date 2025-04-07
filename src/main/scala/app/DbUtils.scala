package app

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.{Random, Try, Using}
import scala.util.chaining.*

object DbUtils:
  case class QueryReq[A](
      sql: String,
      bind: Option[PreparedStatement => Unit],
      mkObj: ResultSet => A,
  )

  case class DmlReq(dml: String, bind: Option[PreparedStatement => Unit])

  def createConnection(databaseURL: String, username: String, password: String): Connection =
    DriverManager
      .getConnection(databaseURL, username, password)
      .tap(_.setAutoCommit(false))

  def evalSelect[A](queryReq: QueryReq[A])(connection: Connection): Try[Seq[A]] =
    Using(connection.prepareStatement(queryReq.sql)) { (st: PreparedStatement) =>

      queryReq.bind match {
        case Some(bind) => bind(st)
        case None => ()
      }

      Using(st.executeQuery()) { rs =>
        var v = Vector.empty[A]
        while (rs.next())
          v = v :+ queryReq.mkObj(rs)
        v
      }
    }.flatten

  def evaldml(dmlReq: DmlReq)(connection: Connection): Unit = {
    val st = connection.prepareStatement(dmlReq.dml)

    dmlReq.bind match {
      case Some(bind) => bind(st)
      case None => ()
    }

    println("Before execute")
    val b = st.execute();
    println("After execute")
    println(b);
    ()
  }

  final case class PreStats(totalCount: Long, nullCount: Long, countDistinct: Long)

  private def formCountQuery(tableName: String, colName: String) =
    s"select count(*) totalCnt, count($colName), count(distinct $colName) columnCount from $tableName"

  def getRowCountForColumn(
      connection: Connection,
      tableName: String,
      colName: String,
  ): Try[PreStats] = {
    val sql = formCountQuery(tableName, colName)
    Using(connection.createStatement()) { st =>
      val rs = st.executeQuery(sql)
      rs.next()
      val totalCount = rs.getLong(1)
      val nonNullCount = rs.getLong(2)
      val countDistinct = rs.getLong(3)

      PreStats(totalCount, totalCount - nonNullCount, countDistinct)
    }
  }

  def deleteExistingData(connection: Connection, tableName: String): Try[Unit] = {
    val sql = s"delete from $tableName"

    Using(connection.prepareStatement(sql)) { st =>
      st.execute()
      connection.commit()
    }
  }

  def populateTextColumn(
      connection: Connection,
      tableName: String,
      colName: String,
      numRows: Int,
      minStrSize: Int,
      maxStrSize: Int,
  ): Try[Unit] = {
    val sql = s"insert into $tableName values(?)"

    Using(connection.prepareStatement(sql)) { st =>
      for (i <- 0 until numRows) {
        val randomString = generateRandomString(minStrSize, maxStrSize)
        st.setString(1, randomString)
        st.addBatch()
        println("hi")
      }
      st.executeBatch()
      println("there")
      connection.commit()
    }
  }

  private val rnd = new Random

  def generateRandomString(minLength: Int, maxLength: Int): String = {
    val length = rnd.nextInt(maxLength - minLength + 1) + minLength
    rnd.shuffle('a' to 'z').view.take(length).mkString
  }

  private type Bucket = (String, String, Long)

  private def formCalcQuery(tableName: String, colName: String): String =
    s"select $colName, count(*) from $tableName group by $colName order by 1"

  def calcStatistics(
      connection: Connection,
      tableName: String,
      colName: String,
      preStats: PreStats,
      defaultNumBuckets: Long,
  ): Try[Seq[Bucket]] = {
    val countDistinct = preStats.countDistinct
    val sql = formCalcQuery(tableName, colName)
    val vb = Vector.newBuilder[Bucket]

    println(sql)
    if (countDistinct <= defaultNumBuckets)
      Using(connection.createStatement()) { st =>
        val rs = st.executeQuery(sql)

        while (rs.next()) {
          val str = rs.getString(1)
          val bucketCnt = rs.getLong(2)
          vb.addOne((str, str, bucketCnt))
        }

        vb.result()
      }
    else {
      val nonNullValuesCount = preStats.totalCount - preStats.nullCount

      val averageBucketSize = nonNullValuesCount / defaultNumBuckets

      Using(connection.createStatement()) { st =>
        val rs = st.executeQuery(sql)

        var left: String = null
        var right: String = null
        var bucketCnt: Long = 0L

        while (rs.next()) {
          if (left == null)
            left = rs.getString(1)
          right = rs.getString(1)

          val currentCnt = rs.getLong(2)
          val newBucketCnt = bucketCnt + currentCnt

          if (newBucketCnt >= averageBucketSize) {
            vb.addOne((left, right, newBucketCnt))

            left = null
            bucketCnt = 0L
          } else
            bucketCnt = newBucketCnt
        }
        if (left != null) // There is an unfinished bucket.
          vb.addOne((left, right, bucketCnt))

        vb.result()
      }
    }
  }
