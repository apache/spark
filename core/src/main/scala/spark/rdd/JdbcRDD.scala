package spark.rdd

import java.sql.{Connection, ResultSet}

import spark.{Logging, Partition, RDD, SparkContext, TaskContext}
import spark.util.NextIterator

private[spark] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index = idx
}

/**
 * An RDD that executes an SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 * @param lowerBound the minimum value of the first placeholder
 * @param upperBound the maximum value of the second placeholder
 *   The lower and upper bounds are inclusive.
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class JdbcRDD[T: ClassManifest](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    val length = 1 + upperBound - lowerBound
    (0 until numPartitions).map(i => {
      val start = lowerBound + ((i * length) / numPartitions).toLong
      val end = lowerBound + (((i + 1) * length) / numPartitions).toLong - 1
      new JdbcPartition(i, start, end)
    }).toArray
  }

  override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
    context.addOnCompleteCallback{ () => closeIfNeeded() }
    val part = thePart.asInstanceOf[JdbcPartition]
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }

    stmt.setLong(1, part.lower)
    stmt.setLong(2, part.upper)
    val rs = stmt.executeQuery()

    override def getNext: T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs && ! rs.isClosed()) rs.close()
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt && ! stmt.isClosed()) stmt.close()
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && ! stmt.isClosed()) conn.close()
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}

object JdbcRDD {
  def resultSetToObjectArray(rs: ResultSet) = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
}
