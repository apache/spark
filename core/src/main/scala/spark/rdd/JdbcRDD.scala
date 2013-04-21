package spark.rdd

import java.sql.{Connection, ResultSet}

import spark.{Logging, Partition, RDD, SparkContext, TaskContext}
import spark.util.NextIterator

/**
  An RDD that executes an SQL query on a JDBC connection and reads results.
  @param getConnection a function that returns an open Connection.
    The RDD takes care of closing the connection.
  @param sql the text of the query.
    The query must contain two ? placeholders for parameters used to partition the results.
    E.g. "select title, author from books where ? <= id and id <= ?" 
  @param lowerBound the minimum value of the first placeholder
  @param upperBound the maximum value of the second placeholder
    The lower and upper bounds are inclusive.
  @param numPartitions the amount of parallelism.
    Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
    the query would be executed twice, once with (1, 10) and once with (11, 20)
  @param mapRow a function from a ResultSet to a single row of the desired result type(s).
    This should only call getInt, getString, etc; the RDD takes care of calling next.
    The default maps a ResultSet to an array of Object.
*/
class JdbcRDD[T: ClassManifest](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] =
    ParallelCollectionRDD.slice(lowerBound to upperBound, numPartitions).
      filter(! _.isEmpty).
      zipWithIndex.
      map(x => new JdbcPartition(x._2, x._1.head, x._1.last)).
      toArray

  override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
    val part = thePart.asInstanceOf[JdbcPartition]
    val conn = getConnection()
    context.addOnCompleteCallback{ () => closeIfNeeded() }
    val stmt = conn.prepareStatement(sql)
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
        logInfo("closing connection")
        conn.close()
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }

}

private[spark] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index = idx
}

object JdbcRDD {
  val resultSetToObjectArray = (rs: ResultSet) =>
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
}
