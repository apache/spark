/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.sql.{Connection, ResultSet}

import scala.reflect.ClassTag

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.util.NextIterator

private[spark] class JdbcPartition(idx: Int, val lower: Long, val upper: Long) extends Partition {
  override def index = idx
}
// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * An RDD that executes an SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results,
 *   when you wan to use more than one partitions.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 *   If numPartitions is set to exactly 1, the query do not need to contain any ? placeholder.
 * @param lowerBound the minimum value of the first placeholder
 * @param upperBound the maximum value of the second placeholder
 *   The lower and upper bounds are inclusive.
 *   If query do not contain any ? placeholder, lowerBound and upperBound can be set to any value.
 * @param numPartitions the number of partitions.
 *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
 *   the query would be executed twice, once with (1, 10) and once with (11, 20)
 *   If query do not contain any ? placeholder, numPartitions must be set to exactly 1.
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  private var schema: Seq[(String, Int, Boolean)] = null

  override def getPartitions: Array[Partition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    val length = 1 + upperBound - lowerBound
    (0 until numPartitions).map(i => {
      val start = lowerBound + ((i * length) / numPartitions).toLong
      val end = lowerBound + (((i + 1) * length) / numPartitions).toLong - 1
      new JdbcPartition(i, start, end)
    }).toArray
  }

  def getSchema: Seq[(String, Int, Boolean)] = {
    if (null != schema) {
      return schema
    }

    val conn = getConnection()
    val stmt = conn.prepareStatement(sql)
    val metadata = stmt.getMetaData
    try {
      if (null != stmt && ! stmt.isClosed()) {
        stmt.close()
      }
    } catch {
      case e: Exception => logWarning("Exception closing statement", e)
    }
    schema = Seq[(String, Int, Boolean)]()
    for(i <- 1 to metadata.getColumnCount) {
      schema :+= (
        metadata.getColumnName(i),
        metadata.getColumnType(i),
        metadata.isNullable(i) == java.sql.ResultSetMetaData.columnNullable
      )
    }
    schema
  }

  override def compute(thePart: Partition, context: TaskContext) = new NextIterator[T] {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
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

    val parameterCount = stmt.getParameterMetaData.getParameterCount
    if (parameterCount > 0) {
      stmt.setLong(1, part.lower)
    }
    if (parameterCount > 1) {
      stmt.setLong(2, part.upper)
    }

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
        if (null != rs && ! rs.isClosed()) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt && ! stmt.isClosed()) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn && ! conn.isClosed()) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
}

object JdbcRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }
}

