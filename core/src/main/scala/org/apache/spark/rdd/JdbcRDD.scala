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

import java.sql.{Connection, ResultSet, PreparedStatement}

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.util.NextIterator

// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * An RDD that executes a SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcRDDSuite.
 *
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query must contain two ? placeholders for parameters used to partition the results.
 *   E.g. "select title, author from books where ? <= id and id <= ?"
 * @param partitions a list of partitions to apply to sql.
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 */
class JdbcRDD[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    partitions: Seq[JdbcPartition],
    mapRow: (ResultSet) => T)
  extends RDD[T](sc, Nil) with Logging {
  
  /**
   * An RDD that executes a SQL query on a JDBC connection and reads results.
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
  def this(
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _) {
    this(sc, getConnection, sql, JdbcPartition.longPartitions(lowerBound, upperBound, numPartitions), mapRow)
  }
  
  override def getPartitions: Array[Partition] = {
    partitions.view.zipWithIndex.map{ case(partition, index) => 
      new JdbcPartitionAdapter(index, partition)
    }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T] {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val part = thePart.asInstanceOf[JdbcPartitionAdapter]
    val conn = getConnection()
    val stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }

    part.fill(stmt)
    val rs = stmt.executeQuery()

    override def getNext(): T = {
      if (rs.next()) {
        mapRow(rs)
      } else {
        finished = true
        null.asInstanceOf[T]
      }
    }

    override def close() {
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }
  }
} 

/**
 * A JdbcParition carries sql partition information, e.g., lowerBound and upperBound.
 */
trait JdbcPartition extends Serializable {
  /**
   * Fill given sql statement with partition information.
   */
  def fill(stmt: PreparedStatement): Unit
}

object JdbcPartition {
  /**
   * Create a partition of long lower&upper bounds.
   *
   * @param lowerBound the minimum value of the partition.
   * @param upperBound the maximum value of the partition.
   *   The lower and upper bounds are inclusive.
   * @param lowerBoundParameterIndexes the sql placeholder indices of lowerBound.
   *   Default to 1. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (1, 3)
   * @param upperBoundParameterIndexes the sql placeholder indices of upperBound.
   *   Default to 2. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (2, 4)
   */
  def longPartition(lowerBound: Long, upperBound: Long,
      lowerBoundParameterIndexes:Seq[Int] = DefaultLowerBoundParameterIndexes, 
      upperBoundParameterIndexes:Seq[Int] = DefaultUpperBoundParameterIndexes): JdbcPartition = {
    new MultiBoundsJdbcPartition(new LongBoundFiller(lowerBound, upperBound),
        lowerBoundParameterIndexes, upperBoundParameterIndexes)
  }
  
  /**
   * Create a list of partitions of long lower&upper bounds. 
   * For usage example, see test case JdbcRDDSuite.
   *
   * @param lowerBound the minimum value of all the partitions.
   * @param upperBound the maximum value of all the partitions.
   *   The lower and upper bounds are inclusive.
   * @param numPartitions the number of partitions.
   *   Given a lowerBound of 1, an upperBound of 20, and a numPartitions of 2,
   *   the query would be executed twice, once with (1, 10) and once with (11, 20)
   * @param lowerBoundParameterIndexes the sql placeholder indices of lowerBound.
   *   Default to 1. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (1, 3)
   * @param upperBoundParameterIndexes the sql placeholder indices of upperBound.
   *   Default to 2. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (2, 4)
   */
  def longPartitions(lowerBound: Long, upperBound: Long, numPartitions: Int, 
      lowerBoundParameterIndexes:Seq[Int] = DefaultLowerBoundParameterIndexes, 
      upperBoundParameterIndexes:Seq[Int] = DefaultUpperBoundParameterIndexes): Seq[JdbcPartition] = {
    // bounds are inclusive, hence the + 1 here and - 1 on end
    val length = BigInt(1) + upperBound - lowerBound
    (0 until numPartitions).map { i =>
      val start = lowerBound + ((i * length) / numPartitions)
      val end = lowerBound + (((i + 1) * length) / numPartitions) - 1
      longPartition(start.toLong, end.toLong,
          lowerBoundParameterIndexes, upperBoundParameterIndexes)
    }
  }

  /**
   * Create a partition of string lower&upper bounds.
   *
   * @param lowerBound the minimum value of the partition.
   * @param upperBound the maximum value of the partition.
   *   The lower and upper bounds are inclusive.
   * @param lowerBoundParameterIndexes the sql placeholder indices of lowerBound.
   *   Default to 1. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (1, 3)
   * @param upperBoundParameterIndexes the sql placeholder indices of upperBound.
   *   Default to 2. There can be more than one lowerBound placeholder, in a sql with sub-queries, e.g., (2, 4)
   */
  def stringPartition(lowerBound: String, upperBound: String,
      lowerBoundParameterIndexes:Seq[Int] = DefaultLowerBoundParameterIndexes, 
      upperBoundParameterIndexes:Seq[Int] = DefaultUpperBoundParameterIndexes): JdbcPartition = {
    new MultiBoundsJdbcPartition(new StringBoundFiller(lowerBound, upperBound),
        lowerBoundParameterIndexes, upperBoundParameterIndexes)
  }
  
  private val DefaultLowerBoundParameterIndexes = Seq(1)
  private val DefaultUpperBoundParameterIndexes = Seq(2)
}

private class MultiBoundsJdbcPartition(
    val boundFiller: BoundFiller,
    val lowerBoundParameterIndexes:Seq[Int],
    val upperBoundParameterIndexes:Seq[Int]  
  ) extends JdbcPartition {
  
  override def fill(stmt: PreparedStatement): Unit = {
    lowerBoundParameterIndexes.foreach { paramterIndex =>
      boundFiller.fillLowerBound(paramterIndex, stmt)
    }
    
    upperBoundParameterIndexes.foreach { paramterIndex =>
      boundFiller.fillUpperBound(paramterIndex, stmt)
    }
  }
}

private trait BoundFiller extends Serializable {
  def fillLowerBound(paramterIndex: Int, stmt: PreparedStatement): Unit

  def fillUpperBound(paramterIndex: Int, stmt: PreparedStatement): Unit
}

private class LongBoundFiller(
    val lowerBound: Long,
    val upperBound: Long) 
  extends BoundFiller {
  override def fillLowerBound(paramterIndex: Int, stmt: PreparedStatement): Unit = {
    stmt.setLong(paramterIndex, lowerBound)
  }

  override def fillUpperBound(paramterIndex: Int, stmt: PreparedStatement): Unit = {
    stmt.setLong(paramterIndex, upperBound)
  }
}

private class StringBoundFiller(
    val lowerBound: String,
    val upperBound: String) 
  extends BoundFiller {
  override def fillLowerBound(paramterIndex: Int, stmt: PreparedStatement): Unit = {
    stmt.setString(paramterIndex, lowerBound)
  }

  override def fillUpperBound(paramterIndex: Int, stmt: PreparedStatement): Unit = {
    stmt.setString(paramterIndex, upperBound)
  }
}

private class JdbcPartitionAdapter(
    val index: Int, partition: JdbcPartition) extends Partition {
  def fill(stmt: PreparedStatement): Unit = {
    partition.fill(stmt)
  }
}

object JdbcRDD {
  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

  /**
   * Create an RDD that executes a SQL query on a JDBC connection and reads results.
   * For usage example, see test case JavaAPISuite.testJavaJdbcRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
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
  def create[T](
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val jdbcRDD = new JdbcRDD[T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      lowerBound,
      upperBound,
      numPartitions,
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)

    new JavaRDD[T](jdbcRDD)(fakeClassTag)
  }

  /**
   * Create an RDD that executes a SQL query on a JDBC connection and reads results. Each row is
   * converted into a `Object` array. For usage example, see test case JavaAPISuite.testJavaJdbcRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
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
   */
  def create(
      sc: JavaSparkContext,
      connectionFactory: ConnectionFactory,
      sql: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, connectionFactory, sql, lowerBound, upperBound, numPartitions, mapRow)
  }
  

  /**
   * Create an RDD that executes a SQL query on a JDBC connection and reads results. Each row is
   * For usage example, see test case JavaAPISuite.testJavaJdbcRDD.
   *
   * @param getConnection a function that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query must contain two ? placeholders for parameters used to partition the results.
   *   E.g. "select title, author from books where ? <= id and id <= ?"
   * @param partitions a list of partitions to apply to sql.
   * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
   *   This should only call getInt, getString, etc; the RDD takes care of calling next.
   *   The default maps a ResultSet to an array of Object.
   */  
  def create[T: ClassTag](
    sc: SparkContext,
    getConnection: () => Connection,
    sql: String,
    partitions: Seq[JdbcPartition],
    mapRow: (ResultSet) => T = JdbcRDD.resultSetToObjectArray _): JdbcRDD[T] = {
    new JdbcRDD(sc, getConnection, sql, partitions, mapRow)
  }
}
