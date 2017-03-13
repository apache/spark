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

import java.sql.{PreparedStatement, Connection, ResultSet}

import scala.reflect.ClassTag

import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.util.NextIterator
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}

import scala.collection.JavaConverters._

private[spark] class JdbcPartitioningPartition[A](idx: Int, val partitionArgs: A) extends Partition {
  override def index: Int = idx
}

// TODO: Expose a jdbcRDD function in SparkContext and mark this as semi-private
/**
 * An RDD that executes an SQL query on a JDBC connection and reads results.
 * For usage example, see test case JdbcPartioningRDDSuite.
 *
 * The number of partitions will be equivalent to the number of items in the partitionArgs argument, for simplicity.
 * @param getConnection a function that returns an open Connection.
 *   The RDD takes care of closing the connection.
 * @param sql the text of the query.
 *   The query should contain ? placeholders for each single value you intend to insert into the query.
 *   For example: select * from some_table where partition_id = ? and something_else = 15;
 *   If you want to handle inserting a set of items for an in-clause or similar use the
 *   JdbcPartitioningRDD.SetPlaceholder value as a placeholder. This ties in with the setupPlaceholders argument that
 *   will convert a specific partition sql query to have the correct ? placeholders for all items
 * @param partitionArgs the value or group of values you require as query parameters for each partition of the query
 *  For example if your query is : select * from some_table where partition_id = ?;
 *  Then your partitionArgs could be a Seq[String] as you want to insert an id for each partition into the query
 * @param insertPlaceholders function that takes your partitionArgs for the current partition and set's the values on
 * the prepared statement for execution
 * @param setupPlaceholders if you have to dynamically size the number of placeholders due to use of SetPlaceholder,
 * then provide a function here that calls JdbcPartitioningRDD.insertStatementSetPlaceholders for each placeholder
 * you need to replace in order of appearance in the String.
 * For example: if your query is: select * from some_table where user_ids in (%SET_PLACEHOLDER%);
 * Then you might provide a function here to get a ? placeholder for each user Id:
 * { (query: String, userIds: Seq[String]) => JdbcPartitioningRDD.insertStatementSetPlaceholders(query, userIds.size) }
 * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
 *   This should only call getInt, getString, etc; the RDD takes care of calling next.
 *   The default maps a ResultSet to an array of Object.
 */
class JdbcPartitioningRDD[A, T: ClassTag](
                            sc: SparkContext,
                            getConnection: () => Connection,
                            sql: String,
                            partitionArgs: Seq[A],
                            insertPlaceholders: (PreparedStatement, A) => Unit,
                            setupPlaceholders: (String, A) => String = JdbcPartitioningRDD.noopSetupPlaceholders[A] _,
                            mapRow: (ResultSet) => T = JdbcPartitioningRDD.resultSetToObjectArray _)
  extends RDD[T](sc, Nil) with Logging {

  override def getPartitions: Array[Partition] = {
    partitionArgs.zipWithIndex.map { case (args, idx) =>
      new JdbcPartitioningPartition(idx, args)
    }.toArray
  }

  override def compute(thePart: Partition, context: TaskContext): Iterator[T] = new NextIterator[T]
  {
    context.addTaskCompletionListener{ context => closeIfNeeded() }
    val part = thePart.asInstanceOf[JdbcPartitioningPartition[A]]
    val conn = getConnection()

    val adjustedQuery = setupPlaceholders(sql, part.partitionArgs)
    val stmt = conn.prepareStatement(adjustedQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    // setFetchSize(Integer.MIN_VALUE) is a mysql driver specific way to force streaming results,
    // rather than pulling entire resultset into memory.
    // see http://dev.mysql.com/doc/refman/5.0/en/connector-j-reference-implementation-notes.html
    if (conn.getMetaData.getURL.matches("jdbc:mysql:.*")) {
      stmt.setFetchSize(Integer.MIN_VALUE)
      logInfo("statement fetch size set to: " + stmt.getFetchSize + " to force MySQL streaming ")
    }

    insertPlaceholders(stmt, part.partitionArgs)

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

object JdbcPartitioningRDD {
  val SetPlaceholder = "%SET_PLACEHOLDER%"

  /**
   * Updates a SQL string by replacing the first instance of %SET_PLACEHOLDER% with n ? placeholders
   * where n is the number of items in the set you want to insert into the statement
   *
   * for example:
   * insertStatementSetPlaceholders(sqlString = "select * from some_table where id in (%SET_PLACEHOLDER%)", 5)
   * should return
   * "select * from some_table where id in (?,?,?,?,?)"
   * so you can now iterate through your set and insert the values into a prepared statement
   */
  def insertStatementSetPlaceholders(sqlString: String, numberOfItems: Int): String = {
    sqlString.replaceFirst(SetPlaceholder, Seq.fill(numberOfItems)("?").mkString(","))
  }

  def resultSetToObjectArray(rs: ResultSet): Array[Object] = {
    Array.tabulate[Object](rs.getMetaData.getColumnCount)(i => rs.getObject(i + 1))
  }

  private def noopSetupPlaceholders[A](query: String, args: A): String = query

  trait ConnectionFactory extends Serializable {
    @throws[Exception]
    def getConnection: Connection
  }

  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results.
   * For usage example, see test case JavaAPISuite.testJavaJdbcPartitioningRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query should contain ? placeholders for each single value you intend to insert into the query.
   *   For example: select * from some_table where partition_id = ? and something_else = 15;
   *   If you want to handle inserting a set of items for an in-clause or similar use the
   *   JdbcPartitioningRDD.SetPlaceholder value as a placeholder. This ties in with the setupPlaceholders argument that
   *   will convert a specific partition sql query to have the correct ? placeholders for all items
   * @param partitionArgs the value or group of values you require as query parameters for each partition of the query
   *  For example if your query is : select * from some_table where partition_id = ?;
   *  Then your partitionArgs could be a Seq[String] as you want to insert an id for each partition into the query
   * @param insertPlaceholders function that takes your partitionArgs for the current partition and set's the values on
   * the prepared statement for execution
   * @param setupPlaceholders if you have to dynamically size the number of placeholders due to use of SetPlaceholder,
   * then provide a function here that calls JdbcPartitioningRDD.insertStatementSetPlaceholders for each placeholder
   * you need to replace in order of appearance in the String.
   * For example: if your query is: select * from some_table where user_ids in (%SET_PLACEHOLDER%);
   * Then you might provide a function here to get a ? placeholder for each user Id:
   * { (query: String, userIds: Seq[String]) => JdbcPartitioningRDD.insertStatementSetPlaceholders(query, userIds.size) }
   * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
   *   This should only call getInt, getString, etc; the RDD takes care of calling next.
   *   The default maps a ResultSet to an array of Object.
   */
  def create[A,T](
                 sc: JavaSparkContext,
                 connectionFactory: ConnectionFactory,
                 sql: String,
                 partitionArgs: java.util.List[A],
                 insertPlaceholders: JFunction2[PreparedStatement, A, Void],
                 setupPlaceholders: JFunction2[String, A, String],
                 mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val jdbcRDD = new JdbcPartitioningRDD[A,T](
      sc.sc,
      () => connectionFactory.getConnection,
      sql,
      partitionArgs.asScala,
      (stmt: PreparedStatement, args: A) => insertPlaceholders.call(stmt, args),
      (query: String, args: A) => setupPlaceholders.call(query, args),
      (resultSet: ResultSet) => mapRow.call(resultSet))(fakeClassTag)

    new JavaRDD[T](jdbcRDD)(fakeClassTag)
  }

  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results. Each row is
   * converted into a `Object` array. For usage example, see test case JavaAPISuite.testJavaJdbcPartitioningRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query should contain ? placeholders for each single value you intend to insert into the query.
   *   For example: select * from some_table where partition_id = ? and something_else = 15;
   *   If you want to handle inserting a set of items for an in-clause or similar use the
   *   JdbcPartitioningRDD.SetPlaceholder value as a placeholder. This ties in with the setupPlaceholders argument that
   *   will convert a specific partition sql query to have the correct ? placeholders for all items
   * @param partitionArgs the value or group of values you require as query parameters for each partition of the query
   *  For example if your query is : select * from some_table where partition_id = ?;
   *  Then your partitionArgs could be a Seq[String] as you want to insert an id for each partition into the query
   * @param insertPlaceholders function that takes your partitionArgs for the current partition and set's the values on
   * the prepared statement for execution
   * @param setupPlaceholders if you have to dynamically size the number of placeholders due to use of SetPlaceholder,
   * then provide a function here that calls JdbcPartitioningRDD.insertStatementSetPlaceholders for each placeholder
   * you need to replace in order of appearance in the String.
   * For example: if your query is: select * from some_table where user_ids in (%SET_PLACEHOLDER%);
   * Then you might provide a function here to get a ? placeholder for each user Id:
   * { (query: String, userIds: Seq[String]) => JdbcPartitioningRDD.insertStatementSetPlaceholders(query, userIds.size) }
   */
  def create[A](
              sc: JavaSparkContext,
              connectionFactory: ConnectionFactory,
              sql: String,
              partitionArgs: java.util.List[A],
              insertPlaceholders: JFunction2[PreparedStatement, A, Void],
              setupPlaceholders: JFunction2[String, A, String]): JavaRDD[Array[Object]] = {

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, connectionFactory, sql, partitionArgs, insertPlaceholders, setupPlaceholders, mapRow)
  }

  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results. Use this if you do not need to
   * insert extra placeholders for a set of items like for an in clause in the query.
   * usage example, see test case JavaAPISuite.testJavaJdbcPartitioningRDD.
   *
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query should contain ? placeholders for each single value you intend to insert into the query.
   *   For example: select * from some_table where partition_id = ? and something_else = 15;
   *   If you want to handle inserting a set of items for an in-clause or similar use the
   *   JdbcPartitioningRDD.SetPlaceholder value as a placeholder. This ties in with the setupPlaceholders argument that
   *   will convert a specific partition sql query to have the correct ? placeholders for all items
   * @param partitionArgs the value or group of values you require as query parameters for each partition of the query
   *  For example if your query is : select * from some_table where partition_id = ?;
   *  Then your partitionArgs could be a Seq[String] as you want to insert an id for each partition into the query
   * @param insertPlaceholders function that takes your partitionArgs for the current partition and set's the values on
   * the prepared statement for execution
   * @param mapRow a function from a ResultSet to a single row of the desired result type(s).
   *   This should only call getInt, getString, etc; the RDD takes care of calling next.
   *   The default maps a ResultSet to an array of Object.
   */
  def create[A,T](
                   sc: JavaSparkContext,
                   connectionFactory: ConnectionFactory,
                   sql: String,
                   partitionArgs: java.util.List[A],
                   insertPlaceholders: JFunction2[PreparedStatement, A, Void],
                   mapRow: JFunction[ResultSet, T]): JavaRDD[T] = {

    val setupPlaceholders = new JFunction2[String, A, String] {
      override def call(query: String, arg: A): String = query
    }

    create(sc, connectionFactory, sql, partitionArgs, insertPlaceholders, setupPlaceholders, mapRow)
  }

  /**
   * Create an RDD that executes an SQL query on a JDBC connection and reads results. Use this if you do not need to
   * insert extra placeholders for a set of items like for an in clause in the query. Each row is converted into a
   * `Object` array. For usage example, see test case JavaAPISuite.testJavaJdbcPartitioningRDD.
   * @param connectionFactory a factory that returns an open Connection.
   *   The RDD takes care of closing the connection.
   * @param sql the text of the query.
   *   The query should contain ? placeholders for each single value you intend to insert into the query.
   *   For example: select * from some_table where partition_id = ? and something_else = 15;
   *   If you want to handle inserting a set of items for an in-clause or similar use the
   *   JdbcPartitioningRDD.SetPlaceholder value as a placeholder. This ties in with the setupPlaceholders argument that
   *   will convert a specific partition sql query to have the correct ? placeholders for all items
   * @param partitionArgs the value or group of values you require as query parameters for each partition of the query
   *  For example if your query is : select * from some_table where partition_id = ?;
   *  Then your partitionArgs could be a Seq[String] as you want to insert an id for each partition into the query
   * @param insertPlaceholders function that takes your partitionArgs for the current partition and set's the values on
   * the prepared statement for execution
   */
  def create[A](
                 sc: JavaSparkContext,
                 connectionFactory: ConnectionFactory,
                 sql: String,
                 partitionArgs: java.util.List[A],
                 insertPlaceholders: JFunction2[PreparedStatement, A, Void]): JavaRDD[Array[Object]] = {

    val setupPlaceholders = new JFunction2[String, A, String] {
      override def call(query: String, arg: A): String = query
    }

    val mapRow = new JFunction[ResultSet, Array[Object]] {
      override def call(resultSet: ResultSet): Array[Object] = {
        resultSetToObjectArray(resultSet)
      }
    }

    create(sc, connectionFactory, sql, partitionArgs, insertPlaceholders, setupPlaceholders, mapRow)
  }
}
