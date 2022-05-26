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

package org.apache.spark.sql.execution.datasources.jdbc

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.util.control.NonFatal

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

/**
 * Data corresponding to one partition of a JDBCRDD.
 */
case class JDBCPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

object JDBCRDD extends Logging {

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param options - JDBC options that contains url, table and other information.
   *
   * @return A StructType giving the table's Catalyst schema.
   * @throws java.sql.SQLException if the table specification is garbage.
   * @throws java.sql.SQLException if the table contains an unsupported type.
   */
  def resolveTable(options: JDBCOptions): StructType = {
    val url = options.url
    val prepareQuery = options.prepareQuery
    val table = options.tableOrQuery
    val dialect = JdbcDialects.get(url)
    getQueryOutputSchema(prepareQuery + dialect.getSchemaQuery(table), options, dialect)
  }

  def getQueryOutputSchema(
      query: String, options: JDBCOptions, dialect: JdbcDialect): StructType = {
    val conn: Connection = dialect.createConnectionFactory(options)(-1)
    try {
      val statement = conn.prepareStatement(query)
      try {
        statement.setQueryTimeout(options.queryTimeout)
        val rs = statement.executeQuery()
        try {
          JdbcUtils.getSchema(rs, dialect, alwaysNullable = true)
        } finally {
          rs.close()
        }
      } finally {
        statement.close()
      }
    } finally {
      conn.close()
    }
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param schema - The Catalyst schema of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields.map(x => x.name -> x): _*)
    new StructType(columns.map(name => fieldMap(name)))
  }

  /**
   * Build and return JDBCRDD from the given information.
   *
   * @param sc - Your SparkContext.
   * @param schema - The Catalyst schema of the underlying database table.
   * @param requiredColumns - The names of the columns or aggregate columns to SELECT.
   * @param predicates - The predicates to include in all WHERE clauses.
   * @param parts - An array of JDBCPartitions specifying partition ids and
   *    per-partition WHERE clauses.
   * @param options - JDBC options that contains url, table and other information.
   * @param outputSchema - The schema of the columns or aggregate columns to SELECT.
   * @param groupByColumns - The pushed down group by columns.
   * @param sample - The pushed down tableSample.
   * @param limit - The pushed down limit. If the value is 0, it means no limit or limit
   *                is not pushed down.
   * @param sortOrders - The sort orders cooperates with limit to realize top N.
   *
   * @return An RDD representing "SELECT requiredColumns FROM fqTable".
   */
  // scalastyle:off argcount
  def scanTable(
      sc: SparkContext,
      schema: StructType,
      requiredColumns: Array[String],
      predicates: Array[Predicate],
      parts: Array[Partition],
      options: JDBCOptions,
      outputSchema: Option[StructType] = None,
      groupByColumns: Option[Array[String]] = None,
      sample: Option[TableSampleInfo] = None,
      limit: Int = 0,
      sortOrders: Array[String] = Array.empty[String]): RDD[InternalRow] = {
    val url = options.url
    val dialect = JdbcDialects.get(url)
    val quotedColumns = if (groupByColumns.isEmpty) {
      requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    } else {
      // these are already quoted in JDBCScanBuilder
      requiredColumns
    }
    new JDBCRDD(
      sc,
      dialect.createConnectionFactory(options),
      outputSchema.getOrElse(pruneSchema(schema, requiredColumns)),
      quotedColumns,
      predicates,
      parts,
      url,
      options,
      groupByColumns,
      sample,
      limit,
      sortOrders)
  }
  // scalastyle:on argcount
}

/**
 * An RDD representing a query is related to a table in a database accessed via JDBC.
 * Both the driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
private[jdbc] class JDBCRDD(
    sc: SparkContext,
    getConnection: Int => Connection,
    schema: StructType,
    columns: Array[String],
    predicates: Array[Predicate],
    partitions: Array[Partition],
    url: String,
    options: JDBCOptions,
    groupByColumns: Option[Array[String]],
    sample: Option[TableSampleInfo],
    limit: Int,
    sortOrders: Array[String])
  extends RDD[InternalRow](sc, Nil) {

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  private val columnList: String = if (columns.isEmpty) "1" else columns.mkString(",")

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  private val filterWhereClause: String = {
    val dialect = JdbcDialects.get(url)
    predicates.flatMap(dialect.compileExpression(_)).map(p => s"($p)").mkString(" AND ")
  }

  /**
   * A WHERE clause representing both `filters`, if any, and the current partition.
   */
  private def getWhereClause(part: JDBCPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      "WHERE " + s"($filterWhereClause)" + " AND " + s"(${part.whereClause})"
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else if (filterWhereClause.length > 0) {
      "WHERE " + filterWhereClause
    } else {
      ""
    }
  }

  /**
   * A GROUP BY clause representing pushed-down grouping columns.
   */
  private def getGroupByClause: String = {
    if (groupByColumns.nonEmpty && groupByColumns.get.nonEmpty) {
      // The GROUP BY columns should already be quoted by the caller side.
      s"GROUP BY ${groupByColumns.get.mkString(", ")}"
    } else {
      ""
    }
  }

  private def getOrderByClause: String = {
    if (sortOrders.nonEmpty) {
      s" ORDER BY ${sortOrders.mkString(", ")}"
    } else {
      ""
    }
  }

  /**
   * Runs the SQL query against the JDBC driver.
   *
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[InternalRow] = {
    var closed = false
    var rs: ResultSet = null
    var stmt: PreparedStatement = null
    var conn: Connection = null

    def close(): Unit = {
      if (closed) return
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
          if (!conn.isClosed && !conn.getAutoCommit) {
            try {
              conn.commit()
            } catch {
              case NonFatal(e) => logWarning("Exception committing transaction", e)
            }
          }
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
      closed = true
    }

    context.addTaskCompletionListener[Unit]{ context => close() }

    val inputMetrics = context.taskMetrics().inputMetrics
    val part = thePart.asInstanceOf[JDBCPartition]
    conn = getConnection(part.idx)
    val dialect = JdbcDialects.get(url)
    import scala.collection.JavaConverters._
    dialect.beforeFetch(conn, options.asProperties.asScala.toMap)

    // This executes a generic SQL statement (or PL/SQL block) before reading
    // the table/query via JDBC. Use this feature to initialize the database
    // session environment, e.g. for optimizations and/or troubleshooting.
    options.sessionInitStatement match {
      case Some(sql) =>
        val statement = conn.prepareStatement(sql)
        logInfo(s"Executing sessionInitStatement: $sql")
        try {
          statement.setQueryTimeout(options.queryTimeout)
          statement.execute()
        } finally {
          statement.close()
        }
      case None =>
    }

    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = getWhereClause(part)

    val myTableSampleClause: String = if (sample.nonEmpty) {
      JdbcDialects.get(url).getTableSample(sample.get)
    } else {
      ""
    }

    val myLimitClause: String = dialect.getLimitClause(limit)

    val sqlText = options.prepareQuery +
      s"SELECT $columnList FROM ${options.tableOrQuery} $myTableSampleClause" +
      s" $myWhereClause $getGroupByClause $getOrderByClause $myLimitClause"
    stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    rs = stmt.executeQuery()
    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(rs, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())
  }
}
