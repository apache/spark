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

import scala.util.Using
import scala.util.control.NonFatal

import org.apache.spark.{InterruptibleIterator, Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.execution.datasources.{DataSourceMetricsMixin, ExternalEngineDatasourceRDD}
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    Using.resource(dialect.createConnectionFactory(options)(-1)) { conn =>
      Using.resource(conn.prepareStatement(query)) { statement =>
        statement.setQueryTimeout(options.queryTimeout)
        Using.resource(statement.executeQuery()) { rs =>
          JdbcUtils.getSchema(rs, dialect, alwaysNullable = true,
            isTimestampNTZ = options.preferTimestampNTZ)
        }
      }
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
    val fieldMap = schema.fields.map(x => x.name -> x).toMap
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
      sortOrders: Array[String] = Array.empty[String],
      offset: Int = 0): RDD[InternalRow] = {
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
      sortOrders,
      offset)
      .withDialect(dialect)
  }
  // scalastyle:on argcount
}

/**
 * An RDD representing a query is related to a table in a database accessed via JDBC.
 * Both the driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
class JDBCRDD(
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
    sortOrders: Array[String],
    offset: Int)
  extends RDD[InternalRow](sc, Nil) with DataSourceMetricsMixin with ExternalEngineDatasourceRDD {

  /**
   * Execution time of the query issued to JDBC connection
   */
  val queryExecutionTimeMetric: SQLMetric = SQLMetrics.createNanoTimingMetric(
    sparkContext,
    name = "JDBC query execution time")

  /**
   * Dialect to use instead of inferring it from the URL.
   */
  private var prescribedDialect: Option[JdbcDialect] = None

  private lazy val dialect = prescribedDialect.getOrElse(JdbcDialects.get(url))

  /**
   * Prescribe a particular dialect to use for this RDD. If not set, the dialect will be
   * automatically resolved from the JDBC URL. This previous behavior is preserved for binary
   * compatibility.
   */
  def withDialect(dialect: JdbcDialect): JDBCRDD = {
    prescribedDialect = Some(dialect)
    this
  }

  def generateJdbcQuery(partition: Option[JDBCPartition]): String = {
    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.
    var builder = dialect
      .getJdbcSQLQueryBuilder(options)
      .withPredicates(predicates, partition.getOrElse(JDBCPartition(whereClause = null, idx = 1)))
      .withColumns(columns)
      .withSortOrders(sortOrders)
      .withLimit(limit)
      .withOffset(offset)

    groupByColumns.foreach { groupByKeys =>
      builder = builder.withGroupByColumns(groupByKeys)
    }

    sample.foreach { tableSampleInfo =>
      builder = builder.withTableSample(tableSampleInfo)
    }

    builder.build()
  }

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  override def getExternalEngineQuery: String = {
    generateJdbcQuery(partition = None)
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
    import scala.jdk.CollectionConverters._
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

    val sqlText = generateJdbcQuery(Some(part))
    stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)

    val startTime = System.nanoTime
    rs = stmt.executeQuery()
    val endTime = System.nanoTime

    val executionTime = endTime - startTime
    queryExecutionTimeMetric.add(executionTime)

    val rowsIterator =
      JdbcUtils.resultSetToSparkInternalRows(rs, dialect, schema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())
  }

  override def getMetrics: Seq[(String, SQLMetric)] = {
    Seq(
      "queryExecutionTime" -> queryExecutionTimeMetric
    )
  }
}
