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
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
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
    val table = options.tableOrQuery
    val dialect = JdbcDialects.get(url)
    val conn: Connection = JdbcUtils.createConnectionFactory(options)()
    try {
      val statement = conn.prepareStatement(dialect.getSchemaQuery(table))
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
   * Turns a single Filter into a String representing a SQL expression.
   * Returns None for an unhandled filter.
   */
  def compileFilter(f: Filter, dialect: JdbcDialect): Option[String] = {
    def quote(colName: String): String = dialect.quoteIdentifier(colName)

    Option(f match {
      case EqualTo(attr, value) => s"${quote(attr)} = ${dialect.compileValue(value)}"
      case EqualNullSafe(attr, value) =>
        val col = quote(attr)
        s"(NOT ($col != ${dialect.compileValue(value)} OR $col IS NULL OR " +
          s"${dialect.compileValue(value)} IS NULL) OR " +
          s"($col IS NULL AND ${dialect.compileValue(value)} IS NULL))"
      case LessThan(attr, value) => s"${quote(attr)} < ${dialect.compileValue(value)}"
      case GreaterThan(attr, value) => s"${quote(attr)} > ${dialect.compileValue(value)}"
      case LessThanOrEqual(attr, value) => s"${quote(attr)} <= ${dialect.compileValue(value)}"
      case GreaterThanOrEqual(attr, value) => s"${quote(attr)} >= ${dialect.compileValue(value)}"
      case IsNull(attr) => s"${quote(attr)} IS NULL"
      case IsNotNull(attr) => s"${quote(attr)} IS NOT NULL"
      case StringStartsWith(attr, value) => s"${quote(attr)} LIKE '${value}%'"
      case StringEndsWith(attr, value) => s"${quote(attr)} LIKE '%${value}'"
      case StringContains(attr, value) => s"${quote(attr)} LIKE '%${value}%'"
      case In(attr, value) if value.isEmpty =>
        s"CASE WHEN ${quote(attr)} IS NULL THEN NULL ELSE FALSE END"
      case In(attr, value) => s"${quote(attr)} IN (${dialect.compileValue(value)})"
      case Not(f) => compileFilter(f, dialect).map(p => s"(NOT ($p))").getOrElse(null)
      case Or(f1, f2) =>
        // We can't compile Or filter unless both sub-filters are compiled successfully.
        // It applies too for the following And filter.
        // If we can make sure compileFilter supports all filters, we can remove this check.
        val or = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (or.size == 2) {
          or.map(p => s"($p)").mkString(" OR ")
        } else {
          null
        }
      case And(f1, f2) =>
        val and = Seq(f1, f2).flatMap(compileFilter(_, dialect))
        if (and.size == 2) {
          and.map(p => s"($p)").mkString(" AND ")
        } else {
          null
        }
      case _ => null
    })
  }

  def compileAggregates(
      aggregates: Seq[AggregateFunc],
      dialect: JdbcDialect): (Map[String, Array[String]], Array[Filter]) = {
    var filters = Array.empty[Filter]
    def quote(colName: String): String = dialect.quoteIdentifier(colName)
    val compiledAggregates = aggregates.map {
      case Min(column, isDistinct, filter) =>
        if (filter.nonEmpty) filters +:= filter.get
        if (isDistinct) {
          Some(quote(column) -> s"MIN(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"MIN(${quote(column)})")
        }
      case Max(column, isDistinct, filter) =>
        if (filter.nonEmpty) filters +:= filter.get
        if (isDistinct) {
          Some(quote(column) -> s"MAX(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"MAX(${quote(column)})")
        }
      case Sum(column, isDistinct, _) =>
        if (isDistinct) {
          Some(quote(column) -> s"SUM(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"SUM(${quote(column)})")
        }
      case Avg(column, isDistinct, _) =>
        if (isDistinct) {
          Some(quote(column) -> s"AVG(DISTINCT(${quote(column)}))")
        } else {
          Some(quote(column) -> s"AVG(${quote(column)})")
        }
      case _ => None
    }
    var map: Map[String, Array[String]] = Map()
    if (!compiledAggregates.contains(None)) {
      for (i <- 0 until compiledAggregates.length) {
        val key = compiledAggregates(i).get._1
        val value = map.get(key)
        if (value == None) {
          map += (key -> Array(compiledAggregates(i).get._2))
        } else {
          map += (key -> (value.get :+ compiledAggregates(i).get._2))
        }
      }
    }
    (map, filters)
  }

  /**
   * Build and return JDBCRDD from the given information.
   *
   * @param sc - Your SparkContext.
   * @param schema - The Catalyst schema of the underlying database table.
   * @param requiredColumns - The names of the columns to SELECT.
   * @param filters - The filters to include in all WHERE clauses.
   * @param parts - An array of JDBCPartitions specifying partition ids and
   *    per-partition WHERE clauses.
   * @param options - JDBC options that contains url, table and other information.
   *
   * @return An RDD representing "SELECT requiredColumns FROM fqTable".
   */
  def scanTable(
      sc: SparkContext,
      schema: StructType,
      requiredColumns: Array[String],
      filters: Array[Filter],
      parts: Array[Partition],
      options: JDBCOptions,
      aggregation: Aggregation = Aggregation(Seq.empty[AggregateFunc], Seq.empty[String]))
    : RDD[InternalRow] = {
    val url = options.url
    val dialect = JdbcDialects.get(url)
    val quotedColumns = requiredColumns.map(colName => dialect.quoteIdentifier(colName))
    new JDBCRDD(
      sc,
      JdbcUtils.createConnectionFactory(options),
      pruneSchema(schema, requiredColumns),
      quotedColumns,
      filters,
      parts,
      url,
      options,
      aggregation)
  }
}

/**
 * An RDD representing a table in a database accessed via JDBC.  Both the
 * driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
private[jdbc] class JDBCRDD(
    sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    columns: Array[String],
    filters: Array[Filter],
    partitions: Array[Partition],
    url: String,
    options: JDBCOptions,
    aggregation: Aggregation = Aggregation(Seq.empty[AggregateFunc], Seq.empty[String]))
  extends RDD[InternalRow](sc, Nil) {

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  private var updatedSchema: StructType = new StructType()

  private var updatedFilters = Array.empty[Filter]

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  private val columnList: String = {
    val compiledAgg = JDBCRDD.compileAggregates(aggregation.aggregateExpressions,
      JdbcDialects.get(url))
    val compiledAggregates = compiledAgg._1
    updatedFilters = filters ++ compiledAgg._2
    val flippedMap = compiledAggregates.map(_.swap)
    val colDataTypeMap: Map[String, StructField] = columns.zip(schema.fields).toMap
    val sb = new StringBuilder()
    columns.map(c => compiledAggregates.getOrElse(c, c)).foreach(
      x => x match {
        case str: String =>
          sb.append(", ").append(str)
          updatedSchema = updatedSchema.add(colDataTypeMap.get(str).get)
        case array: Array[String] =>
          sb.append(", ").append(array.mkString(", "))
          for (a <- array) {
            if (a.contains("MAX") || a.contains("MIN")) {
              // get the original column data type
              updatedSchema = updatedSchema.add(colDataTypeMap.get(flippedMap.get(array).get).get)
            } else if (a.contains("SUM")) {
              // Same as Spark, promote to the largest types to prevent overflows.
              // IntegralType: if not Long, promote to Long
              // FractionalType: if not Double, promote to Double
              // DecimalType.Fixed(precision, scale):
              //   follow what is done in Sum.resultType, +10 to precision
              val dataField = colDataTypeMap.get(flippedMap.get(array).get).get
              dataField.dataType match {
                case DecimalType.Fixed(precision, scale) =>
                  updatedSchema = updatedSchema.add(
                    dataField.name, DecimalType.bounded(precision + 10, scale), dataField.nullable)
                case _: IntegralType =>
                  updatedSchema = updatedSchema.add(dataField.name, LongType, dataField.nullable)
                case _ =>
                  updatedSchema = updatedSchema.add(dataField.name, DoubleType, dataField.nullable)
              }
            } else { // AVG
              // Same as Spark, promote to the largest types to prevent overflows.
              // DecimalType.Fixed(precision, scale):
              //   follow what is done in Average.resultType, +4 to precision and scale
              // promote to Double for other data types
              val dataField = colDataTypeMap.get(flippedMap.get(array).get).get
              dataField.dataType match {
                case DecimalType.Fixed(p, s) => updatedSchema =
                  updatedSchema.add(
                    dataField.name, DecimalType.bounded(p + 4, s + 4), dataField.nullable)
                case _ => updatedSchema =
                  updatedSchema.add(dataField.name, DoubleType, dataField.nullable)
               }
            }
          }
      }
    )

    if (sb.length == 0) "1" else sb.substring(1)
  }

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  private val filterWhereClause: String =
    updatedFilters
      .flatMap(JDBCRDD.compileFilter(_, JdbcDialects.get(url)))
      .map(p => s"($p)").mkString(" AND ")

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
    if (aggregation.groupByExpressions.length > 0) {
      val quotedColumns = aggregation.groupByExpressions.map(JdbcDialects.get(url).quoteIdentifier)
      s"GROUP BY ${quotedColumns.mkString(", ")}"
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
    conn = getConnection()
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

    val sqlText = s"SELECT $columnList FROM ${options.tableOrQuery} $myWhereClause" +
      s" $getGroupByClause"
    stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    stmt.setFetchSize(options.fetchSize)
    stmt.setQueryTimeout(options.queryTimeout)
    rs = stmt.executeQuery()

    val rowsIterator = JdbcUtils.resultSetToSparkInternalRows(rs, updatedSchema, inputMetrics)

    CompletionIterator[InternalRow, Iterator[InternalRow]](
      new InterruptibleIterator(context, rowsIterator), close())
  }
}
