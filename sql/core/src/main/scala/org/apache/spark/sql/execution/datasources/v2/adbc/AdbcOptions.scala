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

package org.apache.spark.sql.execution.datasources.v2.adbc

import java.util.Locale

import org.apache.arrow.adbc.core.AdbcDriver

import org.apache.spark.sql.catalyst.DataSourceOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryExecutionErrors

/**
 * Options for the ADBC data source.
 */
class AdbcOptions(val parameters: CaseInsensitiveMap[String]) extends Serializable {

  import AdbcOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  parameters.originalMap.foreach { case (k, v) =>
    if (v == null) {
      throw QueryExecutionErrors.nullDataSourceOption(k)
    }
  }

  // ------------------------------------------------------------
  // Required parameters
  // ------------------------------------------------------------
  // the class name of the ADBC driver's `AdbcDriverFactory`
  val driver: String = parameters.get(ADBC_DRIVER) match {
    case None => throw QueryExecutionErrors.adbcMissingOptionError(ADBC_DRIVER)
    case Some(d) if d.trim.isEmpty =>
      throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_DRIVER)
    case Some(d) => d.trim
  }

  /** The name of the table to read or write, as a single identifier. */
  val table: Option[String] = parameters.get(ADBC_TABLE_NAME).map { name =>
    if (name.trim.isEmpty) throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_TABLE_NAME)
    name.trim
  }

  /** A query to read, without trailing semicolons. */
  val query: Option[String] = parameters.get(ADBC_QUERY_STRING).map { subquery =>
    val trimmed = subquery.replaceAll("[\\s;]+$", "").trim
    if (trimmed.isEmpty) throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_QUERY_STRING)
    trimmed
  }

  (table, query) match {
    case (Some(_), Some(_)) =>
      throw QueryExecutionErrors.adbcBothTableAndQueryError(ADBC_TABLE_NAME, ADBC_QUERY_STRING)
    case (None, None) =>
      throw QueryExecutionErrors.adbcMissingTableOrQueryError(ADBC_TABLE_NAME, ADBC_QUERY_STRING)
    case _ =>
  }

  /** The database schema of the table, or None for the connection's current schema. */
  val dbSchema: Option[String] = parameters.get(ADBC_SCHEMA_NAME).map { name =>
    if (name.trim.isEmpty) throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_SCHEMA_NAME)
    if (query.isDefined) {
      throw QueryExecutionErrors.adbcSchemaWithQueryError(
        ADBC_SCHEMA_NAME, ADBC_TABLE_NAME, ADBC_QUERY_STRING)
    }
    name.trim
  }

  /** The relation, for messages: the table, qualified by its schema, or the query. */
  def description: String = table match {
    case Some(t) => (dbSchema.toSeq :+ t).mkString(".")
    case None => query.get
  }

  /**
   * The relation as it appears in a FROM clause: the table, with its name quoted with `quote`,
   * or the query as an aliased subquery.
   */
  def fromClause(quote: String): String = table match {
    case Some(t) => (dbSchema.toSeq :+ t).map(AdbcUtils.quoteIdentifier(_, quote)).mkString(".")
    case None => s"(${query.get}) SPARK_GEN_SUBQ"
  }

  /** A query that reads every column of the relation. A user query is sent unchanged. */
  def selectAllQuery(quote: String): String =
    query.getOrElse(s"SELECT * FROM ${fromClause(quote)}")

  /**
   * The string to quote identifiers with, given the string the driver reports, if any. If
   * neither the option nor the driver gives one, table names are quoted with the SQL standard's
   * double quote, but the columns of a query are not quoted, and so are not pruned.
   */
  def quoteChar(reported: => Option[String]): Option[String] =
    identifierQuoteChar.orElse(reported)
      .orElse(table.map(_ => DEFAULT_IDENTIFIER_QUOTE_CHAR))

  // ------------------------------------------------------------
  // Optional parameters
  // ------------------------------------------------------------
  // the string used to quote identifiers, overriding what the driver reports
  val identifierQuoteChar: Option[String] = parameters.get(ADBC_IDENTIFIER_QUOTE_CHAR).map { q =>
    if (q.isEmpty) throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_IDENTIFIER_QUOTE_CHAR)
    q
  }

  // whether to let the driver split the query into partitions with `executePartitioned`
  val driverPartitioning: Boolean = parameters.get(ADBC_DRIVER_PARTITIONING) match {
    case None => true
    case Some(v) if v.trim.equalsIgnoreCase(DRIVER_PARTITIONING_AUTO) => true
    case Some(v) if v.trim.equalsIgnoreCase(DRIVER_PARTITIONING_NONE) => false
    case Some(v) => throw QueryExecutionErrors.adbcInvalidOptionValueError(
      ADBC_DRIVER_PARTITIONING, v, Seq(DRIVER_PARTITIONING_AUTO, DRIVER_PARTITIONING_NONE))
  }

  // split the read into ranges of a column's values, like the JDBC data source
  val rangePartitioning: Option[AdbcRangePartitioning] = {
    val partitionOptions =
      Seq(ADBC_PARTITION_COLUMN, ADBC_LOWER_BOUND, ADBC_UPPER_BOUND, ADBC_NUM_PARTITIONS)
    partitionOptions.count(parameters.contains) match {
      case 0 => None
      case n if n == partitionOptions.length =>
        val column = parameters(ADBC_PARTITION_COLUMN)
        if (column.trim.isEmpty) {
          throw QueryExecutionErrors.adbcEmptyOptionError(ADBC_PARTITION_COLUMN)
        }
        val lowerBound = longOption(ADBC_LOWER_BOUND)
        val upperBound = longOption(ADBC_UPPER_BOUND)
        if (lowerBound > upperBound) {
          throw QueryExecutionErrors.adbcInvalidPartitionBoundsError(
            ADBC_LOWER_BOUND, lowerBound, ADBC_UPPER_BOUND, upperBound)
        }
        Some(AdbcRangePartitioning(
          column, lowerBound, upperBound, positiveIntOption(ADBC_NUM_PARTITIONS)))
      case _ =>
        throw QueryExecutionErrors.adbcIncompletePartitionOptionsError(partitionOptions)
    }
  }

  // ------------------------------------------------------------
  // Write parameters
  // ------------------------------------------------------------
  // the number of rows to send to the database in each bulk ingest call
  val batchSize: Int = parameters.get(ADBC_BATCH_SIZE)
    .map(_ => positiveIntOption(ADBC_BATCH_SIZE))
    .getOrElse(DEFAULT_BATCH_SIZE)

  /** The table to write to, with its SQL name quoted with `quote`. A query can't be written. */
  def writeTarget(quote: String): AdbcWriteTarget = table match {
    case Some(t) => AdbcWriteTarget(t, dbSchema, fromClause(quote))
    case None =>
      throw QueryExecutionErrors.adbcWriteWithQueryError(ADBC_QUERY_STRING, ADBC_TABLE_NAME)
  }

  private def longOption(name: String): Long = {
    val value = parameters(name)
    value.trim.toLongOption.getOrElse {
      throw QueryExecutionErrors.adbcNotAnIntegerOptionError(name, value)
    }
  }

  private def positiveIntOption(name: String): Int = {
    val value = parameters(name)
    value.trim.toIntOption.filter(_ > 0).getOrElse {
      throw QueryExecutionErrors.adbcNotAPositiveIntegerOptionError(name, value)
    }
  }

  /**
   * The database parameters passed to the driver, keyed by their ADBC names: the standard
   * ADBC parameters, and the native driver to load for the JNI driver. Other options are not
   * passed to the driver.
   */
  val databaseParameters: Map[String, String] = Seq(
    ADBC_URI -> AdbcDriver.PARAM_URI.getKey,
    ADBC_USERNAME -> AdbcDriver.PARAM_USERNAME.getKey,
    ADBC_PASSWORD -> AdbcDriver.PARAM_PASSWORD.getKey,
    // `JniDriver.PARAM_DRIVER` in adbc-driver-jni, which Spark doesn't depend on.
    ADBC_JNI_DRIVER -> "jni.driver"
  ).flatMap { case (option, key) => parameters.get(option).map(key -> _) }.toMap
}

object AdbcOptions extends DataSourceOptions {
  val ADBC_DRIVER = newOption("driver")
  val ADBC_TABLE_NAME = newOption("dbtable")
  val ADBC_QUERY_STRING = newOption("query")
  val ADBC_SCHEMA_NAME = newOption("dbschema")
  val ADBC_IDENTIFIER_QUOTE_CHAR = newOption("identifierQuoteChar")
  val ADBC_URI = newOption("uri")
  val ADBC_USERNAME = newOption("username")
  val ADBC_PASSWORD = newOption("password")
  val ADBC_JNI_DRIVER = newOption("jni.driver")
  val ADBC_DRIVER_PARTITIONING = newOption("driverPartitioning")
  val ADBC_PARTITION_COLUMN = newOption("partitionColumn")
  val ADBC_LOWER_BOUND = newOption("lowerBound")
  val ADBC_UPPER_BOUND = newOption("upperBound")
  val ADBC_NUM_PARTITIONS = newOption("numPartitions")
  val ADBC_BATCH_SIZE = newOption("batchsize")

  val DEFAULT_BATCH_SIZE = 10000

  // The SQL standard's identifier quote, used for table names if no other is known.
  val DEFAULT_IDENTIFIER_QUOTE_CHAR = "\""

  /** The options that select a database and a relation in it, in lower case. */
  val RELATION_OPTIONS: Set[String] = Set(ADBC_DRIVER, ADBC_URI, ADBC_USERNAME, ADBC_PASSWORD,
    ADBC_JNI_DRIVER, ADBC_TABLE_NAME, ADBC_SCHEMA_NAME, ADBC_QUERY_STRING)
    .map(_.toLowerCase(Locale.ROOT))

  val DRIVER_PARTITIONING_AUTO = "auto"
  val DRIVER_PARTITIONING_NONE = "none"
}

/**
 * Splits a read into `numPartitions` ranges of the values of an integer column. The bounds only
 * decide the size of the ranges: values outside them fall in the first or last range, and so do
 * NULL values, so every row is read once.
 */
case class AdbcRangePartitioning(
    column: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int) {

  /** Returns one predicate per partition on `quotedColumn`, or none to read a single partition. */
  def predicates(quotedColumn: String): Seq[String] = {
    // There can't be more ranges than values between the bounds.
    val width = BigInt(upperBound) - BigInt(lowerBound)
    val n = width.min(BigInt(numPartitions)).max(BigInt(1)).toInt
    if (n == 1) {
      Seq.empty
    } else {
      val bounds = (1 until n).map(i => (BigInt(lowerBound) + width * i / n).toLong)
      val first = s"$quotedColumn < ${bounds.head} OR $quotedColumn IS NULL"
      val middle = bounds.sliding(2).collect { case Seq(lo, hi) =>
        s"$quotedColumn >= $lo AND $quotedColumn < $hi"
      }
      val last = s"$quotedColumn >= ${bounds.last}"
      (first +: middle.toSeq) :+ last
    }
  }
}
