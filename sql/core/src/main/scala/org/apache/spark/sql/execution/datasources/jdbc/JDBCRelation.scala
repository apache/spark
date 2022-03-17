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

import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.util.{DateFormatter, DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.TableSampleInfo
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DateType, NumericType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Instructions on how to partition the table among workers.
 */
private[sql] case class JDBCPartitioningInfo(
    column: String,
    columnType: DataType,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int)

private[sql] object JDBCRelation extends Logging {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   *
   * Null value predicate is added to the first partition where clause to include
   * the rows with null value for the partitions column.
   *
   * @param schema resolved schema of a JDBC table
   * @param resolver function used to determine if two identifiers are equal
   * @param timeZoneId timezone ID to be used if a partition column type is date or timestamp
   * @param jdbcOptions JDBC options that contains url
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(
      schema: StructType,
      resolver: Resolver,
      timeZoneId: String,
      jdbcOptions: JDBCOptions): Array[Partition] = {
    val partitioning = {
      import JDBCOptions._

      val partitionColumn = jdbcOptions.partitionColumn
      val lowerBound = jdbcOptions.lowerBound
      val upperBound = jdbcOptions.upperBound
      val numPartitions = jdbcOptions.numPartitions

      if (partitionColumn.isEmpty) {
        assert(lowerBound.isEmpty && upperBound.isEmpty, "When 'partitionColumn' is not " +
          s"specified, '$JDBC_LOWER_BOUND' and '$JDBC_UPPER_BOUND' are expected to be empty")
        null
      } else {
        assert(lowerBound.nonEmpty && upperBound.nonEmpty && numPartitions.nonEmpty,
          s"When 'partitionColumn' is specified, '$JDBC_LOWER_BOUND', '$JDBC_UPPER_BOUND', and " +
            s"'$JDBC_NUM_PARTITIONS' are also required")

        val (column, columnType) = verifyAndGetNormalizedPartitionColumn(
          schema, partitionColumn.get, resolver, jdbcOptions)

        val lowerBoundValue = toInternalBoundValue(lowerBound.get, columnType, timeZoneId)
        val upperBoundValue = toInternalBoundValue(upperBound.get, columnType, timeZoneId)
        JDBCPartitioningInfo(
          column, columnType, lowerBoundValue, upperBoundValue, numPartitions.get)
      }
    }

    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
      s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val boundValueToString: Long => String =
      toBoundValueInWhereClause(_, partitioning.columnType, timeZoneId)
    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions || /* check for overflow */
          (upperBound - lowerBound) < 0) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; " +
          s"Lower bound: ${boundValueToString(lowerBound)}; " +
          s"Upper bound: ${boundValueToString(upperBound)}.")
        upperBound - lowerBound
      }

    // Overflow can happen if you subtract then divide. For example:
    // (Long.MaxValue - Long.MinValue) / (numPartitions - 2).
    // Also, using fixed-point decimals here to avoid possible inaccuracy from floating point.
    val upperStride = (upperBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)
    val lowerStride = (lowerBound / BigDecimal(numPartitions))
      .setScale(18, RoundingMode.HALF_EVEN)

    val preciseStride = upperStride - lowerStride
    val stride = preciseStride.toLong

    // Determine the number of strides the last partition will fall short of compared to the
    // supplied upper bound. Take half of those strides, and then add them to the lower bound
    // for better distribution of the first and last partitions.
    val lostNumOfStrides = (preciseStride - stride) * numPartitions / stride
    val lowerBoundWithStrideAlignment = lowerBound +
      ((lostNumOfStrides / 2) * stride).setScale(0, RoundingMode.HALF_UP).toLong

    var i: Int = 0
    val column = partitioning.column
    var currentValue = lowerBoundWithStrideAlignment
    val ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBoundValue = boundValueToString(currentValue)
      val lBound = if (i != 0) s"$column >= $lBoundValue" else null
      currentValue += stride
      val uBoundValue = boundValueToString(currentValue)
      val uBound = if (i != numPartitions - 1) s"$column < $uBoundValue" else null
      val whereClause =
        if (uBound == null) {
          lBound
        } else if (lBound == null) {
          s"$uBound or $column is null"
        } else {
          s"$lBound AND $uBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    val partitions = ans.toArray
    logInfo(s"Number of partitions: $numPartitions, WHERE clauses of these partitions: " +
      partitions.map(_.asInstanceOf[JDBCPartition].whereClause).mkString(", "))
    partitions
  }

  // Verify column name and type based on the JDBC resolved schema
  private def verifyAndGetNormalizedPartitionColumn(
      schema: StructType,
      columnName: String,
      resolver: Resolver,
      jdbcOptions: JDBCOptions): (String, DataType) = {
    val dialect = JdbcDialects.get(jdbcOptions.url)
    val column = schema.find { f =>
      resolver(f.name, columnName) || resolver(dialect.quoteIdentifier(f.name), columnName)
    }.getOrElse {
      val maxNumToStringFields = SQLConf.get.maxToStringFields
      throw QueryCompilationErrors.userDefinedPartitionNotFoundInJDBCRelationError(
        columnName, schema.simpleString(maxNumToStringFields))
    }
    column.dataType match {
      case _: NumericType | DateType | TimestampType =>
      case _ =>
        throw QueryCompilationErrors.invalidPartitionColumnTypeError(column)
    }
    (dialect.quoteIdentifier(column.name), column.dataType)
  }

  private def toInternalBoundValue(
      value: String,
      columnType: DataType,
      timeZoneId: String): Long = {
    def parse[T](f: UTF8String => Option[T]): T = {
      f(UTF8String.fromString(value)).getOrElse {
        throw new IllegalArgumentException(
          s"Cannot parse the bound value $value as ${columnType.catalogString}")
      }
    }
    columnType match {
      case _: NumericType => value.toLong
      case DateType => parse(stringToDate).toLong
      case TimestampType => parse(stringToTimestamp(_, getZoneId(timeZoneId)))
    }
  }

  private def toBoundValueInWhereClause(
      value: Long,
      columnType: DataType,
      timeZoneId: String): String = {
    def dateTimeToString(): String = {
      val dateTimeStr = columnType match {
        case DateType =>
          DateFormatter().format(value.toInt)
        case TimestampType =>
          val timestampFormatter = TimestampFormatter.getFractionFormatter(
            DateTimeUtils.getZoneId(timeZoneId))
          timestampFormatter.format(value)
      }
      s"'$dateTimeStr'"
    }
    columnType match {
      case _: NumericType => value.toString
      case DateType | TimestampType => dateTimeToString()
    }
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst schema.
   * If `customSchema` defined in the JDBC options, replaces the schema's dataType with the
   * custom schema's type.
   *
   * @param resolver function used to determine if two identifiers are equal
   * @param jdbcOptions JDBC options that contains url, table and other information.
   * @return resolved Catalyst schema of a JDBC table
   */
  def getSchema(resolver: Resolver, jdbcOptions: JDBCOptions): StructType = {
    val tableSchema = JDBCRDD.resolveTable(jdbcOptions)
    jdbcOptions.customSchema match {
      case Some(customSchema) => JdbcUtils.getCustomSchema(
        tableSchema, customSchema, resolver)
      case None => tableSchema
    }
  }

  /**
   * Resolves a Catalyst schema of a JDBC table and returns [[JDBCRelation]] with the schema.
   */
  def apply(
      parts: Array[Partition],
      jdbcOptions: JDBCOptions)(
      sparkSession: SparkSession): JDBCRelation = {
    val schema = JDBCRelation.getSchema(sparkSession.sessionState.conf.resolver, jdbcOptions)
    JDBCRelation(schema, parts, jdbcOptions)(sparkSession)
  }
}

private[sql] case class JDBCRelation(
    override val schema: StructType,
    parts: Array[Partition],
    jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  // Check if JdbcDialect can compile input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    if (jdbcOptions.pushDownPredicate) {
      val dialect = JdbcDialects.get(jdbcOptions.url)
      filters.filter(f => dialect.compileExpression(f.toV2).isEmpty)
    } else {
      filters
    }
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // When pushDownPredicate is false, all Filters that need to be pushed down should be ignored
    val pushedPredicates = if (jdbcOptions.pushDownPredicate) {
      filters.map(_.toV2)
    } else {
      Array.empty[Predicate]
    }
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      pushedPredicates,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  def buildScan(
      requiredColumns: Array[String],
      finalSchema: StructType,
      predicates: Array[Predicate],
      groupByColumns: Option[Array[String]],
      tableSample: Option[TableSampleInfo],
      limit: Int,
      sortOrders: Array[SortOrder]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      predicates,
      parts,
      jdbcOptions,
      Some(finalSchema),
      groupByColumns,
      tableSample,
      limit,
      sortOrders).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .jdbc(jdbcOptions.url, jdbcOptions.tableOrQuery, jdbcOptions.asProperties)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.tableOrQuery})" + partitioningInfo
  }
}
