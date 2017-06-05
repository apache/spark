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

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * Instructions on how to partition the table among workers.
 */
private[sql] case class JDBCPartitioningInfo(
    column: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int)

private[sql] object BoundRange extends Enumeration {
  type BoundRange = Value
  val FULLRANGE, LEFTRANGE, MIDRANGE, RIGHTRANGE = Value
}

private[sql] object JDBCRelation extends Logging {
  import BoundRange._

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
   * @param partitioning partition information to generate the where clause for each partition
   * @return an array of partitions with where clause for each partition
   */
  def columnPartition(partitioning: JDBCPartitioningInfo): Array[Partition] = {
    if (partitioning == null || partitioning.numPartitions <= 1 ||
      partitioning.lowerBound == partitioning.upperBound) {
      return Array[Partition](JDBCPartition(null, 0))
    }

    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
      s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val numPartitions =
      if ((upperBound - lowerBound) >= partitioning.numPartitions) {
        partitioning.numPartitions
      } else {
        logWarning("The number of partitions is reduced because the specified number of " +
          "partitions is less than the difference between upper bound and lower bound. " +
          s"Updated number of partitions: ${upperBound - lowerBound}; Input number of " +
          s"partitions: ${partitioning.numPartitions}; Lower bound: $lowerBound; " +
          s"Upper bound: $upperBound.")
        upperBound - lowerBound
      }
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = upperBound / numPartitions - lowerBound / numPartitions
    val column = partitioning.column
    var i: Int = 0
    var currentValue: Long = lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val uBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
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
    ans.toArray
  }

  def columnBalancePartition(
    partitioning: JDBCPartitioningInfo,
    jdbcOptions: JDBCOptions): Array[Partition] = {
    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    require (lowerBound <= upperBound,
      "Operation not allowed: the lower bound of partitioning column is larger than the upper " +
        s"bound. Lower bound: $lowerBound; Upper bound: $upperBound")

    val conn: Connection = JdbcUtils.createConnectionFactory(jdbcOptions)()
    try {
      val startTime = System.currentTimeMillis()
      val partitionIndexes = (0 until partitioning.numPartitions).toArray
      val parts = realColumnBalancePartition(partitioning, conn, jdbcOptions.table,
        partitionIndexes, 0L, FULLRANGE)
      val queryTime = System.currentTimeMillis() - startTime
      logInfo(s"total query cost = ${queryTime} ms" )

      parts
    } finally {
      if (!conn.isClosed) {
        conn.close()
      }
    }
  }

  private def realColumnBalancePartition(
    partitioning: JDBCPartitioningInfo,
    conn: Connection,
    table: String,
    partitionIndexes: Array[Int],
    totalCount: Long,
    boundary: BoundRange): Array[Partition] = {

    val column = partitioning.column
    val lowerBound = partitioning.lowerBound
    val upperBound = partitioning.upperBound
    val numTotalPartitions = partitioning.numPartitions

    require(numTotalPartitions > 0)
    if (numTotalPartitions == 1) {
      val whereClause = getWhereClause(boundary, column, lowerBound, upperBound)
      return Array(JDBCPartition(whereClause, partitionIndexes(0)))
    }
    val midBound = upperBound / 2 + lowerBound / 2
    val leftBoundary = boundary match {
      case FULLRANGE => LEFTRANGE
      case RIGHTRANGE => MIDRANGE
      case _ => boundary
    }
    val rightBoundary = boundary match {
      case FULLRANGE => RIGHTRANGE
      case LEFTRANGE => MIDRANGE
      case _ => boundary
    }
    // just an optimize, so we can reduce one more recursive call
    if (numTotalPartitions == 2) {
      val leftWhereClause = getWhereClause(leftBoundary, column, lowerBound, midBound)
      val rightWhereClause = getWhereClause(rightBoundary, column, midBound, upperBound)
      return Array(
        JDBCPartition(leftWhereClause, partitionIndexes(0)),
        JDBCPartition(rightWhereClause, partitionIndexes(1)))
    }

    val startTime = System.currentTimeMillis()
    val rightCount = getRangeCountByColumn(conn, table, column,
      midBound, upperBound, rightBoundary)
    val countTime = System.currentTimeMillis() - startTime
    val leftCount = boundary match {
      case FULLRANGE =>
        getRangeCountByColumn(conn, table, column, lowerBound, midBound, leftBoundary)
      case _ =>
        totalCount - rightCount
    }

    // when leftCount + rightCount = 0, the numPartitions should equals 1 beside the first calling.
    // so it will return at above code.
    if (boundary == FULLRANGE && leftCount == 0 && rightCount == 0) {
      return columnPartition(partitioning)
    }
    // make the left part or right part owns 1 partition at least,
    // we can guarantee recursive called n -1 times at most.
    val numRightPartitions = Math.min(
      Math.max(
        Math.round(1.0 * rightCount / (rightCount + leftCount) * numTotalPartitions).toInt, 1),
        numTotalPartitions - 1)
    val numLeftPartitions = numTotalPartitions - numRightPartitions
    logDebug(s"lowerBound = $lowerBound, midBound = $midBound, upperbound = $upperBound, " +
      s"left count = $leftCount, left partitions = $numLeftPartitions, " +
      s"right count = $rightCount, right partitions = $numRightPartitions, " +
      s"total count = $totalCount, query cost = $countTime ms")

    val leftPartitioning = JDBCPartitioningInfo(column,
      lowerBound, midBound, numLeftPartitions)
    val rightPartitioning = JDBCPartitioningInfo(column,
      midBound, upperBound, numRightPartitions)
    val leftPartitionIndexes = partitionIndexes.slice(0, numLeftPartitions)
    val rightPartitionIndexes = partitionIndexes.slice(numLeftPartitions, partitionIndexes.length)
    val leftResult = realColumnBalancePartition(leftPartitioning, conn, table,
      leftPartitionIndexes, leftCount, leftBoundary)
    val rightResult = realColumnBalancePartition(rightPartitioning, conn, table,
      rightPartitionIndexes, rightCount, rightBoundary)

    leftResult ++ rightResult
  }

  private def getRangeCountByColumn(
    conn: Connection,
    table: String,
    column: String,
    lowerBound: Long,
    upperBound: Long,
    boundary: BoundRange): Long = {
    val sqlText = boundary match {
      case BoundRange.FULLRANGE =>
        s"SELECT count(*) from $table"
      case BoundRange.LEFTRANGE =>
        s"SELECT count(*) from $table WHERE $column < $upperBound OR $column is null"
      case BoundRange.RIGHTRANGE =>
        s"SELECT count(*) from $table WHERE $column >= $lowerBound"
      case BoundRange.MIDRANGE =>
        s"SELECT count(*) from $table WHERE $column >= $lowerBound AND $column < $upperBound"
    }
    val statement = conn.createStatement()
    try {
      val result = statement.executeQuery(sqlText)
      result.next()
      result.getLong(1)
    } finally {
      statement.close()
    }
  }

  private def getWhereClause(
    boundary: BoundRange,
    column: String,
    lowerBound: Long,
    upperBound: Long) = {
    val whereClause = boundary match {
      case LEFTRANGE =>
        s"$column < $upperBound OR $column is null"
      case RIGHTRANGE =>
        s"$column >= $lowerBound"
      case MIDRANGE =>
        s"$column >= $lowerBound AND $column < $upperBound"
      case FULLRANGE =>
        // since FULLRANGE only occur at the first time,
        // but the numPartitions will not be 1 at the same time,
        // since we will call the origin columnPartition.
        throw new Exception("partition by a FullRange should not occur")
    }
    whereClause
  }
}

private[sql] case class JDBCRelation(
    parts: Array[Partition], jdbcOptions: JDBCOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with PrunedFilteredScan
  with InsertableRelation {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override val needConversion: Boolean = false

  override val schema: StructType = JDBCRDD.resolveTable(jdbcOptions)

  // Check if JDBCRDD.compileFilter can accept input filters
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    filters.filter(JDBCRDD.compileFilter(_, JdbcDialects.get(jdbcOptions.url)).isEmpty)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Rely on a type erasure hack to pass RDD[InternalRow] back as RDD[Row]
    JDBCRDD.scanTable(
      sparkSession.sparkContext,
      schema,
      requiredColumns,
      filters,
      parts,
      jdbcOptions).asInstanceOf[RDD[Row]]
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    import scala.collection.JavaConverters._

    val options = jdbcOptions.asProperties.asScala +
      ("url" -> jdbcOptions.url, "dbtable" -> jdbcOptions.table)
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append

    new JdbcRelationProvider().createRelation(
      data.sparkSession.sqlContext, mode, options.toMap, data)
  }

  override def toString: String = {
    val partitioningInfo = if (parts.nonEmpty) s" [numPartitions=${parts.length}]" else ""
    // credentials should not be included in the plan output, table information is sufficient.
    s"JDBCRelation(${jdbcOptions.table})" + partitioningInfo
  }
}
