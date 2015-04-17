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

package org.apache.spark.sql.jdbc

import java.sql.DriverManager
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Data corresponding to one partition of a JDBCRDD.
 */
private[sql] case class JDBCPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}

/**
 * Instructions on how to partition the table among workers.
 */
private[sql] case class JDBCPartitioningInfo(
    column: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int)

private[sql] object JDBCRelation {
  /**
   * Given a partitioning schematic (a column of integral type, a number of
   * partitions, and upper and lower bounds on the column's value), generate
   * WHERE clauses for each partition so that each row in the table appears
   * exactly once.  The parameters minValue and maxValue are advisory in that
   * incorrect values may cause the partitioning to be poor, but no data
   * will fail to be represented.
   */
  def columnPartition(partitioning: JDBCPartitioningInfo): Array[Partition] = {
    if (partitioning == null) return Array[Partition](JDBCPartition(null, 0))

    val numPartitions = partitioning.numPartitions
    val column = partitioning.column
    if (numPartitions == 1) return Array[Partition](JDBCPartition(null, 0))
    // Overflow and silliness can happen if you subtract then divide.
    // Here we get a little roundoff, but that's (hopefully) OK.
    val stride: Long = (partitioning.upperBound / numPartitions 
                      - partitioning.lowerBound / numPartitions)
    var i: Int = 0
    var currentValue: Long = partitioning.lowerBound
    var ans = new ArrayBuffer[Partition]()
    while (i < numPartitions) {
      val lowerBound = if (i != 0) s"$column >= $currentValue" else null
      currentValue += stride
      val upperBound = if (i != numPartitions - 1) s"$column < $currentValue" else null
      val whereClause =
        if (upperBound == null) {
          lowerBound
        } else if (lowerBound == null) {
          upperBound
        } else {
          s"$lowerBound AND $upperBound"
        }
      ans += JDBCPartition(whereClause, i)
      i = i + 1
    }
    ans.toArray
  }
}

private[sql] class DefaultSource extends RelationProvider {
  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val driver = parameters.getOrElse("driver", null)
    val table = parameters.getOrElse("dbtable", sys.error("Option 'dbtable' not specified"))
    val partitionColumn = parameters.getOrElse("partitionColumn", null)
    val lowerBound = parameters.getOrElse("lowerBound", null)
    val upperBound = parameters.getOrElse("upperBound", null)
    val numPartitions = parameters.getOrElse("numPartitions", null)

    if (driver != null) Utils.getContextOrSparkClassLoader.loadClass(driver)

    if (partitionColumn != null
        && (lowerBound == null || upperBound == null || numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        partitionColumn,
        lowerBound.toLong,
        upperBound.toLong,
        numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JDBCRelation(url, table, parts, properties)(sqlContext)
  }
}

private[sql] case class JDBCRelation(
    url: String,
    table: String,
    parts: Array[Partition],
    properties: Properties = new Properties())(@transient val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedFilteredScan {

  override val needConversion: Boolean = false

  override val schema: StructType = JDBCRDD.resolveTable(url, table, properties)

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val driver: String = DriverManager.getDriver(url).getClass.getCanonicalName
    JDBCRDD.scanTable(
      sqlContext.sparkContext,
      schema,
      driver,
      url,
      properties,
      table,
      requiredColumns,
      filters,
      parts)
  }
}
