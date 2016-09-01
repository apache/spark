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

import java.util.Properties

import org.apache.spark.Partition
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider}

class JdbcRelationProvider
  extends RelationProvider
  with CreatableRelationProvider
  with DataSourceRegister {

  override def shortName(): String = "jdbc"

  /** Returns a new base relation with the given parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val parts = buildJDBCPartition(jdbcOptions)
    val properties = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JDBCRelation(jdbcOptions.url, jdbcOptions.table, parts, properties)(sqlContext.sparkSession)
  }

  /**
   * Save the DataFrame to the destination and return a relation with the given parameters based on
   * the contents of the given DataFrame.
   */
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val jdbcOptions = new JDBCOptions(parameters)
    val parts = buildJDBCPartition(jdbcOptions)
    val properties = new Properties()
    parameters.foreach(kv => properties.setProperty(kv._1, kv._2))
    JdbcUtils.saveTable(mode, parameters, data)
    JDBCRelation(jdbcOptions.url, jdbcOptions.table, parts, properties)(sqlContext.sparkSession)
  }

  /**
   * Build Partitions based on the user-provided options:
   * - "partitionColumn": the column used to partition
   * - "lowerBound": the lower bound of partition column
   * - "upperBound": the upper bound of the partition column
   * - "numPartitions": the number of partitions
   */
  private def buildJDBCPartition(jdbcOptions: JDBCOptions): Array[Partition] = {
    if (jdbcOptions.partitionColumn != null
      && (jdbcOptions.lowerBound == null
        || jdbcOptions.upperBound == null
        || jdbcOptions.numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (jdbcOptions.partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        jdbcOptions.partitionColumn,
        jdbcOptions.lowerBound.toLong,
        jdbcOptions.upperBound.toLong,
        jdbcOptions.numPartitions.toInt)
    }
    JDBCRelation.columnPartition(partitionInfo)
  }
}
