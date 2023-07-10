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

package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

class TPCDSTables(val spark: SparkSession, config: GenTPCDataConfig)
  extends TableGenerator with TPCDSSchema with Logging with Serializable {

  override protected val dataGenerator: DataGenerator = new Dsdgen(config.dsdgenDir)
  override protected val spark: SparkSession = sparkSession
  override protected val scaleFactor: Int = config.scaleFactor
  override protected def tables: Seq[Table] = tableColumns.map { case (tableName, schemaString) =>
    val partitionColumns = tablePartitionColumns.getOrElse(tableName, Nil)
      .map(_.stripPrefix("`").stripSuffix("`"))
    Table(tableName, partitionColumns, StructType.fromDDL(schemaString))
  }.toSeq
}

/**
 * This class generates TPCDS table data by using tpcds-kit:
 *  - https://github.com/databricks/tpcds-kit
 *
 * To run this:
 * {{{
 *   build/sbt "sql/Test/runMain <this class> --dsdgenDir <path> --location <path> --scaleFactor 1"
 * }}}
 *
 * Note: if users specify a small scale factor, GenTPCDSData works good. Otherwise, may encounter
 * OOM and cause failure. Users can retry by setting a larger value for the environment variable
 * HEAP_SIZE(the default size is 4g), e.g. export HEAP_SIZE=10g.
 */
object GenTPCDSData {

  def main(args: Array[String]): Unit = {
    val config = new GenTPCDataConfig(args)

    val spark = SparkSession
      .builder()
      .appName(getClass.getName)
      .master(config.master)
      .getOrCreate()

    val tables = new TPCDSTables(
      spark,
      config)

    tables.genData(
      location = config.location,
      format = config.format,
      overwrite = config.overwrite,
      partitionTables = config.partitionTables,
      clusterByPartitionColumns = config.clusterByPartitionColumns,
      filterOutNullPartitionValues = config.filterOutNullPartitionValues,
      tableFilter = config.tableFilter,
      numPartitions = config.numPartitions)

    spark.stop()
  }
}
