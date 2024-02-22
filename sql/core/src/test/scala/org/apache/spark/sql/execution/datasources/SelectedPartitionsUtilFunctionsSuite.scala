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

// BEGIN-EDGE
// This is a test suite for Edge-only scan utility functions.
// These utility functions will be replaced by a follow-up PR, which will also be backported
// to OSS. The edge guard will be removed after the follow-up PR is backported to OSS.
package org.apache.spark.sql.execution.datasources

import java.util.UUID

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.FileSourceScanLike
import org.apache.spark.sql.test.SharedSparkSession

class SelectedPartitionsUtilFunctionsSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  /**
   * Generate a random table name.
   */
  private def getRandomTableName(): String =
    s"test_${UUID.randomUUID()}".replaceAll("-", "_")

  /**
   * Create a table with the given data and format. Return the randomly generated table name.
   */
  private def createTable(data: Seq[(Int, Int)], format: String = "delta",
                          partitionColumn: String = "value"): String = {
    val tableName = getRandomTableName()
    spark.createDataFrame(data).toDF("id", "value")
      .write.mode("overwrite").partitionBy(partitionColumn).format(format).saveAsTable(tableName)
    tableName
  }

  /**
   * Calculate the total size of the parquet files in the table location.
   * This function filters out the delta log files and only calculates the size of the parquet
   * files that contain the actual data.
   */
  private def filesTotalSize(tableName: String): Long = {
    // Get the location that contains the parquet files for the given table.
    val tablePath = spark.sql(s"DESCRIBE DETAIL `$tableName`")
      .select("location").as[String].first()
    val path = new Path(tablePath)

    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())

    // List all the files in the table location and calculate the total size of the files.
    val files = fs.listFiles(path, true)
    var totalFileSizeInBytes = 0L
    while (files.hasNext) {
      val file = files.next
      // Filter out the delta log files, only calculate the size of the parquet files
      // that contain the actual data.
      if (!file.getPath.toString.contains("_delta_log")) totalFileSizeInBytes += file.getLen
    }
    totalFileSizeInBytes
  }

  test("1k rows, 10 partitions, 10 files") {
    val numOfRows = 1000
    val numOfPartitions = 10
    val data = (1 to numOfRows).map(i => (i, i % numOfPartitions))
    val tableName = createTable(data)
    val scan = sql(s"SELECT * FROM $tableName").queryExecution.executedPlan
      .collectFirst { case p: FileSourceScanLike => p }
      .getOrElse(fail("FileSourceScanLike node not found"))

    assert(scan.totalNumOfFilesInSelectedPartitions == numOfPartitions)
    assert(scan.selectedPartitions.totalFileSize == filesTotalSize(tableName))

    // Test the same thing for count distinct. It should have the same result.
    val scan2 = sql(s"SELECT COUNT(DISTINCT *) FROM $tableName").queryExecution.sparkPlan
      .collectFirst { case p: FileSourceScanLike => p }
      .getOrElse(fail("FileSourceScanLike node not found"))

    assert(scan2.totalNumOfFilesInSelectedPartitions == numOfPartitions)
    assert(scan2.selectedPartitions.totalFileSize == filesTotalSize(tableName))
  }

  test("Empty table") {
    val tableName = createTable(Seq.empty[(Int, Int)])
    val scan = sql(s"SELECT * FROM $tableName").queryExecution.executedPlan
      .collectFirst { case p: FileSourceScanLike => p }
      .getOrElse(fail("FileSourceScanLike node not found"))

    assert(scan.totalNumOfFilesInSelectedPartitions == 0)
    val totalSizeInBytes = scan.selectedPartitions.totalFileSize
    assert(totalSizeInBytes == 0L)
    assert(totalSizeInBytes == filesTotalSize(tableName))

    // Test the same thing for count distinct. It should have the same result.
    val scan2 = sql(s"SELECT COUNT(DISTINCT *) FROM $tableName").queryExecution.sparkPlan
      .collectFirst { case p: FileSourceScanLike => p }
      .getOrElse(fail("FileSourceScanLike node not found"))

    assert(scan2.totalNumOfFilesInSelectedPartitions == 0)
    assert(scan2.selectedPartitions.totalFileSize == filesTotalSize(tableName))
  }

  Seq(1, 1000).foreach { numPartition =>
    test(s"$numPartition partitions") {
      val data = (1 to 1000).map(i => (i, i % numPartition))
      val tableName = createTable(data)
      val scan = sql(s"SELECT * FROM $tableName").queryExecution.executedPlan
        .collectFirst { case p: FileSourceScanLike => p }
        .getOrElse(fail("FileSourceScanLike node not found"))

      assert(scan.totalNumOfFilesInSelectedPartitions == numPartition)
      assert(scan.selectedPartitions.totalFileSize == filesTotalSize(tableName))

      // Test the same thing for count distinct. It should have the same result.
      val scan2 = sql(s"SELECT COUNT(DISTINCT *) FROM $tableName").queryExecution.sparkPlan
        .collectFirst { case p: FileSourceScanLike => p }
        .getOrElse(fail("FileSourceScanLike node not found"))
      assert(scan2.totalNumOfFilesInSelectedPartitions == numPartition)
      assert(scan2.selectedPartitions.totalFileSize == filesTotalSize(tableName))
    }
  }
}
// END-EDGE
