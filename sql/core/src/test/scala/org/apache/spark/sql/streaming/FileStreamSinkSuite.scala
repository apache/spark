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

package org.apache.spark.sql.streaming

import org.apache.spark.sql.{Row, StreamTest}
import org.apache.spark.sql.execution.streaming.{FileStreamSinkWriter, MemoryStream}
import org.apache.spark.sql.execution.datasources.parquet
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class FileStreamSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("FileStreamSinkWriter") {
    val path = Utils.createTempDir()
    path.delete()

    val fileFormat = new parquet.DefaultSource()

    def writeRange(start: Int, end: Int): Unit = {
      val df = sqlContext.range(start, end, 1, 1)
        .select($"id", lit(100).as("data"), lit(1000).as("dummy"))

      val attributes = df.logicalPlan.output

      val writer = new FileStreamSinkWriter(
        df,
        fileFormat,
        path.toString,
        partitionColumnNames = Seq("id"),
        Map.empty)

      val writtenFiles = writer.write()
    }

    // Write and check
    writeRange(0, 10)
    checkAnswer(
      sqlContext.read.load(path.getCanonicalPath),
      (0 until 10).map(Row(100, 1000, _)).toSeq)

    // Append and check
    writeRange(10, 20)
    checkAnswer(
      sqlContext.read.load(path.getCanonicalPath),
      (0 until 20).map(Row(100, 1000, _)).toSeq)
  }

  test("unpartitioned writing") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    val query =
      df.write
        .format("parquet")
        .option("checkpointLocation", checkpointDir)
        .startStream(outputDir)

    inputData.addData(1, 2, 3)
    failAfter(streamingTimeout) { query.processAllAvailable() }

    val outputDf = sqlContext.read.parquet(outputDir).as[Int]
    checkDataset(
      outputDf,
      1, 2, 3)
  }

  ignore("partitioned writing") {
    val inputData = MemoryStream[Int]
    val ds = inputData.toDS()

    val outputDir = Utils.createTempDir(namePrefix = "stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir(namePrefix = "stream.checkpoint").getCanonicalPath

    val query =
      ds.map(i => (i, 1000))
        .toDF("id", "value")
        .write
        .format("parquet")
        .partitionBy("id")
        .option("checkpointLocation", checkpointDir)
        .startStream(outputDir)

    inputData.addData(1, 2, 3)
    failAfter(streamingTimeout) {
      query.processAllAvailable()
    }

    val outputDf = sqlContext.read.parquet(outputDir)
    outputDf.show()
    checkDataset(
      outputDf.as[(Int, Int)],
      (1000, 1), (1000, 2), (1000, 3))
  }
}
