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

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class FileStreamSinkSuite extends StreamTest with SharedSQLContext {
  import testImplicits._

  test("unpartitioned writing") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    val outputDir = Utils.createTempDir("stream.output").getCanonicalPath
    val checkpointDir = Utils.createTempDir("stream.checkpoint").getCanonicalPath

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
}
