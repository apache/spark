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

import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.Futures.timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.SQLHelper
import org.apache.spark.sql.connect.client.util.RemoteSparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.window

class StreamingQuerySuite extends RemoteSparkSession with SQLHelper {

  test("Streaming API with windowed aggregate query") {
    // This verifies standard streaming API by starting a streaming query with windowed count.
    withSQLConf(
      "spark.sql.shuffle.partitions" -> "1" // Avoid too many reducers.
    ) {
      val readDF = spark.readStream
        .format("rate")
        .option("rowsPerSecond", "10")
        .option("numPartitions", "1")
        .load()

      // Verify schema (results in RPC
      assert(readDF.schema.toDDL == "timestamp TIMESTAMP,value BIGINT")

      val countsDF = readDF
        .withWatermark("timestamp", "10 seconds")
        .groupBy(window(col("timestamp"), "5 seconds"))
        .count()
        .selectExpr("window.start as timestamp", "count as num_events")

      assert(countsDF.schema.toDDL == "timestamp TIMESTAMP,num_events BIGINT NOT NULL")

      // Start the query
      val queryName = "sparkConnectStreamingQuery"

      val query = countsDF.writeStream
        .format("memory")
        .queryName(queryName)
        .trigger(Trigger.ProcessingTime("1 second"))
        .start()

      // Verify some of the API.
      assert(query.isActive)

      eventually(timeout(10.seconds)) {
        assert(query.status.isDataAvailable)
        assert(query.recentProgress.length > 0) // Query made progress.
      }

      query.explain() // Prints to console.

      // Don't wait for any processed data. Otherwise the test could take multiple seconds.
      query.stop()
    }
  }
}
