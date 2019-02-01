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

package org.apache.spark.sql.execution.datasources.noop

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{StreamingQuery, StreamTest, Trigger}

class NoopStreamSuite extends StreamTest {
  import testImplicits._

  test("microbatch") {
    val input = MemoryStream[Int]
    val query = input.toDF().writeStream.format("noop").start()
    testMicroBatchQuery(query, input)
  }

  test("microbatch restart with checkpoint") {
    val input = MemoryStream[Int]
    withTempDir { checkpointDir =>
      def testWithCheckpoint(): Unit = {
        val query = input.toDF().writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .format("noop")
          .start()
        testMicroBatchQuery(query, input)
      }
      testWithCheckpoint()
      testWithCheckpoint()
    }
  }

  private def testMicroBatchQuery(
      query: StreamingQuery,
      input: MemoryStream[Int],
      data: Int*): Unit = {
    assert(query.isActive)
    try {
      input.addData(1, 2, 3)
      eventually(timeout(streamingTimeout)) {
        assert(query.recentProgress.map(_.numInputRows).sum == 3)
      }
    } finally {
      query.stop()
    }
  }

  test("continuous") {
    val input = getRateDataFrame()
    val query = input.writeStream.format("noop").trigger(Trigger.Continuous(200)).start()
    assert(query.isActive)
    query.stop()
  }

  test("continuous restart with checkpoint") {
    withTempDir { checkpointDir =>
      def testWithCheckpoint(): Unit = {
        val input = getRateDataFrame()
        val query = input.writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .format("noop")
          .trigger(Trigger.Continuous(200))
          .start()
        assert(query.isActive)
        query.stop()
      }
      testWithCheckpoint()
      testWithCheckpoint()
    }
  }

  private def getRateDataFrame(): DataFrame = {
    spark.readStream
      .format("rate")
      .option("numPartitions", "1")
      .option("rowsPerSecond", "5")
      .load()
      .select('value)
  }
}

