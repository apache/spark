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

import java.io.File

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecutionMetadata}
import org.apache.spark.sql.functions._
import org.apache.spark.util.{SystemClock, Utils}

class StreamExecutionMetadataSuite extends StreamTest {

  private def newMetadataDir =
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath

  test("stream execution metadata") {
    assert(StreamExecutionMetadata(0, 0) ===
      StreamExecutionMetadata("""{}"""))
    assert(StreamExecutionMetadata(1, 0) ===
      StreamExecutionMetadata("""{"batchWatermarkMs":1}"""))
    assert(StreamExecutionMetadata(0, 2) ===
      StreamExecutionMetadata("""{"batchTimestampMs":2}"""))
    assert(StreamExecutionMetadata(1, 2) ===
      StreamExecutionMetadata(
        """{"batchWatermarkMs":1,"batchTimestampMs":2}"""))
  }

  test("metadata is recovered from log when query is restarted") {
    import testImplicits._
    val clock = new SystemClock()
    val ms = new MemoryStream[Long](0, sqlContext)
    val df = ms.toDF().toDF("a")
    val checkpointLoc = newMetadataDir
    val checkpointDir = new File(checkpointLoc, "complete")
    checkpointDir.mkdirs()
    assert(checkpointDir.exists())
    val tableName = "test"
    // Query that prunes timestamps less than current_timestamp, making
    // it easy to use for ensuring that a batch is re-processed with the
    // timestamp used when it was first processed.
    def startQuery: StreamingQuery = {
      df.groupBy("a")
        .count()
        .where('a >= current_timestamp().cast("long"))
        .writeStream
        .format("memory")
        .queryName(tableName)
        .option("checkpointLocation", checkpointLoc)
        .outputMode("complete")
        .start()
    }
    // no exception here
    val t1 = clock.getTimeMillis() + 60L * 1000L
    val t2 = clock.getTimeMillis() + 60L * 1000L + 1000L
    val q = startQuery
    ms.addData(t1, t2)
    q.processAllAvailable()

    checkAnswer(
      spark.table(tableName),
      Seq(Row(t1, 1), Row(t2, 1))
    )

    q.stop()
    Thread.sleep(60L * 1000L + 5000L) // Expire t1 and t2
    assert(t1 < clock.getTimeMillis())
    assert(t2 < clock.getTimeMillis())

    spark.sql(s"drop table $tableName")

    // verify table is dropped
    intercept[AnalysisException](spark.table(tableName).collect())
    val q2 = startQuery
    q2.processAllAvailable()
    checkAnswer(
      spark.table(tableName),
      Seq(Row(t1, 1), Row(t2, 1))
    )

    q2.stop()

  }
}
