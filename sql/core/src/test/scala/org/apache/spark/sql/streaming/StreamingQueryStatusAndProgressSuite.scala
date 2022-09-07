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

import java.util.UUID

import scala.collection.JavaConverters._

import org.json4s.jackson.JsonMethods._
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryStatusAndProgressSuite._
import org.apache.spark.sql.streaming.StreamingQuerySuite.clock
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.StructType

class StreamingQueryStatusAndProgressSuite extends StreamTest with Eventually {
  test("StreamingQueryProgress - prettyJson") {
    val json1 = testProgress1.prettyJson
    assertJson(
      json1,
      s"""
        |{
        |  "id" : "${testProgress1.id.toString}",
        |  "runId" : "${testProgress1.runId.toString}",
        |  "name" : "myName",
        |  "timestamp" : "2016-12-05T20:54:20.827Z",
        |  "batchId" : 2,
        |  "numInputRows" : 678,
        |  "inputRowsPerSecond" : 10.0,
        |  "durationMs" : {
        |    "total" : 0
        |  },
        |  "eventTime" : {
        |    "avg" : "2016-12-05T20:54:20.827Z",
        |    "max" : "2016-12-05T20:54:20.827Z",
        |    "min" : "2016-12-05T20:54:20.827Z",
        |    "watermark" : "2016-12-05T20:54:20.827Z"
        |  },
        |  "stateOperators" : [ {
        |    "operatorName" : "op1",
        |    "numRowsTotal" : 0,
        |    "numRowsUpdated" : 1,
        |    "allUpdatesTimeMs" : 1,
        |    "numRowsRemoved" : 2,
        |    "allRemovalsTimeMs" : 34,
        |    "commitTimeMs" : 23,
        |    "memoryUsedBytes" : 3,
        |    "numRowsDroppedByWatermark" : 0,
        |    "numShufflePartitions" : 2,
        |    "numStateStoreInstances" : 2,
        |    "customMetrics" : {
        |      "loadedMapCacheHitCount" : 1,
        |      "loadedMapCacheMissCount" : 0,
        |      "stateOnCurrentVersionSizeBytes" : 2
        |    }
        |  } ],
        |  "sources" : [ {
        |    "description" : "source",
        |    "startOffset" : 123,
        |    "endOffset" : 456,
        |    "latestOffset" : 789,
        |    "numInputRows" : 678,
        |    "inputRowsPerSecond" : 10.0
        |  } ],
        |  "sink" : {
        |    "description" : "sink",
        |    "numOutputRows" : -1
        |  },
        |  "observedMetrics" : {
        |    "event1" : {
        |      "c1" : 1,
        |      "c2" : 3.0
        |    },
        |    "event2" : {
        |      "rc" : 1,
        |      "min_q" : "hello",
        |      "max_q" : "world"
        |    }
        |  }
        |}
      """.stripMargin.trim)
    assert(compact(parse(json1)) === testProgress1.json)

    val json2 = testProgress2.prettyJson
    assertJson(
      json2,
      s"""
         |{
         |  "id" : "${testProgress2.id.toString}",
         |  "runId" : "${testProgress2.runId.toString}",
         |  "name" : null,
         |  "timestamp" : "2016-12-05T20:54:20.827Z",
         |  "batchId" : 2,
         |  "numInputRows" : 678,
         |  "durationMs" : {
         |    "total" : 0
         |  },
         |  "stateOperators" : [ {
         |    "operatorName" : "op2",
         |    "numRowsTotal" : 0,
         |    "numRowsUpdated" : 1,
         |    "allUpdatesTimeMs" : 1,
         |    "numRowsRemoved" : 2,
         |    "allRemovalsTimeMs" : 34,
         |    "commitTimeMs" : 23,
         |    "memoryUsedBytes" : 2,
         |    "numRowsDroppedByWatermark" : 0,
         |    "numShufflePartitions" : 2,
         |    "numStateStoreInstances" : 2
         |  } ],
         |  "sources" : [ {
         |    "description" : "source",
         |    "startOffset" : 123,
         |    "endOffset" : 456,
         |    "latestOffset" : 789,
         |    "numInputRows" : 678
         |  } ],
         |  "sink" : {
         |    "description" : "sink",
         |    "numOutputRows" : -1
         |  },
         |  "observedMetrics" : {
         |    "event_a" : {
         |      "c1" : null,
         |      "c2" : -20.7
         |    },
         |    "event_b1" : {
         |      "rc" : 33,
         |      "min_q" : "foo",
         |      "max_q" : "bar"
         |    },
         |    "event_b2" : {
         |      "rc" : 200,
         |      "min_q" : "fzo",
         |      "max_q" : "baz"
         |    }
         |  }
         |}
      """.stripMargin.trim)
    assert(compact(parse(json2)) === testProgress2.json)
  }

  test("StreamingQueryProgress - json") {
    assert(compact(parse(testProgress1.json)) === testProgress1.json)
    assert(compact(parse(testProgress2.json)) === testProgress2.json)
  }

  test("StreamingQueryProgress - toString") {
    assert(testProgress1.toString === testProgress1.prettyJson)
    assert(testProgress2.toString === testProgress2.prettyJson)
  }

  test("StreamingQueryStatus - prettyJson") {
    val json = testStatus.prettyJson
    assertJson(
      json,
      """
        |{
        |  "message" : "active",
        |  "isDataAvailable" : true,
        |  "isTriggerActive" : false
        |}
      """.stripMargin.trim)
  }

  test("StreamingQueryStatus - json") {
    assert(compact(parse(testStatus.json)) === testStatus.json)
  }

  test("StreamingQueryStatus - toString") {
    assert(testStatus.toString === testStatus.prettyJson)
  }

  test("progress classes should be Serializable") {
    import testImplicits._

    val inputData = MemoryStream[Int]

    val query = inputData.toDS()
      .groupBy($"value")
      .agg(count("*"))
      .writeStream
      .queryName("progress_serializable_test")
      .format("memory")
      .outputMode("complete")
      .start()
    try {
      inputData.addData(1, 2, 3)
      query.processAllAvailable()

      val progress = query.recentProgress

      // Make sure it generates the progress objects we want to test
      assert(progress.exists { p =>
        p.sources.size >= 1 && p.stateOperators.size >= 1 && p.sink != null
      })

      val array = spark.sparkContext.parallelize(progress).collect()
      assert(array.length === progress.length)
      array.zip(progress).foreach { case (p1, p2) =>
        // Make sure we did serialize and deserialize the object
        assert(p1 ne p2)
        assert(p1.json === p2.json)
      }
    } finally {
      query.stop()
    }
  }

  test("SPARK-19378: Continue reporting stateOp metrics even if there is no active trigger") {
    import testImplicits._

    withSQLConf(SQLConf.STREAMING_NO_DATA_PROGRESS_EVENT_INTERVAL.key -> "10") {
      val inputData = MemoryStream[Int]

      val query = inputData.toDS().toDF("value")
        .select($"value")
        .groupBy($"value")
        .agg(count("*"))
        .writeStream
        .queryName("metric_continuity")
        .format("memory")
        .outputMode("complete")
        .start()
      try {
        inputData.addData(1, 2)
        query.processAllAvailable()

        val progress = query.lastProgress
        assert(progress.stateOperators.length > 0)
        // Should emit new progresses every 10 ms, but we could be facing a slow Jenkins
        eventually(timeout(1.minute)) {
          val nextProgress = query.lastProgress
          assert(nextProgress.timestamp !== progress.timestamp)
          assert(nextProgress.numInputRows === 0)
          assert(nextProgress.stateOperators.head.numRowsTotal === 2)
          assert(nextProgress.stateOperators.head.numRowsUpdated === 0)
          assert(nextProgress.sink.numOutputRows === 0)
        }
      } finally {
        query.stop()
      }
    }
  }

  test("SPARK-29973: Make `processedRowsPerSecond` calculated more accurately and meaningfully") {
    import testImplicits._

    clock = new StreamManualClock
    val inputData = MemoryStream[Int]
    val query = inputData.toDS()

    testStream(query)(
      StartStream(Trigger.ProcessingTime(1000), triggerClock = clock),
      AdvanceManualClock(1000),
      waitUntilBatchProcessed,
      AssertOnQuery(query => {
        assert(query.lastProgress.numInputRows == 0)
        assert(query.lastProgress.processedRowsPerSecond == 0.0d)
        true
      }),
      AddData(inputData, 1, 2),
      AdvanceManualClock(1000),
      waitUntilBatchProcessed,
      AssertOnQuery(query => {
        assert(query.lastProgress.numInputRows == 2)
        assert(query.lastProgress.processedRowsPerSecond == 2000d)
        true
      }),
      StopStream
    )
  }

  def waitUntilBatchProcessed: AssertOnQuery = Execute { q =>
    eventually(Timeout(streamingTimeout)) {
      if (q.exception.isEmpty) {
        assert(clock.isStreamWaitingAt(clock.getTimeMillis()))
      }
    }
    if (q.exception.isDefined) {
      throw q.exception.get
    }
  }

  def assertJson(source: String, expected: String): Unit = {
    assert(
      source.replaceAll("\r\n|\r|\n", System.lineSeparator) ===
        expected.replaceAll("\r\n|\r|\n", System.lineSeparator))
  }
}

object StreamingQueryStatusAndProgressSuite {
  private val schema1 = new StructType()
    .add("c1", "long")
    .add("c2", "double")
  private val schema2 = new StructType()
    .add("rc", "long")
    .add("min_q", "string")
    .add("max_q", "string")
  private def row(schema: StructType, elements: Any*): Row = {
    new GenericRowWithSchema(elements.toArray, schema)
  }

  val testProgress1 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = "myName",
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    batchDuration = 0L,
    durationMs = new java.util.HashMap(Map("total" -> 0L).mapValues(long2Long).toMap.asJava),
    eventTime = new java.util.HashMap(Map(
      "max" -> "2016-12-05T20:54:20.827Z",
      "min" -> "2016-12-05T20:54:20.827Z",
      "avg" -> "2016-12-05T20:54:20.827Z",
      "watermark" -> "2016-12-05T20:54:20.827Z").asJava),
    stateOperators = Array(new StateOperatorProgress(operatorName = "op1",
      numRowsTotal = 0, numRowsUpdated = 1, allUpdatesTimeMs = 1, numRowsRemoved = 2,
      allRemovalsTimeMs = 34, commitTimeMs = 23, memoryUsedBytes = 3, numRowsDroppedByWatermark = 0,
      numShufflePartitions = 2, numStateStoreInstances = 2,
      customMetrics = new java.util.HashMap(Map("stateOnCurrentVersionSizeBytes" -> 2L,
        "loadedMapCacheHitCount" -> 1L, "loadedMapCacheMissCount" -> 0L)
        .mapValues(long2Long).toMap.asJava)
    )),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        latestOffset = "789",
        numInputRows = 678,
        inputRowsPerSecond = 10.0,
        processedRowsPerSecond = Double.PositiveInfinity  // should not be present in the json
      )
    ),
    sink = SinkProgress("sink", None),
    observedMetrics = new java.util.HashMap(Map(
      "event1" -> row(schema1, 1L, 3.0d),
      "event2" -> row(schema2, 1L, "hello", "world")).asJava)
  )

  val testProgress2 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = null, // should not be present in the json
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    batchDuration = 0L,
    durationMs = new java.util.HashMap(Map("total" -> 0L).mapValues(long2Long).toMap.asJava),
    // empty maps should be handled correctly
    eventTime = new java.util.HashMap(Map.empty[String, String].asJava),
    stateOperators = Array(new StateOperatorProgress(operatorName = "op2",
      numRowsTotal = 0, numRowsUpdated = 1, allUpdatesTimeMs = 1, numRowsRemoved = 2,
      allRemovalsTimeMs = 34, commitTimeMs = 23, memoryUsedBytes = 2, numRowsDroppedByWatermark = 0,
      numShufflePartitions = 2, numStateStoreInstances = 2)),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        latestOffset = "789",
        numInputRows = 678,
        inputRowsPerSecond = Double.NaN, // should not be present in the json
        processedRowsPerSecond = Double.NegativeInfinity // should not be present in the json
      )
    ),
    sink = SinkProgress("sink", None),
    observedMetrics = new java.util.HashMap(Map(
      "event_a" -> row(schema1, null, -20.7d),
      "event_b1" -> row(schema2, 33L, "foo", "bar"),
      "event_b2" -> row(schema2, 200L, "fzo", "baz")).asJava)
  )

  val testStatus = new StreamingQueryStatus("active", true, false)
}

