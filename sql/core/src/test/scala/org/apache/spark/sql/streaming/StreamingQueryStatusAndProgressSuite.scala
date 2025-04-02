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

import java.sql.Timestamp
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

import scala.jdk.CollectionConverters._

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
import org.apache.spark.util.ArrayImplicits._

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
        |  "batchDuration" : 0,
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
         |  "batchDuration" : 0,
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
    assert(compact(parse(testProgress3.json)) === testProgress3.json)
  }

  test("StreamingQueryProgress - toString") {
    assert(testProgress1.toString === testProgress1.prettyJson)
    assert(testProgress2.toString === testProgress2.prettyJson)
  }

  test("StreamingQueryProgress - jsonString and fromJson") {
    Seq(testProgress1, testProgress2).foreach { input =>
      val jsonString = StreamingQueryProgress.jsonString(input)
      val result = StreamingQueryProgress.fromJson(jsonString)
      assert(input.id == result.id)
      assert(input.runId == result.runId)
      assert(input.name == result.name)
      assert(input.timestamp == result.timestamp)
      assert(input.batchId == result.batchId)
      assert(input.batchDuration == result.batchDuration)
      assert(input.durationMs == result.durationMs)
      assert(input.eventTime == result.eventTime)

      input.stateOperators.zip(result.stateOperators).foreach { case (o1, o2) =>
        assert(o1.operatorName == o2.operatorName)
        assert(o1.numRowsTotal == o2.numRowsTotal)
        assert(o1.numRowsUpdated == o2.numRowsUpdated)
        assert(o1.allUpdatesTimeMs == o2.allUpdatesTimeMs)
        assert(o1.numRowsRemoved == o2.numRowsRemoved)
        assert(o1.allRemovalsTimeMs == o2.allRemovalsTimeMs)
        assert(o1.commitTimeMs == o2.commitTimeMs)
        assert(o1.memoryUsedBytes == o2.memoryUsedBytes)
        assert(o1.numRowsDroppedByWatermark == o2.numRowsDroppedByWatermark)
        assert(o1.numShufflePartitions == o2.numShufflePartitions)
        assert(o1.numStateStoreInstances == o2.numStateStoreInstances)
        assert(o1.customMetrics == o2.customMetrics)
      }

      input.sources.zip(result.sources).foreach { case (s1, s2) =>
        assert(s1.description == s2.description)
        assert(s1.startOffset == s2.startOffset)
        assert(s1.endOffset == s2.endOffset)
        assert(s1.latestOffset == s2.latestOffset)
        assert(s1.numInputRows == s2.numInputRows)
        if (s1.inputRowsPerSecond.isNaN) {
          assert(s2.inputRowsPerSecond.isNaN)
        } else {
          assert(s1.inputRowsPerSecond == s2.inputRowsPerSecond)
        }
        assert(s1.processedRowsPerSecond == s2.processedRowsPerSecond)
        assert(s1.metrics == s2.metrics)
      }

      Seq(input.sink).zip(Seq(result.sink)).foreach { case (s1, s2) =>
        assert(s1.description == s2.description)
        assert(s1.numOutputRows == s2.numOutputRows)
        assert(s1.metrics == s2.metrics)
      }

      val resultObservedMetrics = result.observedMetrics
      assert(input.observedMetrics.size() == resultObservedMetrics.size())
      assert(input.observedMetrics.keySet() == resultObservedMetrics.keySet())
      input.observedMetrics.entrySet().forEach { e =>
        assert(e.getValue == resultObservedMetrics.get(e.getKey))
      }
    }
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
        p.sources.length >= 1 && p.stateOperators.length >= 1 && p.sink != null
      })

      val array = spark.sparkContext.parallelize(progress.toImmutableArraySeq).collect()
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

  test("SPARK-45655: Use current batch timestamp in observe API") {
    import testImplicits._

    val inputData = MemoryStream[Timestamp]

    // current_date() internally uses current batch timestamp on streaming query
    val query = inputData.toDF()
      .filter("value < current_date()")
      .observe("metrics", count(expr("value >= current_date()")).alias("dropped"))
      .writeStream
      .queryName("ts_metrics_test")
      .format("memory")
      .outputMode("append")
      .start()

    val timeNow = Instant.now().truncatedTo(ChronoUnit.SECONDS)

    // this value would be accepted by the filter and would not count towards
    // dropped metrics.
    val validValue = Timestamp.from(timeNow.minus(2, ChronoUnit.DAYS))
    inputData.addData(validValue)

    // would be dropped by the filter and count towards dropped metrics
    inputData.addData(Timestamp.from(timeNow.plus(2, ChronoUnit.DAYS)))

    query.processAllAvailable()
    query.stop()

    val dropped = query.recentProgress.map { p =>
      val metricVal = Option(p.observedMetrics.get("metrics"))
      metricVal.map(_.getLong(0)).getOrElse(0L)
    }.sum
    // ensure dropped metrics are correct
    assert(dropped == 1)

    val data = spark.read.table("ts_metrics_test").collect()

    // ensure valid value ends up in output
    assert(data(0).getAs[Timestamp](0).equals(validValue))
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
    durationMs = new java.util.HashMap(Map("total" -> 0L).transform((_, v) => long2Long(v)).asJava),
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
        .transform((_, v) => long2Long(v)).asJava)
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
    durationMs = new java.util.HashMap(Map("total" -> 0L).transform((_, v) => long2Long(v)).asJava),
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

  val testProgress3 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = "myName",
    timestamp = "2024-05-28T00:00:00.233Z",
    batchId = 2L,
    batchDuration = 0L,
    durationMs = null,
    eventTime = null,
    stateOperators = Array(new StateOperatorProgress(operatorName = "op1",
      numRowsTotal = 0, numRowsUpdated = 1, allUpdatesTimeMs = 1, numRowsRemoved = 2,
      allRemovalsTimeMs = 34, commitTimeMs = 23, memoryUsedBytes = 3, numRowsDroppedByWatermark = 0,
      numShufflePartitions = 2, numStateStoreInstances = 2,
      customMetrics = new java.util.HashMap(Map("stateOnCurrentVersionSizeBytes" -> 2L,
        "loadedMapCacheHitCount" -> 1L, "loadedMapCacheMissCount" -> 0L)
        .transform((_, v) => long2Long(v)).asJava)
    )),
    sources = Array(),
    sink = SinkProgress("sink", None),
    observedMetrics = null
  )

  val testStatus = new StreamingQueryStatus("active", true, false)
}

