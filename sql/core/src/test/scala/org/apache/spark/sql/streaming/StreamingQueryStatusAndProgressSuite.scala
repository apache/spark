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
import scala.language.postfixOps

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryStatusAndProgressSuite._

class StreamingQueryStatusAndProgressSuite extends StreamTest with Eventually {
  implicit class EqualsIgnoreCRLF(source: String) {
    def equalsIgnoreCRLF(target: String): Boolean = {
      source.replaceAll("\r\n|\r|\n", System.lineSeparator) ===
        target.replaceAll("\r\n|\r|\n", System.lineSeparator)
    }
  }

  test("StreamingQueryProgress - prettyJson") {
    val json1 = testProgress1.prettyJson
    assert(json1.equalsIgnoreCRLF(
      s"""
        |{
        |  "id" : "${testProgress1.id.toString}",
        |  "runId" : "${testProgress1.runId.toString}",
        |  "name" : "myName",
        |  "timestamp" : "2016-12-05T20:54:20.827Z",
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
        |    "numRowsTotal" : 0,
        |    "numRowsUpdated" : 1
        |  } ],
        |  "sources" : [ {
        |    "description" : "source",
        |    "startOffset" : 123,
        |    "endOffset" : 456,
        |    "numInputRows" : 678,
        |    "inputRowsPerSecond" : 10.0
        |  } ],
        |  "sink" : {
        |    "description" : "sink"
        |  }
        |}
      """.stripMargin.trim))
    assert(compact(parse(json1)) === testProgress1.json)

    val json2 = testProgress2.prettyJson
    assert(
      json2.equalsIgnoreCRLF(
        s"""
         |{
         |  "id" : "${testProgress2.id.toString}",
         |  "runId" : "${testProgress2.runId.toString}",
         |  "name" : null,
         |  "timestamp" : "2016-12-05T20:54:20.827Z",
         |  "numInputRows" : 678,
         |  "durationMs" : {
         |    "total" : 0
         |  },
         |  "stateOperators" : [ {
         |    "numRowsTotal" : 0,
         |    "numRowsUpdated" : 1
         |  } ],
         |  "sources" : [ {
         |    "description" : "source",
         |    "startOffset" : 123,
         |    "endOffset" : 456,
         |    "numInputRows" : 678
         |  } ],
         |  "sink" : {
         |    "description" : "sink"
         |  }
         |}
      """.stripMargin.trim))
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
    assert(json.equalsIgnoreCRLF(
      """
        |{
        |  "message" : "active",
        |  "isDataAvailable" : true,
        |  "isTriggerActive" : false
        |}
      """.stripMargin.trim))
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
        .select('value)
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
        eventually(timeout(1 minute)) {
          val nextProgress = query.lastProgress
          assert(nextProgress.timestamp !== progress.timestamp)
          assert(nextProgress.numInputRows === 0)
          assert(nextProgress.stateOperators.head.numRowsTotal === 2)
          assert(nextProgress.stateOperators.head.numRowsUpdated === 0)
        }
      } finally {
        query.stop()
      }
    }
  }
}

object StreamingQueryStatusAndProgressSuite {
  val testProgress1 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = "myName",
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    durationMs = new java.util.HashMap(Map("total" -> 0L).mapValues(long2Long).asJava),
    eventTime = new java.util.HashMap(Map(
      "max" -> "2016-12-05T20:54:20.827Z",
      "min" -> "2016-12-05T20:54:20.827Z",
      "avg" -> "2016-12-05T20:54:20.827Z",
      "watermark" -> "2016-12-05T20:54:20.827Z").asJava),
    stateOperators = Array(new StateOperatorProgress(numRowsTotal = 0, numRowsUpdated = 1)),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        numInputRows = 678,
        inputRowsPerSecond = 10.0,
        processedRowsPerSecond = Double.PositiveInfinity  // should not be present in the json
      )
    ),
    sink = new SinkProgress("sink")
  )

  val testProgress2 = new StreamingQueryProgress(
    id = UUID.randomUUID,
    runId = UUID.randomUUID,
    name = null, // should not be present in the json
    timestamp = "2016-12-05T20:54:20.827Z",
    batchId = 2L,
    durationMs = new java.util.HashMap(Map("total" -> 0L).mapValues(long2Long).asJava),
    // empty maps should be handled correctly
    eventTime = new java.util.HashMap(Map.empty[String, String].asJava),
    stateOperators = Array(new StateOperatorProgress(numRowsTotal = 0, numRowsUpdated = 1)),
    sources = Array(
      new SourceProgress(
        description = "source",
        startOffset = "123",
        endOffset = "456",
        numInputRows = 678,
        inputRowsPerSecond = Double.NaN, // should not be present in the json
        processedRowsPerSecond = Double.NegativeInfinity // should not be present in the json
      )
    ),
    sink = new SinkProgress("sink")
  )

  val testStatus = new StreamingQueryStatus("active", true, false)
}

