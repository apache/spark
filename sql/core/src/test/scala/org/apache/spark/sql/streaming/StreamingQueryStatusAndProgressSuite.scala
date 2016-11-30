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

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.StreamingQueryStatusAndProgressSuite._


class StreamingQueryStatusAndProgressSuite extends SparkFunSuite {

  test("StreamingQueryProgress - prettyJson") {
    val json = testProgress.prettyJson
    assert(json ===
      s"""
        |{
        |  "id" : "${testProgress.id.toString}",
        |  "name" : "name",
        |  "timestamp" : 1,
        |  "numInputRows" : 678,
        |  "inputRowsPerSecond" : 10.0,
        |  "durationMs" : {
        |    "total" : 0
        |  },
        |  "currentWatermark" : 3,
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
      """.stripMargin.trim)
    assert(compact(parse(json)) === testProgress.json)

  }

  test("StreamingQueryProgress - json") {
    assert(compact(parse(testProgress.json)) === testProgress.json)
  }

  test("StreamingQueryProgress - toString") {
    assert(testProgress.toString === testProgress.prettyJson)
  }

  test("StreamingQueryStatus - prettyJson") {
    val json = testStatus.prettyJson
    assert(json ===
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
}

object StreamingQueryStatusAndProgressSuite {
  val testProgress = new StreamingQueryProgress(
    id = UUID.randomUUID(),
    name = "name",
    timestamp = 1L,
    batchId = 2L,
    durationMs = Map("total" -> 0L).mapValues(long2Long).asJava,
    currentWatermark = 3L,
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

  val testStatus = new StreamingQueryStatus("active", true, false)
}

