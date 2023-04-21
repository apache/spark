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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.connect.client.util.ConnectFunSuite
import org.apache.spark.sql.types.StructType

class StreamingQueryProgressSuite extends ConnectFunSuite {
  test("test seder StreamingQueryProgress from json") {
    val jsonStringFromServerSide =
      s"""
         |{
         |  "id" : "33ac26f4-1c39-46ce-b798-f3d2a21211d4",
         |  "runId" : "849c2c9a-b9f8-446f-9180-259a60fd888c",
         |  "name" : "myName",
         |  "timestamp" : "2016-12-05T20:54:20.827Z",
         |  "batchId" : 2,
         |  "batchDuration" : 0,
         |  "durationMs" : {
         |    "total" : 0
         |  },
         |  "eventTime" : {
         |    "min" : "2016-12-05T20:54:20.827Z",
         |    "avg" : "2016-12-05T20:54:20.827Z",
         |    "watermark" : "2016-12-05T20:54:20.827Z",
         |    "max" : "2016-12-05T20:54:20.827Z"
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
         |      "stateOnCurrentVersionSizeBytes" : 2,
         |      "loadedMapCacheHitCount" : 1,
         |      "loadedMapCacheMissCount" : 0
         |    }
         |  } ],
         |  "sources" : [ {
         |    "description" : "source",
         |    "startOffset" : "123",
         |    "endOffset" : "456",
         |    "latestOffset" : "789",
         |    "numInputRows" : 678,
         |    "inputRowsPerSecond" : 10.0,
         |    "processedRowsPerSecond" : "Infinity",
         |    "metrics" : { }
         |  } ],
         |  "sink" : {
         |    "description" : "sink",
         |    "numOutputRows" : -1,
         |    "metrics" : { }
         |  },
         |  "observedMetrics" : {
         |    "event1" : {
         |      "values" : [ 1, 3.0 ],
         |      "schema" : {
         |        "type" : "struct",
         |        "fields" : [ {
         |          "name" : "c1",
         |          "type" : "long",
         |          "nullable" : true,
         |          "metadata" : { }
         |        }, {
         |          "name" : "c2",
         |          "type" : "double",
         |          "nullable" : true,
         |          "metadata" : { }
         |        } ]
         |      }
         |    },
         |    "event2" : {
         |      "values" : [ 1, "hello", "world" ],
         |      "schema" : {
         |        "type" : "struct",
         |        "fields" : [ {
         |          "name" : "rc",
         |          "type" : "long",
         |          "nullable" : true,
         |          "metadata" : { }
         |        }, {
         |          "name" : "min_q",
         |          "type" : "string",
         |          "nullable" : true,
         |          "metadata" : { }
         |        }, {
         |          "name" : "max_q",
         |          "type" : "string",
         |          "nullable" : true,
         |          "metadata" : { }
         |        } ]
         |      }
         |    }
         |  }
         |}
      """.stripMargin.trim

    val result = StreamingQueryProgress.fromJson(jsonStringFromServerSide)
    assert(result.id.toString === "33ac26f4-1c39-46ce-b798-f3d2a21211d4")
    assert(result.runId.toString === "849c2c9a-b9f8-446f-9180-259a60fd888c")
    assert(result.numInputRows === 678)
    assert(result.stateOperators.head.operatorName === "op1")
    assert(result.sources.head.startOffset === "123")

    // check observedMetrics
    val schema1 = new StructType()
      .add("c1", "long")
      .add("c2", "double")
    val schema2 = new StructType()
      .add("rc", "long")
      .add("min_q", "string")
      .add("max_q", "string")
    val observedMetrics = Map[String, Row](
      "event1" -> new GenericRowWithSchema(Array(1L, 3.0d), schema1),
      "event2" -> new GenericRowWithSchema(Array(1L, "hello", "world"), schema2)
    ).asJava
    assert(result.observedMetrics.size() == 2)
    assert(result.observedMetrics == observedMetrics)

    // check `.json`
    val jsonString =
      """
        |{
        |  "id" : "33ac26f4-1c39-46ce-b798-f3d2a21211d4",
        |  "runId" : "849c2c9a-b9f8-446f-9180-259a60fd888c",
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
        |
        |""".stripMargin.trim
    assert(result.prettyJson === jsonString)
  }
}
