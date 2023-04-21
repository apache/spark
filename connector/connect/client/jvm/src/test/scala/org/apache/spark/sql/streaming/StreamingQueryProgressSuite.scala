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

import org.apache.spark.sql.SQLHelper
import org.apache.spark.sql.connect.client.util.RemoteSparkSession

class StreamingQueryProgressSuite extends RemoteSparkSession with SQLHelper {

  test("test from json") {
    val jsonString =
      """{
      |    "id": "52de8a4e-1af7-4358-83e1-74d5d44bbc1b",
      |    "runId": "46680bd2-bb67-4828-90d6-3f76f43acd70",
      |    "name": "myName",
      |    "timestamp": "2023-04-20T04:52:47.890Z",
      |    "batchId": 169095,
      |    "numInputRows": 839,
      |    "inputRowsPerSecond": 0.0,
      |    "processedRowsPerSecond": 252.33082706766916,
      |    "durationMs":
      |    {
      |        "addBatch": 2240,
      |        "getBatch": 99,
      |        "latestOffset": 74,
      |        "queryPlanning": 3,
      |        "triggerExecution": 3325,
      |        "walCommit": 325
      |    },
      |    "eventTime" : {
      |      "avg" : "2016-12-05T20:54:20.827Z",
      |      "max" : "2016-12-05T20:54:20.827Z",
      |      "min" : "2016-12-05T20:54:20.827Z",
      |      "watermark" : "2016-12-05T20:54:20.827Z"
      |    },
      |    "stateOperators" : [ {
      |      "operatorName" : "op1",
      |      "numRowsTotal" : 0,
      |      "numRowsUpdated" : 1,
      |      "allUpdatesTimeMs" : 1,
      |      "numRowsRemoved" : 2,
      |      "allRemovalsTimeMs" : 34,
      |      "commitTimeMs" : 23,
      |      "memoryUsedBytes" : 3,
      |      "numRowsDroppedByWatermark" : 0,
      |      "numShufflePartitions" : 2,
      |      "numStateStoreInstances" : 2,
      |      "customMetrics" : {
      |        "loadedMapCacheHitCount" : 1,
      |        "loadedMapCacheMissCount" : 0,
      |        "stateOnCurrentVersionSizeBytes" : 2
      |    }
      |  } ],
      |    "sources" : [ {
      |      "description" : "source",
      |      "startOffset" : "123",
      |      "endOffset" : "456",
      |      "latestOffset" : "789",
      |      "numInputRows" : 678,
      |      "inputRowsPerSecond" : 10.0
      |  } ],
      |    "sink" : {
      |      "description" : "sink",
      |      "numOutputRows" : -1
      |  }
      |}""".stripMargin

    val streamingQueryProgress = new StreamingQueryProgress(jsonString)
    val result = streamingQueryProgress.fromJson()
    assert("52de8a4e-1af7-4358-83e1-74d5d44bbc1b" === result.id.toString)
    assert("46680bd2-bb67-4828-90d6-3f76f43acd70" === result.runId.toString)
    assert(839 === result.numInputRows)
    assert("op1" === result.stateOperators.head.operatorName)
    assert("123" === result.sources.head.startOffset)
  }
}
