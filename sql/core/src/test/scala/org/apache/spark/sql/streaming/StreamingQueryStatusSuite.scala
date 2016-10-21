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

import org.apache.spark.SparkFunSuite

class StreamingQueryStatusSuite extends SparkFunSuite {
  test("toString") {
    assert(StreamingQueryStatus.testStatus.sourceStatuses(0).toString ===
      """
        |Status of source MySource1
        |    Available offset: #0
        |    Input rate: 15.5 rows/sec
        |    Processing rate: 23.5 rows/sec
        |    Trigger details:
        |        numRows.input.source: 100
        |        latency.getOffset.source: 10
        |        latency.getBatch.source: 20
      """.stripMargin.trim, "SourceStatus.toString does not match")

    assert(StreamingQueryStatus.testStatus.sinkStatus.toString ===
      """
        |Status of sink MySink
        |    Committed offsets: [#1, -]
      """.stripMargin.trim, "SinkStatus.toString does not match")

    assert(StreamingQueryStatus.testStatus.toString ===
      """
        |Status of query 'query'
        |    Query id: 1
        |    Status timestamp: 123
        |    Input rate: 15.5 rows/sec
        |    Processing rate 23.5 rows/sec
        |    Latency: 345.0 ms
        |    Trigger details:
        |        isDataPresentInTrigger: true
        |        isTriggerActive: true
        |        latency.getBatch.total: 20
        |        latency.getOffset.total: 10
        |        numRows.input.total: 100
        |        triggerId: 5
        |    Source statuses [1 source]:
        |        Source 1 - MySource1
        |            Available offset: #0
        |            Input rate: 15.5 rows/sec
        |            Processing rate: 23.5 rows/sec
        |            Trigger details:
        |                numRows.input.source: 100
        |                latency.getOffset.source: 10
        |                latency.getBatch.source: 20
        |    Sink status - MySink
        |        Committed offsets: [#1, -]
      """.stripMargin.trim, "StreamingQueryStatus.toString does not match")

  }

  test("json") {
    assert(StreamingQueryStatus.testStatus.json ===
      """
        |{"sourceStatuses":[{"description":"MySource1","offsetDesc":"#0","inputRate":15.5,
        |"processingRate":23.5,"triggerDetails":{"numRows.input.source":"100",
        |"latency.getOffset.source":"10","latency.getBatch.source":"20"}}],
        |"sinkStatus":{"description":"MySink","offsetDesc":"[#1, -]"}}
      """.stripMargin.replace("\n", "").trim)
  }

  test("prettyJson") {
    assert(
      StreamingQueryStatus.testStatus.prettyJson ===
        """
          |{
          |  "sourceStatuses" : [ {
          |    "description" : "MySource1",
          |    "offsetDesc" : "#0",
          |    "inputRate" : 15.5,
          |    "processingRate" : 23.5,
          |    "triggerDetails" : {
          |      "numRows.input.source" : "100",
          |      "latency.getOffset.source" : "10",
          |      "latency.getBatch.source" : "20"
          |    }
          |  } ],
          |  "sinkStatus" : {
          |    "description" : "MySink",
          |    "offsetDesc" : "[#1, -]"
          |  }
          |}
        """.stripMargin.trim)
  }
}
