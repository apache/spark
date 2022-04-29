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

package org.apache.spark.sql.execution

import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.sql.LocalSparkSession
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.util.JsonProtocol

class SQLJsonProtocolSuite extends SparkFunSuite with LocalSparkSession {

  test("SparkPlanGraph backward compatibility: metadata") {
    Seq(true, false).foreach { newExecutionStartEvent =>
      val event = if (newExecutionStartEvent) {
        "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
      } else {
        "org.apache.spark.sql.execution.OldVersionSQLExecutionStart"
      }
      val SQLExecutionStartJsonString =
        s"""
          |{
          |  "Event":"$event",
          |  "executionId":0,
          |  "description":"test desc",
          |  "details":"test detail",
          |  "physicalPlanDescription":"test plan",
          |  "sparkPlanInfo": {
          |    "nodeName":"TestNode",
          |    "simpleString":"test string",
          |    "children":[],
          |    "metadata":{},
          |    "metrics":[]
          |  },
          |  "time":0,
          |  "modifiedConfigs": {
          |    "k1":"v1"
          |  }
          |}
      """.stripMargin

      val reconstructedEvent = JsonProtocol.sparkEventFromJson(parse(SQLExecutionStartJsonString))
      if (newExecutionStartEvent) {
        val expectedEvent = SparkListenerSQLExecutionStart(0, "test desc", "test detail",
          "test plan", new SparkPlanInfo("TestNode", "test string", Nil, Map(), Nil), 0,
          Map("k1" -> "v1"))
        assert(reconstructedEvent == expectedEvent)
      } else {
        val expectedOldEvent = OldVersionSQLExecutionStart(0, "test desc", "test detail",
          "test plan", new SparkPlanInfo("TestNode", "test string", Nil, Map(), Nil), 0)
        assert(reconstructedEvent == expectedOldEvent)
      }
    }
  }

  test("SparkListenerSQLExecutionEnd backward compatibility") {
    spark = new TestSparkSession()
    val qe = spark.sql("select 1").queryExecution
    val event = SparkListenerSQLExecutionEnd(1, 10)
    event.duration = 1000
    event.executionName = Some("test")
    event.qe = qe
    event.executionFailure = Some(new RuntimeException("test"))
    val json = JsonProtocol.sparkEventToJson(event)
    assert(json == parse(
      """
        |{
        |  "Event" : "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd",
        |  "executionId" : 1,
        |  "time" : 10
        |}
      """.stripMargin))
    val readBack = JsonProtocol.sparkEventFromJson(json)
    event.duration = 0
    event.executionName = None
    event.qe = null
    event.executionFailure = None
    assert(readBack == event)
  }
}

private case class OldVersionSQLExecutionStart(
    executionId: Long,
    description: String,
    details: String,
    physicalPlanDescription: String,
    sparkPlanInfo: SparkPlanInfo,
    time: Long)
  extends SparkListenerEvent
