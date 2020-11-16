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

import org.apache.spark.sql.execution.streaming.{MemoryStream, StreamExecution}
import org.apache.spark.sql.streaming.StreamTest

class QueryPlanningTrackerEndToEndSuite extends StreamTest {
  import testImplicits._

  test("programmatic API") {
    val df = spark.range(1000).selectExpr("count(*)")
    df.collect()
    val tracker = df.queryExecution.tracker
    assert(tracker.phases.keySet == Set("analysis", "optimization", "planning"))
    assert(tracker.rules.nonEmpty)
  }

  test("sql") {
    val df = spark.sql("select * from range(1)")
    df.collect()

    val tracker = df.queryExecution.tracker
    assert(tracker.phases.keySet == Set("parsing", "analysis", "optimization", "planning"))
    assert(tracker.rules.nonEmpty)
  }

  test("SPARK-29227: Track rule info in optimization phase in streaming") {
    val inputData = MemoryStream[Int]
    val df = inputData.toDF()

    def assertStatus(stream: StreamExecution): Unit = {
      stream.processAllAvailable()
      val tracker = stream.lastExecution.tracker
      assert(tracker.phases.keys == Set("analysis", "optimization", "planning"))
      assert(tracker.rules.nonEmpty)
    }

    testStream(df)(
      StartStream(),
      AddData(inputData, 1, 2, 3),
      Execute(assertStatus),
      StopStream)
  }

  test("The start times should be in order: parsing <= analysis <= optimization <= planning") {
    val df = spark.sql("select count(*) from range(1)")
    df.queryExecution.executedPlan
    val phases = df.queryExecution.tracker.phases
    assert(phases("parsing").startTimeMs <= phases("analysis").startTimeMs)
    assert(phases("analysis").startTimeMs <= phases("optimization").startTimeMs)
    assert(phases("optimization").startTimeMs <= phases("planning").startTimeMs)
  }

}
