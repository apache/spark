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

package org.apache.spark.sql.execution.history

import java.util.Properties

import org.apache.spark.SparkFunSuite
import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.{SparkPlanInfo, SQLExecution}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ListenerEventsTestHelper

class SQLEventFilterBuilderSuite extends SparkFunSuite {
  import ListenerEventsTestHelper._

  override protected def beforeEach(): Unit = {
    ListenerEventsTestHelper.reset()
  }

  test("track live SQL executions") {
    var time = 0L

    val listener = new SQLEventFilterBuilder

    listener.onOtherEvent(SparkListenerLogStart("TestSparkVersion"))

    // Start the application.
    time += 1
    listener.onApplicationStart(SparkListenerApplicationStart(
      "name",
      Some("id"),
      time,
      "user",
      Some("attempt"),
      None))

    // Start a couple of executors.
    time += 1
    val execIds = Array("1", "2")
    execIds.foreach { id =>
      listener.onExecutorAdded(createExecutorAddedEvent(id, time))
    }

    // Start SQL Execution
    listener.onOtherEvent(SparkListenerSQLExecutionStart(1, Some(1), "desc1", "details1", "plan",
      new SparkPlanInfo("node", "str", Seq.empty, Map.empty, Seq.empty), time, Map.empty))

    time += 1

    // job 1, 2: coupled with SQL execution 1, finished
    val jobProp = createJobProps()
    val jobPropWithSqlExecution = new Properties(jobProp)
    jobPropWithSqlExecution.setProperty(SQLExecution.EXECUTION_ID_KEY, "1")
    val jobInfoForJob1 = pushJobEventsWithoutJobEnd(listener, 1, jobPropWithSqlExecution,
      execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(1, time, JobSucceeded))

    val jobInfoForJob2 = pushJobEventsWithoutJobEnd(listener, 2, jobPropWithSqlExecution,
      execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(2, time, JobSucceeded))

    // job 3: not coupled with SQL execution 1, finished
    pushJobEventsWithoutJobEnd(listener, 3, jobProp, execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(3, time, JobSucceeded))

    // job 4: not coupled with SQL execution 1, not finished
    pushJobEventsWithoutJobEnd(listener, 4, jobProp, execIds, time)
    listener.onJobEnd(SparkListenerJobEnd(4, time, JobSucceeded))

    assert(listener.liveSQLExecutions === Set(1))

    // only SQL executions related jobs are tracked
    assert(listener.liveJobs === Set(1, 2))
    assert(listener.liveStages ===
      (jobInfoForJob1.stageIds ++ jobInfoForJob2.stageIds).toSet)
    assert(listener.liveTasks ===
      (jobInfoForJob1.stageToTaskIds.values.flatten ++
        jobInfoForJob2.stageToTaskIds.values.flatten).toSet)
    assert(listener.liveRDDs ===
      (jobInfoForJob1.stageToRddIds.values.flatten ++
        jobInfoForJob2.stageToRddIds.values.flatten).toSet)

    // End SQL execution
    listener.onOtherEvent(SparkListenerSQLExecutionEnd(1, 0))

    assert(listener.liveSQLExecutions.isEmpty)
    assert(listener.liveJobs.isEmpty)
    assert(listener.liveStages.isEmpty)
    assert(listener.liveTasks.isEmpty)
    assert(listener.liveRDDs.isEmpty)
  }
}
