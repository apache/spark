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

package org.apache.spark.deploy.history.yarn.unit

import java.io.IOException

import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent
import org.scalatest.BeforeAndAfter

import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.deploy.history.yarn.YarnHistoryService
import org.apache.spark.deploy.history.yarn.YarnTimelineUtils._
import org.apache.spark.deploy.history.yarn.testtools.ExtraAssertions
import org.apache.spark.deploy.history.yarn.testtools.YarnTestUtils._
import org.apache.spark.scheduler.{AccumulableInfo, JobSucceeded, SparkListenerBlockUpdated, SparkListenerEvent, SparkListenerJobEnd, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskGettingResult, SparkListenerTaskStart, StageInfo, TaskInfo, TaskLocality}

/**
 * Test low-level marshalling, robustness and quality of exception messages.
 */
class EventMarshallingSuite extends SparkFunSuite
    with BeforeAndAfter with Logging with ExtraAssertions {

  val stageInfo = new StageInfo(12, 13, "stageinfo-1", 4, Nil, Nil, "staged info")

  val taskInfo = new TaskInfo(100, 101, 102, 103, "executor", "host", TaskLocality.ANY, true)

  before {
    stageInfo.submissionTime = Some(100000)
    stageInfo.completionTime = Some(200000)
    stageInfo.failureReason = Some("network problems")
    val ai = new AccumulableInfo(1, "accumulator", Some("update"), "value", false)
    stageInfo.accumulables.put(1, ai)
  }

  test("unmarshall empty event") {
    val event = new TimelineEvent
    val ex = intercept[IOException] {
      toSparkEvent(event)
    }
    assertExceptionMessageContains(ex, E_EMPTY_EVENTINFO)
  }

  test("unmarshall entity type") {
    val event = new TimelineEvent
    event.setEventType(YarnHistoryService.SPARK_EVENT_ENTITY_TYPE)
    val ex = intercept[IOException] {
      toSparkEvent(event)
    }
    assertExceptionMessageContains(ex, E_EMPTY_EVENTINFO)
  }

  test("round trip app start") {
    val startEvent = appStartEvent(1)
    assert(APP_USER === startEvent.sparkUser)
    assert(APP_NAME === startEvent.appName)
    val dest = validateRoundTrip(startEvent)
    assert(startEvent.time === dest.time )
    assert(startEvent.sparkUser === dest.sparkUser )
    assert(APP_NAME === dest.appName)
  }

  test("round trip app end") {
    validateRoundTrip(appStopEvent(1))
  }

  test("SparkListenerStageSubmitted") {
    val src = new SparkListenerStageSubmitted(stageInfo)
    val dest = roundTrip(src)
    assert(isEqual(stageInfo, dest.stageInfo))
  }

  test("SparkListenerStageCompleted") {
    val src = new SparkListenerStageCompleted(stageInfo)
    val dest = roundTrip(src)
    assert(isEqual(stageInfo, dest.stageInfo))
  }

  test("SparkListenerTaskStart") {
    val src = new SparkListenerTaskStart(1, 2, taskInfo)
    val dest = roundTrip(src)
    assert(isEqual(taskInfo, dest.taskInfo))
  }

  test("SparkListenerTaskGettingResult") {
    val src = new SparkListenerTaskGettingResult(taskInfo)
    val dest = roundTrip(src)
    assert(isEqual(taskInfo, dest.taskInfo))
  }

  test("SparkListenerJobEnd") {
    val endTime = 3000L
    val id = 3
    val result = JobSucceeded
    val src = new SparkListenerJobEnd(id, endTime, result)
    val dest = roundTrip(src)
    assert(endTime === dest.time)
    assert(id === dest.jobId)
    assert(result === dest.jobResult)
  }

  test("SparkListenerBlockUpdated is ignored") {
    assert(toTimelineEvent(new SparkListenerBlockUpdated(null), 0).isEmpty)
  }

  def validateRoundTrip[T <: SparkListenerEvent](sparkEvt: T): T = {
    val trip = roundTrip(sparkEvt)
    assertResult(sparkEvt) {
      trip
    }
    trip
  }

  /**
   * Marshall then unmarshall a spark event.
   *
   * @param src source
   * @return a new spark event built from the marshalled JSON value.
   */
  private def roundTrip[T <: SparkListenerEvent ](src: T): T = {
    val event = toSparkEvent(toTimelineEvent(src, 100).get)
    event.asInstanceOf[T]
  }

  /**
   * Task info equality; does not check accumulables.
   *
   * @param l left item
   * @param r right item
   * @return true if the values are equal
   */
  def isEqual(l: TaskInfo, r: TaskInfo) : Boolean = {
    l.taskId == r.taskId &&
    l.index == r.index &&
    l.attemptNumber == r.attemptNumber &&
    l.executorId == r.executorId &&
    l.host == r.host &&
    l.speculative == r.speculative &&
    l.taskLocality == r.taskLocality &&
    l.gettingResultTime == r.gettingResultTime &&
    l.finishTime == r.finishTime &&
    l.failed == r.failed &&
    l.accumulables.size == r.accumulables.size
  }

  def isEqual(l: StageInfo, r: StageInfo): Boolean = {
    l.stageId == r.stageId &&
    l.name == r.name &&
    l.attemptId == r.attemptId &&
    l.numTasks == r.numTasks &&
    l.details == r.details &&
    l.submissionTime == r.submissionTime &&
    l.completionTime == r.completionTime &&
    l.failureReason == r.failureReason &&
    l.accumulables.size == r.accumulables.size
  }
}
