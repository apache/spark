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

import java.util.Properties

import scala.collection.mutable.{ArrayBuffer, HashMap}

import org.apache.spark.{InternalAccumulator, SparkFunSuite, Success}
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.scheduler._

class ScanLimitListenerSuite extends SparkFunSuite {

  private val executionId = 7L
  private val jobTag = "execution-7"

  /** The job tags that the listener cancelled, in order. */
  private val cancelledTags = new ArrayBuffer[String]()
  /** The task metrics of the latest attempt of each stage, by stage ID. */
  private val stageMetrics = new HashMap[Int, TaskMetrics]()
  private var listener: ScanLimitListener = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    cancelledTags.clear()
    stageMetrics.clear()
    listener = new ScanLimitListener((tag, _) => cancelledTags += tag)
  }

  private def submitStage(
      stageId: Int,
      attemptId: Int = 0,
      rootExecutionId: Long = executionId): Unit = {
    stageMetrics(stageId) = TaskMetrics.empty
    val stageInfo = new StageInfo(
      stageId = stageId,
      attemptId = attemptId,
      name = "",
      numTasks = 2,
      rddInfos = Nil,
      parentIds = Nil,
      details = "",
      taskMetrics = stageMetrics(stageId),
      resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    listener.onStageSubmitted(
      SparkListenerStageSubmitted(stageInfo, jobProperties(rootExecutionId)))
  }

  /**
   * The start of a job of an execution that depends on the given stages. A stage that has been
   * submitted has the metrics of its latest attempt.
   */
  private def startJob(stageIds: Seq[Int], rootExecutionId: Long): Unit = {
    val stageInfos = stageIds.map { stageId =>
      new StageInfo(
        stageId = stageId,
        attemptId = 0,
        name = "",
        numTasks = 2,
        rddInfos = Nil,
        parentIds = Nil,
        details = "",
        taskMetrics = stageMetrics.getOrElse(stageId, null),
        resourceProfileId = ResourceProfile.DEFAULT_RESOURCE_PROFILE_ID)
    }
    listener.onJobStart(SparkListenerJobStart(0, 0L, stageInfos, jobProperties(rootExecutionId)))
  }

  private def jobProperties(rootExecutionId: Long): Properties = {
    val properties = new Properties()
    properties.setProperty(SQLExecution.EXECUTION_ROOT_ID_KEY, rootExecutionId.toString)
    properties
  }

  private def taskInfo(taskId: Long, partitionId: Int): TaskInfo = {
    new TaskInfo(
      taskId = taskId,
      index = partitionId,
      attemptNumber = 0,
      partitionId = partitionId,
      launchTime = 0L,
      executorId = "",
      host = "",
      taskLocality = TaskLocality.ANY,
      speculative = false)
  }

  private def startTask(stageId: Int, taskId: Long, partitionId: Int): Unit = {
    listener.onTaskStart(SparkListenerTaskStart(stageId, 0, taskInfo(taskId, partitionId)))
  }

  /**
   * A heartbeat in which a running task reports the bytes it has read so far. Its accumulators
   * have no name, as in local mode.
   */
  private def heartbeat(stageId: Int, taskId: Long, bytesRead: Long): Unit = {
    def update(name: String, value: Long): AccumulableInfo = {
      val id = stageMetrics(stageId).nameToAccums(name).id
      new AccumulableInfo(id, None, Some(value), None, true, true)
    }
    val accumUpdates = Seq(
      update(InternalAccumulator.input.RECORDS_READ, Long.MaxValue),
      update(InternalAccumulator.input.BYTES_READ, bytesRead))
    listener.onExecutorMetricsUpdate(
      SparkListenerExecutorMetricsUpdate("", Seq((taskId, stageId, 0, accumUpdates))))
  }

  private def endTask(stageId: Int, taskId: Long, partitionId: Int, bytesRead: Long): Unit = {
    val taskMetrics = new TaskMetrics()
    taskMetrics.inputMetrics.setBytesRead(bytesRead)
    listener.onTaskEnd(SparkListenerTaskEnd(
      stageId, 0, "", Success, taskInfo(taskId, partitionId), new ExecutorMetrics, taskMetrics))
  }

  test("cancels an execution once its tasks have read more than the limit") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    submitStage(stageId = 0)
    startTask(stageId = 0, taskId = 1, partitionId = 0)
    startTask(stageId = 0, taskId = 2, partitionId = 1)
    heartbeat(stageId = 0, taskId = 1, bytesRead = 60)
    heartbeat(stageId = 0, taskId = 2, bytesRead = 40)
    // Reading as much as the limit is allowed.
    assert(cancelledTags.isEmpty)
    assert(listener.bytesReadOverLimit(executionId).isEmpty)

    endTask(stageId = 0, taskId = 1, partitionId = 0, bytesRead = 61)
    assert(cancelledTags.toSeq == Seq(jobTag))
    assert(listener.bytesReadOverLimit(executionId).contains(101))

    // What a task reads while it is being cancelled does not cancel the execution again.
    heartbeat(stageId = 0, taskId = 2, bytesRead = 90)
    assert(cancelledTags.toSeq == Seq(jobTag))
    assert(listener.bytesReadOverLimit(executionId).contains(101))
  }

  test("counts a partition once over the attempts of its task and of its stage") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    submitStage(stageId = 0)
    // The first attempt of the task fails after it read 60 bytes, and the second one reads the
    // same data again.
    startTask(stageId = 0, taskId = 1, partitionId = 0)
    endTask(stageId = 0, taskId = 1, partitionId = 0, bytesRead = 60)
    startTask(stageId = 0, taskId = 2, partitionId = 0)
    heartbeat(stageId = 0, taskId = 2, bytesRead = 50)
    endTask(stageId = 0, taskId = 2, partitionId = 0, bytesRead = 70)
    // A new attempt of the stage computes the partition once more.
    submitStage(stageId = 0, attemptId = 1)
    startTask(stageId = 0, taskId = 3, partitionId = 0)
    endTask(stageId = 0, taskId = 3, partitionId = 0, bytesRead = 70)
    assert(cancelledTags.isEmpty)

    // Only another partition adds to the 70 bytes read.
    startTask(stageId = 0, taskId = 4, partitionId = 1)
    endTask(stageId = 0, taskId = 4, partitionId = 1, bytesRead = 31)
    assert(cancelledTags.toSeq == Seq(jobTag))
    assert(listener.bytesReadOverLimit(executionId).contains(101))
  }

  test("cancels the jobs that an execution starts after it went over the limit") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    submitStage(stageId = 0)
    endTask(stageId = 0, taskId = 1, partitionId = 0, bytesRead = 101)
    assert(cancelledTags.toSeq == Seq(jobTag))

    // The job waits for a stage that a job of another execution is running, and submits none.
    submitStage(stageId = 1, rootExecutionId = executionId + 1)
    startJob(stageIds = Seq(1, 2), rootExecutionId = executionId)
    assert(cancelledTags.toSeq == Seq(jobTag, jobTag))
  }

  test("counts a stage against every execution that depends on it") {
    val otherExecutionId = executionId + 1
    val otherJobTag = "execution-8"
    listener.register(executionId, maxScanBytes = 100, jobTag)
    listener.register(otherExecutionId, maxScanBytes = 100, otherJobTag)
    submitStage(stageId = 0)
    startTask(stageId = 0, taskId = 1, partitionId = 0)
    heartbeat(stageId = 0, taskId = 1, bytesRead = 50)
    // A job of the other execution depends on the running stage, which is not submitted again.
    startJob(stageIds = Seq(0, 1), rootExecutionId = otherExecutionId)
    // The first execution ends, and the stage goes on running for the other one.
    listener.unregister(executionId)
    heartbeat(stageId = 0, taskId = 1, bytesRead = 151)
    assert(cancelledTags.toSeq == Seq(otherJobTag))
    // What the stage read before the job of the other execution started does not count for it.
    assert(listener.bytesReadOverLimit(otherExecutionId).contains(101))
  }

  test("counts a running stage that an execution without a limit submitted") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    submitStage(stageId = 0, rootExecutionId = executionId + 1)
    startTask(stageId = 0, taskId = 1, partitionId = 0)
    // A job of the execution depends on the running stage, which is not submitted again.
    startJob(stageIds = Seq(0), rootExecutionId = executionId)
    startTask(stageId = 0, taskId = 2, partitionId = 1)
    heartbeat(stageId = 0, taskId = 2, bytesRead = 60)
    // The task that was already running when the job started only counts when it ends.
    heartbeat(stageId = 0, taskId = 1, bytesRead = 30)
    assert(cancelledTags.isEmpty)

    endTask(stageId = 0, taskId = 1, partitionId = 0, bytesRead = 41)
    assert(cancelledTags.toSeq == Seq(jobTag))
    assert(listener.bytesReadOverLimit(executionId).contains(101))
  }

  test("counts a waiting stage that an execution without a limit submits later") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    // A job of the execution depends on a stage that has not been submitted yet.
    startJob(stageIds = Seq(0), rootExecutionId = executionId)
    // An earlier job of another execution, without a limit, depends on it too and submits it.
    submitStage(stageId = 0, rootExecutionId = executionId + 1)
    startTask(stageId = 0, taskId = 1, partitionId = 0)
    heartbeat(stageId = 0, taskId = 1, bytesRead = 101)
    assert(cancelledTags.toSeq == Seq(jobTag))
    assert(listener.bytesReadOverLimit(executionId).contains(101))
  }

  test("only tracks the registered executions") {
    listener.register(executionId, maxScanBytes = 100, jobTag)
    // A stage of another execution.
    submitStage(stageId = 0, rootExecutionId = executionId + 1)
    endTask(stageId = 0, taskId = 1, partitionId = 0, bytesRead = 1000)
    // A heartbeat of a task whose start event has not arrived.
    submitStage(stageId = 1)
    heartbeat(stageId = 1, taskId = 2, bytesRead = 1000)
    assert(cancelledTags.isEmpty)

    listener.unregister(executionId)
    endTask(stageId = 1, taskId = 2, partitionId = 0, bytesRead = 1000)
    assert(cancelledTags.isEmpty)
    assert(listener.bytesReadOverLimit(executionId).isEmpty)
  }
}
