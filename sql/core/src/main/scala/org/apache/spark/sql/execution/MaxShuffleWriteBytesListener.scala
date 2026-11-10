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

import scala.collection.mutable

import org.apache.spark.{SparkContext, Success}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent,
  SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * A [[SparkListener]] that cancels a running SQL query (or stops the application) once the
 * cumulative shuffle-write bytes of a single SQL execution exceed
 * `spark.sql.maxShuffleWriteBytes`.
 *
 * The threshold and kill mode are read from each job's properties rather than from the
 * [[SQLConf]] of the current thread, because this listener is registered once per
 * [[SparkContext]] and is shared by all sessions.
 */
class MaxShuffleWriteBytesListener(sc: SparkContext) extends SparkListener with Logging {

  // rootExecutionId -> running total and kill state of a single top-level SQL statement
  private val rootStates = new mutable.HashMap[Long, RootState]

  // stageId -> rootExecutionId, to attribute a finished task's bytes to its SQL execution
  private val stageToRoot = new mutable.HashMap[Int, Long]

  // stageId -> latest submitted attempt id, used to detect that a stage has been retried
  private val stageCurrentAttempt = new mutable.HashMap[Int, Int]

  // (stageId, attemptId) -> bytes written so far, so a retried attempt's partial total can be
  // subtracted instead of double-counted into the root running total
  private val stageAttemptBytes = new mutable.HashMap[(Int, Int), Long]

  // executionId -> rootExecutionId, to reap a root's state when its root execution ends
  private val executionToRoot = new mutable.HashMap[Long, Long]

  // jobId -> rootExecutionId, to drop a finished job from its root's active-job set
  private val jobToRoot = new mutable.HashMap[Int, Long]

  // rootExecutionId -> active job ids, the set cancelled when the threshold is exceeded
  private val rootToJobs = new mutable.HashMap[Long, mutable.HashSet[Int]]

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    val properties = event.properties
    if (properties == null) {
      return
    }
    val executionId = property(properties, SQLExecution.EXECUTION_ID_KEY).map(_.toLong)
    val threshold = property(properties, SQLConf.MAX_SHUFFLE_WRITE_BYTES.key)
      .flatMap(SQLConf.MAX_SHUFFLE_WRITE_BYTES.valueConverter)
    if (executionId.isEmpty || threshold.isEmpty) {
      return
    }
    val rootExecutionId = property(properties, SQLExecution.EXECUTION_ROOT_ID_KEY)
      .map(_.toLong).getOrElse(executionId.get)
    val stopApplication =
      property(properties, SQLConf.MAX_SHUFFLE_WRITE_BYTES_STOP_APPLICATION.key)
        .map(SQLConf.MAX_SHUFFLE_WRITE_BYTES_STOP_APPLICATION.valueConverter)
        .getOrElse(false)

    executionToRoot(executionId.get) = rootExecutionId
    event.stageIds.foreach { stageId => stageToRoot(stageId) = rootExecutionId }
    rootStates.getOrElseUpdate(rootExecutionId, new RootState(threshold.get, stopApplication))
    jobToRoot(event.jobId) = rootExecutionId
    rootToJobs.getOrElseUpdate(rootExecutionId, new mutable.HashSet[Int]) += event.jobId
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    val stageId = event.stageInfo.stageId
    if (!stageToRoot.contains(stageId)) {
      return
    }
    val newAttemptId = event.stageInfo.attemptNumber()
    val prevAttemptId = stageCurrentAttempt.put(stageId, newAttemptId)
    // A retried stage re-runs its successful tasks, so drop the previous attempt's partial
    // count to avoid double counting those bytes.
    prevAttemptId.foreach { oldAttemptId =>
      if (oldAttemptId != newAttemptId) {
        val partial = stageAttemptBytes.remove((stageId, oldAttemptId)).getOrElse(0L)
        rootStates.get(stageToRoot(stageId)).foreach(_.bytes -= partial)
      }
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    // Only successful tasks contribute to the cap: a failed or speculatively-killed task's
    // shuffle output is discarded and recomputed, so counting it would double-count the same
    // logical partition's bytes.
    if (event.reason != Success || event.taskMetrics == null) {
      return
    }
    val rootExecutionId = stageToRoot.get(event.stageId)
    if (rootExecutionId.isEmpty) {
      return
    }
    val state = rootStates.get(rootExecutionId.get)
    if (state.isEmpty) {
      return
    }
    val bytes = event.taskMetrics.shuffleWriteMetrics.bytesWritten
    if (bytes == 0) {
      return
    }
    // Ignore a late Success from an attempt that onStageSubmitted has already superseded, so
    // the same partition's bytes are not counted twice across a stage retry.
    if (stageCurrentAttempt.get(event.stageId).exists(_ != event.stageAttemptId)) {
      return
    }
    val attemptKey = (event.stageId, event.stageAttemptId)
    stageAttemptBytes(attemptKey) = stageAttemptBytes.getOrElse(attemptKey, 0L) + bytes
    state.get.bytes += bytes
    if (!state.get.killed && state.get.bytes > state.get.threshold) {
      state.get.killed = true
      kill(rootExecutionId.get, state.get)
    }
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    jobToRoot.remove(event.jobId).foreach { rootExecutionId =>
      rootToJobs.get(rootExecutionId).foreach(_ -= event.jobId)
      rootStates.get(rootExecutionId).foreach(stopIfAllJobsEnded(rootExecutionId, _))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd =>
      executionToRoot.remove(e.executionId).foreach { rootExecutionId =>
        if (rootExecutionId == e.executionId) {
          rootStates.remove(rootExecutionId)
          stageToRoot.filterInPlace((_, root) => root != rootExecutionId)
          stageAttemptBytes.filterInPlace((key, _) => stageToRoot.contains(key._1))
          stageCurrentAttempt.filterInPlace((stage, _) => stageToRoot.contains(stage))
          jobToRoot.filterInPlace((_, root) => root != rootExecutionId)
          rootToJobs.remove(rootExecutionId)
        }
      }
    case _ =>
  }

  private def kill(rootExecutionId: Long, state: RootState): Unit = {
    val reason = s"shuffle write size ${Utils.bytesToString(state.bytes)} exceeds " +
      s"the limit of ${Utils.bytesToString(state.threshold)}. Please optimize the SQL query"
    logError(s"Cancelling SQL execution with root execution id $rootExecutionId: $reason")
    // Cancel the offending jobs with the descriptive reason before tearing the application down,
    // so the reason that survives in the Spark UI is the friendly one rather than the generic
    // "Stage cancelled because SparkContext was shut down" from cleanUpAfterSchedulerStop.
    rootToJobs.get(rootExecutionId).foreach(_.toSeq.foreach(sc.cancelJob(_, reason)))
    if (state.stopApplication) {
      state.stopAfterJobsEnd = true
      stopIfAllJobsEnded(rootExecutionId, state)
    }
  }

  // In stopApplication mode, wait until the offending jobs have actually been cancelled (their
  // SparkListenerJobEnd has fired) before stopping the SparkContext. Otherwise sc.stop() discards
  // the queued JobCancelled events and DAGScheduler.cleanUpAfterSchedulerStop re-fails the jobs
  // with the generic shutdown reason, losing the descriptive one.
  private def stopIfAllJobsEnded(rootExecutionId: Long, state: RootState): Unit = {
    if (state.stopAfterJobsEnd && rootToJobs.get(rootExecutionId).forall(_.isEmpty)) {
      stopSparkContext()
    }
  }

  private def stopSparkContext(): Unit = {
    // SparkContext.stop() must not run on the listener bus thread. Offload it to a daemon thread
    // and clear the inheritable within-listener-thread flag, which the new thread would otherwise
    // inherit and let SparkContext.stop() reject the call.
    val thread = new Thread(() => LiveListenerBus.withinListenerThread.withValue(false) {
      sc.stop()
    })
    thread.setDaemon(true)
    thread.start()
  }

  private def property(properties: java.util.Properties, key: String): Option[String] = {
    Option(properties.getProperty(key))
  }

  private[sql] def rootShuffleWriteBytes(rootExecutionId: Long): Option[Long] =
    rootStates.get(rootExecutionId).map(_.bytes)
}

// `threshold` caps cumulative shuffle-write bytes; `stopApplication` stops the whole app instead
// of cancelling only the offending query.
private class RootState(val threshold: Long, val stopApplication: Boolean) {
  // Running total of shuffle-write bytes of successful tasks attributed to this root.
  var bytes: Long = 0L

  // True once the kill has fired, so later task ends don't re-trigger cancellation.
  var killed: Boolean = false

  // Stop the app only after the offending jobs have ended, so their descriptive cancellation
  // reason stays visible rather than being overwritten by cleanUpAfterSchedulerStop.
  var stopAfterJobsEnd: Boolean = false
}
