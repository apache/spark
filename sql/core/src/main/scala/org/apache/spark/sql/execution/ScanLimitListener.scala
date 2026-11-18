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
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.apache.spark.InternalAccumulator
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{CONFIG, EXECUTION_ID, NUM_BYTES, NUM_BYTES_MAX}
import org.apache.spark.scheduler._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.OpenHashMap

/**
 * Enforces `spark.sql.execution.maxScanBytes`. It adds up the bytes read by the tasks of every
 * root SQL execution registered with it, and cancels the jobs of an execution once the total is
 * over the limit of that execution.
 *
 * The bytes are the task input metric `bytesRead`. It arrives with the executor heartbeats while
 * a task runs and with the task end event, so an execution is cancelled some time after it went
 * over its limit, not at that point.
 *
 * A task retry, a speculative task and a new attempt of a stage read the data of a partition
 * again. To keep that from counting against the limit, the listener keeps one value per
 * partition of a stage, the largest that any attempt reported.
 *
 * A job can depend on a stage that a job of another execution is already running, and that stage
 * is not submitted again. So the listener learns from the start of each job which stages it
 * depends on, and counts what a stage reads against every execution that depends on it. If no
 * execution with a limit depended on the stage before, the tasks that were already running then
 * only count when they end.
 *
 * @param cancelJobsWithTag cancels the active jobs that have the given tag, for the given reason
 */
private[sql] class ScanLimitListener(cancelJobsWithTag: (String, String) => Unit)
  extends SparkListener with Logging {

  // The registered executions by root execution ID. The event handlers read it without the lock,
  // to return early when no execution has a limit.
  private val executions = new ConcurrentHashMap[Long, ExecutionScanState]()

  // The stages of the registered executions by stage ID. Guarded by `this`, as is all the state
  // of the executions and the stages.
  private val stages = new mutable.HashMap[Int, StageScanState]()

  /**
   * Starts to track a root execution. All its jobs, including the ones of its nested executions,
   * must have `jobTag`.
   */
  def register(rootExecutionId: Long, maxScanBytes: Long, jobTag: String): Unit = {
    executions.put(rootExecutionId, new ExecutionScanState(rootExecutionId, maxScanBytes, jobTag))
  }

  /** Stops tracking a root execution. */
  def unregister(rootExecutionId: Long): Unit = synchronized {
    val execution = executions.remove(rootExecutionId)
    stages.filterInPlace { (_, stage) =>
      stage.executions -= execution
      stage.executions.nonEmpty
    }
  }

  /** The bytes that an execution had read when it went over its limit, if it did. */
  def bytesReadOverLimit(rootExecutionId: Long): Option[Long] = synchronized {
    Option(executions.get(rootExecutionId)).flatMap(_.bytesReadOverLimit)
  }

  /** The registered execution of a job with the given properties, if any. */
  private def executionOf(properties: Properties): Option[ExecutionScanState] = {
    Option(properties)
      .flatMap(p => Option(p.getProperty(SQLExecution.EXECUTION_ROOT_ID_KEY)))
      .flatMap(rootExecutionId => Option(executions.get(rootExecutionId.toLong)))
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    if (executions.isEmpty) return
    synchronized {
      executionOf(event.properties).foreach { execution =>
        event.stageInfos.foreach { stageInfo =>
          val stage = stages.getOrElseUpdate(stageInfo.stageId, new StageScanState)
          stage.executions += execution
          // A running stage that was submitted for an execution without a limit was not tracked
          // then, so its submission did not record the accumulator of its current attempt.
          recordBytesReadAccum(stage, stageInfo)
        }
      }
    }
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    if (executions.isEmpty) return
    synchronized {
      val stageId = event.stageInfo.stageId
      val execution = executionOf(event.properties)
      // A new attempt of a stage keeps what the earlier attempts read.
      execution.foreach { e =>
        stages.getOrElseUpdate(stageId, new StageScanState).executions += e
      }
      // A stage is submitted for the earliest active job that depends on it. That job may be of
      // an execution without a limit, while a job of an execution with one waits for the stage.
      stages.get(stageId).foreach(recordBytesReadAccum(_, event.stageInfo))
      // Cancelling only reaches the jobs that are active at that time, and the execution may
      // submit more of them before it fails.
      execution.filter(_.bytesReadOverLimit.isDefined).foreach(cancel)
    }
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    if (executions.isEmpty) return
    synchronized {
      stages.get(event.stageId).foreach { stage =>
        stage.runningTasks(event.taskInfo.taskId) = event.taskInfo.partitionId
      }
    }
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    if (executions.isEmpty) return
    synchronized {
      event.accumUpdates.foreach { case (taskId, stageId, _, accumUpdates) =>
        for {
          stage <- stages.get(stageId)
          // The start event of the task may not have arrived yet. A later heartbeat reports
          // the bytes again.
          partitionId <- stage.runningTasks.get(taskId)
        } {
          accumUpdates.find(update => stage.bytesReadAccumIds.contains(update.id))
            .flatMap(_.update)
            .foreach {
              case bytesRead: Long => recordBytesRead(stage, partitionId, bytesRead)
              case _ =>
            }
        }
      }
    }
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    if (executions.isEmpty) return
    synchronized {
      stages.get(event.stageId).foreach { stage =>
        stage.runningTasks.remove(event.taskInfo.taskId)
        // A task that failed before it reported its metrics has none.
        if (event.taskMetrics != null) {
          recordBytesRead(
            stage, event.taskInfo.partitionId, event.taskMetrics.inputMetrics.bytesRead)
        }
      }
    }
  }

  /** Records the accumulator of the bytes read of the stage attempt that `info` describes. */
  private def recordBytesReadAccum(stage: StageScanState, info: StageInfo): Unit = {
    // A stage that has not been submitted yet has no metrics.
    Option(info.taskMetrics).foreach { metrics =>
      stage.bytesReadAccumIds += metrics.nameToAccums(InternalAccumulator.input.BYTES_READ).id
    }
  }

  /** Records the bytes that an attempt of a task has read so far. */
  private def recordBytesRead(stage: StageScanState, partitionId: Int, bytesRead: Long): Unit = {
    val previous = stage.partitionBytes.get(partitionId).getOrElse(0L)
    if (bytesRead > previous) {
      stage.partitionBytes(partitionId) = bytesRead
      stage.executions.filter(_.bytesReadOverLimit.isEmpty).foreach { execution =>
        execution.bytesRead += bytesRead - previous
        if (execution.bytesRead > execution.maxScanBytes) {
          logWarning(log"Cancelling SQL execution " +
            log"${MDC(EXECUTION_ID, execution.executionId)}: it read " +
            log"${MDC(NUM_BYTES, execution.bytesRead)} bytes, more than the limit of " +
            log"${MDC(NUM_BYTES_MAX, execution.maxScanBytes)} bytes set by " +
            log"${MDC(CONFIG, SQLConf.MAX_SCAN_BYTES.key)}.")
          cancel(execution)
        }
      }
    }
  }

  private def cancel(execution: ExecutionScanState): Unit = {
    val bytesRead = Utils.bytesToString(execution.bytesReadOverLimit.get)
    val maxScanBytes = Utils.bytesToString(execution.maxScanBytes)
    cancelJobsWithTag(execution.jobTag, s"because the query read $bytesRead, more than the " +
      s"limit of $maxScanBytes set by ${SQLConf.MAX_SCAN_BYTES.key}")
  }
}

/** What a root execution with a scan limit has read. */
private class ExecutionScanState(
    val executionId: Long,
    val maxScanBytes: Long,
    val jobTag: String) {

  /**
   * The bytes that the tasks of the execution have read. It stops growing once it is over the
   * limit, so it keeps what the execution had read at that point.
   */
  var bytesRead = 0L

  /** The bytes that the execution had read when it went over its limit, if it did. */
  def bytesReadOverLimit: Option[Long] = Some(bytesRead).filter(_ > maxScanBytes)
}

/** What the tasks of a stage have read, over all the attempts of the stage. */
private class StageScanState {

  /** The executions that depend on the stage. What the stage reads counts against each. */
  val executions = new mutable.HashSet[ExecutionScanState]()

  /**
   * The ID of the accumulator of the bytes read, of each attempt of the stage. A heartbeat
   * identifies an accumulator by its ID: in local mode it has no name for an internal one.
   */
  val bytesReadAccumIds = new mutable.HashSet[Long]()

  /** The partition ID of each running task by task ID. A heartbeat only has the task ID. */
  val runningTasks = new mutable.HashMap[Long, Int]()

  /** The most bytes that an attempt of a task read, by the partition ID of the task. */
  val partitionBytes = new OpenHashMap[Int, Long]()
}
