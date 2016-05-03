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

package org.apache.spark

import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.util._

private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty)
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes. */
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  // Whether the task has failed.
  @volatile private var failed: Boolean = false

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    onFailureCallbacks += listener
    this
  }

  /** Marks the task as failed and triggers the failure listeners. */
  private[spark] def markTaskFailed(error: Throwable): Unit = {
    // failure callbacks should only be called once
    if (failed) return
    failed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process failure callbacks in the reverse order of registration
    onFailureCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskFailure(this, error)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskFailureListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs, Option(error))
    }
  }

  /** Marks the task as completed and triggers the completion listeners. */
  private[spark] def markTaskCompleted(): Unit = {
    completed = true
    val errorMsgs = new ArrayBuffer[String](2)
    // Process complete callbacks in the reverse order of registration
    onCompleteCallbacks.reverse.foreach { listener =>
      try {
        listener.onTaskCompletion(this)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError("Error in TaskCompletionListener", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs)
    }
  }

  /** Marks the task for interruption, i.e. cancellation. */
  private[spark] def markInterrupted(): Unit = {
    interrupted = true
  }

  override def isCompleted(): Boolean = completed

  override def isRunningLocally(): Boolean = false

  override def isInterrupted(): Boolean = interrupted

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

}
