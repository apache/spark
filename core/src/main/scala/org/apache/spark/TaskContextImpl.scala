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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{TaskCompletionListener, TaskCompletionListenerException}

private[spark] class TaskContextImpl(
    val stageId: Int,
    val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    @transient private val metricsSystem: MetricsSystem,
    internalAccumulators: Seq[Accumulator[Long]],
    val runningLocally: Boolean = false)
  extends TaskContext
  with Logging {

  /**
   * Metrics associated with this task.
   */
  override val taskMetrics: TaskMetrics = new TaskMetrics(internalAccumulators)

  /**
   * All accumulators used in this task indexed by accumulator ID.
   */
  @transient private val _accumulators = new mutable.HashMap[Long, Accumulable[_, _]]

  // Register the initial set of internal accumulators.
  // Future ones will be registered through `registerAccumulator`.
  internalAccumulators.foreach { a => _accumulators(a.id) = a }

  // For backwards-compatibility; this method is now deprecated as of 1.3.0.
  override val attemptId: Long = taskAttemptId

  // List of callback functions to execute when the task completes.
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  // Whether the corresponding task has been killed.
  @volatile private var interrupted: Boolean = false

  // Whether the task has completed.
  @volatile private var completed: Boolean = false

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    onCompleteCallbacks += listener
    this
  }

  override def addTaskCompletionListener(f: TaskContext => Unit): this.type = {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f(context)
    }
    this
  }

  @deprecated("use addTaskCompletionListener", "1.1.0")
  override def addOnCompleteCallback(f: () => Unit) {
    onCompleteCallbacks += new TaskCompletionListener {
      override def onTaskCompletion(context: TaskContext): Unit = f()
    }
  }

  /** Marks the task as completed and triggers the listeners. */
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

  override def isRunningLocally(): Boolean = runningLocally

  override def isInterrupted(): Boolean = interrupted

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] def accumulators: Seq[Accumulable[_, _]] = _accumulators.values.toSeq

  private[spark] override def registerAccumulator(a: Accumulable[_, _]): Unit = synchronized {
    if (a.isInternal) {
      taskMetrics.registerInternalAccum(a)
    }
    _accumulators(a.id) = a
  }

  private[spark] override def collectAccumulatorValues(): Map[Long, Any] = synchronized {
    _accumulators.mapValues(_.localValue).toMap
  }

  // For testing only.
  private[spark] def findTestAccum(): Option[Accumulator[Long]] = {
    accumulators
      .find(_.name == Some(InternalAccumulator.TEST_ACCUM))
      .map(_.asInstanceOf[Accumulator[Long]])
  }
}
