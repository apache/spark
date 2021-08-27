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
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.metrics.source.Source
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util._


/**
 * A [[TaskContext]] implementation.
 *
 * A small note on thread safety. The interrupted & fetchFailed fields are volatile, this makes
 * sure that updates are always visible across threads. The complete & failed flags and their
 * callbacks are protected by locking on the context instance. For instance, this ensures
 * that you cannot add a completion listener in one thread while we are completing (and calling
 * the completion listeners) in another thread. Other state is immutable, however the exposed
 * `TaskMetrics` & `MetricsSystem` objects are not thread safe.
 */
private[spark] class TaskContextImpl(
    override val stageId: Int,
    override val stageAttemptNumber: Int,
    override val partitionId: Int,
    override val taskAttemptId: Long,
    override val attemptNumber: Int,
    override val taskMemoryManager: TaskMemoryManager,
    localProperties: Properties,
    @transient private val metricsSystem: MetricsSystem,
    // The default value is only used in tests.
    override val taskMetrics: TaskMetrics = TaskMetrics.empty,
    override val cpus: Int = SparkEnv.get.conf.get(config.CPUS_PER_TASK),
    override val resources: Map[String, ResourceInformation] = Map.empty)
  extends TaskContext
  with Logging {

  /** List of callback functions to execute when the task completes. */
  @transient private val onCompleteCallbacks = new ArrayBuffer[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  @transient private val onFailureCallbacks = new ArrayBuffer[TaskFailureListener]

  // If defined, the corresponding task has been killed and this option contains the reason.
  @volatile private var reasonIfKilled: Option[String] = None

  // Whether the task has completed.
  private var completed: Boolean = false

  // Whether the task has failed.
  private var failed: Boolean = false

  // Throwable that caused the task to fail
  private var failure: Throwable = _

  // If there was a fetch failure in the task, we store it here, to make sure user-code doesn't
  // hide the exception.  See SPARK-19276
  @volatile private var _fetchFailedException: Option[FetchFailedException] = None

  @GuardedBy("this")
  override def addTaskCompletionListener(listener: TaskCompletionListener)
      : this.type = synchronized {
    if (completed) {
      listener.onTaskCompletion(this)
    } else {
      onCompleteCallbacks += listener
    }
    this
  }

  @GuardedBy("this")
  override def addTaskFailureListener(listener: TaskFailureListener)
      : this.type = synchronized {
    if (failed) {
      listener.onTaskFailure(this, failure)
    } else {
      onFailureCallbacks += listener
    }
    this
  }

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources.asJava
  }

  @GuardedBy("this")
  private[spark] override def markTaskFailed(error: Throwable): Unit = synchronized {
    if (failed) return
    failed = true
    failure = error
    invokeListeners(onFailureCallbacks.toSeq, "TaskFailureListener", Option(error)) {
      _.onTaskFailure(this, error)
    }
  }

  @GuardedBy("this")
  private[spark] override def markTaskCompleted(error: Option[Throwable]): Unit = synchronized {
    if (completed) return
    completed = true
    invokeListeners(onCompleteCallbacks.toSeq, "TaskCompletionListener", error) {
      _.onTaskCompletion(this)
    }
  }

  private def invokeListeners[T](
      listeners: Seq[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit = {
    val errorMsgs = new ArrayBuffer[String](2)
    // Process callbacks in the reverse order of registration
    listeners.reverse.foreach { listener =>
      try {
        callback(listener)
      } catch {
        case e: Throwable =>
          errorMsgs += e.getMessage
          logError(s"Error in $name", e)
      }
    }
    if (errorMsgs.nonEmpty) {
      throw new TaskCompletionListenerException(errorMsgs.toSeq, error)
    }
  }

  private[spark] override def markInterrupted(reason: String): Unit = {
    reasonIfKilled = Some(reason)
  }

  private[spark] override def killTaskIfInterrupted(): Unit = {
    val reason = reasonIfKilled
    if (reason.isDefined) {
      throw new TaskKilledException(reason.get)
    }
  }

  private[spark] override def getKillReason(): Option[String] = {
    reasonIfKilled
  }

  @GuardedBy("this")
  override def isCompleted(): Boolean = synchronized(completed)

  override def isInterrupted(): Boolean = reasonIfKilled.isDefined

  override def getLocalProperty(key: String): String = localProperties.getProperty(key)

  override def getMetricsSources(sourceName: String): Seq[Source] =
    metricsSystem.getSourcesByName(sourceName)

  private[spark] override def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    taskMetrics.registerAccumulator(a)
  }

  private[spark] override def setFetchFailed(fetchFailed: FetchFailedException): Unit = {
    this._fetchFailedException = Option(fetchFailed)
  }

  private[spark] override def fetchFailed: Option[FetchFailedException] = _fetchFailedException

  private[spark] override def getLocalProperties(): Properties = localProperties
}
