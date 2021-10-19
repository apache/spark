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

import java.util.{Properties, Stack}
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
 * that you cannot add a completion listener in one thread while we are completing in another
 * thread. Other state is immutable, however the exposed `TaskMetrics` & `MetricsSystem` objects are
 * not thread safe.
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

  /**
   * List of callback functions to execute when the task completes.
   *
   * Using a stack causes us to process listeners in reverse order of registration. As listeners are
   * invoked, they are popped from the stack.
   */
  @transient private val onCompleteCallbacks = new Stack[TaskCompletionListener]

  /** List of callback functions to execute when the task fails. */
  @transient private val onFailureCallbacks = new Stack[TaskFailureListener]

  /**
   * The thread currently executing task completion or failure listeners, if any.
   *
   * `invokeListeners()` uses this to ensure listeners are called sequentially.
   */
  @transient private var listenerInvocationThread: Option[Thread] = None

  // If defined, the corresponding task has been killed and this option contains the reason.
  @volatile private var reasonIfKilled: Option[String] = None

  // Whether the task has completed.
  private var completed: Boolean = false

  // If defined, the task has failed and this option contains the Throwable that caused the task to
  // fail.
  private var failureCauseOpt: Option[Throwable] = None

  // If there was a fetch failure in the task, we store it here, to make sure user-code doesn't
  // hide the exception.  See SPARK-19276
  @volatile private var _fetchFailedException: Option[FetchFailedException] = None

  override def addTaskCompletionListener(listener: TaskCompletionListener): this.type = {
    val needToCallListener = synchronized {
      // If there is already a thread invoking listeners, adding the new listener to
      // `onCompleteCallbacks` will cause that thread to execute the new listener, and the call to
      // `invokeTaskCompletionListeners()` below will be a no-op.
      //
      // If there is no such thread, the call to `invokeTaskCompletionListeners()` below will
      // execute all listeners, including the new listener.
      onCompleteCallbacks.push(listener)
      completed
    }
    if (needToCallListener) {
      invokeTaskCompletionListeners(None)
    }
    this
  }

  override def addTaskFailureListener(listener: TaskFailureListener): this.type = {
    synchronized {
      onFailureCallbacks.push(listener)
      failureCauseOpt
    }.foreach(invokeTaskFailureListeners)
    this
  }

  override def resourcesJMap(): java.util.Map[String, ResourceInformation] = {
    resources.asJava
  }

  private[spark] override def markTaskFailed(error: Throwable): Unit = {
    synchronized {
      if (failureCauseOpt.isDefined) return
      failureCauseOpt = Some(error)
    }
    invokeTaskFailureListeners(error)
  }

  private[spark] override def markTaskCompleted(error: Option[Throwable]): Unit = {
    synchronized {
      if (completed) return
      completed = true
    }
    invokeTaskCompletionListeners(error)
  }

  private def invokeTaskCompletionListeners(error: Option[Throwable]): Unit = {
    // It is safe to access the reference to `onCompleteCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onCompleteCallbacks, "TaskCompletionListener", error) {
      _.onTaskCompletion(this)
    }
  }

  private def invokeTaskFailureListeners(error: Throwable): Unit = {
    // It is safe to access the reference to `onFailureCallbacks` without holding the TaskContext
    // lock. `invokeListeners()` acquires the lock before accessing the contents.
    invokeListeners(onFailureCallbacks, "TaskFailureListener", Option(error)) {
      _.onTaskFailure(this, error)
    }
  }

  private def invokeListeners[T](
      listeners: Stack[T],
      name: String,
      error: Option[Throwable])(
      callback: T => Unit): Unit = {
    // This method is subject to two constraints:
    //
    // 1. Listeners must be run sequentially to uphold the guarantee provided by the TaskContext
    //    API.
    //
    // 2. Listeners may spawn threads that call methods on this TaskContext. To avoid deadlock, we
    //    cannot call listeners while holding the TaskContext lock.
    //
    // We meet these constraints by ensuring there is at most one thread invoking listeners at any
    // point in time.
    synchronized {
      if (listenerInvocationThread.nonEmpty) {
        // If another thread is already invoking listeners, do nothing.
        return
      } else {
        // If no other thread is invoking listeners, register this thread as the listener invocation
        // thread. This prevents other threads from invoking listeners until this thread is
        // deregistered.
        listenerInvocationThread = Some(Thread.currentThread())
      }
    }

    def getNextListenerOrDeregisterThread(): Option[T] = synchronized {
      if (listeners.empty()) {
        // We have executed all listeners that have been added so far. Deregister this thread as the
        // callback invocation thread.
        listenerInvocationThread = None
        None
      } else {
        Some(listeners.pop())
      }
    }

    val errorMsgs = new ArrayBuffer[String](2)
    var listenerOption: Option[T] = None
    while ({listenerOption = getNextListenerOrDeregisterThread(); listenerOption.nonEmpty}) {
      val listener = listenerOption.get
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
