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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param numPartitions Total number of partitions in the stage that this task belongs to.
 * @param artifacts list of artifacts (may be session-specific) of the job this task belongs to.
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */
private[spark] abstract class Task[T](
    val stageId: Int,
    val stageAttemptId: Int,
    val partitionId: Int,
    val numPartitions: Int,
    val artifacts: JobArtifactSet,
    @transient var localProperties: Properties = new Properties,
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),
    val jobId: Option[Int] = None,
    val appId: Option[String] = None,
    val appAttemptId: Option[String] = None,
    val isBarrier: Boolean = false) extends Serializable with Logging {

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @param resources other host resources (like gpus) that this task attempt can access
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem,
      cpus: Int,
      resources: Map[String, ResourceInformation],
      plugins: Option[PluginContainer]): T = {

    require(cpus > 0, "CPUs per task should be > 0")

    // Use the blockManager at start of the task through out the task - particularly in
    // case of local mode, a SparkEnv can be initialized when spark context is restarted
    // and we want to ensure the right env and block manager is used (given lazy initialization of
    // block manager)
    val blockManager = SparkEnv.get.blockManager
    blockManager.registerTask(taskAttemptId)
    // TODO SPARK-24874 Allow create BarrierTaskContext based on partitions, instead of whether
    // the stage is barrier.
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      numPartitions,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics,
      cpus,
      resources)

    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)
    } else {
      taskContext
    }

    InputFileBlockHolder.initialize()
    TaskContext.setTaskContext(context)
    taskThread = Thread.currentThread()

    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    plugins.foreach(_.onTaskStart())

    try {
      context.runTaskWithListeners(this)
    } finally {
      try {
        Utils.tryLogNonFatalError {
          // Release memory used by this thread for unrolling blocks
          blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)
          blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.OFF_HEAP)
          // Notify any tasks waiting for execution memory to be freed to wake up and try to
          // acquire memory again. This makes impossible the scenario where a task sleeps forever
          // because there are no other tasks left to notify it. Since this is safe to do but may
          // not be strictly necessary, we should revisit whether we can remove this in the
          // future.

          val memoryManager = blockManager.memoryManager
          memoryManager.synchronized { memoryManager.notifyAll() }
        }
      } finally {
        // Though we unset the ThreadLocal here, the context member variable itself is still
        // queried directly in the TaskRunner to check for FetchFailedExceptions.
        TaskContext.unset()
        InputFileBlockHolder.unset()
      }
    }
  }

  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  var epoch: Long = -1

  // Task context, to be initialized in run().
  @transient var context: TaskContext = _

  // The actual Thread on which the task is running, if any. Initialized in run().
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null

  protected var _executorDeserializeTimeNs: Long = 0
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTimeNs: Long = _executorDeserializeTimeNs
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) {
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics().nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
        context.taskMetrics().withExternalAccums(_.filter(a => !taskFailed || a.countFailedValues))
    } else {
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */
  def kill(interruptThread: Boolean, reason: String): Unit = {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      TaskContext.synchronized {
        if (context.interruptible()) {
          context.markInterrupted(reason)
          if (interruptThread && taskThread != null) {
            taskThread.interrupt()
          }
        } else {
          logInfo(log"Task ${MDC(LogKeys.TASK_ID, context.taskAttemptId())} " +
            log"is currently not interruptible. ")
          val threadToInterrupt = if (interruptThread) Option(taskThread) else None
          context.pendingInterrupt(threadToInterrupt, reason)
        }
      }
    }
  }
}
