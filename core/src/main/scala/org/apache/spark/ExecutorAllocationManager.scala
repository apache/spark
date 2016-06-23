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

import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.util.control.ControlThrowable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.dynamicallocation.{AllocationStrategy, DefaultAllocationStrategy, Metric}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.scheduler._
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager periodically calls the configured dynamic allocation strategy to
 * perform the actual executor allocation/release.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.strategy - The class name of the dynamic allocation strategy
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  // strategy to use for dynamic allocation
  private val allocationStrategyClass = conf.get("spark.dynamicAllocation.strategy",
    classOf[DefaultAllocationStrategy].getName)

  private[spark] lazy val allocationStrategy = try {
    Utils.classForName(allocationStrategyClass)
      .getDeclaredConstructor(classOf[SparkConf],
        classOf[ExecutorAllocationManager], classOf[ExecutorAllocationClient])
      .newInstance(conf, this, client)
      .asInstanceOf[AllocationStrategy]
  } catch {
    case e: Exception =>
      throw new SparkException(s"Unable to load ${allocationStrategyClass}", e)
  }

  private val initialNumExecutors = conf.getInt("spark.dynamicAllocation.initialExecutors",
    conf.getInt("spark.dynamicAllocation.minExecutors", 0))

  // All known executors
  private[spark] val executorIds = new mutable.HashSet[String]

  // Polling loop interval (ms)
  private val intervalMillis: Long = 100

  // Clock used to schedule when executors should be added and removed
  private var clock: Clock = new SystemClock()

  // Listener for Spark events that impact the allocation policy
  private[spark] val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation")

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Whether we are still waiting for the initial set of executors to be allocated.
  // While this is true, we will not cancel outstanding executor requests. This is
  // set to false when:
  //   (1) a stage is submitted, or
  //   (2) an executor idle timeout has elapsed.
  @volatile private[spark] var initializing: Boolean = true

  // Number of locality aware tasks, used for executor placement.
  private[spark] var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  private[spark] var hostToLocalTaskCount: Map[String, Int] = Map.empty

  /**
   * Use a different clock for this allocation manager. This is mainly used for testing.
   */
  def setClock(newClock: Clock): Unit = {
    clock = newClock
    allocationStrategy.setClock(newClock)
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   */
  def start(): Unit = {
    listenerBus.addListener(listener)

    val scheduleTask = new Runnable() {
      override def run(): Unit = {
        try {
          schedule()
        } catch {
          case ct: ControlThrowable =>
            throw ct
          case t: Throwable =>
            logWarning(s"Uncaught exception in thread ${Thread.currentThread().getName}", t)
        }
      }
    }
    executor.scheduleAtFixedRate(scheduleTask, 0, intervalMillis, TimeUnit.MILLISECONDS)

    client.requestTotalExecutors(initialNumExecutors, localityAwareTasks, hostToLocalTaskCount)
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    executor.shutdown()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  /**
   * Reset the allocation manager to the initial state. Currently this will only be called in
   * yarn-client mode when AM re-registers after a failure.
   */
  def reset(): Unit = synchronized {
    initializing = true
    allocationStrategy.reset()
  }

  /**
   * This is called at a fixed interval to regulate the number of pending executor requests
   * and number of executors running.
   *
   * First, call allocation strategy's allocate() to get new executors if needed.
   * Then, call release() remove the executors that are no longer needed.
   *
   * This is factored out into its own method for testing.
   */
  private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis
    allocationStrategy.allocate(now)
    if (allocationStrategy.release(now)) {
      initializing = false
    }
  }

  /**
   * Callback invoked when the specified executor has been added.
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      executorIds.add(executorId)
      // If an executor (call this executor X) is not removed because the lower bound
      // has been reached, it will no longer be marked as idle. When new executors join,
      // however, we are no longer at the lower bound, and so we must mark executor X
      // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
      executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
      logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
    } else {
      logWarning(s"Duplicate executor $executorId has registered")
    }
    allocationStrategy.onExecutorAdded(executorId)
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      executorIds.remove(executorId)
      logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")
      allocationStrategy.onExecutorRemoved(executorId)
    } else {
      logWarning(s"Unknown executor $executorId has been removed!")
    }
  }

  /**
   * Callback invoked when the scheduler receives new pending tasks.
   * This sets a time in the future that decides when executors should be added
   * if it is not already set.
   */
  private def onSchedulerBacklogged(): Unit = synchronized {
    allocationStrategy.onSchedulerBacklogged()
  }

  /**
   * Callback invoked when the scheduler queue is drained.
   * This resets all variables used for adding executors.
   */
  private def onSchedulerQueueEmpty(): Unit = synchronized {
    allocationStrategy.onSchedulerQueueEmpty()
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      allocationStrategy.onExecutorIdle(executorId)
    } else {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
    }
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    allocationStrategy.onExecutorBusy(executorId)
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
   */
  private[spark] class ExecutorAllocationListener extends SparkListener {

    private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
    private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
    private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
    // Number of tasks currently running on the cluster.  Should be 0 when no stages are active.
    private var numRunningTasks: Int = _

    // stageId to tuple (the number of task with locality preferences, a map where each pair is a
    // node and the number of tasks that would like to be scheduled on that node) map,
    // maintain the executor placement hints for each stage Id used by resource framework to better
    // place the executors.
    private val stageIdToExecutorPlacementHints = new mutable.HashMap[Int, (Int, Map[String, Int])]

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      initializing = false
      val stageId = stageSubmitted.stageInfo.stageId
      val numTasks = stageSubmitted.stageInfo.numTasks
      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks
        allocationManager.onSchedulerBacklogged()

        // Compute the number of tasks requested by the stage on each host
        var numTasksPending = 0
        val hostToLocalTaskCountPerStage = new mutable.HashMap[String, Int]()
        stageSubmitted.stageInfo.taskLocalityPreferences.foreach { locality =>
          if (!locality.isEmpty) {
            numTasksPending += 1
            locality.foreach { location =>
              val count = hostToLocalTaskCountPerStage.getOrElse(location.host, 0) + 1
              hostToLocalTaskCountPerStage(location.host) = count
            }
          }
        }
        stageIdToExecutorPlacementHints.put(stageId,
          (numTasksPending, hostToLocalTaskCountPerStage.toMap))

        // Update the executor placement hints
        updateExecutorPlacementHints()
      }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      val stageId = stageCompleted.stageInfo.stageId
      allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId
        stageIdToExecutorPlacementHints -= stageId

        // Update the executor placement hints
        updateExecutorPlacementHints()

        // If this is the last stage with pending tasks, mark the scheduler queue as empty
        // This is needed in case the stage is aborted for any reason
        if (stageIdToNumTasks.isEmpty) {
          allocationManager.onSchedulerQueueEmpty()
          if (numRunningTasks != 0) {
            logWarning("No stages are running, but numRunningTasks != 0")
            numRunningTasks = 0
          }
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }

        // If this is the last pending task, mark the scheduler queue as empty
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex
        if (totalPendingTasks() == 0) {
          allocationManager.onSchedulerQueueEmpty()
        }

        // Mark the executor on which this task is scheduled as busy
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)
      }
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      val taskIndex = taskEnd.taskInfo.index
      val stageId = taskEnd.stageId
      allocationManager.synchronized {
        numRunningTasks -= 1
        // If the executor is no longer running any scheduled tasks, mark it as idle
        if (executorIdToTaskIds.contains(executorId)) {
          executorIdToTaskIds(executorId) -= taskId
          if (executorIdToTaskIds(executorId).isEmpty) {
            executorIdToTaskIds -= executorId
            allocationManager.onExecutorIdle(executorId)
          }
        }

        // If the task failed, we expect it to be resubmitted later. To ensure we have
        // enough resources to run the resubmitted task, we need to mark the scheduler
        // as backlogged again if it's not already marked as such (SPARK-8366)
        if (taskEnd.reason != Success) {
          if (totalPendingTasks() == 0) {
            allocationManager.onSchedulerBacklogged()
          }
          stageIdToTaskIndices.get(stageId).foreach { _.remove(taskIndex) }
        }
      }
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        if (!allocationManager.executorIds.contains(executorId)) {
          allocationManager.onExecutorAdded(executorId)
        }
      }
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
      allocationManager.onExecutorRemoved(executorRemoved.executorId)
    }

    /**
     * An estimate of the total number of pending tasks remaining for currently running stages. Does
     * not account for tasks which may have failed and been resubmitted.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def totalPendingTasks(): Int = {
      stageIdToNumTasks.map { case (stageId, numTasks) =>
        numTasks - stageIdToTaskIndices.get(stageId).map(_.size).getOrElse(0)
      }.sum
    }

    /**
     * The number of tasks currently running across all stages.
     */
    def totalRunningTasks(): Int = numRunningTasks

    /**
     * Return true if an executor is not currently running a task, and false otherwise.
     *
     * Note: This is not thread-safe without the caller owning the `allocationManager` lock.
     */
    def isExecutorIdle(executorId: String): Boolean = {
      !executorIdToTaskIds.contains(executorId)
    }

    /**
     * Update the Executor placement hints (the number of tasks with locality preferences,
     * a map where each pair is a node and the number of tasks that would like to be scheduled
     * on that node).
     *
     * These hints are updated when stages arrive and complete, so are not up-to-date at task
     * granularity within stages.
     */
    def updateExecutorPlacementHints(): Unit = {
      var localityAwareTasks = 0
      val localityToCount = new mutable.HashMap[String, Int]()
      stageIdToExecutorPlacementHints.values.foreach { case (numTasksPending, localities) =>
        localityAwareTasks += numTasksPending
        localities.foreach { case (hostname, count) =>
          val updatedCount = localityToCount.getOrElse(hostname, 0) + count
          localityToCount(hostname) = updatedCount
        }
      }

      allocationManager.localityAwareTasks = localityAwareTasks
      allocationManager.hostToLocalTaskCount = localityToCount.toMap
    }
  }

  /**
   * Metric source for ExecutorAllocationManager to expose its internal executor allocation
   * status to MetricsSystem.
   * Note: These metrics heavily rely on the internal implementation of
   * ExecutorAllocationManager, metrics or value of metrics will be changed when internal
   * implementation is changed, so these metrics are not stable across Spark version.
   */
  private[spark] class ExecutorAllocationManagerSource extends Source {
    val sourceName = "ExecutorAllocationManager"
    val metricRegistry = new MetricRegistry()

    def registerGauge[T](metric: Metric[T]): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", metric.name), new Gauge[T] {
        override def getValue: T = synchronized {
          Option(metric.getValue).getOrElse(metric.defaultValue)
        }
      })
    }
    registerGauge(new Metric("numberAllExecutors", executorIds.size, 0))
    allocationStrategy.getMetricsToRegister().foreach(metric => registerGauge(metric))
  }
}
