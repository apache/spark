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

import java.util.concurrent.{TimeUnit, SynchronousQueue, ScheduledFuture, Callable}

import scala.collection.mutable
import scala.util.control.ControlThrowable

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.scheduler._
import org.apache.spark.metrics.source.Source
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * An agent that dynamically allocates and removes executors based on the workload.
 *
 * The ExecutorAllocationManager maintains a moving target number of executors which is periodically
 * synced to the cluster manager. The target starts at a configured initial value and changes with
 * the number of pending and running tasks.
 *
 * Decreasing the target number of executors happens when the current target is more than needed to
 * handle the current load. The target number of executors is always truncated to the number of
 * executors that could run all current running and pending tasks at once.
 *
 * Increasing the target number of executors happens in response to backlogged tasks waiting to be
 * scheduled. If the scheduler queue is not drained in N seconds, then new executors are added. If
 * the queue persists for another M seconds, then more executors are added and so on. The number
 * added in each round increases exponentially from the previous round until an upper bound has been
 * reached. The upper bound is based both on a configured property and on the current number of
 * running and pending tasks, as described above.
 *
 * The rationale for the exponential increase is twofold: (1) Executors should be added slowly
 * in the beginning in case the number of extra executors needed turns out to be small. Otherwise,
 * we may add more executors than we need just to remove them later. (2) Executors should be added
 * quickly over time in case the maximum number of executors is very high. Otherwise, it will take
 * a long time to ramp up under heavy workloads.
 *
 * The remove policy is simpler: If an executor has been idle for K seconds, meaning it has not
 * been scheduled to run any tasks, then it is removed.
 *
 * There is no retry logic in either case because we make the assumption that the cluster manager
 * will eventually fulfill all requests it receives asynchronously.
 *
 * The relevant Spark properties include the following:
 *
 *   spark.dynamicAllocation.enabled - Whether this feature is enabled
 *   spark.dynamicAllocation.minExecutors - Lower bound on the number of executors
 *   spark.dynamicAllocation.maxExecutors - Upper bound on the number of executors
 *   spark.dynamicAllocation.initialExecutors - Number of executors to start with
 *
 *   spark.dynamicAllocation.schedulerBacklogTimeout (M) -
 *     If there are backlogged tasks for this duration, add new executors
 *
 *   spark.dynamicAllocation.sustainedSchedulerBacklogTimeout (N) -
 *     If the backlog is sustained for this duration, add more executors
 *     This is used only after the initial backlog timeout is exceeded
 *
 *   spark.dynamicAllocation.executorIdleTimeout (K) -
 *     If an executor has been idle for this duration, remove it
 */
private[spark] class ExecutorAllocationManager(
    client: ExecutorAllocationClient,
    listenerBus: LiveListenerBus,
    conf: SparkConf)
  extends Logging {

  allocationManager =>

  import ExecutorAllocationManager._

  // Lower and upper bounds on the number of executors.
  private val minNumExecutors = conf.getInt("spark.dynamicAllocation.minExecutors", 0)
  private val maxNumExecutors = conf.getInt("spark.dynamicAllocation.maxExecutors",
    Integer.MAX_VALUE)
  private val initialNumExecutors = conf.getInt("spark.dynamicAllocation.initialExecutors",
    minNumExecutors)

  // How long there must be backlogged tasks for before an addition is triggered (seconds)
  private val schedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "1s")

  // Same as above, but used only after `schedulerBacklogTimeoutS` is exceeded
  private val sustainedSchedulerBacklogTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", s"${schedulerBacklogTimeoutS}s")

  // How long an executor must be idle for before it is removed (seconds)
  private val executorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.executorIdleTimeout", "60s")

  private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(
    "spark.dynamicAllocation.cachedExecutorIdleTimeout", s"${Integer.MAX_VALUE}s")

  // During testing, the methods to actually kill and add executors are mocked out
  private val testing = conf.getBoolean("spark.dynamicAllocation.testing", false)

  // TODO: The default value of 1 for spark.executor.cores works right now because dynamic
  // allocation is only supported for YARN and the default number of cores per executor in YARN is
  // 1, but it might need to be attained differently for different cluster managers
  private val tasksPerExecutor =
    conf.getInt("spark.executor.cores", 1) / conf.getInt("spark.task.cpus", 1)

  validateSettings()

  // Number of executors to add in the next round
  private var numExecutorsToAdd = 1

  // The desired number of executors at this moment in time. If all our executors were to die, this
  // is the number of executors we would immediately want from the cluster manager.
  private var numExecutorsTarget =
    conf.getInt("spark.dynamicAllocation.initialExecutors", minNumExecutors)

  private var prevNumExecutorsTarget = 0

  // Executors that have been requested to be removed but have not been killed yet
  private val executorsPendingToRemove = new mutable.HashSet[String]

  // All known executors
  private val executorIds = new mutable.HashSet[String]

  // A collection of futures that will kill idle executors after they timeout.
  private val idleExecutors = new mutable.HashMap[String, ScheduledFuture[Unit]]

  // Listener for Spark events that impact the allocation policy
  private val listener = new ExecutorAllocationListener

  // Executor that handles the scheduling task.
  private val executor =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("spark-dynamic-executor-allocation-events")

  // Executor that handles the scheduling task.
  private val thread: Thread = new Thread("spark-dynamic-executor-allocation-state-machine") { override def run() = allocationManager.run }

  // Metric source for ExecutorAllocationManager to expose internal status to MetricsSystem.
  val executorAllocationManagerSource = new ExecutorAllocationManagerSource

  // Number of locality aware tasks, used for executor placement.
  private var localityAwareTasks = 0

  // Host to possible task running on it, used for executor placement.
  private var hostToLocalTaskCount: Map[String, Int] = Map.empty

  private var _inbox = new SynchronousQueue[Message]

  private var state: State = new GrowableState

  private var workAvailable = false

  sealed trait Message
  case object ExecutorRemoved extends Message
  case object Exit extends Message
  case object Ping extends Message
  case object Reset extends Message
  case object Timeout extends Message
  case object WorkAvailable extends Message
  case object WorkFinished extends Message

  // Case objects are lazy, and creating them would take a deadlock-prone
  // lock from State to allocationManager. Accessing them here creates them
  // eagerly so we don't have that problem.
  ExecutorRemoved
  Exit
  Ping
  Reset
  Timeout
  WorkAvailable
  WorkFinished

  class State(timeout: Long = 60*60) {
    def execute(): State = {
      val msg = inbox.poll(timeout, TimeUnit.SECONDS) match {
        case null => Timeout
        case m => m
      }
      val res = handle(msg)
      logInfo(s"Transitioned to $res in response to $msg")
      res
    }

    def handle(msg: Message): State = msg match {
      case ExecutorRemoved => onExecutorRemoved
      case Exit => onExit
      case Ping => onPing
      case Reset => onReset
      case Timeout => onTimeout
      case WorkAvailable => onWorkAvailable
      case WorkFinished => onWorkFinished
    }

    def onExecutorRemoved(): State = {
      numExecutorsTarget -= 1
      return this
    }

    def onExit(): State = new StopState

    def onPing(): State = this

    def onReset(): State = {
      numExecutorsTarget = 1
      numExecutorsToAdd = 1

      idleExecutors.clear
      executorsPendingToRemove.clear

      new GrowableState
    }

    def onTimeout(): State = this

    def onWorkAvailable(): State = {
      workAvailable = true
      this
    }

    def onWorkFinished(): State = {
      workAvailable = false
      new GrowableState
    }
  }

  class StopState extends State {
    override def handle(msg: Message): State = {
      logWarning(s"$this recieved $msg")
      this
    }
  }

  class StableState extends State {
    override def execute(): State = {
      numExecutorsToAdd = 1
      super.execute()
    }

    override def onExecutorRemoved() = {
      super.onExecutorRemoved()
      new GrowableState
    }

    override def onWorkFinished(): State = {
      super.onWorkFinished()
      this
    }
  }

  class GrowableState extends State {
    override def execute(): State = {
      numExecutorsToAdd = 1
      if (numExecutorsTarget >= maxNumExecutors) {
        // This shouldn't occur unless someone is giving us executors we didn't
        // ask for.
        logWarning(s"Too many executors. Have $numExecutorsTarget/$maxNumExecutors")
        return new StableState
      }
      if (workAvailable) {
        return new ThrottleGrowthState(sustainedSchedulerBacklogTimeoutS)
      }
      super.execute()
    }

    override def onWorkAvailable(): State = {
      new ThrottleGrowthState(schedulerBacklogTimeoutS)
    }
  }

  class ThrottleGrowthState(timeout: Long) extends State(timeout) {
    override def onTimeout(): State = {
      numExecutorsTarget += numExecutorsToAdd
      numExecutorsToAdd *= 2

      val next = if (numExecutorsTarget >= maxNumExecutors) {
        numExecutorsTarget = maxNumExecutors
        new StableState
      } else if (numExecutorsTarget >= maxNumExecutorsNeeded) {
        numExecutorsTarget = maxNumExecutorsNeeded
        new GrowableState
      } else {
        new ThrottleGrowthState(sustainedSchedulerBacklogTimeoutS)
      }

      syncExecutorTarget()
      return next
    }
  }

  /**
   * Verify that the settings specified through the config are valid.
   * If not, throw an appropriate exception.
   */
  private def validateSettings(): Unit = {
    if (minNumExecutors < 0 || maxNumExecutors < 0) {
      throw new SparkException("spark.dynamicAllocation.{min/max}Executors must be positive!")
    }
    if (maxNumExecutors == 0) {
      throw new SparkException("spark.dynamicAllocation.maxExecutors cannot be 0!")
    }
    if (minNumExecutors > maxNumExecutors) {
      throw new SparkException(s"spark.dynamicAllocation.minExecutors ($minNumExecutors) must " +
        s"be less than or equal to spark.dynamicAllocation.maxExecutors ($maxNumExecutors)!")
    }
    if (schedulerBacklogTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.schedulerBacklogTimeout must be > 0!")
    }
    if (sustainedSchedulerBacklogTimeoutS <= 0) {
      throw new SparkException(
        "spark.dynamicAllocation.sustainedSchedulerBacklogTimeout must be > 0!")
    }
    if (executorIdleTimeoutS <= 0) {
      throw new SparkException("spark.dynamicAllocation.executorIdleTimeout must be > 0!")
    }
    // Require external shuffle service for dynamic allocation
    // Otherwise, we may lose shuffle files when killing executors
    if (!conf.getBoolean("spark.shuffle.service.enabled", false) && !testing) {
      throw new SparkException("Dynamic allocation of executors requires the external " +
        "shuffle service. You may enable this through spark.shuffle.service.enabled.")
    }
    if (tasksPerExecutor == 0) {
      throw new SparkException("spark.executor.cores must not be less than spark.task.cpus.")
    }
  }

  /**
   * Register for scheduler callbacks to decide when to add and remove executors, and start
   * the scheduling task.
   */
  def start(): Unit = {
    listenerBus.addListener(listener)
    thread.start
  }

  private def run(): Unit = {
    while (!state.isInstanceOf[StopState])
      state = state.execute()
  }

  /**
   * Stop the allocation manager.
   */
  def stop(): Unit = {
    val i = inbox

    if (i != null) {
      _inbox = null
      i.offer(Exit, 10, TimeUnit.SECONDS)
    }
    executor.shutdown();

    thread.join(10*1000)
    executor.awaitTermination(10, TimeUnit.SECONDS);
  }

  private def inbox(): SynchronousQueue[Message] = {
    if (Thread.holdsLock(allocationManager)) {
      throw new IllegalStateException("Accessing the inbox while you are locking " +
        "allocationManager will lead to deadlocks.")
    }
    _inbox
  }

  private def tryInbox[A](block: SynchronousQueue[Message] => A): A = inbox match {
    case null => null.asInstanceOf[A]
    case i => block(i)
  }

  /**
   * Reset the allocation manager to the initial state. Currently this will only be called in
   * yarn-client mode when AM re-registers after a failure.
   */
  def reset(): Unit = {
    inbox.put(Reset)
  }

  /**
   * The maximum number of executors we would need under the current load to satisfy all running
   * and pending tasks, rounded up.
   */
  private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }

  /**
   * Synchronize the number of requested executors to the desired target.
   */
  private def syncExecutorTarget(): Unit = synchronized {
    var delta = numExecutorsTarget - prevNumExecutorsTarget

    if (delta == 0) return

    val addRequestAcknowledged = testing ||
      client.requestTotalExecutors(numExecutorsTarget, localityAwareTasks, hostToLocalTaskCount)
    if (!addRequestAcknowledged) {
      logWarning(
        s"Unable to reach the cluster manager to request $numExecutorsTarget total executors!")
      return
    }

    val executorsString = if (delta == 1) "executor" else "executors"
    logInfo(s"Modifying number of executors from $prevNumExecutorsTarget to $numExecutorsTarget (delta $delta)")

    prevNumExecutorsTarget = numExecutorsTarget
  }

  /**
   * Request the cluster manager to remove the given executor.
   * Return whether the request is received.
   */
  private def removeIdleExecutor(executorId: String): Boolean = {
    allocationManager.synchronized {
      // Do not kill the executor if we are not aware of it (should never happen)
      if (!executorIds.contains(executorId)) {
        logWarning(s"Attempted to remove unknown executor $executorId!")
        return false
      }

      // Do not kill the executor again if it is already pending to be killed (should never happen)
      if (executorsPendingToRemove.contains(executorId)) {
        logWarning(s"Attempted to remove executor $executorId " +
          s"when it is already pending to be removed!")
        return false
      }

      idleExecutors.remove(executorId).foreach(_.cancel(false))

      // Do not kill the executor if we have already reached the lower bound
      val numExistingExecutors = executorIds.size - executorsPendingToRemove.size
      if (numExistingExecutors - 1 < minNumExecutors) {
        logDebug(s"Not removing idle executor $executorId because there are only " +
          s"$numExistingExecutors executor(s) left (limit $minNumExecutors)")
        return false
      }

      // Send a request to the backend to kill this executor
      if (!(testing || client.killExecutor(executorId))) {
        logWarning(s"Unable to reach the cluster manager to kill executor $executorId!")
        return false
      }

      logInfo(s"Removing executor $executorId because it has been idle for " +
        s"$executorIdleTimeoutS seconds (new desired total will be ${numExistingExecutors - 1})")
      executorsPendingToRemove.add(executorId)
    }

    inbox.put(ExecutorRemoved)
    true
  }

  /**
   * Callback invoked when the specified executor has been added.
   */
  private def onExecutorAdded(executorId: String): Unit = synchronized {
    if (executorIds.contains(executorId)) {
      logWarning(s"Duplicate executor $executorId has registered")
      return
    }

    executorIds.add(executorId)
    // If an executor (call this executor X) is not removed because the lower bound
    // has been reached, it will no longer be marked as idle. When new executors join,
    // however, we are no longer at the lower bound, and so we must mark executor X
    // as idle again so as not to forget that it is a candidate for removal. (see SPARK-4951)
    executorIds.filter(listener.isExecutorIdle).foreach(onExecutorIdle)
    logInfo(s"New executor $executorId has registered (new total is ${executorIds.size})")
  }

  /**
   * Callback invoked when the specified executor has been removed.
   */
  private def onExecutorRemoved(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      logWarning(s"Unknown executor $executorId has been removed!")
      return
    }

    executorIds.remove(executorId)
    idleExecutors.remove(executorId).foreach(_.cancel(false))
    logInfo(s"Existing executor $executorId has been removed (new total is ${executorIds.size})")

    if (executorsPendingToRemove.remove(executorId)) {
      logDebug(s"Executor $executorId is no longer pending to " +
        s"be removed (${executorsPendingToRemove.size} left)")
    }
  }

  /**
   * Callback invoked when the specified executor is no longer running any tasks.
   * This sets a time in the future that decides when this executor should be removed if
   * the executor is not already marked as idle.
   */
  private def onExecutorIdle(executorId: String): Unit = synchronized {
    if (!executorIds.contains(executorId)) {
      logWarning(s"Attempted to mark unknown executor $executorId idle")
      return
    }
    if (idleExecutors.contains(executorId)) {
      // Already marked idle.
      return
    }
    if (executorsPendingToRemove.contains(executorId)) {
      // Already going to be removed.
      return
    }

    // Note that it is not necessary to query the executors since all the cached
    // blocks we are concerned with are reported to the driver. Note that this
    // does not include broadcast blocks.
    val hasCachedBlocks = SparkEnv.get.blockManager.master.hasCachedBlocks(executorId)

    // Use a different timeout if the executor has cached blocks.
    val timeout = if (hasCachedBlocks) cachedExecutorIdleTimeoutS else executorIdleTimeoutS

    idleExecutors(executorId) = executor.schedule(new Callable[Unit] {
      override def call(): Unit = {
        removeIdleExecutor(executorId)
      }
    }, timeout, TimeUnit.SECONDS)

    logDebug(s"Starting idle timer for $executorId because there are no more tasks " +
      s"scheduled to run on the executor (to expire in ${timeout} seconds)")
  }

  /**
   * Callback invoked when the specified executor is now running a task.
   * This resets all variables used for removing this executor.
   */
  private def onExecutorBusy(executorId: String): Unit = synchronized {
    logDebug(s"Clearing idle timer for $executorId because it is now running a task")
    idleExecutors.remove(executorId).foreach(_.cancel(false))
  }

  /**
   * A listener that notifies the given allocation manager of when to add and remove executors.
   *
   * This class is intentionally conservative in its assumptions about the relative ordering
   * and consistency of events returned by the listener. For simplicity, it does not account
   * for speculated tasks.
   */
  private class ExecutorAllocationListener extends SparkListener {

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
      val stageId = stageSubmitted.stageInfo.stageId
      val numTasks = stageSubmitted.stageInfo.numTasks

      inbox.put(WorkAvailable)

      allocationManager.synchronized {
        stageIdToNumTasks(stageId) = numTasks

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
      val empty = allocationManager.synchronized {
        stageIdToNumTasks -= stageId
        stageIdToTaskIndices -= stageId
        stageIdToExecutorPlacementHints -= stageId

        // Update the executor placement hints
        updateExecutorPlacementHints()

        stageIdToNumTasks.isEmpty
      }

      if (empty) {
        inbox.put(WorkFinished)

        if (numRunningTasks != 0) {
          logWarning("No stages are running, but numRunningTasks != 0")
          numRunningTasks = 0
        }
      }
    }

    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
      val stageId = taskStart.stageId
      val taskId = taskStart.taskInfo.taskId
      val taskIndex = taskStart.taskInfo.index
      val executorId = taskStart.taskInfo.executorId

      val empty = allocationManager.synchronized {
        numRunningTasks += 1
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        allocationManager.onExecutorAdded(executorId)

        // If this is the last pending task, mark the scheduler queue as empty
        stageIdToTaskIndices.getOrElseUpdate(stageId, new mutable.HashSet[Int]) += taskIndex

        // Mark the executor on which this task is scheduled as busy
        executorIdToTaskIds.getOrElseUpdate(executorId, new mutable.HashSet[Long]) += taskId
        allocationManager.onExecutorBusy(executorId)

        totalPendingTasks() == 0
      }

      if (empty)
        inbox.put(WorkFinished)
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
      val executorId = taskEnd.taskInfo.executorId
      val taskId = taskEnd.taskInfo.taskId
      val taskIndex = taskEnd.taskInfo.index
      val stageId = taskEnd.stageId
      val newlyBacklogged = allocationManager.synchronized {
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
          stageIdToTaskIndices.get(stageId).foreach { _.remove(taskIndex) }

          totalPendingTasks() == 0
        } else {
          false
        }
      }

      if (newlyBacklogged)
        inbox.put(WorkAvailable)
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
      val executorId = executorAdded.executorId
      if (executorId != SparkContext.DRIVER_IDENTIFIER) {
        // This guards against the race condition in which the `SparkListenerTaskStart`
        // event is posted before the `SparkListenerBlockManagerAdded` event, which is
        // possible because these events are posted in different threads. (see SPARK-4951)
        allocationManager.onExecutorAdded(executorId)
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
    def totalRunningTasks(): Int = allocationManager.synchronized { numRunningTasks }

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

    private def registerGauge[T](name: String, value: => T, defaultValue: T): Unit = {
      metricRegistry.register(MetricRegistry.name("executors", name), new Gauge[T] {
        override def getValue: T = synchronized { Option(value).getOrElse(defaultValue) }
      })
    }

    registerGauge("numberExecutorsToAdd", numExecutorsToAdd, 0)
    registerGauge("numberExecutorsPendingToRemove", executorsPendingToRemove.size, 0)
    registerGauge("numberAllExecutors", executorIds.size, 0)
    registerGauge("numberTargetExecutors", numExecutorsTarget, 0)
    registerGauge("numberMaxNeededExecutors", maxNumExecutorsNeeded(), 0)
  }
}

private object ExecutorAllocationManager {
  val NOT_SET = Long.MaxValue
}
