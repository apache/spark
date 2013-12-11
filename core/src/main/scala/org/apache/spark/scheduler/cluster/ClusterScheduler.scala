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

package org.apache.spark.scheduler.cluster

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.{TimerTask, Timer}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.concurrent.duration._

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode

/**
 * The main TaskScheduler implementation, for running tasks on a cluster. Clients should first call
 * initialize() and start(), then submit task sets through the runTasks method.
 *
 * This class can work with multiple types of clusters by acting through a SchedulerBackend.
 * It handles common logic, like determining a scheduling order across jobs, waking up to launch
 * speculative tasks, etc.
 *
 * THREADING: SchedulerBackends and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * SchedulerBackends sycnchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.
 */
private[spark] class ClusterScheduler(val sc: SparkContext)
  extends TaskScheduler
  with Logging
{
  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = System.getProperty("spark.speculation.interval", "100").toLong

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = System.getProperty("spark.starvation.timeout", "15000").toLong

  // ClusterTaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.
  val activeTaskSets = new HashMap[String, ClusterTaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  private val executorsByHost = new HashMap[String, HashSet[String]]

  private val executorIdToHost = new HashMap[String, String]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  // default scheduler is FIFO
  val schedulingMode: SchedulingMode = SchedulingMode.withName(
    System.getProperty("spark.scheduler.mode", "FIFO"))

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  override def setDAGScheduler(dagScheduler: DAGScheduler) {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend) {
    this.backend = backend
    // temporarily set rootPool name to empty
    rootPool = new Pool("", schedulingMode, 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool)
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (System.getProperty("spark.speculation", "false").toBoolean) {
      logInfo("Starting speculative execution thread")
      import sc.env.actorSystem.dispatcher
      sc.env.actorSystem.scheduler.schedule(SPECULATION_INTERVAL milliseconds,
            SPECULATION_INTERVAL milliseconds) {
        checkSpeculatableTasks()
      }
    }
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = new ClusterTaskSetManager(this, taskSet)
      activeTaskSets(taskSet.id) = manager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
      taskSetTaskIds(taskSet.id) = new HashSet[Long]()

      if (!hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient memory")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  override def cancelTasks(stageId: Int): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    activeTaskSets.find(_._2.stageId == stageId).foreach { case (_, tsm) =>
      // There are two possible cases here:
      // 1. The task set manager has been created and some tasks have been scheduled.
      //    In this case, send a kill signal to the executors to kill the task and then abort
      //    the stage.
      // 2. The task set manager has been created but no tasks has been scheduled. In this case,
      //    simply abort the stage.
      val taskIds = taskSetTaskIds(tsm.taskSet.id)
      if (taskIds.size > 0) {
        taskIds.foreach { tid =>
          val execId = taskIdToExecutorId(tid)
          backend.killTask(tid, execId)
        }
      }
      logInfo("Stage %d was cancelled".format(stageId))
      tsm.removeAllRunningTasks()
      taskSetFinished(tsm)
    }
  }

  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    // Check to see if the given task set has been removed. This is possible in the case of
    // multiple unrecoverable task failures (e.g. if the entire task set is killed when it has
    // more than one running tasks).
    if (activeTaskSets.contains(manager.taskSet.id)) {
      activeTaskSets -= manager.taskSet.id
      manager.parent.removeSchedulable(manager)
      logInfo("Remove TaskSet %s from pool %s".format(manager.taskSet.id, manager.parent.name))
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskIdToExecutorId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds.remove(manager.taskSet.id)
    }
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = synchronized {
    SparkEnv.set(sc.env)

    // Mark each slave as alive and remember its hostname
    for (o <- offers) {
      executorIdToHost(o.executorId) = o.host
      if (!executorsByHost.contains(o.host)) {
        executorsByHost(o.host) = new HashSet[String]()
        executorGained(o.executorId, o.host)
      }
    }

    // Build a list of tasks to assign to each worker
    val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = offers.map(o => o.cores).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue()
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
    }

    // Take each TaskSet in our scheduling order, and then offer it each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    var launchedTask = false
    for (taskSet <- sortedTaskSets; maxLocality <- TaskLocality.values) {
      do {
        launchedTask = false
        for (i <- 0 until offers.size) {
          val execId = offers(i).executorId
          val host = offers(i).host
          for (task <- taskSet.resourceOffer(execId, host, availableCpus(i), maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetId(tid) = taskSet.taskSet.id
            taskSetTaskIds(taskSet.taskSet.id) += tid
            taskIdToExecutorId(tid) = execId
            activeExecutorIds += execId
            executorsByHost(host) += execId
            availableCpus(i) -= 1
            launchedTask = true
          }
        }
      } while (launchedTask)
    }

    if (tasks.size > 0) {
      hasLaunchedTask = true
    }
    return tasks
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var failedExecutor: Option[String] = None
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToExecutorId.remove(tid)
            }
            activeTaskSets.get(taskSetId).foreach { taskSet =>
              if (state == TaskState.FINISHED) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskSet.removeRunningTask(tid)
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor != None) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  def handleTaskGettingResult(taskSetManager: ClusterTaskSetManager, tid: Long) {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
    taskSetManager: ClusterTaskSetManager,
    tid: Long,
    taskResult: DirectTaskResult[_]) = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
    taskSetManager: ClusterTaskSetManager,
    tid: Long,
    taskState: TaskState,
    reason: Option[TaskEndReason]) = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (taskState != TaskState.KILLED) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  def error(message: String) {
    synchronized {
      if (activeTaskSets.size > 0) {
        // Have each task set throw a SparkException with the error
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.error(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }

    // sleeping for an arbitrary 5 seconds : to ensure that messages are sent out.
    // TODO: Do something better !
    Thread.sleep(5000L)
  }

  override def defaultParallelism() = backend.defaultParallelism()


  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks()
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  // Check for pending tasks in all our active jobs.
  def hasPendingTasks: Boolean = {
    synchronized {
      rootPool.hasPendingTasks()
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor != None) {
      dagScheduler.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
    }
    executorIdToHost -= executorId
    rootPool.executorLost(executorId, host)
  }

  def executorGained(execId: String, host: String) {
    dagScheduler.executorGained(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    executorsByHost.get(host).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    executorsByHost.contains(host)
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    activeExecutorIds.contains(execId)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None
}


object ClusterScheduler {
  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used.  The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given <h1, [o1, o2, o3]>, <h2, [o4]>, <h1, [o5, o6]>, returns
   * [o1, o5, o4, 02, o6, o3]
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
