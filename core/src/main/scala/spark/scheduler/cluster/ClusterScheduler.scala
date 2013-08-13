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

package spark.scheduler.cluster

import java.lang.{Boolean => JBoolean}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark._
import spark.TaskState.TaskState
import spark.scheduler._
import spark.scheduler.cluster.SchedulingMode.SchedulingMode
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.{TimerTask, Timer}

/**
 * The main TaskScheduler implementation, for running tasks on a cluster. Clients should first call
 * start(), then submit task sets through the runTasks method.
 */
private[spark] class ClusterScheduler(val sc: SparkContext)
  extends TaskScheduler
  with Logging
{
  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = System.getProperty("spark.speculation.interval", "100").toLong

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = System.getProperty("spark.starvation.timeout", "15000").toLong

  val activeTaskSets = new HashMap[String, TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing Mesos task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  private val executorsByHost = new HashMap[String, HashSet[String]]

  private val executorIdToHost = new HashMap[String, String]

  // JAR server, if any JARs were added by the user to the SparkContext
  var jarServer: HttpServer = null

  // URIs of JARs to pass to executor
  var jarUris: String = ""

  // Listener object to pass upcalls into
  var listener: TaskSchedulerListener = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  // default scheduler is FIFO
  val schedulingMode: SchedulingMode = SchedulingMode.withName(
    System.getProperty("spark.cluster.schedulingmode", "FIFO"))

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  def initialize(context: SchedulerBackend) {
    backend = context
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
      new Thread("ClusterScheduler speculation check") {
        setDaemon(true)

        override def run() {
          logInfo("Starting speculative execution thread")
          while (true) {
            try {
              Thread.sleep(SPECULATION_INTERVAL)
            } catch {
              case e: InterruptedException => {}
            }
            checkSpeculatableTasks()
          }
        }
      }.start()
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

  def taskSetFinished(manager: TaskSetManager) {
    this.synchronized {
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

    // Build a list of tasks to assign to each slave
    val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
    val availableCpus = offers.map(o => o.cores).toArray
    val sortedTaskSetQueue = rootPool.getSortedTaskSetQueue()
    for (manager <- sortedTaskSetQueue) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        manager.parent.name, manager.name, manager.runningTasks))
    }

    var launchedTask = false
    for (manager <- sortedTaskSetQueue; offer <- offers) {
      do {
        launchedTask = false
        for (i <- 0 until offers.size) {
          val execId = offers(i).executorId
          val host = offers(i).host
          for (task <- manager.resourceOffer(execId, host, availableCpus(i))) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetId(tid) = manager.taskSet.id
            taskSetTaskIds(manager.taskSet.id) += tid
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
    var taskSetToUpdate: Option[TaskSetManager] = None
    var failedExecutor: Option[String] = None
    var taskFailed = false
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
            if (activeTaskSets.contains(taskSetId)) {
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToExecutorId.remove(tid)
            }
            if (state == TaskState.FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, since that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(tid, state, serializedData)
    }
    if (failedExecutor != None) {
      listener.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
    if (taskFailed) {
      // Also revive offers if a task had failed for some reason other than host lost
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
    if (jarServer != null) {
      jarServer.stop()
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
    // Call listener.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor != None) {
      listener.executorLost(failedExecutor.get)
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
    listener.executorGained(execId, host)
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

  // Used to 'spray' available containers across the available set to ensure too many containers on same host
  // are not used up. Used in yarn mode and in task scheduling (when there are multiple containers available
  // to execute a task)
  // For example: yarn can returns more containers than we would have requested under ANY, this method
  // prioritizes how to use the allocated containers.
  // flatten the map such that the array buffer entries are spread out across the returned value.
  // given <host, list[container]> == <h1, [c1 .. c5]>, <h2, [c1 .. c3]>, <h3, [c1, c2]>, <h4, c1>, <h5, c1>, i
  // the return value would be something like : h1c1, h2c1, h3c1, h4c1, h5c1, h1c2, h2c2, h3c2, h1c3, h2c3, h1c4, h1c5
  // We then 'use' the containers in this order (consuming only the top K from this list where
  // K = number to be user). This is to ensure that if we have multiple eligible allocations,
  // they dont end up allocating all containers on a small number of hosts - increasing probability of
  // multiple container failure when a host goes down.
  // Note, there is bias for keys with higher number of entries in value to be picked first (by design)
  // Also note that invocation of this method is expected to have containers of same 'type'
  // (host-local, rack-local, off-rack) and not across types : so that reordering is simply better from
  // the available list - everything else being same.
  // That is, we we first consume data local, then rack local and finally off rack nodes. So the
  // prioritization from this method applies to within each category
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      // TODO(matei): not sure why we're using getOrElse if keyList = map.keys... see if it matters
      (left, right) => map.get(left).getOrElse(Set()).size > map.get(right).getOrElse(Set()).size
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
