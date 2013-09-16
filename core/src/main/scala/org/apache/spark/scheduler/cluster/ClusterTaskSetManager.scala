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
import java.util.{Arrays, NoSuchElementException}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.max
import scala.math.min

import org.apache.spark.{FetchFailed, Logging, Resubmitted, SparkEnv, Success, TaskEndReason, TaskState}
import org.apache.spark.{ExceptionFailure, SparkException, TaskResultTooBigFailure}
import org.apache.spark.TaskState.TaskState
import org.apache.spark.scheduler._
import scala.Some
import org.apache.spark.FetchFailed
import org.apache.spark.ExceptionFailure
import org.apache.spark.TaskResultTooBigFailure
import org.apache.spark.util.{SystemClock, Clock}


/**
 * Schedules the tasks within a single TaskSet in the ClusterScheduler. This class keeps track of
 * the status of each task, retries tasks if they fail (up to a limited number of times), and
 * handles locality-aware scheduling for this TaskSet via delay scheduling. The main interfaces
 * to it are resourceOffer, which asks the TaskSet whether it wants to run a task on one node,
 * and statusUpdate, which tells it that one of its tasks changed state (e.g. finished).
 *
 * THREADING: This class is designed to only be called from code with a lock on the
 * ClusterScheduler (e.g. its event handlers). It should not be called from other threads.
 */
private[spark] class ClusterTaskSetManager(
    sched: ClusterScheduler,
    val taskSet: TaskSet,
    clock: Clock = SystemClock)
  extends TaskSetManager
  with Logging
{
  // CPUs to request per task
  val CPUS_PER_TASK = System.getProperty("spark.task.cpus", "1").toInt

  // Maximum times a task is allowed to fail before failing the job
  val MAX_TASK_FAILURES = System.getProperty("spark.task.maxFailures", "4").toInt

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = System.getProperty("spark.speculation.quantile", "0.75").toDouble
  val SPECULATION_MULTIPLIER = System.getProperty("spark.speculation.multiplier", "1.5").toDouble

  // Serializer for closures and tasks.
  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksFinished = 0

  var weight = 1
  var minShare = 0
  var runningTasks = 0
  var priority = taskSet.priority
  var stageId = taskSet.stageId
  var name = "TaskSet_"+taskSet.stageId.toString
  var parent: Schedulable = null

  // Set of pending tasks for each executor. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  private val pendingTasksForExecutor = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each host. Similar to pendingTasksForExecutor,
  // but at host level.
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // Set of pending tasks for each rack -- similar to the above.
  private val pendingTasksForRack = new HashMap[String, ArrayBuffer[Int]]

  // Set containing pending tasks with no locality preferences.
  val pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // Set containing all pending tasks (also used as a stack, as above).
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  val taskInfos = new HashMap[Long, TaskInfo]

  // Did the TaskSet fail?
  var failed = false
  var causeOfFailure = ""

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    System.getProperty("spark.logging.exceptionPrintInterval", "10000").toLong

  // Map of recent exceptions (identified by string representation and top stack frame) to
  // duplicate count (how many times the same exception has appeared) and time the full exception
  // was printed. This should ideally be an LRU map that can drop old exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker epoch and set it on all tasks
  val epoch = sched.mapOutputTracker.getEpoch
  logDebug("Epoch for " + taskSet + ": " + epoch)
  for (t <- tasks) {
    t.epoch = epoch
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Figure out which locality levels we have in our TaskSet, so we can do delay scheduling
  val myLocalityLevels = computeValidLocalityLevels()
  val localityWaits = myLocalityLevels.map(getLocalityWait) // Time to wait at each level

  // Delay scheduling variables: we keep track of our current locality level and the time we
  // last launched a task at that level, and move up a level when localityWaits[curLevel] expires.
  // We then move down if we manage to launch a "more local" task.
  var currentLocalityIndex = 0    // Index of our current locality level in validLocalityLevels
  var lastLaunchTime = clock.getTime()  // Time we last launched a task at this level

  /**
   * Add a task to all the pending-task lists that it should be on. If readding is set, we are
   * re-adding the task so only include it in each list if it's not already there.
   */
  private def addPendingTask(index: Int, readding: Boolean = false) {
    // Utility method that adds `index` to a list only if readding=false or it's not already there
    def addTo(list: ArrayBuffer[Int]) {
      if (!readding || !list.contains(index)) {
        list += index
      }
    }

    var hadAliveLocations = false
    for (loc <- tasks(index).preferredLocations) {
      for (execId <- loc.executorId) {
        if (sched.isExecutorAlive(execId)) {
          addTo(pendingTasksForExecutor.getOrElseUpdate(execId, new ArrayBuffer))
          hadAliveLocations = true
        }
      }
      if (sched.hasExecutorsAliveOnHost(loc.host)) {
        addTo(pendingTasksForHost.getOrElseUpdate(loc.host, new ArrayBuffer))
        for (rack <- sched.getRackForHost(loc.host)) {
          addTo(pendingTasksForRack.getOrElseUpdate(rack, new ArrayBuffer))
        }
        hadAliveLocations = true
      }
    }

    if (!hadAliveLocations) {
      // Even though the task might've had preferred locations, all of those hosts or executors
      // are dead; put it in the no-prefs list so we can schedule it elsewhere right away.
      addTo(pendingTasksWithNoPrefs)
    }

    if (!readding) {
      allPendingTasks += index  // No point scanning this whole list to find the old task there
    }
  }

  /**
   * Return the pending tasks list for a given executor ID, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForExecutor(executorId: String): ArrayBuffer[Int] = {
    pendingTasksForExecutor.getOrElse(executorId, ArrayBuffer())
  }

  /**
   * Return the pending tasks list for a given host, or an empty list if
   * there is no map entry for that host
   */
  private def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  /**
   * Return the pending rack-local task list for a given rack, or an empty list if
   * there is no map entry for that rack
   */
  private def getPendingTasksForRack(rack: String): ArrayBuffer[Int] = {
    pendingTasksForRack.getOrElse(rack, ArrayBuffer())
  }

  /**
   * Dequeue a pending task from the given list and return its index.
   * Return None if the list is empty.
   * This method also cleans up any tasks in the list that have already
   * been launched, since we want that to happen lazily.
   */
  private def findTaskFromList(list: ArrayBuffer[Int]): Option[Int] = {
    while (!list.isEmpty) {
      val index = list.last
      list.trimEnd(1)
      if (copiesRunning(index) == 0 && !finished(index)) {
        return Some(index)
      }
    }
    return None
  }

  /** Check whether a task is currently running an attempt on a given host */
  private def hasAttemptOnHost(taskIndex: Int, host: String): Boolean = {
    !taskAttempts(taskIndex).exists(_.host == host)
  }

  /**
   * Return a speculative task for a given executor if any are available. The task should not have
   * an attempt running on this host, in case the host is slow. In addition, the task should meet
   * the given locality constraint.
   */
  private def findSpeculativeTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    speculatableTasks.retain(index => !finished(index)) // Remove finished tasks from set

    if (!speculatableTasks.isEmpty) {
      // Check for process-local or preference-less tasks; note that tasks can be process-local
      // on multiple nodes when we replicate cached blocks, as in Spark Streaming
      for (index <- speculatableTasks if !hasAttemptOnHost(index, host)) {
        val prefs = tasks(index).preferredLocations
        val executors = prefs.flatMap(_.executorId)
        if (prefs.size == 0 || executors.contains(execId)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.PROCESS_LOCAL))
        }
      }

      // Check for node-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
        for (index <- speculatableTasks if !hasAttemptOnHost(index, host)) {
          val locations = tasks(index).preferredLocations.map(_.host)
          if (locations.contains(host)) {
            speculatableTasks -= index
            return Some((index, TaskLocality.NODE_LOCAL))
          }
        }
      }

      // Check for rack-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        for (rack <- sched.getRackForHost(host)) {
          for (index <- speculatableTasks if !hasAttemptOnHost(index, host)) {
            val racks = tasks(index).preferredLocations.map(_.host).map(sched.getRackForHost)
            if (racks.contains(rack)) {
              speculatableTasks -= index
              return Some((index, TaskLocality.RACK_LOCAL))
            }
          }
        }
      }

      // Check for non-local tasks
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        for (index <- speculatableTasks if !hasAttemptOnHost(index, host)) {
          speculatableTasks -= index
          return Some((index, TaskLocality.ANY))
        }
      }
    }

    return None
  }

  /**
   * Dequeue a pending task for a given node and return its index and locality level.
   * Only search for tasks matching the given locality constraint.
   */
  private def findTask(execId: String, host: String, locality: TaskLocality.Value)
    : Option[(Int, TaskLocality.Value)] =
  {
    for (index <- findTaskFromList(getPendingTasksForExecutor(execId))) {
      return Some((index, TaskLocality.PROCESS_LOCAL))
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL)) {
      for (index <- findTaskFromList(getPendingTasksForHost(host))) {
        return Some((index, TaskLocality.NODE_LOCAL))
      }
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
      for {
        rack <- sched.getRackForHost(host)
        index <- findTaskFromList(getPendingTasksForRack(rack))
      } {
        return Some((index, TaskLocality.RACK_LOCAL))
      }
    }

    // Look for no-pref tasks after rack-local tasks since they can run anywhere.
    for (index <- findTaskFromList(pendingTasksWithNoPrefs)) {
      return Some((index, TaskLocality.PROCESS_LOCAL))
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
      for (index <- findTaskFromList(allPendingTasks)) {
        return Some((index, TaskLocality.ANY))
      }
    }

    // Finally, if all else has failed, find a speculative task
    return findSpeculativeTask(execId, host, locality)
  }

  /**
   * Respond to an offer of a single executor from the scheduler by finding a task
   */
  override def resourceOffer(
      execId: String,
      host: String,
      availableCpus: Int,
      maxLocality: TaskLocality.TaskLocality)
    : Option[TaskDescription] =
  {
    if (tasksFinished < numTasks && availableCpus >= CPUS_PER_TASK) {
      val curTime = clock.getTime()

      var allowedLocality = getAllowedLocalityLevel(curTime)
      if (allowedLocality > maxLocality) {
        allowedLocality = maxLocality   // We're not allowed to search for farther-away tasks
      }

      findTask(execId, host, allowedLocality) match {
        case Some((index, taskLocality)) => {
          // Found a task; do some bookkeeping and return a task description
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Figure out whether this should count as a preferred launch
          logInfo("Starting task %s:%d as TID %s on executor %s: %s (%s)".format(
            taskSet.id, index, taskId, execId, host, taskLocality))
          // Do various bookkeeping
          copiesRunning(index) += 1
          val info = new TaskInfo(taskId, index, curTime, execId, host, taskLocality)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          // Update our locality level for delay scheduling
          currentLocalityIndex = getLocalityIndex(taskLocality)
          lastLaunchTime = curTime
          // Serialize and return the task
          val startTime = clock.getTime()
          // We rely on the DAGScheduler to catch non-serializable closures and RDDs, so in here
          // we assume the task can be serialized without exceptions.
          val serializedTask = Task.serializeWithDependencies(
            task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          val timeTaken = clock.getTime() - startTime
          increaseRunningTasks(1)
          logInfo("Serialized task %s:%d as %d bytes in %d ms".format(
            taskSet.id, index, serializedTask.limit, timeTaken))
          val taskName = "task %s:%d".format(taskSet.id, index)
          if (taskAttempts(index).size == 1)
            taskStarted(task,info)
          return Some(new TaskDescription(taskId, execId, taskName, index, serializedTask))
        }
        case _ =>
      }
    }
    return None
  }

  /**
   * Get the level we can launch tasks according to delay scheduling, based on current wait time.
   */
  private def getAllowedLocalityLevel(curTime: Long): TaskLocality.TaskLocality = {
    while (curTime - lastLaunchTime >= localityWaits(currentLocalityIndex) &&
        currentLocalityIndex < myLocalityLevels.length - 1)
    {
      // Jump to the next locality level, and remove our waiting time for the current one since
      // we don't want to count it again on the next one
      lastLaunchTime += localityWaits(currentLocalityIndex)
      currentLocalityIndex += 1
    }
    myLocalityLevels(currentLocalityIndex)
  }

  /**
   * Find the index in myLocalityLevels for a given locality. This is also designed to work with
   * localities that are not in myLocalityLevels (in case we somehow get those) by returning the
   * next-biggest level we have. Uses the fact that the last value in myLocalityLevels is ANY.
   */
  def getLocalityIndex(locality: TaskLocality.TaskLocality): Int = {
    var index = 0
    while (locality > myLocalityLevels(index)) {
      index += 1
    }
    index
  }

  /** Called by cluster scheduler when one of our tasks changes state */
  override def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    SparkEnv.set(env)
    state match {
      case TaskState.FINISHED =>
        taskFinished(tid, state, serializedData)
      case TaskState.LOST =>
        taskLost(tid, state, serializedData)
      case TaskState.FAILED =>
        taskLost(tid, state, serializedData)
      case TaskState.KILLED =>
        taskLost(tid, state, serializedData)
      case _ =>
    }
  }

  def taskStarted(task: Task[_], info: TaskInfo) {
    sched.listener.taskStarted(task, info)
  }

  def taskFinished(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    if (info.failed) {
      // We might get two task-lost messages for the same task in coarse-grained Mesos mode,
      // or even from Mesos itself when acks get delayed.
      return
    }
    val index = info.index
    info.markSuccessful()
    decreaseRunningTasks(1)
    if (!finished(index)) {
      tasksFinished += 1
      logInfo("Finished TID %s in %d ms on %s (progress: %d/%d)".format(
        tid, info.duration, info.host, tasksFinished, numTasks))
      // Deserialize task result and pass it to the scheduler
      try {
        val result = ser.deserialize[TaskResult[_]](serializedData)
        result.metrics.resultSize = serializedData.limit()
        sched.listener.taskEnded(
          tasks(index), Success, result.value, result.accumUpdates, info, result.metrics)
      } catch {
        case cnf: ClassNotFoundException =>
          val loader = Thread.currentThread().getContextClassLoader
          throw new SparkException("ClassNotFound with classloader: " + loader, cnf)
        case ex => throw ex
      }
      // Mark finished and stop if we've finished all the tasks
      finished(index) = true
      if (tasksFinished == numTasks) {
        sched.taskSetFinished(this)
      }
    } else {
      logInfo("Ignoring task-finished event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def taskLost(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    if (info.failed) {
      // We might get two task-lost messages for the same task in coarse-grained Mesos mode,
      // or even from Mesos itself when acks get delayed.
      return
    }
    val index = info.index
    info.markFailed()
    decreaseRunningTasks(1)
    if (!finished(index)) {
      logInfo("Lost TID %s (task %s:%d)".format(tid, taskSet.id, index))
      copiesRunning(index) -= 1
      // Check if the problem is a map output fetch failure. In that case, this
      // task will never succeed on any node, so tell the scheduler about it.
      if (serializedData != null && serializedData.limit() > 0) {
        val reason = ser.deserialize[TaskEndReason](serializedData, getClass.getClassLoader)
        reason match {
          case fetchFailed: FetchFailed =>
            logInfo("Loss was due to fetch failure from " + fetchFailed.bmAddress)
            sched.listener.taskEnded(tasks(index), fetchFailed, null, null, info, null)
            finished(index) = true
            tasksFinished += 1
            sched.taskSetFinished(this)
            decreaseRunningTasks(runningTasks)
            return

          case taskResultTooBig: TaskResultTooBigFailure =>
            logInfo("Loss was due to task %s result exceeding Akka frame size; aborting job".format(
              tid))
            abort("Task %s result exceeded Akka frame size".format(tid))
            return

          case ef: ExceptionFailure =>
            sched.listener.taskEnded(tasks(index), ef, null, null, info, ef.metrics.getOrElse(null))
            val key = ef.description
            val now = clock.getTime()
            val (printFull, dupCount) = {
              if (recentExceptions.contains(key)) {
                val (dupCount, printTime) = recentExceptions(key)
                if (now - printTime > EXCEPTION_PRINT_INTERVAL) {
                  recentExceptions(key) = (0, now)
                  (true, 0)
                } else {
                  recentExceptions(key) = (dupCount + 1, printTime)
                  (false, dupCount + 1)
                }
              } else {
                recentExceptions(key) = (0, now)
                (true, 0)
              }
            }
            if (printFull) {
              val locs = ef.stackTrace.map(loc => "\tat %s".format(loc.toString))
              logInfo("Loss was due to %s\n%s\n%s".format(
                ef.className, ef.description, locs.mkString("\n")))
            } else {
              logInfo("Loss was due to %s [duplicate %d]".format(ef.description, dupCount))
            }

          case _ => {}
        }
      }
      // On non-fetch failures, re-enqueue the task as pending for a max number of retries
      addPendingTask(index)
      // Count failed attempts only on FAILED and LOST state (not on KILLED)
      if (state == TaskState.FAILED || state == TaskState.LOST) {
        numFailures(index) += 1
        if (numFailures(index) > MAX_TASK_FAILURES) {
          logError("Task %s:%d failed more than %d times; aborting job".format(
            taskSet.id, index, MAX_TASK_FAILURES))
          abort("Task %s:%d failed more than %d times".format(taskSet.id, index, MAX_TASK_FAILURES))
        }
      }
    } else {
      logInfo("Ignoring task-lost event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  override def error(message: String) {
    // Save the error message
    abort("Error: " + message)
  }

  def abort(message: String) {
    failed = true
    causeOfFailure = message
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.listener.taskSetFailed(taskSet, message)
    decreaseRunningTasks(runningTasks)
    sched.taskSetFinished(this)
  }

  override def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  override def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  override def getSchedulableByName(name: String): Schedulable = {
    return null
  }

  override def addSchedulable(schedulable: Schedulable) {}

  override def removeSchedulable(schedulable: Schedulable) {}

  override def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = ArrayBuffer[TaskSetManager](this)
    sortedTaskSetQueue += this
    return sortedTaskSetQueue
  }

  /** Called by cluster scheduler when an executor is lost so we can re-enqueue our tasks */
  override def executorLost(execId: String, host: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // Re-enqueue pending tasks for this host based on the status of the cluster -- for example, a
    // task that used to have locations on only this host might now go to the no-prefs list. Note
    // that it's okay if we add a task to the same queue twice (if it had multiple preferred
    // locations), because findTaskFromList will skip already-running tasks.
    for (index <- getPendingTasksForExecutor(execId)) {
      addPendingTask(index, readding=true)
    }
    for (index <- getPendingTasksForHost(host)) {
      addPendingTask(index, readding=true)
    }

    // Re-enqueue any tasks that ran on the failed executor if this is a shuffle map stage
    if (tasks(0).isInstanceOf[ShuffleMapTask]) {
      for ((tid, info) <- taskInfos if info.executorId == execId) {
        val index = taskInfos(tid).index
        if (finished(index)) {
          finished(index) = false
          copiesRunning(index) -= 1
          tasksFinished -= 1
          addPendingTask(index)
          // Tell the DAGScheduler that this task was resubmitted so that it doesn't think our
          // stage finishes when a total of tasks.size tasks finish.
          sched.listener.taskEnded(tasks(index), Resubmitted, null, null, info, null)
        }
      }
    }
    // Also re-enqueue any tasks that were running on the node
    for ((tid, info) <- taskInfos if info.running && info.executorId == execId) {
      taskLost(tid, TaskState.KILLED, null)
    }
  }

  /**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the ClusterScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, or if all tasks have finished.
    if (numTasks == 1 || tasksFinished == numTasks) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksFinished >= minFinishedForSpeculation) {
      val time = clock.getTime()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      val medianDuration = durations(min((0.5 * numTasks).round.toInt, durations.size - 1))
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, 100)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        if (!finished(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %s:%d (on %s) as speculatable because it ran more than %.0f ms".format(
              taskSet.id, index, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    return foundTasks
  }

  override def hasPendingTasks(): Boolean = {
    numTasks > 0 && tasksFinished < numTasks
  }

  private def getLocalityWait(level: TaskLocality.TaskLocality): Long = {
    val defaultWait = System.getProperty("spark.locality.wait", "3000")
    level match {
      case TaskLocality.PROCESS_LOCAL =>
        System.getProperty("spark.locality.wait.process", defaultWait).toLong
      case TaskLocality.NODE_LOCAL =>
        System.getProperty("spark.locality.wait.node", defaultWait).toLong
      case TaskLocality.RACK_LOCAL =>
        System.getProperty("spark.locality.wait.rack", defaultWait).toLong
      case TaskLocality.ANY =>
        0L
    }
  }

  /**
   * Compute the locality levels used in this TaskSet. Assumes that all tasks have already been
   * added to queues using addPendingTask.
   */
  private def computeValidLocalityLevels(): Array[TaskLocality.TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (!pendingTasksForExecutor.isEmpty && getLocalityWait(PROCESS_LOCAL) != 0) {
      levels += PROCESS_LOCAL
    }
    if (!pendingTasksForHost.isEmpty && getLocalityWait(NODE_LOCAL) != 0) {
      levels += NODE_LOCAL
    }
    if (!pendingTasksForRack.isEmpty && getLocalityWait(RACK_LOCAL) != 0) {
      levels += RACK_LOCAL
    }
    levels += ANY
    logDebug("Valid locality levels for " + taskSet + ": " + levels.mkString(", "))
    levels.toArray
  }
}
