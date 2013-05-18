package spark.scheduler.cluster

import java.util.{HashMap => JHashMap, NoSuchElementException, Arrays}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.math.max
import scala.math.min

import spark._
import spark.scheduler._
import spark.TaskState.TaskState
import java.nio.ByteBuffer

private[spark] object TaskLocality extends Enumeration("PROCESS_LOCAL", "NODE_LOCAL", "RACK_LOCAL", "ANY") with Logging {

  // process local is expected to be used ONLY within tasksetmanager for now.
  val PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY = Value

  type TaskLocality = Value

  def isAllowed(constraint: TaskLocality, condition: TaskLocality): Boolean = {

    // Must not be the constraint.
    assert (constraint != TaskLocality.PROCESS_LOCAL)

    constraint match {
      case TaskLocality.NODE_LOCAL => condition == TaskLocality.NODE_LOCAL
      case TaskLocality.RACK_LOCAL => condition == TaskLocality.NODE_LOCAL || condition == TaskLocality.RACK_LOCAL
      // For anything else, allow
      case _ => true
    }
  }

  def parse(str: String): TaskLocality = {
    // better way to do this ?
    try {
      val retval = TaskLocality.withName(str)
      // Must not specify PROCESS_LOCAL !
      assert (retval != TaskLocality.PROCESS_LOCAL)

      retval
    } catch {
      case nEx: NoSuchElementException => {
        logWarning("Invalid task locality specified '" + str + "', defaulting to NODE_LOCAL");
        // default to preserve earlier behavior
        NODE_LOCAL
      }
    }
  }
}

/**
 * Schedules the tasks within a single TaskSet in the ClusterScheduler.
 */
private[spark] class TaskSetManager(sched: ClusterScheduler, val taskSet: TaskSet) extends Logging {

  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "3000").toLong

  // CPUs to request per task
  val CPUS_PER_TASK = System.getProperty("spark.task.cpus", "1").toDouble

  // Maximum times a task is allowed to fail before failing the job
  val MAX_TASK_FAILURES = 4

  // Quantile of tasks at which to start speculation
  val SPECULATION_QUANTILE = System.getProperty("spark.speculation.quantile", "0.75").toDouble
  val SPECULATION_MULTIPLIER = System.getProperty("spark.speculation.multiplier", "1.5").toDouble

  // Serializer for closures and tasks.
  val ser = SparkEnv.get.closureSerializer.newInstance()

  val priority = taskSet.priority
  val tasks = taskSet.tasks
  val numTasks = tasks.length
  val copiesRunning = new Array[Int](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val taskAttempts = Array.fill[List[TaskInfo]](numTasks)(Nil)
  var tasksFinished = 0

  // Last time when we launched a preferred task (for delay scheduling)
  var lastPreferredLaunchTime = System.currentTimeMillis

  // List of pending tasks for each node (process local to container). These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  private val pendingTasksForHostPort = new HashMap[String, ArrayBuffer[Int]]

  // List of pending tasks for each node.
  // Essentially, similar to pendingTasksForHostPort, except at host level
  private val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // List of pending tasks for each node based on rack locality.
  // Essentially, similar to pendingTasksForHost, except at rack level
  private val pendingRackLocalTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // List containing pending tasks with no locality preferences
  val pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // List containing all pending tasks (also used as a stack, as above)
  val allPendingTasks = new ArrayBuffer[Int]

  // Tasks that can be speculated. Since these will be a small fraction of total
  // tasks, we'll just hold them in a HashSet.
  val speculatableTasks = new HashSet[Int]

  // Task index, start and finish time for each task attempt (indexed by task ID)
  val taskInfos = new HashMap[Long, TaskInfo]

  // Did the job fail?
  var failed = false
  var causeOfFailure = ""

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    System.getProperty("spark.logging.exceptionPrintInterval", "10000").toLong
  // Map of recent exceptions (identified by string representation and
  // top stack frame) to duplicate count (how many times the same
  // exception has appeared) and time the full exception was
  // printed. This should ideally be an LRU map that can drop old
  // exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

  // Figure out the current map output tracker generation and set it on all tasks
  val generation = sched.mapOutputTracker.getGeneration
  logDebug("Generation for " + taskSet.id + ": " + generation)
  for (t <- tasks) {
    t.generation = generation
  }

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Note that it follows the hierarchy.
  // if we search for NODE_LOCAL, the output will include PROCESS_LOCAL and
  // if we search for RACK_LOCAL, it will include PROCESS_LOCAL & NODE_LOCAL
  private def findPreferredLocations(_taskPreferredLocations: Seq[String], scheduler: ClusterScheduler,
                                     taskLocality: TaskLocality.TaskLocality): HashSet[String] = {

    if (TaskLocality.PROCESS_LOCAL == taskLocality) {
      // straight forward comparison ! Special case it.
      val retval = new HashSet[String]()
      scheduler.synchronized {
        for (location <- _taskPreferredLocations) {
          if (scheduler.isExecutorAliveOnHostPort(location)) {
            retval += location
          }
        }
      }

      return retval
    }

    val taskPreferredLocations =
      if (TaskLocality.NODE_LOCAL == taskLocality) {
        _taskPreferredLocations
      } else {
        assert (TaskLocality.RACK_LOCAL == taskLocality)
        // Expand set to include all 'seen' rack local hosts.
        // This works since container allocation/management happens within master - so any rack locality information is updated in msater.
        // Best case effort, and maybe sort of kludge for now ... rework it later ?
        val hosts = new HashSet[String]
        _taskPreferredLocations.foreach(h => {
          val rackOpt = scheduler.getRackForHost(h)
          if (rackOpt.isDefined) {
            val hostsOpt = scheduler.getCachedHostsForRack(rackOpt.get)
            if (hostsOpt.isDefined) {
              hosts ++= hostsOpt.get
            }
          }

          // Ensure that irrespective of what scheduler says, host is always added !
          hosts += h
        })

        hosts
      }

    val retval = new HashSet[String]
    scheduler.synchronized {
      for (prefLocation <- taskPreferredLocations) {
        val aliveLocationsOpt = scheduler.getExecutorsAliveOnHost(Utils.parseHostPort(prefLocation)._1)
        if (aliveLocationsOpt.isDefined) {
          retval ++= aliveLocationsOpt.get
        }
      }
    }

    retval
  }

  // Add a task to all the pending-task lists that it should be on.
  private def addPendingTask(index: Int) {
    // We can infer hostLocalLocations from rackLocalLocations by joining it against tasks(index).preferredLocations (with appropriate
    // hostPort <-> host conversion). But not doing it for simplicity sake. If this becomes a performance issue, modify it.
    val processLocalLocations = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.PROCESS_LOCAL)
    val hostLocalLocations = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.NODE_LOCAL)
    val rackLocalLocations = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.RACK_LOCAL)

    if (rackLocalLocations.size == 0) {
      // Current impl ensures this.
      assert (processLocalLocations.size == 0)
      assert (hostLocalLocations.size == 0)
      pendingTasksWithNoPrefs += index
    } else {

      // process local locality
      for (hostPort <- processLocalLocations) {
        // DEBUG Code
        Utils.checkHostPort(hostPort)

        val hostPortList = pendingTasksForHostPort.getOrElseUpdate(hostPort, ArrayBuffer())
        hostPortList += index
      }

      // host locality (includes process local)
      for (hostPort <- hostLocalLocations) {
        // DEBUG Code
        Utils.checkHostPort(hostPort)

        val host = Utils.parseHostPort(hostPort)._1
        val hostList = pendingTasksForHost.getOrElseUpdate(host, ArrayBuffer())
        hostList += index
      }

      // rack locality (includes process local and host local)
      for (rackLocalHostPort <- rackLocalLocations) {
        // DEBUG Code
        Utils.checkHostPort(rackLocalHostPort)

        val rackLocalHost = Utils.parseHostPort(rackLocalHostPort)._1
        val list = pendingRackLocalTasksForHost.getOrElseUpdate(rackLocalHost, ArrayBuffer())
        list += index
      }
    }

    allPendingTasks += index
  }

  // Return the pending tasks list for a given host port (process local), or an empty list if
  // there is no map entry for that host
  private def getPendingTasksForHostPort(hostPort: String): ArrayBuffer[Int] = {
    // DEBUG Code
    Utils.checkHostPort(hostPort)
    pendingTasksForHostPort.getOrElse(hostPort, ArrayBuffer())
  }

  // Return the pending tasks list for a given host, or an empty list if
  // there is no map entry for that host
  private def getPendingTasksForHost(hostPort: String): ArrayBuffer[Int] = {
    val host = Utils.parseHostPort(hostPort)._1
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  // Return the pending tasks (rack level) list for a given host, or an empty list if
  // there is no map entry for that host
  private def getRackLocalPendingTasksForHost(hostPort: String): ArrayBuffer[Int] = {
    val host = Utils.parseHostPort(hostPort)._1
    pendingRackLocalTasksForHost.getOrElse(host, ArrayBuffer())
  }

  // Number of pending tasks for a given host Port (which would be process local)
  def numPendingTasksForHostPort(hostPort: String): Int = {
    getPendingTasksForHostPort(hostPort).count( index => copiesRunning(index) == 0 && !finished(index) )
  }

  // Number of pending tasks for a given host (which would be data local)
  def numPendingTasksForHost(hostPort: String): Int = {
    getPendingTasksForHost(hostPort).count( index => copiesRunning(index) == 0 && !finished(index) )
  }

  // Number of pending rack local tasks for a given host
  def numRackLocalPendingTasksForHost(hostPort: String): Int = {
    getRackLocalPendingTasksForHost(hostPort).count( index => copiesRunning(index) == 0 && !finished(index) )
  }


  // Dequeue a pending task from the given list and return its index.
  // Return None if the list is empty.
  // This method also cleans up any tasks in the list that have already
  // been launched, since we want that to happen lazily.
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

  // Return a speculative task for a given host if any are available. The task should not have an
  // attempt running on this host, in case the host is slow. In addition, if locality is set, the
  // task must have a preference for this host/rack/no preferred locations at all.
  private def findSpeculativeTask(hostPort: String, locality: TaskLocality.TaskLocality): Option[Int] = {

    assert (TaskLocality.isAllowed(locality, TaskLocality.NODE_LOCAL))
    speculatableTasks.retain(index => !finished(index)) // Remove finished tasks from set

    if (speculatableTasks.size > 0) {
      val localTask = speculatableTasks.find {
          index =>
            val locations = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.NODE_LOCAL)
            val attemptLocs = taskAttempts(index).map(_.hostPort)
            (locations.size == 0 || locations.contains(hostPort)) && !attemptLocs.contains(hostPort)
        }

      if (localTask != None) {
        speculatableTasks -= localTask.get
        return localTask
      }

      // check for rack locality
      if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
        val rackTask = speculatableTasks.find {
          index =>
            val locations = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.RACK_LOCAL)
            val attemptLocs = taskAttempts(index).map(_.hostPort)
            locations.contains(hostPort) && !attemptLocs.contains(hostPort)
        }

        if (rackTask != None) {
          speculatableTasks -= rackTask.get
          return rackTask
        }
      }

      // Any task ...
      if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
        // Check for attemptLocs also ?
        val nonLocalTask = speculatableTasks.find(i => !taskAttempts(i).map(_.hostPort).contains(hostPort))
        if (nonLocalTask != None) {
          speculatableTasks -= nonLocalTask.get
          return nonLocalTask
        }
      }
    }
    return None
  }

  // Dequeue a pending task for a given node and return its index.
  // If localOnly is set to false, allow non-local tasks as well.
  private def findTask(hostPort: String, locality: TaskLocality.TaskLocality): Option[Int] = {
    val processLocalTask = findTaskFromList(getPendingTasksForHostPort(hostPort))
    if (processLocalTask != None) {
      return processLocalTask
    }

    val localTask = findTaskFromList(getPendingTasksForHost(hostPort))
    if (localTask != None) {
      return localTask
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.RACK_LOCAL)) {
      val rackLocalTask = findTaskFromList(getRackLocalPendingTasksForHost(hostPort))
      if (rackLocalTask != None) {
        return rackLocalTask
      }
    }

    // Look for no pref tasks AFTER rack local tasks - this has side effect that we will get to failed tasks later rather than sooner.
    // TODO: That code path needs to be revisited (adding to no prefs list when host:port goes down).
    val noPrefTask = findTaskFromList(pendingTasksWithNoPrefs)
    if (noPrefTask != None) {
      return noPrefTask
    }

    if (TaskLocality.isAllowed(locality, TaskLocality.ANY)) {
      val nonLocalTask = findTaskFromList(allPendingTasks)
      if (nonLocalTask != None) {
        return nonLocalTask
      }
    }

    // Finally, if all else has failed, find a speculative task
    return findSpeculativeTask(hostPort, locality)
  }

  private def isProcessLocalLocation(task: Task[_], hostPort: String): Boolean = {
    Utils.checkHostPort(hostPort)

    val locs = task.preferredLocations

    locs.contains(hostPort)
  }

  private def isHostLocalLocation(task: Task[_], hostPort: String): Boolean = {
    val locs = task.preferredLocations

    // If no preference, consider it as host local
    if (locs.isEmpty) return true

    val host = Utils.parseHostPort(hostPort)._1
    locs.find(h => Utils.parseHostPort(h)._1 == host).isDefined
  }

  // Does a host count as a rack local preferred location for a task? (assumes host is NOT preferred location).
  // This is true if either the task has preferred locations and this host is one, or it has
  // no preferred locations (in which we still count the launch as preferred).
  private def isRackLocalLocation(task: Task[_], hostPort: String): Boolean = {

    val locs = task.preferredLocations

    val preferredRacks = new HashSet[String]()
    for (preferredHost <- locs) {
      val rack = sched.getRackForHost(preferredHost)
      if (None != rack) preferredRacks += rack.get
    }

    if (preferredRacks.isEmpty) return false

    val hostRack = sched.getRackForHost(hostPort)

    return None != hostRack && preferredRacks.contains(hostRack.get)
  }

  // Respond to an offer of a single slave from the scheduler by finding a task
  def slaveOffer(execId: String, hostPort: String, availableCpus: Double, overrideLocality: TaskLocality.TaskLocality = null): Option[TaskDescription] = {

    if (tasksFinished < numTasks && availableCpus >= CPUS_PER_TASK) {
      // If explicitly specified, use that
      val locality = if (overrideLocality != null) overrideLocality else {
        // expand only if we have waited for more than LOCALITY_WAIT for a host local task ...
        val time = System.currentTimeMillis
        if (time - lastPreferredLaunchTime < LOCALITY_WAIT) TaskLocality.NODE_LOCAL else TaskLocality.ANY
      }

      findTask(hostPort, locality) match {
        case Some(index) => {
          // Found a task; do some bookkeeping and return a Mesos task for it
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Figure out whether this should count as a preferred launch
          val taskLocality =
            if (isProcessLocalLocation(task, hostPort)) TaskLocality.PROCESS_LOCAL else
            if (isHostLocalLocation(task, hostPort)) TaskLocality.NODE_LOCAL else
            if (isRackLocalLocation(task, hostPort)) TaskLocality.RACK_LOCAL else
              TaskLocality.ANY
          val prefStr = taskLocality.toString
          logInfo("Starting task %s:%d as TID %s on slave %s: %s (%s)".format(
            taskSet.id, index, taskId, execId, hostPort, prefStr))
          // Do various bookkeeping
          copiesRunning(index) += 1
          val time = System.currentTimeMillis
          val info = new TaskInfo(taskId, index, time, execId, hostPort, taskLocality)
          taskInfos(taskId) = info
          taskAttempts(index) = info :: taskAttempts(index)
          if (TaskLocality.NODE_LOCAL == taskLocality) {
            lastPreferredLaunchTime = time
          }
          // Serialize and return the task
          val startTime = System.currentTimeMillis
          val serializedTask = Task.serializeWithDependencies(
            task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          val timeTaken = System.currentTimeMillis - startTime
          logInfo("Serialized task %s:%d as %d bytes in %d ms".format(
            taskSet.id, index, serializedTask.limit, timeTaken))
          val taskName = "task %s:%d".format(taskSet.id, index)
          return Some(new TaskDescription(taskId, execId, taskName, serializedTask))
        }
        case _ =>
      }
    }
    return None
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
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

  def taskFinished(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    if (info.failed) {
      // We might get two task-lost messages for the same task in coarse-grained Mesos mode,
      // or even from Mesos itself when acks get delayed.
      return
    }
    val index = info.index
    info.markSuccessful()
    if (!finished(index)) {
      tasksFinished += 1
      logInfo("Finished TID %s in %d ms (progress: %d/%d)".format(
        tid, info.duration, tasksFinished, numTasks))
      // Deserialize task result and pass it to the scheduler
      val result = ser.deserialize[TaskResult[_]](serializedData, getClass.getClassLoader)
      result.metrics.resultSize = serializedData.limit()
      sched.listener.taskEnded(tasks(index), Success, result.value, result.accumUpdates, info, result.metrics)
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
            return

          case taskResultTooBig: TaskResultTooBigFailure =>
            logInfo("Loss was due to task %s result exceeding Akka frame size;" +
                    "aborting job".format(tid))
            abort("Task %s result exceeded Akka frame size".format(tid))
            return

          case ef: ExceptionFailure =>
            val key = ef.description
            val now = System.currentTimeMillis
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

  def error(message: String) {
    // Save the error message
    abort("Error: " + message)
  }

  def abort(message: String) {
    failed = true
    causeOfFailure = message
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.listener.taskSetFailed(taskSet, message)
    sched.taskSetFinished(this)
  }

  def executorLost(execId: String, hostPort: String) {
    logInfo("Re-queueing tasks for " + execId + " from TaskSet " + taskSet.id)

    // If some task has preferred locations only on hostname, and there are no more executors there,
    // put it in the no-prefs list to avoid the wait from delay scheduling

    // host local tasks - should we push this to rack local or no pref list ? For now, preserving behavior and moving to
    // no prefs list. Note, this was done due to impliations related to 'waiting' for data local tasks, etc.
    // Note: NOT checking process local list - since host local list is super set of that. We need to ad to no prefs only if
    // there is no host local node for the task (not if there is no process local node for the task)
    for (index <- getPendingTasksForHost(Utils.parseHostPort(hostPort)._1)) {
      // val newLocs = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.RACK_LOCAL)
      val newLocs = findPreferredLocations(tasks(index).preferredLocations, sched, TaskLocality.NODE_LOCAL)
      if (newLocs.isEmpty) {
        pendingTasksWithNoPrefs += index
      }
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
  def checkSpeculatableTasks(): Boolean = {
    // Can't speculate if we only have one task, or if all tasks have finished.
    if (numTasks == 1 || tasksFinished == numTasks) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksFinished >= minFinishedForSpeculation) {
      val time = System.currentTimeMillis()
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
              taskSet.id, index, info.hostPort, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    return foundTasks
  }

  def hasPendingTasks(): Boolean = {
    numTasks > 0 && tasksFinished < numTasks
  }
}
