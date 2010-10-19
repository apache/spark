package spark

import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import mesos._


/**
 * A Job that runs a set of tasks with no interdependencies.
 */
class SimpleJob[T: ClassManifest](
  sched: MesosScheduler, tasks: Array[Task[T]], val jobId: Int)
extends Job(jobId) with Logging
{
  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "3000").toLong

  // CPUs and memory to request per task
  val CPUS_PER_TASK = System.getProperty("spark.task.cpus", "1").toInt
  val MEM_PER_TASK = System.getProperty("spark.task.mem", "512").toInt

  // Maximum times a task is allowed to fail before failing the job
  val MAX_TASK_FAILURES = 4

  val callingThread = currentThread
  val numTasks = tasks.length
  val results = new Array[T](numTasks)
  val launched = new Array[Boolean](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val tidToIndex = HashMap[Int, Int]()

  var allFinished = false
  val joinLock = new Object() // Used to wait for all tasks to finish

  var tasksLaunched = 0
  var tasksFinished = 0

  // Last time when we launched a preferred task (for delay scheduling)
  var lastPreferredLaunchTime = System.currentTimeMillis

  // List of pending tasks for each node. These collections are actually
  // treated as stacks, in which new tasks are added to the end of the
  // ArrayBuffer and removed from the end. This makes it faster to detect
  // tasks that repeatedly fail because whenever a task failed, it is put
  // back at the head of the stack. They are also only cleaned up lazily;
  // when a task is launched, it remains in all the pending lists except
  // the one that it was launched from, but gets removed from them later.
  val pendingTasksForHost = new HashMap[String, ArrayBuffer[Int]]

  // List containing pending tasks with no locality preferences
  val pendingTasksWithNoPrefs = new ArrayBuffer[Int]

  // List containing all pending tasks (also used as a stack, as above)
  val allPendingTasks = new ArrayBuffer[Int]

  // Did the job fail?
  var failed = false
  var causeOfFailure = ""

  // Add all our tasks to the pending lists. We do this in reverse order
  // of task index so that tasks with low indices get launched first.
  for (i <- (0 until numTasks).reverse) {
    addPendingTask(i)
  }

  // Add a task to all the pending-task lists that it should be on.
  def addPendingTask(index: Int) {
    val locations = tasks(index).preferredLocations
    if (locations.size == 0) {
      pendingTasksWithNoPrefs += index
    } else {
      for (host <- locations) {
        val list = pendingTasksForHost.getOrElseUpdate(host, ArrayBuffer())
        list += index
      }
    }
    allPendingTasks += index
  }

  // Mark the job as finished and wake up any threads waiting on it
  def setAllFinished() {
    joinLock.synchronized {
      allFinished = true
      joinLock.notifyAll()
    }
  }

  // Wait until the job finishes and return its results
  def join(): Array[T] = {
    joinLock.synchronized {
      while (!allFinished) {
        joinLock.wait()
      }
      if (failed) {
        throw new SparkException(causeOfFailure)
      } else {
        return results
      }
    }
  }

  // Return the pending tasks list for a given host, or an empty list if
  // there is no map entry for that host
  def getPendingTasksForHost(host: String): ArrayBuffer[Int] = {
    pendingTasksForHost.getOrElse(host, ArrayBuffer())
  }

  // Dequeue a pending task from the given list and return its index.
  // Return None if the list is empty.
  // This method also cleans up any tasks in the list that have already
  // been launched, since we want that to happen lazily.
  def findTaskFromList(list: ArrayBuffer[Int]): Option[Int] = {
    while (!list.isEmpty) {
      val index = list.last
      list.trimEnd(1)
      if (!launched(index) && !finished(index)) {
        return Some(index)
      }
    }
    return None
  }

  // Dequeue a pending task for a given node and return its index.
  // If localOnly is set to false, allow non-local tasks as well.
  def findTask(host: String, localOnly: Boolean): Option[Int] = {
    val localTask = findTaskFromList(getPendingTasksForHost(host))
    if (localTask != None) {
      return localTask
    }
    val noPrefTask = findTaskFromList(pendingTasksWithNoPrefs)
    if (noPrefTask != None) {
      return noPrefTask
    }
    if (!localOnly) {
      return findTaskFromList(allPendingTasks) // Look for non-local task
    } else {
      return None
    }
  }

  // Does a host count as a preferred location for a task? This is true if
  // either the task has preferred locations and this host is one, or it has
  // no preferred locations (in which we still count the launch as preferred).
  def isPreferredLocation(task: Task[T], host: String): Boolean = {
    val locs = task.preferredLocations
    return (locs.contains(host) || locs.isEmpty)
  }

  // Respond to an offer of a single slave from the scheduler by finding a task
  def slaveOffer(offer: SlaveOffer, availableCpus: Int, availableMem: Int)
      : Option[TaskDescription] = {
    if (tasksLaunched < numTasks && availableCpus >= CPUS_PER_TASK &&
        availableMem >= MEM_PER_TASK) {
      val time = System.currentTimeMillis
      val localOnly = (time - lastPreferredLaunchTime < LOCALITY_WAIT)
      val host = offer.getHost
      findTask(host, localOnly) match {
        case Some(index) => {
          // Found a task; do some bookkeeping and return a Mesos task for it
          val task = tasks(index)
          val taskId = sched.newTaskId()
          // Figure out whether this should count as a preferred launch
          val preferred = isPreferredLocation(task, host)
          val prefStr = if(preferred) "preferred" else "non-preferred"
          val message =
            "Starting task %d:%d as TID %s on slave %s: %s (%s)".format(
              jobId, index, taskId, offer.getSlaveId, host, prefStr)
          logInfo(message)
          // Do various bookkeeping
          tidToIndex(taskId) = index
          task.markStarted(offer)
          launched(index) = true
          tasksLaunched += 1
          if (preferred)
            lastPreferredLaunchTime = time
          // Create and return the Mesos task object
          val params = new JHashMap[String, String]
          params.put("cpus", CPUS_PER_TASK.toString)
          params.put("mem", MEM_PER_TASK.toString)
          val serializedTask = Utils.serialize(task)
          logDebug("Serialized size: " + serializedTask.size)
          val taskName = "task %d:%d".format(jobId, index)
          return Some(new TaskDescription(
            taskId, offer.getSlaveId, taskName, params, serializedTask))
        }
        case _ =>
      }
    }
    return None
  }

  def statusUpdate(status: TaskStatus) {
    status.getState match {
      case TaskState.TASK_FINISHED =>
        taskFinished(status)
      case TaskState.TASK_LOST =>
        taskLost(status)
      case TaskState.TASK_FAILED =>
        taskLost(status)
      case TaskState.TASK_KILLED =>
        taskLost(status)
      case _ =>
    }
  }

  def taskFinished(status: TaskStatus) {
    val tid = status.getTaskId
    val index = tidToIndex(tid)
    if (!finished(index)) {
      tasksFinished += 1
      logInfo("Finished TID %d (progress: %d/%d)".format(
        tid, tasksFinished, numTasks))
      // Deserialize task result
      val result = Utils.deserialize[TaskResult[T]](status.getData)
      results(index) = result.value
      // Update accumulators
      Accumulators.add(callingThread, result.accumUpdates)
      // Mark finished and stop if we've finished all the tasks
      finished(index) = true
      if (tasksFinished == numTasks)
        setAllFinished()
    } else {
      logInfo("Ignoring task-finished event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def taskLost(status: TaskStatus) {
    val tid = status.getTaskId
    val index = tidToIndex(tid)
    if (!finished(index)) {
      logInfo("Lost TID %d (task %d:%d)".format(tid, jobId, index))
      launched(index) = false
      tasksLaunched -= 1
      // Re-enqueue the task as pending
      addPendingTask(index)
      // Mark it as failed
      if (status.getState == TaskState.TASK_FAILED ||
          status.getState == TaskState.TASK_LOST) {
        numFailures(index) += 1
        if (numFailures(index) > MAX_TASK_FAILURES) {
          logError("Task %d:%d failed more than %d times; aborting job".format(
            jobId, index, MAX_TASK_FAILURES))
          abort("Task %d failed more than %d times".format(
            index, MAX_TASK_FAILURES))
        }
      }
    } else {
      logInfo("Ignoring task-lost event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def error(code: Int, message: String) {
    // Save the error message
    abort("Mesos error: %s (error code: %d)".format(message, code))
  }

  def abort(message: String) {
    joinLock.synchronized {
      failed = true
      causeOfFailure = message
      // TODO: Kill running tasks if we were not terminated due to a Mesos error
      // Indicate to any joining thread that we're done
      setAllFinished()
    }
  }
}
