package spark

import java.util.{HashMap => JHashMap}

import scala.collection.mutable.HashMap

import mesos._


/**
 * A simple implementation of Job that just runs each task in an array.
 */
class SimpleJob[T: ClassManifest](
  sched: MesosScheduler, tasks: Array[Task[T]], val jobId: Int)
extends Job with Logging
{
  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "3000").toLong

  val callingThread = currentThread
  val numTasks = tasks.length
  val results = new Array[T](numTasks)
  val launched = new Array[Boolean](numTasks)
  val finished = new Array[Boolean](numTasks)
  val tidToIndex = HashMap[Int, Int]()

  var allFinished = false
  val joinLock = new Object()

  var errorHappened = false
  var errorCode = 0
  var errorMessage = ""

  var tasksLaunched = 0
  var tasksFinished = 0
  var lastPreferredLaunchTime = System.currentTimeMillis

  val cpusPerTask = System.getProperty("spark.task.cpus", "1").toInt
  val memPerTask = System.getProperty("spark.task.mem", "512").toInt

  def setAllFinished() {
    joinLock.synchronized {
      allFinished = true
      joinLock.notifyAll()
    }
  }

  def join() {
    joinLock.synchronized {
      while (!allFinished)
        joinLock.wait()
    }
  }

  def slaveOffer(offer: SlaveOffer, availableCpus: Int, availableMem: Int)
      : Option[TaskDescription] = {
    if (tasksLaunched < numTasks) {
      var checkPrefVals: Array[Boolean] = Array(true)
      val time = System.currentTimeMillis
      if (time - lastPreferredLaunchTime > LOCALITY_WAIT)
        checkPrefVals = Array(true, false) // Allow non-preferred tasks
      if ((availableCpus < cpusPerTask) || (availableMem < memPerTask))
        return None
      for (checkPref <- checkPrefVals; i <- 0 until numTasks) {
        if (!launched(i) && (!checkPref ||
            tasks(i).preferredLocations.contains(offer.getHost) ||
            tasks(i).preferredLocations.isEmpty))
        {
          val taskId = sched.newTaskId()
          sched.taskIdToJobId(taskId) = jobId
          tidToIndex(taskId) = i
          val preferred = if(checkPref) "preferred" else "non-preferred"
          val message =
            "Starting task %d:%d as TID %s on slave %s: %s (%s)".format(
              i, jobId, taskId, offer.getSlaveId, offer.getHost, preferred)
          logInfo(message)
          tasks(i).markStarted(offer)
          launched(i) = true
          tasksLaunched += 1
          if (checkPref)
            lastPreferredLaunchTime = time
          val params = new JHashMap[String, String]
          params.put("cpus", "" + cpusPerTask)
          params.put("mem", "" + memPerTask)
          val serializedTask = Utils.serialize(tasks(i))
          logDebug("Serialized size: " + serializedTask.size)
          return Some(new TaskDescription(taskId, offer.getSlaveId,
            "task_" + taskId, params, serializedTask))
        }
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
      // Remove TID -> jobId mapping from sched
      sched.taskIdToJobId.remove(tid)
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
      sched.taskIdToJobId.remove(tid)
      tasksLaunched -= 1
    } else {
      logInfo("Ignoring task-lost event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def error(code: Int, message: String) {
    // Save the error message
    errorHappened = true
    errorCode = code
    errorMessage = message
    // Indicate to caller thread that we're done
    setAllFinished()
  }
}
