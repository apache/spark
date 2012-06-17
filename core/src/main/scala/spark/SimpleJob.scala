package spark

import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import com.google.protobuf.ByteString

import org.apache.mesos._
import org.apache.mesos.Protos._

/**
 * A Job that runs a set of tasks with no interdependencies.
 */
class SimpleJob(
    sched: MesosScheduler, 
    tasksSeq: Seq[Task[_]], 
    runId: Int,
    jobId: Int) 
  extends Job(runId, jobId)
  with Logging {
  
  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "5000").toLong

  // CPUs to request per task
  val CPUS_PER_TASK = System.getProperty("spark.task.cpus", "1").toDouble

  // Maximum times a task is allowed to fail before failing the job
  val MAX_TASK_FAILURES = 4

  // Serializer for closures and tasks.
  val ser = SparkEnv.get.closureSerializer.newInstance()

  val callingThread = Thread.currentThread
  val tasks = tasksSeq.toArray
  val numTasks = tasks.length
  val launched = new Array[Boolean](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)
  val tidToIndex = HashMap[String, Int]()

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

  // How frequently to reprint duplicate exceptions in full, in milliseconds
  val EXCEPTION_PRINT_INTERVAL =
    System.getProperty("spark.logging.exceptionPrintInterval", "10000").toLong
  // Map of recent exceptions (identified by string representation and
  // top stack frame) to duplicate count (how many times the same
  // exception has appeared) and time the full exception was
  // printed. This should ideally be an LRU map that can drop old
  // exceptions automatically.
  val recentExceptions = HashMap[String, (Int, Long)]()

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
  def isPreferredLocation(task: Task[_], host: String): Boolean = {
    val locs = task.preferredLocations
    return (locs.contains(host) || locs.isEmpty)
  }

  // Respond to an offer of a single slave from the scheduler by finding a task
  def slaveOffer(offer: Offer, availableCpus: Double): Option[TaskInfo] = {
    if (tasksLaunched < numTasks && availableCpus >= CPUS_PER_TASK) {
      val time = System.currentTimeMillis
      val localOnly = (time - lastPreferredLaunchTime < LOCALITY_WAIT)
      val host = offer.getHostname
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
              jobId, index, taskId.getValue, offer.getSlaveId.getValue, host, prefStr)
          logInfo(message)
          // Do various bookkeeping
          tidToIndex(taskId.getValue) = index
          launched(index) = true
          tasksLaunched += 1
          if (preferred)
            lastPreferredLaunchTime = time
          // Create and return the Mesos task object
          val cpuRes = Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder().setValue(CPUS_PER_TASK).build())
            .build()

          val startTime = System.currentTimeMillis
          val serializedTask = ser.serialize(task)
          val timeTaken = System.currentTimeMillis - startTime

          logInfo("Size of task %d:%d is %d bytes and took %d ms to serialize by %s"
            .format(jobId, index, serializedTask.size, timeTaken, ser.getClass.getName))

          val taskName = "task %d:%d".format(jobId, index)
          return Some(TaskInfo.newBuilder()
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId)
              .setExecutor(sched.executorInfo)
              .setName(taskName)
              .addResources(cpuRes)
              .setData(ByteString.copyFrom(serializedTask))
              .build())
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
    val tid = status.getTaskId.getValue
    val index = tidToIndex(tid)
    if (!finished(index)) {
      tasksFinished += 1
      logInfo("Finished TID %s (progress: %d/%d)".format(tid, tasksFinished, numTasks))
      // Deserialize task result
      val result = ser.deserialize[TaskResult[_]](
        status.getData.toByteArray, getClass.getClassLoader)
      sched.taskEnded(tasks(index), Success, result.value, result.accumUpdates)
      // Mark finished and stop if we've finished all the tasks
      finished(index) = true
      if (tasksFinished == numTasks)
        sched.jobFinished(this)
    } else {
      logInfo("Ignoring task-finished event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def taskLost(status: TaskStatus) {
    val tid = status.getTaskId.getValue
    val index = tidToIndex(tid)
    if (!finished(index)) {
      logInfo("Lost TID %s (task %d:%d)".format(tid, jobId, index))
      launched(index) = false
      tasksLaunched -= 1
      // Check if the problem is a map output fetch failure. In that case, this
      // task will never succeed on any node, so tell the scheduler about it.
      if (status.getData != null && status.getData.size > 0) {
        val reason = ser.deserialize[TaskEndReason](
          status.getData.toByteArray, getClass.getClassLoader)
        reason match {
          case fetchFailed: FetchFailed =>
            logInfo("Loss was due to fetch failure from " + fetchFailed.serverUri)
            sched.taskEnded(tasks(index), fetchFailed, null, null)
            finished(index) = true
            tasksFinished += 1
            if (tasksFinished == numTasks) {
              sched.jobFinished(this)
            }
            return
          case ef: ExceptionFailure =>
            val key = ef.exception.toString
            val now = System.currentTimeMillis
            val (printFull, dupCount) =
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
                recentExceptions += Tuple(key, (0, now))
                (true, 0)
              }

            if (printFull) {
              val stackTrace =
                for (elem <- ef.exception.getStackTrace)
                yield "\tat %s".format(elem.toString)
              logInfo("Loss was due to %s\n%s".format(
                ef.exception.toString, stackTrace.mkString("\n")))
            } else {
              logInfo("Loss was due to %s [duplicate %d]".format(
                ef.exception.toString, dupCount))
            }
          case _ => {}
        }
      }
      // On other failures, re-enqueue the task as pending for a max number of retries
      addPendingTask(index)
      // Count attempts only on FAILED and LOST state (not on KILLED)
      if (status.getState == TaskState.TASK_FAILED ||
          status.getState == TaskState.TASK_LOST) {
        numFailures(index) += 1
        if (numFailures(index) > MAX_TASK_FAILURES) {
          logError("Task %d:%d failed more than %d times; aborting job".format(
            jobId, index, MAX_TASK_FAILURES))
          abort("Task %d failed more than %d times".format(index, MAX_TASK_FAILURES))
        }
      }
    } else {
      logInfo("Ignoring task-lost event for TID " + tid +
        " because task " + index + " is already finished")
    }
  }

  def error(message: String) {
    // Save the error message
    abort("Mesos error: " + message)
  }

  def abort(message: String) {
    failed = true
    causeOfFailure = message
    // TODO: Kill running tasks if we were not terminated due to a Mesos error
    sched.jobFinished(this)
  }
}
