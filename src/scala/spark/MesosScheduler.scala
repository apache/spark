package spark

import java.io.File

import scala.collection.mutable.Map
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import mesos.{Scheduler => NScheduler}
import mesos._

// The main Scheduler implementation, which talks to Mesos. Clients are expected
// to first call start(), then submit tasks through the runTasks method.
//
// This implementation is currently a little quick and dirty. The following
// improvements need to be made to it:
// 1) Right now, the scheduler uses a linear scan through the tasks to find a
//    local one for a given node. It would be faster to have a separate list of
//    pending tasks for each node.
// 2) Presenting a single slave in ParallelOperation.slaveOffer makes it
//    difficult to balance tasks across nodes. It would be better to pass
//    all the offers to the ParallelOperation and have it load-balance.
private class MesosScheduler(
  master: String, frameworkName: String, execArg: Array[Byte])
extends NScheduler with spark.Scheduler with Logging
{
  // Lock used by runTasks to ensure only one thread can be in it
  val runTasksMutex = new Object()

  // Lock used to wait for  scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Current callback object (may be null)
  var activeOpsQueue = new Queue[Int]
  var activeOps = new HashMap[Int, ParallelOperation]
  private var nextOpId = 0
  private[spark] var taskIdToOpId = new HashMap[Int, Int]
  
  def newOpId(): Int = {
    val id = nextOpId
    nextOpId += 1
    return id
  }

  // Incrementing task ID
  private var nextTaskId = 0

  def newTaskId(): Int = {
    val id = nextTaskId;
    nextTaskId += 1;
    return id
  }

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null
  
  override def start() {
    new Thread("Spark scheduler") {
      setDaemon(true)
      override def run {
        val ns = MesosScheduler.this
        ns.driver = new MesosSchedulerDriver(ns, master)
        ns.driver.run()
      }
    }.start
  }

  override def getFrameworkName(d: SchedulerDriver): String = frameworkName
  
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo =
    new ExecutorInfo(new File("spark-executor").getCanonicalPath(), execArg)

  override def runTasks[T: ClassManifest](tasks: Array[Task[T]]): Array[T] = {
    var opId = 0
    waitForRegister()
    this.synchronized {
      opId = newOpId()
    }
    val myOp = new SimpleParallelOperation(this, tasks, opId)

    try {
      this.synchronized {
        this.activeOps(myOp.opId) = myOp
        this.activeOpsQueue += myOp.opId
      }
      driver.reviveOffers();
      myOp.join();
    } finally {
      this.synchronized {
        this.activeOps.remove(myOp.opId)
        this.activeOpsQueue.dequeueAll(x => (x == myOp.opId))
      }
    }

    if (myOp.errorHappened)
      throw new SparkException(myOp.errorMessage, myOp.errorCode)
    else
      return myOp.results
  }

  override def registered(d: SchedulerDriver, frameworkId: String) {
    logInfo("Registered as framework ID " + frameworkId)
    registeredLock.synchronized {
      isRegistered = true
      registeredLock.notifyAll()
    }
  }
  
  override def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered)
        registeredLock.wait()
    }
  }

  override def resourceOffer(
      d: SchedulerDriver, oid: String, offers: java.util.List[SlaveOffer]) {
    synchronized {
      val tasks = new java.util.ArrayList[TaskDescription]
      val availableCpus = offers.map(_.getParams.get("cpus").toInt)
      val availableMem = offers.map(_.getParams.get("mem").toInt)
      var launchedTask = true
      for (opId <- activeOpsQueue) {
        launchedTask = true
        while (launchedTask) {
          launchedTask = false
          for (i <- 0 until offers.size.toInt) {
            try {
              activeOps(opId).slaveOffer(offers.get(i), availableCpus(i), availableMem(i)) match {
                case Some(task) =>
                  tasks.add(task)
                  availableCpus(i) -= task.getParams.get("cpus").toInt
                  availableMem(i) -= task.getParams.get("mem").toInt
                  launchedTask = launchedTask || true
                case None => {}
              }
            } catch {
              case e: Exception => logError("Exception in resourceOffer", e)
            }
          }
        }
      }
      val params = new java.util.HashMap[String, String]
      params.put("timeout", "1")
      d.replyToOffer(oid, tasks, params) // TODO: use smaller timeout
    }
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    synchronized {
      try {
        taskIdToOpId.get(status.getTaskId) match {
          case Some(opId) =>
            if (activeOps.contains(opId)) {
              activeOps(opId).statusUpdate(status)
            }
          case None =>
            logInfo("TID " + status.getTaskId + " already finished")
        }

      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
  }

  override def error(d: SchedulerDriver, code: Int, message: String) {
    synchronized {
      if (activeOps.size > 0) {
        for ((opId, activeOp) <- activeOps) {
          try {
            activeOp.error(code, message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        logError("Mesos error: %s (error code: %d)".format(message, code))
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (driver != null)
      driver.stop()
  }

  // TODO: query Mesos for number of cores
  override def numCores() = System.getProperty("spark.default.parallelism", "2").toInt
}


// Trait representing an object that manages a parallel operation by
// implementing various scheduler callbacks.
trait ParallelOperation {
  def slaveOffer(s: SlaveOffer, availableCpus: Int, availableMem: Int): Option[TaskDescription]
  def statusUpdate(t: TaskStatus): Unit
  def error(code: Int, message: String): Unit
}


class SimpleParallelOperation[T: ClassManifest](
  sched: MesosScheduler, tasks: Array[Task[T]], val opId: Int)
extends ParallelOperation with Logging
{
  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "1000").toLong

  val callingThread = currentThread
  val numTasks = tasks.length
  val results = new Array[T](numTasks)
  val launched = new Array[Boolean](numTasks)
  val finished = new Array[Boolean](numTasks)
  val tidToIndex = Map[Int, Int]()

  var allFinished = false
  val joinLock = new Object()

  var errorHappened = false
  var errorCode = 0
  var errorMessage = ""

  var tasksLaunched = 0
  var tasksFinished = 0
  var lastPreferredLaunchTime = System.currentTimeMillis

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

  def slaveOffer(offer: SlaveOffer, availableCpus: Int, availableMem: Int): Option[TaskDescription] = {
    if (tasksLaunched < numTasks) {
      var checkPrefVals: Array[Boolean] = Array(true)
      val time = System.currentTimeMillis
      if (time - lastPreferredLaunchTime > LOCALITY_WAIT)
        checkPrefVals = Array(true, false) // Allow non-preferred tasks
      // TODO: Make desiredCpus and desiredMem configurable
      val desiredCpus = 1
      val desiredMem = 500
      if ((availableCpus < desiredCpus) || (availableMem < desiredMem))
        return None
      for (checkPref <- checkPrefVals; i <- 0 until numTasks) {
        if (!launched(i) && (!checkPref ||
            tasks(i).preferredLocations.contains(offer.getHost) ||
            tasks(i).preferredLocations.isEmpty))
        {
          val taskId = sched.newTaskId()
          sched.taskIdToOpId(taskId) = opId
          tidToIndex(taskId) = i
          val preferred = if(checkPref) "preferred" else "non-preferred"
          val message =
            "Starting task %d as opId %d, TID %s on slave %s: %s (%s)".format(
              i, opId, taskId, offer.getSlaveId, offer.getHost, preferred)
          logInfo(message)
          tasks(i).markStarted(offer)
          launched(i) = true
          tasksLaunched += 1
          if (checkPref)
            lastPreferredLaunchTime = time
          val params = new java.util.HashMap[String, String]
          params.put("cpus", "" + desiredCpus)
          params.put("mem", "" + desiredMem)
          val serializedTask = Utils.serialize(tasks(i))
          //logInfo("Serialized size: " + serializedTask.size)
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
    logInfo("Finished opId " + opId + " TID " + tid)
    val index = tidToIndex(tid)
    if (!finished(index)) {
      // Deserialize task result
      val result = Utils.deserialize[TaskResult[T]](status.getData)
      results(index) = result.value
      // Update accumulators
      Accumulators.add(callingThread, result.accumUpdates)
      // Mark finished and stop if we've finished all the tasks
      finished(index) = true
      // Remove TID -> opId mapping from sched
      sched.taskIdToOpId.remove(tid)
      tasksFinished += 1
      logInfo("Progress: " + tasksFinished + "/" + numTasks)
      if (tasksFinished == numTasks)
        setAllFinished()
    } else {
      logInfo("Task " + index + " has already finished, so ignoring it")
    }
  }

  def taskLost(status: TaskStatus) {
    val tid = status.getTaskId
    logInfo("Lost opId " + opId + " TID " + tid)
    val index = tidToIndex(tid)
    if (!finished(index)) {
      launched(index) = false
      sched.taskIdToOpId.remove(tid)
      tasksLaunched -= 1
    } else {
      logInfo("Task " + index + " has already finished, so ignoring it")
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
