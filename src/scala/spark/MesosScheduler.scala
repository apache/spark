package spark

import java.io.File

import scala.collection.mutable.Map

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
extends NScheduler with spark.Scheduler
{
  // Lock used by runTasks to ensure only one thread can be in it
  val runTasksMutex = new Object()

  // Lock used to wait for  scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Current callback object (may be null)
  var activeOp: ParallelOperation = null

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
    runTasksMutex.synchronized {
      waitForRegister()
      val myOp = new SimpleParallelOperation(this, tasks)

      try {
        this.synchronized {
          this.activeOp = myOp
        }
        driver.reviveOffers();
        myOp.join();
      } finally {
        this.synchronized {
          this.activeOp = null
        }
      }

      if (myOp.errorHappened)
        throw new SparkException(myOp.errorMessage, myOp.errorCode)
      else
        return myOp.results
    }
  }

  override def registered(d: SchedulerDriver, frameworkId: String) {
    println("Registered as framework ID " + frameworkId)
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
      d: SchedulerDriver, oid: String, offers: SlaveOfferVector) {
    synchronized {
      val tasks = new TaskDescriptionVector
      if (activeOp != null) {
        try {
          for (i <- 0 until offers.size.toInt) {
            activeOp.slaveOffer(offers.get(i)) match {
              case Some(task) => tasks.add(task)
              case None => {}
            }
          }
        } catch {
          case e: Exception => e.printStackTrace
        }
      }  
      val params = new StringMap
      params.set("timeout", "1")
      d.replyToOffer(oid, tasks, params) // TODO: use smaller timeout
    }
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    synchronized {
      try {
        if (activeOp != null) {
          activeOp.statusUpdate(status)
        }
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
  }

  override def error(d: SchedulerDriver, code: Int, message: String) {
    synchronized {
      if (activeOp != null) {
        try {
          activeOp.error(code, message)
        } catch {
          case e: Exception => e.printStackTrace
        }
      } else {
        val msg = "Mesos error: %s (error code: %d)".format(message, code)
        System.err.println(msg)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (driver != null)
      driver.stop()
  }
}


// Trait representing an object that manages a parallel operation by
// implementing various scheduler callbacks.
trait ParallelOperation {
  def slaveOffer(s: SlaveOffer): Option[TaskDescription]
  def statusUpdate(t: TaskStatus): Unit
  def error(code: Int, message: String): Unit
}


class SimpleParallelOperation[T: ClassManifest](
  sched: MesosScheduler, tasks: Array[Task[T]])
extends ParallelOperation
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

  def slaveOffer(offer: SlaveOffer): Option[TaskDescription] = {
    if (tasksLaunched < numTasks) {
      var checkPrefVals: Array[Boolean] = Array(true)
      val time = System.currentTimeMillis
      if (time - lastPreferredLaunchTime > LOCALITY_WAIT)
        checkPrefVals = Array(true, false) // Allow non-preferred tasks
      // TODO: Make desiredCpus and desiredMem configurable
      val desiredCpus = 1
      val desiredMem = 750L * 1024L * 1024L
      if (offer.getParams.get("cpus").toInt < desiredCpus || 
          offer.getParams.get("mem").toLong < desiredMem)
        return None
      for (checkPref <- checkPrefVals; i <- 0 until numTasks) {
        if (!launched(i) && (!checkPref ||
            tasks(i).preferredLocations.contains(offer.getHost) ||
            tasks(i).preferredLocations.isEmpty))
        {
          val taskId = sched.newTaskId()
          tidToIndex(taskId) = i
          printf("Starting task %d as TID %s on slave %s: %s (%s)\n",
            i, taskId, offer.getSlaveId, offer.getHost, 
            if(checkPref) "preferred" else "non-preferred")
          tasks(i).markStarted(offer)
          launched(i) = true
          tasksLaunched += 1
          if (checkPref)
            lastPreferredLaunchTime = time
          val params = new StringMap
          params.set("cpus", "" + desiredCpus)
          params.set("mem", "" + desiredMem)
          val serializedTask = Utils.serialize(tasks(i))
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
    println("Finished TID " + tid)
    // Deserialize task result
    val result = Utils.deserialize[TaskResult[T]](status.getData)
    results(tidToIndex(tid)) = result.value
    // Update accumulators
    Accumulators.add(callingThread, result.accumUpdates)
    // Mark finished and stop if we've finished all the tasks
    finished(tidToIndex(tid)) = true
    tasksFinished += 1
    if (tasksFinished == numTasks)
      setAllFinished()
  }

  def taskLost(status: TaskStatus) {
    val tid = status.getTaskId
    println("Lost TID " + tid)
    launched(tidToIndex(tid)) = false
    tasksLaunched -= 1
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
