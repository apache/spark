package spark

import java.io.File
import java.util.concurrent.Semaphore

import nexus.{ExecutorInfo, TaskDescription, TaskState, TaskStatus}
import nexus.{SlaveOffer, SchedulerDriver, NexusSchedulerDriver}
import nexus.{SlaveOfferVector, TaskDescriptionVector, StringMap}

// The main Scheduler implementation, which talks to Nexus. Clients are expected
// to first call start(), then submit tasks through the runTasks method.
//
// This implementation is currently a little quick and dirty. The following
// improvements need to be made to it:
// 1) Fault tolerance should be added - if a task fails, just re-run it anywhere.
// 2) Right now, the scheduler uses a linear scan through the tasks to find a
//    local one for a given node. It would be faster to have a separate list of
//    pending tasks for each node.
// 3) The Callbacks way of organizing things didn't work out too well, so the
//    way the scheduler keeps track of the currently active runTasks operation
//    can be made cleaner.
private class NexusScheduler(
  master: String, frameworkName: String, execArg: Array[Byte])
extends nexus.Scheduler with spark.Scheduler
{
  // Semaphore used by runTasks to ensure only one thread can be in it
  val semaphore = new Semaphore(1)

  // Lock used to wait for  scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  // Trait representing a set of  scheduler callbacks
  trait Callbacks {
    def slotOffer(s: SlaveOffer): Option[TaskDescription]
    def taskFinished(t: TaskStatus): Unit
    def error(code: Int, message: String): Unit
  }

  // Current callback object (may be null)
  var callbacks: Callbacks = null

  // Incrementing task ID
  var nextTaskId = 0

  // Maximum time to wait to run a task in a preferred location (in ms)
  val LOCALITY_WAIT = System.getProperty("spark.locality.wait", "1000").toLong

  // Driver for talking to Nexus
  var driver: SchedulerDriver = null
  
  override def start() {
    new Thread("Spark scheduler") {
      setDaemon(true)
      override def run {
        val ns = NexusScheduler.this
        ns.driver = new NexusSchedulerDriver(ns, master)
        ns.driver.run()
      }
    }.start
  }

  override def getFrameworkName(d: SchedulerDriver): String = frameworkName
  
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo =
    new ExecutorInfo(new File("spark-executor").getCanonicalPath(), execArg)

  override def runTasks[T: ClassManifest](tasks: Array[Task[T]]) : Array[T] = {
    val results = new Array[T](tasks.length)
    if (tasks.length == 0)
      return results

    val launched = new Array[Boolean](tasks.length)

    val callingThread = currentThread

    var errorHappened = false
    var errorCode = 0
    var errorMessage = ""

    // Wait for scheduler to be registered with Nexus
    waitForRegister()

    try {
      // Acquire the runTasks semaphore
      semaphore.acquire()

      val myCallbacks = new Callbacks {
        val firstTaskId = nextTaskId
        var tasksLaunched = 0
        var tasksFinished = 0
        var lastPreferredLaunchTime = System.currentTimeMillis

        def slotOffer(slot: SlaveOffer): Option[TaskDescription] = {
          try {
            if (tasksLaunched < tasks.length) {
              // TODO: Add a short wait if no task with location pref is found
              // TODO: Figure out why a function is needed around this to
              // avoid  scala.runtime.NonLocalReturnException
              def findTask: Option[TaskDescription] = {
                var checkPrefVals: Array[Boolean] = Array(true)
                val time = System.currentTimeMillis
                if (time - lastPreferredLaunchTime > LOCALITY_WAIT)
                  checkPrefVals = Array(true, false) // Allow non-preferred tasks
                // TODO: Make desiredCpus and desiredMem configurable
                val desiredCpus = 1
                val desiredMem = 750L * 1024L * 1024L
                if (slot.getParams.get("cpus").toInt < desiredCpus || 
                    slot.getParams.get("mem").toLong < desiredMem)
                  return None
                for (checkPref <- checkPrefVals;
                     i <- 0 until tasks.length;
                     if !launched(i) && (!checkPref || tasks(i).prefers(slot)))
                {
                  val taskId = nextTaskId
                  nextTaskId += 1
                  printf("Starting task %d as TID %d on slave %d: %s (%s)\n",
                    i, taskId, slot.getSlaveId, slot.getHost, 
                    if(checkPref) "preferred" else "non-preferred")
                  tasks(i).markStarted(slot)
                  launched(i) = true
                  tasksLaunched += 1
                  if (checkPref)
                    lastPreferredLaunchTime = time
                  val params = new StringMap
                  params.set("cpus", "" + desiredCpus)
                  params.set("mem", "" + desiredMem)
                  val serializedTask = Utils.serialize(tasks(i))
                  return Some(new TaskDescription(taskId, slot.getSlaveId,
                    "task_" + taskId, params, serializedTask))
                }
                return None
              }
              return findTask
            } else {
              return None
            }
          } catch {
            case e: Exception => { 
              e.printStackTrace
              System.exit(1)
              return None
            }
          }
        }

        def taskFinished(status: TaskStatus) {
          println("Finished TID " + status.getTaskId)
          // Deserialize task result
          val result = Utils.deserialize[TaskResult[T]](status.getData)
          results(status.getTaskId - firstTaskId) = result.value
          // Update accumulators
          Accumulators.add(callingThread, result.accumUpdates)
          // Stop if we've finished all the tasks
          tasksFinished += 1
          if (tasksFinished == tasks.length) {
            NexusScheduler.this.callbacks = null
            NexusScheduler.this.notifyAll()
          }
        }

        def error(code: Int, message: String) {
          // Save the error message
          errorHappened = true
          errorCode = code
          errorMessage = message
          // Indicate to caller thread that we're done
          NexusScheduler.this.callbacks = null
          NexusScheduler.this.notifyAll()
        }
      }

      this.synchronized {
        this.callbacks = myCallbacks
      }
      driver.reviveOffers();
      this.synchronized {
        while (this.callbacks != null) this.wait()
      }
    } finally {
      semaphore.release()
    }

    if (errorHappened)
      throw new SparkException(errorMessage, errorCode)
    else
      return results
  }

  override def registered(d: SchedulerDriver, frameworkId: Int) {
    println("Registered as framework ID " + frameworkId)
    registeredLock.synchronized {
      isRegistered = true
      registeredLock.notifyAll()
    }
  }
  
  override def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered) registeredLock.wait()
    }
  }

  override def resourceOffer(
      d: SchedulerDriver, oid: Long, slots: SlaveOfferVector) {
    synchronized {
      val tasks = new TaskDescriptionVector
      if (callbacks != null) {
        try {
          for (i <- 0 until slots.size.toInt) {
            callbacks.slotOffer(slots.get(i)) match {
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
      if (callbacks != null && status.getState == TaskState.TASK_FINISHED) {
        try {
          callbacks.taskFinished(status)
        } catch {
          case e: Exception => e.printStackTrace
        }
      }
    }
  }

  override def error(d: SchedulerDriver, code: Int, message: String) {
    synchronized {
      if (callbacks != null) {
        try {
          callbacks.error(code, message)
        } catch {
          case e: Exception => e.printStackTrace
        }
      } else {
        val msg = "Nexus error: %s (error code: %d)".format(message, code)
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
