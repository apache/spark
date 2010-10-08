package spark

import java.io.File
import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}
import java.util.{HashMap => JHashMap}

import scala.collection.mutable.Map
import scala.collection.mutable.Queue
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import mesos.{Scheduler => MScheduler}
import mesos._

// The main Scheduler implementation, which talks to Mesos. Clients are expected
// to first call start(), then submit tasks through the runTasks method.
//
// This implementation is currently a little quick and dirty. The following
// improvements need to be made to it:
// 1) Right now, the scheduler uses a linear scan through the tasks to find a
//    local one for a given node. It would be faster to have a separate list of
//    pending tasks for each node.
// 2) Presenting a single slave in Job.slaveOffer makes it
//    difficult to balance tasks across nodes. It would be better to pass
//    all the offers to the Job and have it load-balance.
private class MesosScheduler(
  master: String, frameworkName: String, execArg: Array[Byte])
extends MScheduler with spark.Scheduler with Logging
{
  // Lock used to wait for  scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  var activeJobs = new HashMap[Int, Job]
  var activeJobsQueue = new Queue[Job]

  private[spark] var taskIdToJobId = new HashMap[Int, Int]

  private var nextJobId = 0
  
  def newJobId(): Int = this.synchronized {
    val id = nextJobId
    nextJobId += 1
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
        val sched = MesosScheduler.this
        sched.driver = new MesosSchedulerDriver(sched, master)
        sched.driver.run()
      }
    }.start
  }

  override def getFrameworkName(d: SchedulerDriver): String = frameworkName
  
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo =
    new ExecutorInfo(new File("spark-executor").getCanonicalPath(), execArg)

  /**
   * The primary means to submit a job to the scheduler. Given a list of tasks,
   * runs them and returns an array of the results.
   */
  override def runTasks[T: ClassManifest](tasks: Array[Task[T]]): Array[T] = {
    waitForRegister()
    val jobId = newJobId()
    val myJob = new SimpleJob(this, tasks, jobId)

    try {
      this.synchronized {
        this.activeJobs(myJob.jobId) = myJob
        this.activeJobsQueue += myJob
      }
      driver.reviveOffers();
      myJob.join();
    } finally {
      this.synchronized {
        this.activeJobs.remove(myJob.jobId)
        this.activeJobsQueue.dequeueAll(x => (x == myJob))
      }
    }

    if (myJob.errorHappened)
      throw new SparkException(myJob.errorMessage, myJob.errorCode)
    else
      return myJob.results
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
      d: SchedulerDriver, oid: String, offers: JList[SlaveOffer]) {
    synchronized {
      val tasks = new JArrayList[TaskDescription]
      val availableCpus = offers.map(_.getParams.get("cpus").toInt)
      val availableMem = offers.map(_.getParams.get("mem").toInt)
      var launchedTask = false
      for (job <- activeJobsQueue) {
        do {
          launchedTask = false
          for (i <- 0 until offers.size.toInt) {
            try {
              job.slaveOffer(offers(i), availableCpus(i), availableMem(i)) match {
                case Some(task) =>
                  tasks.add(task)
                  availableCpus(i) -= task.getParams.get("cpus").toInt
                  availableMem(i) -= task.getParams.get("mem").toInt
                  launchedTask = true
                case None => {}
              }
            } catch {
              case e: Exception => logError("Exception in resourceOffer", e)
            }
          }
        } while (launchedTask)
      }
      val params = new JHashMap[String, String]
      params.put("timeout", "1")
      d.replyToOffer(oid, tasks, params) // TODO: use smaller timeout?
    }
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    synchronized {
      try {
        taskIdToJobId.get(status.getTaskId) match {
          case Some(jobId) =>
            if (activeJobs.contains(jobId)) {
              activeJobs(jobId).statusUpdate(status)
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
    logError("Mesos error: %s (error code: %d)".format(message, code))
    synchronized {
      if (activeJobs.size > 0) {
        // Have each job throw a SparkException with the error
        for ((jobId, activeJob) <- activeJobs) {
          try {
            activeJob.error(code, message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No jobs are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
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
