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

/**
 * The main Scheduler implementation, which runs jobs on Mesos. Clients should
 * first call start(), then submit tasks through the runTasks method.
 */
private class MesosScheduler(
  sc: SparkContext, master: String, frameworkName: String, execArg: Array[Byte])
extends MScheduler with spark.Scheduler with Logging
{
  // Environment variables to pass to our executors
  val ENV_VARS_TO_SEND_TO_EXECUTORS = Array(
    "SPARK_MEM",
    "SPARK_CLASSPATH",
    "SPARK_LIBRARY_PATH"
  )

  // Lock used to wait for  scheduler to be registered
  private var isRegistered = false
  private val registeredLock = new Object()

  private var activeJobs = new HashMap[Int, Job]
  private var activeJobsQueue = new Queue[Job]

  private var taskIdToJobId = new HashMap[Int, Int]

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
  
  override def getExecutorInfo(d: SchedulerDriver): ExecutorInfo = {
    val sparkHome = sc.getSparkHome match {
      case Some(path) => path
      case None =>
        throw new SparkException("Spark home is not set; either set the " +
          "spark.home system property or the SPARK_HOME environment variable " +
          "or call SparkContext.setSparkHome")
    }
    val execScript = new File(sparkHome, "spark-executor").getCanonicalPath
    val params = new JHashMap[String, String]
    for (key <- ENV_VARS_TO_SEND_TO_EXECUTORS) {
      if (System.getenv(key) != null)
        params("env." + key) = System.getenv(key)
    }
    new ExecutorInfo(execScript, execArg)
  }

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
      return myJob.join();
    } finally {
      this.synchronized {
        this.activeJobs.remove(myJob.jobId)
        this.activeJobsQueue.dequeueAll(x => (x == myJob))
      }
    }
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

  /**
   * Method called by Mesos to offer resources on slaves. We resond by asking
   * our active jobs for tasks in FIFO order. We fill each node with tasks in
   * a round-robin manner so that tasks are balanced across the cluster.
   */
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
                  taskIdToJobId(task.getTaskId) = job.getId
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

  // Check whether a Mesos task state represents a finished task
  def isFinished(state: TaskState) = {
    state == TaskState.TASK_FINISHED ||
    state == TaskState.TASK_FAILED ||
    state == TaskState.TASK_KILLED ||
    state == TaskState.TASK_LOST
  }

  override def statusUpdate(d: SchedulerDriver, status: TaskStatus) {
    synchronized {
      try {
        taskIdToJobId.get(status.getTaskId) match {
          case Some(jobId) =>
            if (activeJobs.contains(jobId)) {
              activeJobs(jobId).statusUpdate(status)
            }
            if (isFinished(status.getState)) {
              taskIdToJobId.remove(status.getTaskId)
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
