package spark

import java.io.{File, FileInputStream, FileOutputStream}
import java.util.{ArrayList => JArrayList}
import java.util.{List => JList}
import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map
import scala.collection.mutable.PriorityQueue
import scala.collection.JavaConversions._
import scala.math.Ordering

import com.google.protobuf.ByteString

import org.apache.mesos.{Scheduler => MScheduler}
import org.apache.mesos._
import org.apache.mesos.Protos._

/**
 * The main Scheduler implementation, which runs jobs on Mesos. Clients should first call start(),
 * then submit tasks through the runTasks method.
 */
private class MesosScheduler(
    sc: SparkContext,
    master: String,
    frameworkName: String)
  extends MScheduler
  with DAGScheduler
  with Logging {
  
  // Environment variables to pass to our executors
  val ENV_VARS_TO_SEND_TO_EXECUTORS = Array(
    "SPARK_MEM",
    "SPARK_CLASSPATH",
    "SPARK_LIBRARY_PATH",
    "SPARK_JAVA_OPTS"
  )

  // Memory used by each executor (in megabytes)
  val EXECUTOR_MEMORY = {
    if (System.getenv("SPARK_MEM") != null) {
      memoryStringToMb(System.getenv("SPARK_MEM"))
      // TODO: Might need to add some extra memory for the non-heap parts of the JVM
    } else {
      512
    }
  }

  // Lock used to wait for scheduler to be registered
  private var isRegistered = false
  private val registeredLock = new Object()

  private val activeJobs = new HashMap[Int, Job]
  private var activeJobsQueue = new PriorityQueue[Job]()(jobOrdering)

  private val taskIdToJobId = new HashMap[String, Int]
  private val taskIdToSlaveId = new HashMap[String, String]
  private val jobTasks = new HashMap[Int, HashSet[String]]

  // Incrementing job and task IDs
  private var nextJobId = 0
  private var nextTaskId = 0

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  // Which nodes we have executors on
  private val slavesWithExecutors = new HashSet[String]

  // JAR server, if any JARs were added by the user to the SparkContext
  var jarServer: HttpServer = null

  // URIs of JARs to pass to executor
  var jarUris: String = ""

  // Sorts jobs in reverse order of run ID for use in our priority queue (so lower IDs run first)
  private val jobOrdering = new Ordering[Job] {
    override def compare(j1: Job, j2: Job): Int = {
      return j2.runId - j1.runId
    }
  }
  
  def newJobId(): Int = this.synchronized {
    val id = nextJobId
    nextJobId += 1
    return id
  }

  def newTaskId(): TaskID = {
    val id = "" + nextTaskId;
    nextTaskId += 1;
    return TaskID.newBuilder().setValue(id).build()
  }
  
  override def start() {
    if (sc.jars.size > 0) {
      // If the user added any JARS to the SparkContext, create an HTTP server
      // to serve them to our executors
      createJarServer()
    }
    new Thread("Spark scheduler") {
      setDaemon(true)
      override def run {
        val sched = MesosScheduler.this
        driver = new MesosSchedulerDriver(sched, frameworkName, getExecutorInfo, master)
        try {
          val ret = driver.run()
          logInfo("driver.run() returned with code " + ret)
        } catch {
          case e: Exception => logError("driver.run() failed", e)
        }
      }
    }.start
  }

  def getExecutorInfo(): ExecutorInfo = {
    val sparkHome = sc.getSparkHome match {
      case Some(path) => path
      case None =>
        throw new SparkException("Spark home is not set; set it through the spark.home system " +
        		"property, the SPARK_HOME environment variable or the SparkContext constructor")
    }
    val execScript = new File(sparkHome, "spark-executor").getCanonicalPath
    val environment = Environment.newBuilder()
    for (key <- ENV_VARS_TO_SEND_TO_EXECUTORS) {
      if (System.getenv(key) != null) {
        environment.addVariables(Environment.Variable.newBuilder()
          .setName(key)
          .setValue(System.getenv(key))
          .build())
      }
    }
    environment.build()
    val memory = Resource.newBuilder()
      .setName("mem")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(EXECUTOR_MEMORY).build())
      .build()
    val command = CommandInfo.newBuilder()
      .setValue(execScript)
      .setEnvironment(environment)
      .build()
    ExecutorInfo.newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue("default").build())
      .setCommand(command)
      .setData(ByteString.copyFrom(createExecArg()))
      .addResources(memory)
      .build()
  }
  
  def submitTasks(tasks: Seq[Task[_]], runId: Int) {
    logInfo("Got a job with " + tasks.size + " tasks")
    waitForRegister()
    this.synchronized {
      val jobId = newJobId()
      val myJob = new SimpleJob(this, tasks, runId, jobId)
      activeJobs(jobId) = myJob
      activeJobsQueue += myJob
      logInfo("Adding job with ID " + jobId)
      jobTasks(jobId) = new HashSet()
    }
    driver.reviveOffers();
  }
  
  def jobFinished(job: Job) {
    this.synchronized {
      activeJobs -= job.jobId
      activeJobsQueue = activeJobsQueue.filterNot(_ == job)
      taskIdToJobId --= jobTasks(job.jobId)
      taskIdToSlaveId --= jobTasks(job.jobId)
      jobTasks.remove(job.jobId)
    }
  }

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID) {
    logInfo("Registered as framework ID " + frameworkId.getValue)
    registeredLock.synchronized {
      isRegistered = true
      registeredLock.notifyAll()
    }
  }
  
  override def waitForRegister() {
    registeredLock.synchronized {
      while (!isRegistered) {
        registeredLock.wait()
      }
    }
  }

  /**
   * Method called by Mesos to offer resources on slaves. We resond by asking our active jobs for 
   * tasks in FIFO order. We fill each node with tasks in a round-robin manner so that tasks are
   * balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    synchronized {
      val tasks = offers.map(o => new JArrayList[TaskDescription])
      val availableCpus = offers.map(o => getResource(o.getResourcesList(), "cpus"))
      val enoughMem = offers.map(o => {
        val mem = getResource(o.getResourcesList(), "mem")
        val slaveId = o.getSlaveId.getValue
        mem >= EXECUTOR_MEMORY || slavesWithExecutors.contains(slaveId)
      })
      var launchedTask = false
      for (job <- activeJobsQueue) {
        do {
          launchedTask = false
          for (i <- 0 until offers.size if enoughMem(i)) {
            job.slaveOffer(offers(i), availableCpus(i)) match {
              case Some(task) => 
                tasks(i).add(task)
                val tid = task.getTaskId.getValue
                val sid = offers(i).getSlaveId.getValue
                taskIdToJobId(tid) = job.jobId
                jobTasks(job.jobId) += tid
                taskIdToSlaveId(tid) = sid
                slavesWithExecutors += sid
                availableCpus(i) -= getResource(task.getResourcesList(), "cpus")
                launchedTask = true
                
              case None => {}
            }
          }
        } while (launchedTask)
      }
      val filters = Filters.newBuilder().setRefuseSeconds(1).build() // TODO: lower timeout?
      for (i <- 0 until offers.size) {
        d.launchTasks(offers(i).getId(), tasks(i), filters)
      }
    }
  }

  // Helper function to pull out a resource from a Mesos Resources protobuf
  def getResource(res: JList[Resource], name: String): Double = {
    for (r <- res if r.getName == name) {
      return r.getScalar.getValue
    }
    
    throw new IllegalArgumentException("No resource called " + name + " in " + res)
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
        val tid = status.getTaskId.getValue
        if (status.getState == TaskState.TASK_LOST 
            && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          slavesWithExecutors -= taskIdToSlaveId(tid)
        }
        taskIdToJobId.get(tid) match {
          case Some(jobId) =>
            if (activeJobs.contains(jobId)) {
              activeJobs(jobId).statusUpdate(status)
            }
            if (isFinished(status.getState)) {
              taskIdToJobId.remove(tid)
              if (jobTasks.contains(jobId)) {
                jobTasks(jobId) -= tid
              }
              taskIdToSlaveId.remove(tid)
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its job is gone")
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
    if (driver != null) {
      driver.stop()
    }
    if (jarServer != null) {
      jarServer.stop()
    }
  }

  // TODO: query Mesos for number of cores
  override def defaultParallelism() =
    System.getProperty("spark.default.parallelism", "8").toInt

  // Create a server for all the JARs added by the user to SparkContext.
  // We first copy the JARs to a temp directory for easier server setup.
  private def createJarServer() {
    val jarDir = Utils.createTempDir()
    logInfo("Temp directory for JARs: " + jarDir)
    val filenames = ArrayBuffer[String]()
    // Copy each JAR to a unique filename in the jarDir
    for ((path, index) <- sc.jars.zipWithIndex) {
      val file = new File(path)
      if (file.exists) {
        val filename = index + "_" + file.getName
        copyFile(file, new File(jarDir, filename))
        filenames += filename
      }
    }
    // Create the server
    jarServer = new HttpServer(jarDir)
    jarServer.start()
    // Build up the jar URI list
    val serverUri = jarServer.uri
    jarUris = filenames.map(f => serverUri + "/" + f).mkString(",")
    logInfo("JAR server started at " + serverUri)
  }

  // Copy a file on the local file system
  private def copyFile(source: File, dest: File) {
    val in = new FileInputStream(source)
    val out = new FileOutputStream(dest)
    Utils.copyStream(in, out, true)
  }

  // Create and serialize the executor argument to pass to Mesos.
  // Our executor arg is an array containing all the spark.* system properties
  // in the form of (String, String) pairs.
  private def createExecArg(): Array[Byte] = {
    val props = new HashMap[String, String]
    val iter = System.getProperties.entrySet.iterator
    while (iter.hasNext) {
      val entry = iter.next
      val (key, value) = (entry.getKey.toString, entry.getValue.toString)
      if (key.startsWith("spark.")) {
        props(key) = value
      }
    }
    // Set spark.jar.uris to our JAR URIs, regardless of system property
    props("spark.jar.uris") = jarUris
    // Serialize the map as an array of (String, String) pairs
    return Utils.serialize(props.toArray)
  }

  override def frameworkMessage(
      d: SchedulerDriver, 
      s: SlaveID,
      e: ExecutorID,
      b: Array[Byte]) {}

  override def slaveLost(d: SchedulerDriver, s: SlaveID) {
    slavesWithExecutors.remove(s.getValue)
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  /**
   * Convert a Java memory parameter passed to -Xmx (such as 300m or 1g) to a number of megabytes. 
   * This is used to figure out how much memory to claim from Mesos based on the SPARK_MEM 
   * environment variable.
   */
  def memoryStringToMb(str: String): Int = {
    val lower = str.toLowerCase
    if (lower.endsWith("k")) {
      (lower.substring(0, lower.length-1).toLong / 1024).toInt
    } else if (lower.endsWith("m")) {
      lower.substring(0, lower.length-1).toInt
    } else if (lower.endsWith("g")) {
      lower.substring(0, lower.length-1).toInt * 1024
    } else if (lower.endsWith("t")) {
      lower.substring(0, lower.length-1).toInt * 1024 * 1024
    } else {// no suffix, so it's just a number in bytes
      (lower.toLong / 1024 / 1024).toInt
    }
  }
}
