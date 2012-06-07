package spark.scheduler.mesos

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
import org.apache.mesos.Protos.{TaskInfo => MTaskInfo, _}

import spark._
import spark.scheduler._

/**
 * The main TaskScheduler implementation, which runs tasks on Mesos. Clients should first call
 * start(), then submit task sets through the runTasks method.
 */
class MesosScheduler(
    sc: SparkContext,
    master: String,
    frameworkName: String)
  extends TaskScheduler
  with MScheduler
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
      MesosScheduler.memoryStringToMb(System.getenv("SPARK_MEM"))
      // TODO: Might need to add some extra memory for the non-heap parts of the JVM
    } else {
      512
    }
  }

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = System.getProperty("spark.speculation.interval", "100").toLong

  // Lock used to wait for scheduler to be registered
  var isRegistered = false
  val registeredLock = new Object()

  val activeTaskSets = new HashMap[String, TaskSetManager]
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]

  val taskIdToTaskSetId = new HashMap[String, String]
  val taskIdToSlaveId = new HashMap[String, String]
  val taskSetTaskIds = new HashMap[String, HashSet[String]]

  // Incrementing Mesos task IDs
  var nextTaskId = 0

  // Driver for talking to Mesos
  var driver: SchedulerDriver = null

  // Which hosts in the cluster are alive (contains hostnames)
  val hostsAlive = new HashSet[String]

  // Which slave IDs we have executors on
  val slaveIdsWithExecutors = new HashSet[String]

  val slaveIdToHost = new HashMap[String, String]

  // JAR server, if any JARs were added by the user to the SparkContext
  var jarServer: HttpServer = null

  // URIs of JARs to pass to executor
  var jarUris: String = ""
  
  // Create an ExecutorInfo for our tasks
  val executorInfo = createExecutorInfo()

  // Listener object to pass upcalls into
  var listener: TaskSchedulerListener = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  override def setListener(listener: TaskSchedulerListener) { 
    this.listener = listener
  }

  def newTaskId(): TaskID = {
    val id = TaskID.newBuilder().setValue("" + nextTaskId).build()
    nextTaskId += 1
    return id
  }
  
  override def start() {
    new Thread("MesosScheduler driver") {
      setDaemon(true)
      override def run {
        val sched = MesosScheduler.this
        val fwInfo = FrameworkInfo.newBuilder().setUser("").setName(frameworkName).build()
        driver = new MesosSchedulerDriver(sched, fwInfo, master)
        try {
          val ret = driver.run()
          logInfo("driver.run() returned with code " + ret)
        } catch {
          case e: Exception => logError("driver.run() failed", e)
        }
      }
    }.start()
    if (System.getProperty("spark.speculation", "false") == "true") {
      new Thread("MesosScheduler speculation check") {
        setDaemon(true)
        override def run {
          waitForRegister()
          while (true) {
            try {
              Thread.sleep(SPECULATION_INTERVAL)
            } catch { case e: InterruptedException => {} }
            checkSpeculatableTasks()
          }
        }
      }.start()
    }
  }

  def createExecutorInfo(): ExecutorInfo = {
    val sparkHome = sc.getSparkHome match {
      case Some(path) =>
        path
      case None =>
        throw new SparkException("Spark home is not set; set it through the spark.home system " +
            "property, the SPARK_HOME environment variable or the SparkContext constructor")
    }
    // If the user added JARs to the SparkContext, create an HTTP server to ship them to executors
    if (sc.jars.size > 0) {
      createJarServer()
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
  
  def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.size + " tasks")
    waitForRegister()
    this.synchronized {
      val manager = new TaskSetManager(this, taskSet)
      activeTaskSets(taskSet.id) = manager
      activeTaskSetsQueue += manager
      taskSetTaskIds(taskSet.id) = new HashSet()
    }
    reviveOffers();
  }
  
  def taskSetFinished(manager: TaskSetManager) {
    this.synchronized {
      activeTaskSets -= manager.taskSet.id
      activeTaskSetsQueue -= manager
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskIdToSlaveId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds.remove(manager.taskSet.id)
    }
  }

  override def registered(d: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo) {
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

  override def disconnected(d: SchedulerDriver) {}

  override def reregistered(d: SchedulerDriver, masterInfo: MasterInfo) {}

  /**
   * Method called by Mesos to offer resources on slaves. We resond by asking our active task sets 
   * for tasks in order of priority. We fill each node with tasks in a round-robin manner so that
   * tasks are balanced across the cluster.
   */
  override def resourceOffers(d: SchedulerDriver, offers: JList[Offer]) {
    synchronized {
      // Mark each slave as alive and remember its hostname
      for (o <- offers) {
        slaveIdToHost(o.getSlaveId.getValue) = o.getHostname
        hostsAlive += o.getHostname
      }
      // Build a list of tasks to assign to each slave
      val tasks = offers.map(o => new JArrayList[MTaskInfo])
      val availableCpus = offers.map(o => getResource(o.getResourcesList(), "cpus"))
      val enoughMem = offers.map(o => {
        val mem = getResource(o.getResourcesList(), "mem")
        val slaveId = o.getSlaveId.getValue
        mem >= EXECUTOR_MEMORY || slaveIdsWithExecutors.contains(slaveId)
      })
      var launchedTask = false
      for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {
        do {
          launchedTask = false
          for (i <- 0 until offers.size if enoughMem(i)) {
            val sid = offers(i).getSlaveId.getValue
            val host = offers(i).getHostname
            manager.slaveOffer(sid, host, availableCpus(i)) match {
              case Some(task) => 
                tasks(i).add(task)
                val tid = task.getTaskId.getValue
                taskIdToTaskSetId(tid) = manager.taskSet.id
                taskSetTaskIds(manager.taskSet.id) += tid
                taskIdToSlaveId(tid) = sid
                slaveIdsWithExecutors += sid
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
    val tid = status.getTaskId.getValue
    var taskSetToUpdate: Option[TaskSetManager] = None
    var failedHost: Option[String] = None
    var taskFailed = false
    synchronized {
      try {
        if (status.getState == TaskState.TASK_LOST && taskIdToSlaveId.contains(tid)) {
          // We lost the executor on this slave, so remember that it's gone
          val slaveId = taskIdToSlaveId(tid)
          val host = slaveIdToHost(slaveId)
          if (hostsAlive.contains(host)) {
            slaveIdsWithExecutors -= slaveId
            hostsAlive -= host
            activeTaskSetsQueue.foreach(_.hostLost(host))
            failedHost = Some(host)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (activeTaskSets.contains(taskSetId)) {
              //activeTaskSets(taskSetId).statusUpdate(status)
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (isFinished(status.getState)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToSlaveId.remove(tid)
            }
            if (status.getState == TaskState.TASK_FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, because that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(status)
    }
    if (failedHost != None) {
      listener.hostLost(failedHost.get)
      reviveOffers();
    }
    if (taskFailed) {
      // Also revive offers if a task had failed for some reason other than host lost
      reviveOffers()
    }
  }

  override def error(d: SchedulerDriver, message: String) {
    logError("Mesos error: " + message)
    synchronized {
      if (activeTaskSets.size > 0) {
        // Have each task set throw a SparkException with the error
        for ((taskSetId, manager) <- activeTaskSets) {
          try {
            manager.error(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
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

  override def frameworkMessage(d: SchedulerDriver, e: ExecutorID, s: SlaveID, b: Array[Byte]) {}

  override def slaveLost(d: SchedulerDriver, s: SlaveID) {
    var failedHost: Option[String] = None
    synchronized {
      val slaveId = s.getValue
      val host = slaveIdToHost(slaveId)
      if (hostsAlive.contains(host)) {
        slaveIdsWithExecutors -= slaveId
        hostsAlive -= host
        activeTaskSetsQueue.foreach(_.hostLost(host))
        failedHost = Some(host)
      }
    }
    if (failedHost != None) {
      listener.hostLost(failedHost.get)
      reviveOffers();
    }
  }

  override def executorLost(d: SchedulerDriver, e: ExecutorID, s: SlaveID, status: Int) {
    logInfo("Executor lost: %s, marking slave %s as lost".format(e.getValue, s.getValue))
    slaveLost(d, s)
  }

  override def offerRescinded(d: SchedulerDriver, o: OfferID) {}

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      for (ts <- activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }
    }
    if (shouldRevive) {
      reviveOffers()
    }
  }

  def reviveOffers() {
    driver.reviveOffers()
  }
}

object MesosScheduler {
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
