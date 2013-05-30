package spark.scheduler.local

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import java.nio.ByteBuffer
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark._
import spark.TaskState.TaskState
import spark.executor.ExecutorURLClassLoader
import spark.scheduler._
import spark.scheduler.cluster._
import akka.actor._

/**
 * A simple TaskScheduler implementation that runs tasks locally in a thread pool. Optionally
 * the scheduler also allows each task to fail up to maxFailures times, which is useful for
 * testing fault recovery.
 */

private[spark] case class LocalReviveOffers()
private[spark] case class LocalStatusUpdate(taskId: Long, state: TaskState, serializedData: ByteBuffer)

private[spark] class LocalActor(localScheduler: LocalScheduler, var freeCores: Int) extends Actor with Logging {
  def receive = {
    case LocalReviveOffers =>
      logInfo("LocalReviveOffers")
      launchTask(localScheduler.resourceOffer(freeCores))
    case LocalStatusUpdate(taskId, state, serializeData) =>
      logInfo("LocalStatusUpdate")
      freeCores += 1
      localScheduler.statusUpdate(taskId, state, serializeData)
      launchTask(localScheduler.resourceOffer(freeCores))
  }

  def launchTask(tasks : Seq[TaskDescription]) {
    for (task <- tasks)
    {
      freeCores -= 1
      localScheduler.threadPool.submit(new Runnable {
        def run() {
          localScheduler.runTask(task.taskId,task.serializedTask)
        }
      })
    }
  }
}

private[spark] class LocalTaskSetManager(sched: LocalScheduler, val taskSet: TaskSet) extends TaskSetManager with Logging {
  var parent: Schedulable = null
  var weight: Int = 1
  var minShare: Int = 0
  var runningTasks: Int = 0
  var priority: Int = taskSet.priority
  var stageId: Int = taskSet.stageId
  var name: String = "TaskSet_"+taskSet.stageId.toString


  var failCount = new Array[Int](taskSet.tasks.size)
  val taskInfos = new HashMap[Long, TaskInfo]
  val numTasks = taskSet.tasks.size
  var numFinished = 0
  val ser = SparkEnv.get.closureSerializer.newInstance()
  val copiesRunning = new Array[Int](numTasks)
  val finished = new Array[Boolean](numTasks)
  val numFailures = new Array[Int](numTasks)

  def increaseRunningTasks(taskNum: Int): Unit = {
     runningTasks += taskNum
     if (parent != null) {
       parent.increaseRunningTasks(taskNum)
     }
  }

  def decreaseRunningTasks(taskNum: Int): Unit = {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  def addSchedulable(schedulable: Schedulable): Unit = {
    //nothing
  }

  def removeSchedulable(schedulable: Schedulable): Unit = {
    //nothing
  }

  def getSchedulableByName(name: String): Schedulable = {
    return null
  }

  def executorLost(executorId: String, host: String): Unit = {
    //nothing
  }

  def checkSpeculatableTasks(): Boolean = {
    return true
  }

  def getSortedTaskSetQueue(): ArrayBuffer[TaskSetManager] = {
    var sortedTaskSetQueue = new ArrayBuffer[TaskSetManager]
    sortedTaskSetQueue += this
    return sortedTaskSetQueue
  }

  def hasPendingTasks(): Boolean = {
    return true
  }

  def findTask(): Option[Int] = {
    for (i <- 0 to numTasks-1) {
      if (copiesRunning(i) == 0 && !finished(i)) {
        return Some(i)
      }
    }
    return None
  }

  def slaveOffer(execId: String, hostPort: String, availableCpus: Double, overrideLocality: TaskLocality.TaskLocality = null): Option[TaskDescription] = {
    Thread.currentThread().setContextClassLoader(sched.classLoader)
    SparkEnv.set(sched.env)
    logInfo("availableCpus:%d,numFinished:%d,numTasks:%d".format(availableCpus.toInt, numFinished, numTasks))
    if (availableCpus > 0 && numFinished < numTasks) {
      findTask() match {
        case Some(index) =>
          logInfo(taskSet.tasks(index).toString)
          val taskId = sched.attemptId.getAndIncrement()
          val task = taskSet.tasks(index)
          logInfo("taskId:%d,task:%s".format(index,task))
          val info = new TaskInfo(taskId, index, System.currentTimeMillis(), "local", "local:1", TaskLocality.NODE_LOCAL)
          taskInfos(taskId) = info
          val bytes = Task.serializeWithDependencies(task, sched.sc.addedFiles, sched.sc.addedJars, ser)
          logInfo("Size of task " + taskId + " is " + bytes.limit + " bytes")
          val taskName = "task %s:%d".format(taskSet.id, index)
          copiesRunning(index) += 1
          increaseRunningTasks(1)
          return Some(new TaskDescription(taskId, null, taskName, bytes))
        case None => {}
      }
    }
    return None
  }

  def numPendingTasksForHostPort(hostPort: String): Int = {
    return 0
  }

  def numRackLocalPendingTasksForHost(hostPort :String): Int = {
    return 0
  }

  def numPendingTasksForHost(hostPort: String): Int = {
    return 0
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    state match {
      case TaskState.FINISHED =>
        taskEnded(tid, state, serializedData)
      case TaskState.FAILED =>
        taskFailed(tid, state, serializedData)
      case _ => {}
    }
  }

  def taskEnded(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    val index = info.index
    val task = taskSet.tasks(index)
    info.markSuccessful()
    val result = ser.deserialize[TaskResult[_]](serializedData, getClass.getClassLoader)
    result.metrics.resultSize = serializedData.limit()
    sched.listener.taskEnded(task, Success, result.value, result.accumUpdates, info, result.metrics)
    numFinished += 1
    decreaseRunningTasks(1)
    finished(index) = true
    if (numFinished == numTasks) {
      sched.taskSetFinished(this)
    }
  }

  def taskFailed(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    val info = taskInfos(tid)
    val index = info.index
    val task = taskSet.tasks(index)
    info.markFailed()
    decreaseRunningTasks(1)
    val reason: ExceptionFailure = ser.deserialize[ExceptionFailure](serializedData, getClass.getClassLoader)
    if (!finished(index)) {
      copiesRunning(index) -= 1
      numFailures(index) += 1
      val locs = reason.stackTrace.map(loc => "\tat %s".format(loc.toString))
      logInfo("Loss was due to %s\n%s\n%s".format(reason.className, reason.description, locs.mkString("\n")))
      if (numFailures(index) > 4) {
        val errorMessage = "Task %s:%d failed more than %d times; aborting job %s".format(taskSet.id, index, 4, reason.description)
        //logError(errorMessage)
        //sched.listener.taskEnded(task, reason, null, null, info, null)
        sched.listener.taskSetFailed(taskSet, errorMessage)
        sched.taskSetFinished(this)
        decreaseRunningTasks(runningTasks)
      }
    }
  }

  def error(message: String) {
  }
}

private[spark] class LocalScheduler(threads: Int, val maxFailures: Int, val sc: SparkContext)
  extends TaskScheduler
  with Logging {

  var attemptId = new AtomicInteger(0)
  var threadPool = Utils.newDaemonFixedThreadPool(threads)
  val env = SparkEnv.get
  var listener: TaskSchedulerListener = null

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  val classLoader = new ExecutorURLClassLoader(Array(), Thread.currentThread.getContextClassLoader)

  var schedulableBuilder: SchedulableBuilder = null
  var rootPool: Pool = null
  val activeTaskSets = new HashMap[String, TaskSetManager]
  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  var localActor: ActorRef = null
  // TODO: Need to take into account stage priority in scheduling

  override def start() {
    //default scheduler is FIFO
    val schedulingMode = System.getProperty("spark.cluster.schedulingmode", "FIFO")
    //temporarily set rootPool name to empty
    rootPool = new Pool("", SchedulingMode.withName(schedulingMode), 0, 0)
    schedulableBuilder = {
      schedulingMode match {
        case "FIFO" =>
          new FIFOSchedulableBuilder(rootPool)
        case "FAIR" =>
          new FairSchedulableBuilder(rootPool)
      }
    }
    schedulableBuilder.buildPools()

    //val properties = new ArrayBuffer[(String, String)]
    localActor = env.actorSystem.actorOf(
      Props(new LocalActor(this, threads)), "Test")
  }

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  override def submitTasks(taskSet: TaskSet) {
    var manager = new LocalTaskSetManager(this, taskSet)
    schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)
    activeTaskSets(taskSet.id) = manager
    taskSetTaskIds(taskSet.id) = new HashSet[Long]()
    localActor ! LocalReviveOffers
  }

  def resourceOffer(freeCores: Int): Seq[TaskDescription] = {
    var freeCpuCores = freeCores
    val tasks = new ArrayBuffer[TaskDescription](freeCores)
    val sortedTaskSetQueue = rootPool.getSortedTaskSetQueue()
    for (manager <- sortedTaskSetQueue) {
       logInfo("parentName:%s,name:%s,runningTasks:%s".format(manager.parent.name, manager.name, manager.runningTasks))
    }

    var launchTask = false
    for (manager <- sortedTaskSetQueue) {
        do {
          launchTask = false
          logInfo("freeCores is" + freeCpuCores)
          manager.slaveOffer(null,null,freeCpuCores) match {
            case Some(task) =>
            tasks += task
            taskIdToTaskSetId(task.taskId) = manager.taskSet.id
            taskSetTaskIds(manager.taskSet.id) += task.taskId
            freeCpuCores -= 1
            launchTask = true
            case None => {}
          }
        } while(launchTask)
    }
    return tasks
  }

  def taskSetFinished(manager: TaskSetManager) {
    activeTaskSets -= manager.taskSet.id
    manager.parent.removeSchedulable(manager)
    logInfo("Remove TaskSet %s from pool %s".format(manager.taskSet.id, manager.parent.name))
    taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
    taskSetTaskIds -= manager.taskSet.id
  }

  def runTask(taskId: Long, bytes: ByteBuffer) {
    logInfo("Running " + taskId)
    val info = new TaskInfo(taskId, 0 , System.currentTimeMillis(), "local", "local:1", TaskLocality.NODE_LOCAL)
    // Set the Spark execution environment for the worker thread
    SparkEnv.set(env)
    val ser = SparkEnv.get.closureSerializer.newInstance()
    try {
      Accumulators.clear()
      Thread.currentThread().setContextClassLoader(classLoader)

      // Serialize and deserialize the task so that accumulators are changed to thread-local ones;
      // this adds a bit of unnecessary overhead but matches how the Mesos Executor works.
      val (taskFiles, taskJars, taskBytes) = Task.deserializeWithDependencies(bytes)
      updateDependencies(taskFiles, taskJars)   // Download any files added with addFile
      val deserStart = System.currentTimeMillis()
      val deserializedTask = ser.deserialize[Task[_]](
        taskBytes, Thread.currentThread.getContextClassLoader)
      val deserTime = System.currentTimeMillis() - deserStart

      // Run it
      val result: Any = deserializedTask.run(taskId)

      // Serialize and deserialize the result to emulate what the Mesos
      // executor does. This is useful to catch serialization errors early
      // on in development (so when users move their local Spark programs
      // to the cluster, they don't get surprised by serialization errors).
      val serResult = ser.serialize(result)
      deserializedTask.metrics.get.resultSize = serResult.limit()
      val resultToReturn = ser.deserialize[Any](serResult)
      val accumUpdates = ser.deserialize[collection.mutable.Map[Long, Any]](
        ser.serialize(Accumulators.values))
      logInfo("Finished " + taskId)
      deserializedTask.metrics.get.executorRunTime = deserTime.toInt//info.duration.toInt  //close enough
      deserializedTask.metrics.get.executorDeserializeTime = deserTime.toInt

      val taskResult = new TaskResult(result, accumUpdates, deserializedTask.metrics.getOrElse(null))
      val serializedResult = ser.serialize(taskResult)
      localActor ! LocalStatusUpdate(taskId, TaskState.FINISHED, serializedResult)
    } catch {
      case t: Throwable => {
        val failure = new ExceptionFailure(t.getClass.getName, t.toString, t.getStackTrace)
        localActor ! LocalStatusUpdate(taskId, TaskState.FAILED, ser.serialize(failure))
        }
      }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: HashMap[String, Long], newJars: HashMap[String, Long]) {
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
        currentFiles(name) = timestamp
      }

      for ((name, timestamp) <- newJars if currentJars.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory))
        currentJars(name) = timestamp
        // Add it to our class loader
        val localName = name.split("/").last
        val url = new File(SparkFiles.getRootDirectory, localName).toURI.toURL
        if (!classLoader.getURLs.contains(url)) {
          logInfo("Adding " + url + " to class loader")
          classLoader.addURL(url)
        }
      }
    }
  }

  def statusUpdate(taskId :Long, state: TaskState, serializedData: ByteBuffer)
  {
    val taskSetId = taskIdToTaskSetId(taskId)
    val taskSetManager = activeTaskSets(taskSetId)
    taskSetManager.statusUpdate(taskId, state, serializedData)
  }

   override def stop() {
    threadPool.shutdownNow()
  }

  override def defaultParallelism() = threads
}
