package spark.scheduler.cluster

import java.io.{File, FileInputStream, FileOutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

import spark._
import spark.TaskState.TaskState
import spark.scheduler._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import java.util.{TimerTask, Timer}

/**
 * The main TaskScheduler implementation, for running tasks on a cluster. Clients should first call
 * start(), then submit task sets through the runTasks method.
 */
private[spark] class ClusterScheduler(val sc: SparkContext)
  extends TaskScheduler
  with Logging {

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL = System.getProperty("spark.speculation.interval", "100").toLong
  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT = System.getProperty("spark.starvation.timeout", "15000").toLong

  val activeTaskSets = new HashMap[String, TaskSetManager]
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  var hasReceivedTask = false
  var hasLaunchedTask = false
  val starvationTimer = new Timer(true)

  // Incrementing Mesos task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  val executorsByHost = new HashMap[String, HashSet[String]]

  val executorIdToHost = new HashMap[String, String]

  // JAR server, if any JARs were added by the user to the SparkContext
  var jarServer: HttpServer = null

  // URIs of JARs to pass to executor
  var jarUris: String = ""

  // Listener object to pass upcalls into
  var listener: TaskSchedulerListener = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  override def setListener(listener: TaskSchedulerListener) {
    this.listener = listener
  }

  def initialize(context: SchedulerBackend) {
    backend = context
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (System.getProperty("spark.speculation", "false") == "true") {
      new Thread("ClusterScheduler speculation check") {
        setDaemon(true)

        override def run() {
          while (true) {
            try {
              Thread.sleep(SPECULATION_INTERVAL)
            } catch {
              case e: InterruptedException => {}
            }
            checkSpeculatableTasks()
          }
        }
      }.start()
    }
  }

  override def submitTasks(taskSet: TaskSet) {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks")
    this.synchronized {
      val manager = new TaskSetManager(this, taskSet)
      activeTaskSets(taskSet.id) = manager
      activeTaskSetsQueue += manager
      taskSetTaskIds(taskSet.id) = new HashSet[Long]()

      if (hasReceivedTask == false) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run() {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT, STARVATION_TIMEOUT)
      }
      hasReceivedTask = true;
    }
    backend.reviveOffers()
  }

  def taskSetFinished(manager: TaskSetManager) {
    this.synchronized {
      activeTaskSets -= manager.taskSet.id
      activeTaskSetsQueue -= manager
      taskIdToTaskSetId --= taskSetTaskIds(manager.taskSet.id)
      taskIdToExecutorId --= taskSetTaskIds(manager.taskSet.id)
      taskSetTaskIds.remove(manager.taskSet.id)
    }
  }

  /**
   * Called by cluster manager to offer resources on slaves. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(offers: Seq[WorkerOffer]): Seq[Seq[TaskDescription]] = {
    synchronized {
      SparkEnv.set(sc.env)
      // Mark each slave as alive and remember its hostname
      for (o <- offers) {
        executorIdToHost(o.executorId) = o.hostname
        if (!executorsByHost.contains(o.hostname)) {
          executorsByHost(o.hostname) = new HashSet()
        }
      }
      // Build a list of tasks to assign to each slave
      val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
      val availableCpus = offers.map(o => o.cores).toArray
      var launchedTask = false
      for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {
        do {
          launchedTask = false
          for (i <- 0 until offers.size) {
            val execId = offers(i).executorId
            val host = offers(i).hostname
            manager.slaveOffer(execId, host, availableCpus(i)) match {
              case Some(task) =>
                tasks(i) += task
                val tid = task.taskId
                taskIdToTaskSetId(tid) = manager.taskSet.id
                taskSetTaskIds(manager.taskSet.id) += tid
                taskIdToExecutorId(tid) = execId
                activeExecutorIds += execId
                executorsByHost(host) += execId
                availableCpus(i) -= 1
                launchedTask = true

              case None => {}
            }
          }
        } while (launchedTask)
      }
      if (tasks.size > 0) {
        hasLaunchedTask = true
      }
      return tasks
    }
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer) {
    var taskSetToUpdate: Option[TaskSetManager] = None
    var failedExecutor: Option[String] = None
    var taskFailed = false
    synchronized {
      try {
        if (state == TaskState.LOST && taskIdToExecutorId.contains(tid)) {
          // We lost this entire executor, so remember that it's gone
          val execId = taskIdToExecutorId(tid)
          if (activeExecutorIds.contains(execId)) {
            removeExecutor(execId)
            failedExecutor = Some(execId)
          }
        }
        taskIdToTaskSetId.get(tid) match {
          case Some(taskSetId) =>
            if (activeTaskSets.contains(taskSetId)) {
              taskSetToUpdate = Some(activeTaskSets(taskSetId))
            }
            if (TaskState.isFinished(state)) {
              taskIdToTaskSetId.remove(tid)
              if (taskSetTaskIds.contains(taskSetId)) {
                taskSetTaskIds(taskSetId) -= tid
              }
              taskIdToExecutorId.remove(tid)
            }
            if (state == TaskState.FAILED) {
              taskFailed = true
            }
          case None =>
            logInfo("Ignoring update from TID " + tid + " because its task set is gone")
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the task set and DAGScheduler without holding a lock on this, since that can deadlock
    if (taskSetToUpdate != None) {
      taskSetToUpdate.get.statusUpdate(tid, state, serializedData)
    }
    if (failedExecutor != None) {
      listener.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
    if (taskFailed) {
      // Also revive offers if a task had failed for some reason other than host lost
      backend.reviveOffers()
    }
  }

  def error(message: String) {
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
        logError("Exiting due to error from cluster scheduler: " + message)
        System.exit(1)
      }
    }
  }

  override def stop() {
    if (backend != null) {
      backend.stop()
    }
    if (jarServer != null) {
      jarServer.stop()
    }
  }

  override def defaultParallelism() = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks() {
    var shouldRevive = false
    synchronized {
      for (ts <- activeTaskSetsQueue) {
        shouldRevive |= ts.checkSpeculatableTasks()
      }
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None
    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val host = executorIdToHost(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, host, reason))
        removeExecutor(executorId)
        failedExecutor = Some(executorId)
      } else {
         // We may get multiple executorLost() calls with different loss reasons. For example, one
         // may be triggered by a dropped connection from the slave while another may be a report
         // of executor termination from Mesos. We produce log messages for both so we eventually
         // report the termination reason.
         logError("Lost an executor " + executorId + " (already removed): " + reason)
      }
    }
    // Call listener.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor != None) {
      listener.executorLost(failedExecutor.get)
      backend.reviveOffers()
    }
  }

  /** Get a list of hosts that currently have executors */
  def hostsAlive: scala.collection.Set[String] = executorsByHost.keySet

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val host = executorIdToHost(executorId)
    val execs = executorsByHost.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHost -= host
    }
    executorIdToHost -= executorId
    activeTaskSetsQueue.foreach(_.executorLost(executorId, host))
  }
}
