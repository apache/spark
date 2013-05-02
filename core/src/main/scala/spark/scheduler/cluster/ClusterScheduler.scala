package spark.scheduler.cluster

import java.lang.{Boolean => JBoolean}

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
  // How often to revive offers in case there are pending tasks - that is how often to try to get
  // tasks scheduled in case there are nodes available : default 0 is to disable it - to preserve existing behavior
  // Note that this is required due to delayed scheduling due to data locality waits, etc.
  // TODO: rename property ?
  val TASK_REVIVAL_INTERVAL = System.getProperty("spark.tasks.revive.interval", "0").toLong

  /*
   This property controls how aggressive we should be to modulate waiting for node local task scheduling.
   To elaborate, currently there is a time limit (3 sec def) to ensure that spark attempts to wait for node locality of tasks before
   scheduling on other nodes. We have modified this in yarn branch such that offers to task set happen in prioritized order :
   node-local, rack-local and then others
   But once all available node local (and no pref) tasks are scheduled, instead of waiting for 3 sec before
   scheduling to other nodes (which degrades performance for time sensitive tasks and on larger clusters), we can
   modulate that : to also allow rack local nodes or any node. The default is still set to HOST - so that previous behavior is
   maintained. This is to allow tuning the tension between pulling rdd data off node and scheduling computation asap.

   TODO: rename property ? The value is one of
   - NODE_LOCAL (default, no change w.r.t current behavior),
   - RACK_LOCAL and
   - ANY

   Note that this property makes more sense when used in conjugation with spark.tasks.revive.interval > 0 : else it is not very effective.

   Additional Note: For non trivial clusters, there is a 4x - 5x reduction in running time (in some of our experiments) based on whether
   it is left at default NODE_LOCAL, RACK_LOCAL (if cluster is configured to be rack aware) or ANY.
   If cluster is rack aware, then setting it to RACK_LOCAL gives best tradeoff and a 3x - 4x performance improvement while minimizing IO impact.
   Also, it brings down the variance in running time drastically.
    */
  val TASK_SCHEDULING_AGGRESSION = TaskLocality.parse(System.getProperty("spark.tasks.schedule.aggression", "NODE_LOCAL"))

  val activeTaskSets = new HashMap[String, TaskSetManager]
  var activeTaskSetsQueue = new ArrayBuffer[TaskSetManager]

  val taskIdToTaskSetId = new HashMap[Long, String]
  val taskIdToExecutorId = new HashMap[Long, String]
  val taskSetTaskIds = new HashMap[String, HashSet[Long]]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer(true)

  // Incrementing Mesos task IDs
  val nextTaskId = new AtomicLong(0)

  // Which executor IDs we have executors on
  val activeExecutorIds = new HashSet[String]

  // TODO: We might want to remove this and merge it with execId datastructures - but later.
  // Which hosts in the cluster are alive (contains hostPort's) - used for process local and node local task locality.
  private val hostPortsAlive = new HashSet[String]
  private val hostToAliveHostPorts = new HashMap[String, HashSet[String]]

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  private val executorsByHostPort = new HashMap[String, HashSet[String]]

  private val executorIdToHostPort = new HashMap[String, String]

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
    // resolve executorId to hostPort mapping.
    def executorToHostPort(executorId: String, defaultHostPort: String): String = {
      executorIdToHostPort.getOrElse(executorId, defaultHostPort)
    }

    // Unfortunately, this means that SparkEnv is indirectly referencing ClusterScheduler
    // Will that be a design violation ?
    SparkEnv.get.executorIdToHostPort = Some(executorToHostPort)
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start() {
    backend.start()

    if (JBoolean.getBoolean("spark.speculation")) {
      new Thread("ClusterScheduler speculation check") {
        setDaemon(true)

        override def run() {
          logInfo("Starting speculative execution thread")
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


    // Change to always run with some default if TASK_REVIVAL_INTERVAL <= 0 ?
    if (TASK_REVIVAL_INTERVAL > 0) {
      new Thread("ClusterScheduler task offer revival check") {
        setDaemon(true)

        override def run() {
          logInfo("Starting speculative task offer revival thread")
          while (true) {
            try {
              Thread.sleep(TASK_REVIVAL_INTERVAL)
            } catch {
              case e: InterruptedException => {}
            }

            if (hasPendingTasks()) backend.reviveOffers()
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
        // DEBUG Code
        Utils.checkHostPort(o.hostPort)

        executorIdToHostPort(o.executorId) = o.hostPort
        if (! executorsByHostPort.contains(o.hostPort)) {
          executorsByHostPort(o.hostPort) = new HashSet[String]()
        }

        hostPortsAlive += o.hostPort
        hostToAliveHostPorts.getOrElseUpdate(Utils.parseHostPort(o.hostPort)._1, new HashSet[String]).add(o.hostPort)
        executorGained(o.executorId, o.hostPort)
      }
      // Build a list of tasks to assign to each slave
      val tasks = offers.map(o => new ArrayBuffer[TaskDescription](o.cores))
      // merge availableCpus into nodeToAvailableCpus block ?
      val availableCpus = offers.map(o => o.cores).toArray
      val nodeToAvailableCpus = {
        val map = new HashMap[String, Int]()
        for (offer <- offers) {
          val hostPort = offer.hostPort
          val cores = offer.cores
          // DEBUG code
          Utils.checkHostPort(hostPort)

          val host = Utils.parseHostPort(hostPort)._1

          map.put(host, map.getOrElse(host, 0) + cores)
        }

        map
      }
      var launchedTask = false


      for (manager <- activeTaskSetsQueue.sortBy(m => (m.taskSet.priority, m.taskSet.stageId))) {

        // Split offers based on node local, rack local and off-rack tasks.
        val processLocalOffers = new HashMap[String, ArrayBuffer[Int]]()
        val nodeLocalOffers = new HashMap[String, ArrayBuffer[Int]]()
        val rackLocalOffers = new HashMap[String, ArrayBuffer[Int]]()
        val otherOffers = new HashMap[String, ArrayBuffer[Int]]()

        for (i <- 0 until offers.size) {
          val hostPort = offers(i).hostPort
          // DEBUG code
          Utils.checkHostPort(hostPort)

          val numProcessLocalTasks =  math.max(0, math.min(manager.numPendingTasksForHostPort(hostPort), availableCpus(i)))
          if (numProcessLocalTasks > 0){
            val list = processLocalOffers.getOrElseUpdate(hostPort, new ArrayBuffer[Int])
            for (j <- 0 until numProcessLocalTasks) list += i
          }

          val host = Utils.parseHostPort(hostPort)._1
          val numNodeLocalTasks =  math.max(0,
            // Remove process local tasks (which are also host local btw !) from this
            math.min(manager.numPendingTasksForHost(hostPort) - numProcessLocalTasks, nodeToAvailableCpus(host)))
          if (numNodeLocalTasks > 0){
            val list = nodeLocalOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            for (j <- 0 until numNodeLocalTasks) list += i
          }

          val numRackLocalTasks =  math.max(0,
            // Remove node local tasks (which are also rack local btw !) from this
            math.min(manager.numRackLocalPendingTasksForHost(hostPort) - numProcessLocalTasks - numNodeLocalTasks, nodeToAvailableCpus(host)))
          if (numRackLocalTasks > 0){
            val list = rackLocalOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            for (j <- 0 until numRackLocalTasks) list += i
          }
          if (numNodeLocalTasks <= 0 && numRackLocalTasks <= 0){
            // add to others list - spread even this across cluster.
            val list = otherOffers.getOrElseUpdate(host, new ArrayBuffer[Int])
            list += i
          }
        }

        val offersPriorityList = new ArrayBuffer[Int](
          processLocalOffers.size + nodeLocalOffers.size + rackLocalOffers.size + otherOffers.size)

        // First process local, then host local, then rack, then others

        // numNodeLocalOffers contains count of both process local and host offers.
        val numNodeLocalOffers = {
          val processLocalPriorityList = ClusterScheduler.prioritizeContainers(processLocalOffers)
          offersPriorityList ++= processLocalPriorityList

          val nodeLocalPriorityList = ClusterScheduler.prioritizeContainers(nodeLocalOffers)
          offersPriorityList ++= nodeLocalPriorityList

          processLocalPriorityList.size + nodeLocalPriorityList.size
        }
        val numRackLocalOffers = {
          val rackLocalPriorityList = ClusterScheduler.prioritizeContainers(rackLocalOffers)
          offersPriorityList ++= rackLocalPriorityList
          rackLocalPriorityList.size
        }
        offersPriorityList ++= ClusterScheduler.prioritizeContainers(otherOffers)

        var lastLoop = false
        val lastLoopIndex = TASK_SCHEDULING_AGGRESSION match {
          case TaskLocality.NODE_LOCAL => numNodeLocalOffers
          case TaskLocality.RACK_LOCAL => numRackLocalOffers + numNodeLocalOffers
          case TaskLocality.ANY => offersPriorityList.size
        }

        do {
          launchedTask = false
          var loopCount = 0
          for (i <- offersPriorityList) {
            val execId = offers(i).executorId
            val hostPort = offers(i).hostPort

            // If last loop and within the lastLoopIndex, expand scope - else use null (which will use default/existing)
            val overrideLocality = if (lastLoop && loopCount < lastLoopIndex) TASK_SCHEDULING_AGGRESSION else null

            // If last loop, override waiting for host locality - we scheduled all local tasks already and there might be more available ...
            loopCount += 1

            manager.slaveOffer(execId, hostPort, availableCpus(i), overrideLocality) match {
              case Some(task) =>
                tasks(i) += task
                val tid = task.taskId
                taskIdToTaskSetId(tid) = manager.taskSet.id
                taskSetTaskIds(manager.taskSet.id) += tid
                taskIdToExecutorId(tid) = execId
                activeExecutorIds += execId
                executorsByHostPort(hostPort) += execId
                availableCpus(i) -= 1
                launchedTask = true
                
              case None => {}
            }
          }
          // Loop once more - when lastLoop = true, then we try to schedule task on all nodes irrespective of
          // data locality (we still go in order of priority : but that would not change anything since
          // if data local tasks had been available, we would have scheduled them already)
          if (lastLoop) {
            // prevent more looping
            launchedTask = false
          } else if (!lastLoop && !launchedTask) {
            // Do this only if TASK_SCHEDULING_AGGRESSION != NODE_LOCAL
            if (TASK_SCHEDULING_AGGRESSION != TaskLocality.NODE_LOCAL) {
              // fudge launchedTask to ensure we loop once more
              launchedTask = true
              // dont loop anymore
              lastLoop = true
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

    // sleeping for an arbitrary 5 seconds : to ensure that messages are sent out.
    // TODO: Do something better !
    Thread.sleep(5000L)
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

  // Check for pending tasks in all our active jobs.
  def hasPendingTasks(): Boolean = {
    synchronized {
      activeTaskSetsQueue.exists( _.hasPendingTasks() )
    }
  }

  def executorLost(executorId: String, reason: ExecutorLossReason) {
    var failedExecutor: Option[String] = None

    synchronized {
      if (activeExecutorIds.contains(executorId)) {
        val hostPort = executorIdToHostPort(executorId)
        logError("Lost executor %s on %s: %s".format(executorId, hostPort, reason))
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

  /** Remove an executor from all our data structures and mark it as lost */
  private def removeExecutor(executorId: String) {
    activeExecutorIds -= executorId
    val hostPort = executorIdToHostPort(executorId)
    if (hostPortsAlive.contains(hostPort)) {
      // DEBUG Code
      Utils.checkHostPort(hostPort)

      hostPortsAlive -= hostPort
      hostToAliveHostPorts.getOrElseUpdate(Utils.parseHostPort(hostPort)._1, new HashSet[String]).remove(hostPort)
    }
      
    val execs = executorsByHostPort.getOrElse(hostPort, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      executorsByHostPort -= hostPort
    }
    executorIdToHostPort -= executorId
    activeTaskSetsQueue.foreach(_.executorLost(executorId, hostPort))
  }

  def executorGained(execId: String, hostPort: String) {
    listener.executorGained(execId, hostPort)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = {
    Utils.checkHost(host)

    val retval = hostToAliveHostPorts.get(host)
    if (retval.isDefined) {
      return Some(retval.get.toSet)
    }

    None
  }

  def isExecutorAliveOnHostPort(hostPort: String): Boolean = {
    // Even if hostPort is a host, it does not matter - it is just a specific check.
    // But we do have to ensure that only hostPort get into hostPortsAlive !
    // So no check against Utils.checkHostPort
    hostPortsAlive.contains(hostPort)
  }

  // By default, rack is unknown
  def getRackForHost(value: String): Option[String] = None

  // By default, (cached) hosts for rack is unknown
  def getCachedHostsForRack(rack: String): Option[Set[String]] = None
}


object ClusterScheduler {

  // Used to 'spray' available containers across the available set to ensure too many containers on same host
  // are not used up. Used in yarn mode and in task scheduling (when there are multiple containers available
  // to execute a task)
  // For example: yarn can returns more containers than we would have requested under ANY, this method
  // prioritizes how to use the allocated containers.
  // flatten the map such that the array buffer entries are spread out across the returned value.
  // given <host, list[container]> == <h1, [c1 .. c5]>, <h2, [c1 .. c3]>, <h3, [c1, c2]>, <h4, c1>, <h5, c1>, i
  // the return value would be something like : h1c1, h2c1, h3c1, h4c1, h5c1, h1c2, h2c2, h3c2, h1c3, h2c3, h1c4, h1c5
  // We then 'use' the containers in this order (consuming only the top K from this list where
  // K = number to be user). This is to ensure that if we have multiple eligible allocations,
  // they dont end up allocating all containers on a small number of hosts - increasing probability of
  // multiple container failure when a host goes down.
  // Note, there is bias for keys with higher number of entries in value to be picked first (by design)
  // Also note that invocation of this method is expected to have containers of same 'type'
  // (host-local, rack-local, off-rack) and not across types : so that reordering is simply better from
  // the available list - everything else being same.
  // That is, we we first consume data local, then rack local and finally off rack nodes. So the
  // prioritization from this method applies to within each category
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map.get(left).getOrElse(Set()).size > map.get(right).getOrElse(Set()).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found){
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.get(key).getOrElse(null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size){
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }
}
