/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.{Timer, TimerTask}
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Buffer, HashMap, HashSet}
import scala.util.Random

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.ExecutorMetrics
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ResourceInformation, ResourceProfile}
import org.apache.spark.rpc.RpcEndpoint
import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{AccumulatorV2, Clock, SystemClock, ThreadUtils, Utils}

/**
 * Schedules tasks for multiple types of clusters by acting through a SchedulerBackend.
 * It can also work with a local setup by using a `LocalSchedulerBackend` and setting
 * isLocal to true. It handles common logic, like determining a scheduling order across jobs, waking
 * up to launch speculative tasks, etc.
 *
 * Clients should first call initialize() and start(), then submit task sets through the
 * submitTasks method.
 *
 * THREADING: [[SchedulerBackend]]s and task-submitting clients can call this class from multiple
 * threads, so it needs locks in public API methods to maintain its state. In addition, some
 * [[SchedulerBackend]]s synchronize on themselves when they want to send events here, and then
 * acquire a lock on us, so we need to make sure that we don't try to lock the backend while
 * we are holding a lock on ourselves.  This class is called from many threads, notably:
 *   * The DAGScheduler Event Loop
 *   * The RPCHandler threads, responding to status updates from Executors
 *   * Periodic revival of all offers from the CoarseGrainedSchedulerBackend, to accommodate delay
 *      scheduling
 *   * task-result-getter threads
 *
 * CAUTION: Any non fatal exception thrown within Spark RPC framework can be swallowed.
 * Thus, throwing exception in methods like resourceOffers, statusUpdate won't fail
 * the application, but could lead to undefined behavior. Instead, we shall use method like
 * TaskSetManger.abort() to abort a stage and then fail the application (SPARK-31485).
 *
 * Delay Scheduling:
 *  Delay scheduling is an optimization that sacrifices job fairness for data locality in order to
 *  improve cluster and workload throughput. One useful definition of "delay" is how much time
 *  has passed since the TaskSet was using its fair share of resources. Since it is impractical to
 *  calculate this delay without a full simulation, the heuristic used is the time since the
 *  TaskSetManager last launched a task and has not rejected any resources due to delay scheduling
 *  since it was last offered its "fair share". A "fair share" offer is when [[resourceOffers]]'s
 *  parameter "isAllFreeResources" is set to true. A "delay scheduling reject" is when a resource
 *  is not utilized despite there being pending tasks (implemented inside [[TaskSetManager]]).
 *  The legacy heuristic only measured the time since the [[TaskSetManager]] last launched a task,
 *  and can be re-enabled by setting spark.locality.wait.legacyResetOnTaskLaunch to true.
 */
private[spark] class TaskSchedulerImpl(
    val sc: SparkContext,
    val maxTaskFailures: Int,
    isLocal: Boolean = false,
    clock: Clock = new SystemClock)
  extends TaskScheduler with Logging {

  import TaskSchedulerImpl._

  def this(sc: SparkContext) = {
    this(sc, sc.conf.get(config.TASK_MAX_FAILURES))
  }

  // Lazily initializing healthTrackerOpt to avoid getting empty ExecutorAllocationClient,
  // because ExecutorAllocationClient is created after this TaskSchedulerImpl.
  private[scheduler] lazy val healthTrackerOpt = maybeCreateHealthTracker(sc)

  val conf = sc.conf

  // How often to check for speculative tasks
  val SPECULATION_INTERVAL_MS = conf.get(SPECULATION_INTERVAL)

  // Duplicate copies of a task will only be launched if the original copy has been running for
  // at least this amount of time. This is to avoid the overhead of launching speculative copies
  // of tasks that are very short.
  val MIN_TIME_TO_SPECULATION = conf.get(SPECULATION_MIN_THRESHOLD)

  private val speculationScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("task-scheduler-speculation")

  // Threshold above which we warn user initial TaskSet may be starved
  val STARVATION_TIMEOUT_MS = conf.getTimeAsMs("spark.starvation.timeout", "15s")

  // CPUs to request per task
  val CPUS_PER_TASK = conf.get(config.CPUS_PER_TASK)

  // TaskSetManagers are not thread safe, so any access to one should be synchronized
  // on this class.  Protected by `this`
  private val taskSetsByStageIdAndAttempt = new HashMap[Int, HashMap[Int, TaskSetManager]]

  // keyed by taskset
  // value is true if the task set has not rejected any resources due to locality
  // since the timer was last reset
  private val noRejectsSinceLastReset = new mutable.HashMap[TaskSet, Boolean]()
  private val legacyLocalityWaitReset = conf.get(LEGACY_LOCALITY_WAIT_RESET)

  // Protected by `this`
  private[scheduler] val taskIdToTaskSetManager = new ConcurrentHashMap[Long, TaskSetManager]
  // Protected by `this`
  val taskIdToExecutorId = new HashMap[Long, String]

  @volatile private var hasReceivedTask = false
  @volatile private var hasLaunchedTask = false
  private val starvationTimer = new Timer("task-starvation-timer", true)

  // Incrementing task IDs
  val nextTaskId = new AtomicLong(0)

  // IDs of the tasks running on each executor
  private val executorIdToRunningTaskIds = new HashMap[String, HashSet[Long]]

  // We add executors here when we first get decommission notification for them. Executors can
  // continue to run even after being asked to decommission, but they will eventually exit.
  val executorsPendingDecommission = new HashMap[String, ExecutorDecommissionState]

  def runningTasksByExecutors: Map[String, Int] = synchronized {
    executorIdToRunningTaskIds.toMap.mapValues(_.size).toMap
  }

  // The set of executors we have on each host; this is used to compute hostsAlive, which
  // in turn is used to decide when we can attain data locality on a given host
  protected val hostToExecutors = new HashMap[String, HashSet[String]]

  protected val hostsByRack = new HashMap[String, HashSet[String]]

  protected val executorIdToHost = new HashMap[String, String]

  private val abortTimer = new Timer("task-abort-timer", true)
  // Exposed for testing
  val unschedulableTaskSetToExpiryTime = new HashMap[TaskSetManager, Long]

  // Listener object to pass upcalls into
  var dagScheduler: DAGScheduler = null

  var backend: SchedulerBackend = null

  val mapOutputTracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]

  private var schedulableBuilder: SchedulableBuilder = null
  // default scheduler is FIFO
  private val schedulingModeConf = conf.get(SCHEDULER_MODE)
  val schedulingMode: SchedulingMode =
    try {
      SchedulingMode.withName(schedulingModeConf)
    } catch {
      case e: java.util.NoSuchElementException =>
        throw SparkCoreErrors.unrecognizedSchedulerModePropertyError(SCHEDULER_MODE_PROPERTY,
          schedulingModeConf)
    }

  val rootPool: Pool = new Pool("", schedulingMode, 0, 0)

  // This is a var so that we can reset it for testing purposes.
  private[spark] var taskResultGetter = new TaskResultGetter(sc.env, this)

  private lazy val barrierSyncTimeout = conf.get(config.BARRIER_SYNC_TIMEOUT)

  private[scheduler] var barrierCoordinator: RpcEndpoint = null

  protected val defaultRackValue: Option[String] = None

  private def maybeInitBarrierCoordinator(): Unit = {
    if (barrierCoordinator == null) {
      barrierCoordinator = new BarrierCoordinator(barrierSyncTimeout, sc.listenerBus,
        sc.env.rpcEnv)
      sc.env.rpcEnv.setupEndpoint("barrierSync", barrierCoordinator)
      logInfo("Registered BarrierCoordinator endpoint")
    }
  }

  override def setDAGScheduler(dagScheduler: DAGScheduler): Unit = {
    this.dagScheduler = dagScheduler
  }

  def initialize(backend: SchedulerBackend): Unit = {
    this.backend = backend
    schedulableBuilder = {
      schedulingMode match {
        case SchedulingMode.FIFO =>
          new FIFOSchedulableBuilder(rootPool)
        case SchedulingMode.FAIR =>
          new FairSchedulableBuilder(rootPool, sc)
        case _ =>
          throw new IllegalArgumentException(s"Unsupported $SCHEDULER_MODE_PROPERTY: " +
          s"$schedulingMode")
      }
    }
    schedulableBuilder.buildPools()
  }

  def newTaskId(): Long = nextTaskId.getAndIncrement()

  override def start(): Unit = {
    backend.start()

    if (!isLocal && conf.get(SPECULATION_ENABLED)) {
      logInfo("Starting speculative execution thread")
      speculationScheduler.scheduleWithFixedDelay(
        () => Utils.tryOrStopSparkContext(sc) { checkSpeculatableTasks() },
        SPECULATION_INTERVAL_MS, SPECULATION_INTERVAL_MS, TimeUnit.MILLISECONDS)
    }
  }

  override def postStartHook(): Unit = {
    waitBackendReady()
  }

  override def submitTasks(taskSet: TaskSet): Unit = {
    val tasks = taskSet.tasks
    logInfo("Adding task set " + taskSet.id + " with " + tasks.length + " tasks "
      + "resource profile " + taskSet.resourceProfileId)
    this.synchronized {
      val manager = createTaskSetManager(taskSet, maxTaskFailures)
      val stage = taskSet.stageId
      val stageTaskSets =
        taskSetsByStageIdAndAttempt.getOrElseUpdate(stage, new HashMap[Int, TaskSetManager])

      // Mark all the existing TaskSetManagers of this stage as zombie, as we are adding a new one.
      // This is necessary to handle a corner case. Let's say a stage has 10 partitions and has 2
      // TaskSetManagers: TSM1(zombie) and TSM2(active). TSM1 has a running task for partition 10
      // and it completes. TSM2 finishes tasks for partition 1-9, and thinks he is still active
      // because partition 10 is not completed yet. However, DAGScheduler gets task completion
      // events for all the 10 partitions and thinks the stage is finished. If it's a shuffle stage
      // and somehow it has missing map outputs, then DAGScheduler will resubmit it and create a
      // TSM3 for it. As a stage can't have more than one active task set managers, we must mark
      // TSM2 as zombie (it actually is).
      stageTaskSets.foreach { case (_, ts) =>
        ts.isZombie = true
      }
      stageTaskSets(taskSet.stageAttemptId) = manager
      schedulableBuilder.addTaskSetManager(manager, manager.taskSet.properties)

      if (!isLocal && !hasReceivedTask) {
        starvationTimer.scheduleAtFixedRate(new TimerTask() {
          override def run(): Unit = {
            if (!hasLaunchedTask) {
              logWarning("Initial job has not accepted any resources; " +
                "check your cluster UI to ensure that workers are registered " +
                "and have sufficient resources")
            } else {
              this.cancel()
            }
          }
        }, STARVATION_TIMEOUT_MS, STARVATION_TIMEOUT_MS)
      }
      hasReceivedTask = true
    }
    backend.reviveOffers()
  }

  // Label as private[scheduler] to allow tests to swap in different task set managers if necessary
  private[scheduler] def createTaskSetManager(
      taskSet: TaskSet,
      maxTaskFailures: Int): TaskSetManager = {
    new TaskSetManager(this, taskSet, maxTaskFailures, healthTrackerOpt, clock)
  }

  override def cancelTasks(stageId: Int, interruptThread: Boolean): Unit = synchronized {
    logInfo("Cancelling stage " + stageId)
    // Kill all running tasks for the stage.
    killAllTaskAttempts(stageId, interruptThread, reason = "Stage cancelled")
    // Cancel all attempts for the stage.
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        tsm.abort("Stage %s cancelled".format(stageId))
        logInfo("Stage %d was cancelled".format(stageId))
      }
    }
  }

  override def killTaskAttempt(
      taskId: Long,
      interruptThread: Boolean,
      reason: String): Boolean = synchronized {
    logInfo(s"Killing task $taskId: $reason")
    val execId = taskIdToExecutorId.get(taskId)
    if (execId.isDefined) {
      backend.killTask(taskId, execId.get, interruptThread, reason)
      true
    } else {
      logWarning(s"Could not kill task $taskId because no task with that ID was found.")
      false
    }
  }

  override def killAllTaskAttempts(
      stageId: Int,
      interruptThread: Boolean,
      reason: String): Unit = synchronized {
    logInfo(s"Killing all running tasks in stage $stageId: $reason")
    taskSetsByStageIdAndAttempt.get(stageId).foreach { attempts =>
      attempts.foreach { case (_, tsm) =>
        // There are two possible cases here:
        // 1. The task set manager has been created and some tasks have been scheduled.
        //    In this case, send a kill signal to the executors to kill the task.
        // 2. The task set manager has been created but no tasks have been scheduled. In this case,
        //    simply continue.
        tsm.runningTasksSet.foreach { tid =>
          taskIdToExecutorId.get(tid).foreach { execId =>
            backend.killTask(tid, execId, interruptThread, reason)
          }
        }
      }
    }
  }

  override def notifyPartitionCompletion(stageId: Int, partitionId: Int): Unit = {
    taskResultGetter.enqueuePartitionCompletionNotification(stageId, partitionId)
  }

  /**
   * Called to indicate that all task attempts (including speculated tasks) associated with the
   * given TaskSetManager have completed, so state associated with the TaskSetManager should be
   * cleaned up.
   */
  def taskSetFinished(manager: TaskSetManager): Unit = synchronized {
    taskSetsByStageIdAndAttempt.get(manager.taskSet.stageId).foreach { taskSetsForStage =>
      taskSetsForStage -= manager.taskSet.stageAttemptId
      if (taskSetsForStage.isEmpty) {
        taskSetsByStageIdAndAttempt -= manager.taskSet.stageId
      }
    }
    noRejectsSinceLastReset -= manager.taskSet
    manager.parent.removeSchedulable(manager)
    logInfo(s"Removed TaskSet ${manager.taskSet.id}, whose tasks have all completed, from pool" +
      s" ${manager.parent.name}")
  }

  /**
   * Offers resources to a single [[TaskSetManager]] at a given max allowed [[TaskLocality]].
   *
   * @param taskSet task set manager to offer resources to
   * @param maxLocality max locality to allow when scheduling
   * @param shuffledOffers shuffled resource offers to use for scheduling,
   *                       remaining resources are tracked by below fields as tasks are scheduled
   * @param availableCpus  remaining cpus per offer,
   *                       value at index 'i' corresponds to shuffledOffers[i]
   * @param availableResources remaining resources per offer,
   *                           value at index 'i' corresponds to shuffledOffers[i]
   * @param tasks tasks scheduled per offer, value at index 'i' corresponds to shuffledOffers[i]
   * @return tuple of (no delay schedule rejects?, option of min locality of launched task)
   */
  private def resourceOfferSingleTaskSet(
      taskSet: TaskSetManager,
      maxLocality: TaskLocality,
      shuffledOffers: Seq[WorkerOffer],
      availableCpus: Array[Int],
      availableResources: Array[Map[String, Buffer[String]]],
      tasks: IndexedSeq[ArrayBuffer[TaskDescription]])
    : (Boolean, Option[TaskLocality]) = {
    var noDelayScheduleRejects = true
    var minLaunchedLocality: Option[TaskLocality] = None
    // nodes and executors that are excluded for the entire application have already been
    // filtered out by this point
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      val taskSetRpID = taskSet.taskSet.resourceProfileId
      // make the resource profile id a hard requirement for now - ie only put tasksets
      // on executors where resource profile exactly matches.
      if (taskSetRpID == shuffledOffers(i).resourceProfileId) {
        val taskResAssignmentsOpt = resourcesMeetTaskRequirements(taskSet, availableCpus(i),
          availableResources(i))
        taskResAssignmentsOpt.foreach { taskResAssignments =>
          try {
            val prof = sc.resourceProfileManager.resourceProfileFromId(taskSetRpID)
            val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(prof, conf)
            val (taskDescOption, didReject, index) =
              taskSet.resourceOffer(execId, host, maxLocality, taskCpus, taskResAssignments)
            noDelayScheduleRejects &= !didReject
            for (task <- taskDescOption) {
              val (locality, resources) = if (task != null) {
                tasks(i) += task
                addRunningTask(task.taskId, execId, taskSet)
                (taskSet.taskInfos(task.taskId).taskLocality, task.resources)
              } else {
                assert(taskSet.isBarrier, "TaskDescription can only be null for barrier task")
                val barrierTask = taskSet.barrierPendingLaunchTasks(index)
                barrierTask.assignedOfferIndex = i
                barrierTask.assignedCores = taskCpus
                (barrierTask.taskLocality, barrierTask.assignedResources)
              }

              minLaunchedLocality = minTaskLocality(minLaunchedLocality, Some(locality))
              availableCpus(i) -= taskCpus
              assert(availableCpus(i) >= 0)
              resources.foreach { case (rName, rInfo) =>
                // Remove the first n elements from availableResources addresses, these removed
                // addresses are the same as that we allocated in taskResourceAssignments since it's
                // synchronized. We don't remove the exact addresses allocated because the current
                // approach produces the identical result with less time complexity.
                availableResources(i)(rName).remove(0, rInfo.addresses.size)
              }
            }
          } catch {
            case e: TaskNotSerializableException =>
              logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
              // Do not offer resources for this task, but don't throw an error to allow other
              // task sets to be submitted.
              return (noDelayScheduleRejects, minLaunchedLocality)
          }
        }
      }
    }
    (noDelayScheduleRejects, minLaunchedLocality)
  }

  /**
   * Add the running task to TaskScheduler's related structures
   */
  private def addRunningTask(tid: Long, execId: String, taskSet: TaskSetManager): Unit = {
    taskIdToTaskSetManager.put(tid, taskSet)
    taskIdToExecutorId(tid) = execId
    executorIdToRunningTaskIds(execId).add(tid)
  }

  /**
   * Check whether the resources from the WorkerOffer are enough to run at least one task.
   * Returns None if the resources don't meet the task requirements, otherwise returns
   * the task resource assignments to give to the next task. Note that the assignments maybe
   * be empty if no custom resources are used.
   */
  private def resourcesMeetTaskRequirements(
      taskSet: TaskSetManager,
      availCpus: Int,
      availWorkerResources: Map[String, Buffer[String]]
      ): Option[Map[String, ResourceInformation]] = {
    val rpId = taskSet.taskSet.resourceProfileId
    val taskSetProf = sc.resourceProfileManager.resourceProfileFromId(rpId)
    val taskCpus = ResourceProfile.getTaskCpusOrDefaultForProfile(taskSetProf, conf)
    // check if the ResourceProfile has cpus first since that is common case
    if (availCpus < taskCpus) return None
    // only look at the resource other then cpus
    val tsResources = ResourceProfile.getCustomTaskResources(taskSetProf)
    if (tsResources.isEmpty) return Some(Map.empty)
    val localTaskReqAssign = HashMap[String, ResourceInformation]()
    // we go through all resources here so that we can make sure they match and also get what the
    // assignments are for the next task
    for ((rName, taskReqs) <- tsResources) {
      val taskAmount = taskSetProf.getSchedulerTaskResourceAmount(rName)
      availWorkerResources.get(rName) match {
        case Some(workerRes) =>
          if (workerRes.size >= taskAmount) {
            localTaskReqAssign.put(rName, new ResourceInformation(rName,
              workerRes.take(taskAmount).toArray))
          } else {
            return None
          }
        case None => return None
      }
    }
    Some(localTaskReqAssign.toMap)
  }

  private def minTaskLocality(
      l1: Option[TaskLocality],
      l2: Option[TaskLocality]) : Option[TaskLocality] = {
    if (l1.isEmpty) {
      l2
    } else if (l2.isEmpty) {
      l1
    } else if (l1.get < l2.get) {
      l1
    } else {
      l2
    }
  }

  /**
   * Called by cluster manager to offer resources on workers. We respond by asking our active task
   * sets for tasks in order of priority. We fill each node with tasks in a round-robin manner so
   * that tasks are balanced across the cluster.
   */
  def resourceOffers(
      offers: IndexedSeq[WorkerOffer],
      isAllFreeResources: Boolean = true): Seq[Seq[TaskDescription]] = synchronized {
    // Mark each worker as alive and remember its hostname
    // Also track if new executor is added
    var newExecAvail = false
    for (o <- offers) {
      if (!hostToExecutors.contains(o.host)) {
        hostToExecutors(o.host) = new HashSet[String]()
      }
      if (!executorIdToRunningTaskIds.contains(o.executorId)) {
        hostToExecutors(o.host) += o.executorId
        executorAdded(o.executorId, o.host)
        executorIdToHost(o.executorId) = o.host
        executorIdToRunningTaskIds(o.executorId) = HashSet[Long]()
        newExecAvail = true
      }
    }
    val hosts = offers.map(_.host).distinct
    for ((host, Some(rack)) <- hosts.zip(getRacksForHosts(hosts))) {
      hostsByRack.getOrElseUpdate(rack, new HashSet[String]()) += host
    }

    // Before making any offers, include any nodes whose expireOnFailure timeout has expired. Do
    // this here to avoid a separate thread and added synchronization overhead, and also because
    // updating the excluded executors and nodes is only relevant when task offers are being made.
    healthTrackerOpt.foreach(_.applyExcludeOnFailureTimeout())

    val filteredOffers = healthTrackerOpt.map { healthTracker =>
      offers.filter { offer =>
        !healthTracker.isNodeExcluded(offer.host) &&
          !healthTracker.isExecutorExcluded(offer.executorId)
      }
    }.getOrElse(offers)

    val shuffledOffers = shuffleOffers(filteredOffers)
    // Build a list of tasks to assign to each worker.
    // Note the size estimate here might be off with different ResourceProfiles but should be
    // close estimate
    val tasks = shuffledOffers.map(o => new ArrayBuffer[TaskDescription](o.cores / CPUS_PER_TASK))
    val availableResources = shuffledOffers.map(_.resources).toArray
    val availableCpus = shuffledOffers.map(o => o.cores).toArray
    val resourceProfileIds = shuffledOffers.map(o => o.resourceProfileId).toArray
    val sortedTaskSets = rootPool.getSortedTaskSetQueue
    for (taskSet <- sortedTaskSets) {
      logDebug("parentName: %s, name: %s, runningTasks: %s".format(
        taskSet.parent.name, taskSet.name, taskSet.runningTasks))
      if (newExecAvail) {
        taskSet.executorAdded()
      }
    }

    // Take each TaskSet in our scheduling order, and then offer it to each node in increasing order
    // of locality levels so that it gets a chance to launch local tasks on all of them.
    // NOTE: the preferredLocality order: PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY
    for (taskSet <- sortedTaskSets) {
      // we only need to calculate available slots if using barrier scheduling, otherwise the
      // value is -1
      val numBarrierSlotsAvailable = if (taskSet.isBarrier) {
        val rpId = taskSet.taskSet.resourceProfileId
        val availableResourcesAmount = availableResources.map { resourceMap =>
          // available addresses already takes into account if there are fractional
          // task resource requests
          resourceMap.map { case (name, addresses) => (name, addresses.length) }
        }
        calculateAvailableSlots(this, conf, rpId, resourceProfileIds, availableCpus,
          availableResourcesAmount)
      } else {
        -1
      }
      // Skip the barrier taskSet if the available slots are less than the number of pending tasks.
      if (taskSet.isBarrier && numBarrierSlotsAvailable < taskSet.numTasks) {
        // Skip the launch process.
        // TODO SPARK-24819 If the job requires more slots than available (both busy and free
        // slots), fail the job on submit.
        logInfo(s"Skip current round of resource offers for barrier stage ${taskSet.stageId} " +
          s"because the barrier taskSet requires ${taskSet.numTasks} slots, while the total " +
          s"number of available slots is $numBarrierSlotsAvailable.")
      } else {
        var launchedAnyTask = false
        var noDelaySchedulingRejects = true
        var globalMinLocality: Option[TaskLocality] = None
        for (currentMaxLocality <- taskSet.myLocalityLevels) {
          var launchedTaskAtCurrentMaxLocality = false
          do {
            val (noDelayScheduleReject, minLocality) = resourceOfferSingleTaskSet(
              taskSet, currentMaxLocality, shuffledOffers, availableCpus,
              availableResources, tasks)
            launchedTaskAtCurrentMaxLocality = minLocality.isDefined
            launchedAnyTask |= launchedTaskAtCurrentMaxLocality
            noDelaySchedulingRejects &= noDelayScheduleReject
            globalMinLocality = minTaskLocality(globalMinLocality, minLocality)
          } while (launchedTaskAtCurrentMaxLocality)
        }

        if (!legacyLocalityWaitReset) {
          if (noDelaySchedulingRejects) {
            if (launchedAnyTask &&
              (isAllFreeResources || noRejectsSinceLastReset.getOrElse(taskSet.taskSet, true))) {
              taskSet.resetDelayScheduleTimer(globalMinLocality)
              noRejectsSinceLastReset.update(taskSet.taskSet, true)
            }
          } else {
            noRejectsSinceLastReset.update(taskSet.taskSet, false)
          }
        }

        if (!launchedAnyTask) {
          taskSet.getCompletelyExcludedTaskIfAny(hostToExecutors).foreach { taskIndex =>
              // If the taskSet is unschedulable we try to find an existing idle excluded
              // executor and kill the idle executor and kick off an abortTimer which if it doesn't
              // schedule a task within the timeout will abort the taskSet if we were unable to
              // schedule any task from the taskSet.
              // Note 1: We keep track of schedulability on a per taskSet basis rather than on a per
              // task basis.
              // Note 2: The taskSet can still be aborted when there are more than one idle
              // excluded executors and dynamic allocation is on. This can happen when a killed
              // idle executor isn't replaced in time by ExecutorAllocationManager as it relies on
              // pending tasks and doesn't kill executors on idle timeouts, resulting in the abort
              // timer to expire and abort the taskSet.
              //
              // If there are no idle executors and dynamic allocation is enabled, then we would
              // notify ExecutorAllocationManager to allocate more executors to schedule the
              // unschedulable tasks else we will abort immediately.
              executorIdToRunningTaskIds.find(x => !isExecutorBusy(x._1)) match {
                case Some ((executorId, _)) =>
                  if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                    healthTrackerOpt.foreach(blt => blt.killExcludedIdleExecutor(executorId))
                    updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                  }
                case None =>
                  //  Notify ExecutorAllocationManager about the unschedulable task set,
                  // in order to provision more executors to make them schedulable
                  if (Utils.isDynamicAllocationEnabled(conf)) {
                    if (!unschedulableTaskSetToExpiryTime.contains(taskSet)) {
                      logInfo("Notifying ExecutorAllocationManager to allocate more executors to" +
                        " schedule the unschedulable task before aborting" +
                        " stage ${taskSet.stageId}.")
                      dagScheduler.unschedulableTaskSetAdded(taskSet.taskSet.stageId,
                        taskSet.taskSet.stageAttemptId)
                      updateUnschedulableTaskSetTimeoutAndStartAbortTimer(taskSet, taskIndex)
                    }
                  } else {
                    // Abort Immediately
                    logInfo("Cannot schedule any task because all executors excluded from " +
                      "failures. No idle executors can be found to kill. Aborting stage " +
                      s"${taskSet.stageId}.")
                    taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
                  }
              }
          }
        } else {
          // We want to defer killing any taskSets as long as we have a non excluded executor
          // which can be used to schedule a task from any active taskSets. This ensures that the
          // job can make progress.
          // Note: It is theoretically possible that a taskSet never gets scheduled on a
          // non-excluded executor and the abort timer doesn't kick in because of a constant
          // submission of new TaskSets. See the PR for more details.
          if (unschedulableTaskSetToExpiryTime.nonEmpty) {
            logInfo("Clearing the expiry times for all unschedulable taskSets as a task was " +
              "recently scheduled.")
            // Notify ExecutorAllocationManager as well as other subscribers that a task now
            // recently becomes schedulable
            dagScheduler.unschedulableTaskSetRemoved(taskSet.taskSet.stageId,
              taskSet.taskSet.stageAttemptId)
            unschedulableTaskSetToExpiryTime.clear()
          }
        }

        if (launchedAnyTask && taskSet.isBarrier) {
          val barrierPendingLaunchTasks = taskSet.barrierPendingLaunchTasks.values.toArray
          // Check whether the barrier tasks are partially launched.
          if (barrierPendingLaunchTasks.length != taskSet.numTasks) {
            if (legacyLocalityWaitReset) {
              // Legacy delay scheduling always reset the timer when there's a task that is able
              // to be scheduled. Thus, whenever there's a timer reset could happen during a single
              // round resourceOffer, tasks that don't get or have the preferred locations would
              // always reject the offered resources. As a result, the barrier taskset can't get
              // launched. And if we retry the resourceOffer, we'd go through the same path again
              // and get into the endless loop in the end.
              val errorMsg = s"Fail resource offers for barrier stage ${taskSet.stageId} " +
                s"because only ${barrierPendingLaunchTasks.length} out of a total number " +
                s"of ${taskSet.numTasks} tasks got resource offers. We highly recommend " +
                "you to use the non-legacy delay scheduling by setting " +
                s"${LEGACY_LOCALITY_WAIT_RESET.key} to false to get rid of this error."
              logWarning(errorMsg)
              taskSet.abort(errorMsg)
              throw SparkCoreErrors.sparkError(errorMsg)
            } else {
              val curTime = clock.getTimeMillis()
              if (curTime - taskSet.lastResourceOfferFailLogTime >
                TaskSetManager.BARRIER_LOGGING_INTERVAL) {
                logInfo("Releasing the assigned resource offers since only partial tasks can " +
                  "be launched. Waiting for later round resource offers.")
                taskSet.lastResourceOfferFailLogTime = curTime
              }
              barrierPendingLaunchTasks.foreach { task =>
                // revert all assigned resources
                availableCpus(task.assignedOfferIndex) += task.assignedCores
                task.assignedResources.foreach { case (rName, rInfo) =>
                  availableResources(task.assignedOfferIndex)(rName).appendAll(rInfo.addresses)
                }
                // re-add the task to the schedule pending list
                taskSet.addPendingTask(task.index)
              }
            }
          } else {
            // All tasks are able to launch in this barrier task set. Let's do
            // some preparation work before launching them.
            val launchTime = clock.getTimeMillis()
            val addressesWithDescs = barrierPendingLaunchTasks.map { task =>
              val taskDesc = taskSet.prepareLaunchingTask(
                task.execId,
                task.host,
                task.index,
                task.taskLocality,
                false,
                task.assignedCores,
                task.assignedResources,
                launchTime)
              addRunningTask(taskDesc.taskId, taskDesc.executorId, taskSet)
              tasks(task.assignedOfferIndex) += taskDesc
              shuffledOffers(task.assignedOfferIndex).address.get -> taskDesc
            }

            // materialize the barrier coordinator.
            maybeInitBarrierCoordinator()

            // Update the taskInfos into all the barrier task properties.
            val addressesStr = addressesWithDescs
              // Addresses ordered by partitionId
              .sortBy(_._2.partitionId)
              .map(_._1)
              .mkString(",")
            addressesWithDescs.foreach(_._2.properties.setProperty("addresses", addressesStr))

            logInfo(s"Successfully scheduled all the ${addressesWithDescs.size} tasks for " +
              s"barrier stage ${taskSet.stageId}.")
          }
          taskSet.barrierPendingLaunchTasks.clear()
        }
      }
    }

    // TODO SPARK-24823 Cancel a job that contains barrier stage(s) if the barrier tasks don't get
    // launched within a configured time.
    if (tasks.nonEmpty) {
      hasLaunchedTask = true
    }
    return tasks.map(_.toSeq)
  }

  private def updateUnschedulableTaskSetTimeoutAndStartAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): Unit = {
    val timeout = conf.get(config.UNSCHEDULABLE_TASKSET_TIMEOUT) * 1000
    unschedulableTaskSetToExpiryTime(taskSet) = clock.getTimeMillis() + timeout
    logInfo(s"Waiting for $timeout ms for completely " +
      s"excluded task to be schedulable again before aborting stage ${taskSet.stageId}.")
    abortTimer.schedule(
      createUnschedulableTaskSetAbortTimer(taskSet, taskIndex), timeout)
  }

  private def createUnschedulableTaskSetAbortTimer(
      taskSet: TaskSetManager,
      taskIndex: Int): TimerTask = {
    new TimerTask() {
      override def run(): Unit = TaskSchedulerImpl.this.synchronized {
        if (unschedulableTaskSetToExpiryTime.contains(taskSet) &&
            unschedulableTaskSetToExpiryTime(taskSet) <= clock.getTimeMillis()) {
          logInfo("Cannot schedule any task because all executors excluded due to failures. " +
            s"Wait time for scheduling expired. Aborting stage ${taskSet.stageId}.")
          taskSet.abortSinceCompletelyExcludedOnFailure(taskIndex)
        } else {
          this.cancel()
        }
      }
    }
  }

  /**
   * Shuffle offers around to avoid always placing tasks on the same workers.  Exposed to allow
   * overriding in tests, so it can be deterministic.
   */
  protected def shuffleOffers(offers: IndexedSeq[WorkerOffer]): IndexedSeq[WorkerOffer] = {
    Random.shuffle(offers)
  }

  def statusUpdate(tid: Long, state: TaskState, serializedData: ByteBuffer): Unit = {
    var failedExecutor: Option[String] = None
    var reason: Option[ExecutorLossReason] = None
    synchronized {
      try {
        Option(taskIdToTaskSetManager.get(tid)) match {
          case Some(taskSet) =>
            if (state == TaskState.LOST) {
              // TaskState.LOST is only used by the deprecated Mesos fine-grained scheduling mode,
              // where each executor corresponds to a single task, so mark the executor as failed.
              val execId = taskIdToExecutorId.getOrElse(tid, {
                val errorMsg =
                  "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)"
                taskSet.abort(errorMsg)
                throw new SparkException(
                  "taskIdToTaskSetManager.contains(tid) <=> taskIdToExecutorId.contains(tid)")
              })
              if (executorIdToRunningTaskIds.contains(execId)) {
                reason = Some(
                  ExecutorProcessLost(
                    s"Task $tid was lost, so marking the executor as lost as well."))
                removeExecutor(execId, reason.get)
                failedExecutor = Some(execId)
              }
            }
            if (TaskState.isFinished(state)) {
              cleanupTaskState(tid)
              taskSet.removeRunningTask(tid)
              if (state == TaskState.FINISHED) {
                taskResultGetter.enqueueSuccessfulTask(taskSet, tid, serializedData)
              } else if (Set(TaskState.FAILED, TaskState.KILLED, TaskState.LOST).contains(state)) {
                taskResultGetter.enqueueFailedTask(taskSet, tid, state, serializedData)
              }
            }
          case None =>
            logError(
              ("Ignoring update with state %s for TID %s because its task set is gone (this is " +
                "likely the result of receiving duplicate task finished status updates) or its " +
                "executor has been marked as failed.")
                .format(state, tid))
        }
      } catch {
        case e: Exception => logError("Exception in statusUpdate", e)
      }
    }
    // Update the DAGScheduler without holding a lock on this, since that can deadlock
    if (failedExecutor.isDefined) {
      assert(reason.isDefined)
      dagScheduler.executorLost(failedExecutor.get, reason.get)
      backend.reviveOffers()
    }
  }

  /**
   * Update metrics for in-progress tasks and executor metrics, and let the master know that the
   * BlockManager is still alive. Return true if the driver knows about the given block manager.
   * Otherwise, return false, indicating that the block manager should re-register.
   */
  override def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId,
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    // (taskId, stageId, stageAttemptId, accumUpdates)
    val accumUpdatesWithTaskIds: Array[(Long, Int, Int, Seq[AccumulableInfo])] = {
      accumUpdates.flatMap { case (id, updates) =>
        val accInfos = updates.map(acc => acc.toInfo(Some(acc.value), None))
        Option(taskIdToTaskSetManager.get(id)).map { taskSetMgr =>
          (id, taskSetMgr.stageId, taskSetMgr.taskSet.stageAttemptId, accInfos)
        }
      }
    }
    dagScheduler.executorHeartbeatReceived(execId, accumUpdatesWithTaskIds, blockManagerId,
      executorUpdates)
  }

  def handleTaskGettingResult(taskSetManager: TaskSetManager, tid: Long): Unit = synchronized {
    taskSetManager.handleTaskGettingResult(tid)
  }

  def handleSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskResult: DirectTaskResult[_]): Unit = synchronized {
    taskSetManager.handleSuccessfulTask(tid, taskResult)
  }

  def handleFailedTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      taskState: TaskState,
      reason: TaskFailedReason): Unit = synchronized {
    taskSetManager.handleFailedTask(tid, taskState, reason)
    if (!taskSetManager.isZombie && !taskSetManager.someAttemptSucceeded(tid)) {
      // Need to revive offers again now that the task set manager state has been updated to
      // reflect failed tasks that need to be re-run.
      backend.reviveOffers()
    }
  }

  /**
   * Marks the task has completed in the active TaskSetManager for the given stage.
   *
   * After stage failure and retry, there may be multiple TaskSetManagers for the stage.
   * If an earlier zombie attempt of a stage completes a task, we can ask the later active attempt
   * to skip submitting and running the task for the same partition, to save resource. That also
   * means that a task completion from an earlier zombie attempt can lead to the entire stage
   * getting marked as successful.
   */
  private[scheduler] def handlePartitionCompleted(stageId: Int, partitionId: Int) = synchronized {
    taskSetsByStageIdAndAttempt.get(stageId).foreach(_.values.filter(!_.isZombie).foreach { tsm =>
      tsm.markPartitionCompleted(partitionId)
    })
  }

  def error(message: String): Unit = {
    synchronized {
      if (taskSetsByStageIdAndAttempt.nonEmpty) {
        // Have each task set throw a SparkException with the error
        for {
          attempts <- taskSetsByStageIdAndAttempt.values
          manager <- attempts.values
        } {
          try {
            manager.abort(message)
          } catch {
            case e: Exception => logError("Exception in error callback", e)
          }
        }
      } else {
        // No task sets are active but we still got an error. Just exit since this
        // must mean the error is during registration.
        // It might be good to do something smarter here in the future.
        throw SparkCoreErrors.clusterSchedulerError(message)
      }
    }
  }

  override def stop(): Unit = {
    speculationScheduler.shutdown()
    if (backend != null) {
      backend.stop()
    }
    if (taskResultGetter != null) {
      taskResultGetter.stop()
    }
    if (barrierCoordinator != null) {
      barrierCoordinator.stop()
    }
    starvationTimer.cancel()
    abortTimer.cancel()
  }

  override def defaultParallelism(): Int = backend.defaultParallelism()

  // Check for speculatable tasks in all our active jobs.
  def checkSpeculatableTasks(): Unit = {
    var shouldRevive = false
    synchronized {
      shouldRevive = rootPool.checkSpeculatableTasks(MIN_TIME_TO_SPECULATION)
    }
    if (shouldRevive) {
      backend.reviveOffers()
    }
  }

  override def executorDecommission(
      executorId: String, decommissionInfo: ExecutorDecommissionInfo): Unit = {
    synchronized {
      // Don't bother noting decommissioning for executors that we don't know about
      if (executorIdToHost.contains(executorId)) {
        executorsPendingDecommission(executorId) =
          ExecutorDecommissionState(clock.getTimeMillis(), decommissionInfo.workerHost)
      }
    }
    rootPool.executorDecommission(executorId)
    backend.reviveOffers()
  }

  override def getExecutorDecommissionState(executorId: String)
    : Option[ExecutorDecommissionState] = synchronized {
    executorsPendingDecommission.get(executorId)
  }

  override def executorLost(executorId: String, reason: ExecutorLossReason): Unit = {
    var failedExecutor: Option[String] = None

    synchronized {
      if (executorIdToRunningTaskIds.contains(executorId)) {
        val hostPort = executorIdToHost(executorId)
        logExecutorLoss(executorId, hostPort, reason)
        removeExecutor(executorId, reason)
        failedExecutor = Some(executorId)
      } else {
        executorIdToHost.get(executorId) match {
          case Some(hostPort) =>
            // If the host mapping still exists, it means we don't know the loss reason for the
            // executor. So call removeExecutor() to update tasks running on that executor when
            // the real loss reason is finally known.
            logExecutorLoss(executorId, hostPort, reason)
            removeExecutor(executorId, reason)

          case None =>
            // We may get multiple executorLost() calls with different loss reasons. For example,
            // one may be triggered by a dropped connection from the worker while another may be a
            // report of executor termination from Mesos. We produce log messages for both so we
            // eventually report the termination reason.
            logError(s"Lost an executor $executorId (already removed): $reason")
        }
      }
    }
    // Call dagScheduler.executorLost without holding the lock on this to prevent deadlock
    if (failedExecutor.isDefined) {
      dagScheduler.executorLost(failedExecutor.get, reason)
      backend.reviveOffers()
    }
  }

  override def workerRemoved(workerId: String, host: String, message: String): Unit = {
    logInfo(s"Handle removed worker $workerId: $message")
    dagScheduler.workerRemoved(workerId, host, message)
  }

  private def logExecutorLoss(
      executorId: String,
      hostPort: String,
      reason: ExecutorLossReason): Unit = reason match {
    case LossReasonPending =>
      logDebug(s"Executor $executorId on $hostPort lost, but reason not yet known.")
    case ExecutorKilled =>
      logInfo(s"Executor $executorId on $hostPort killed by driver.")
    case _ =>
      logError(s"Lost executor $executorId on $hostPort: $reason")
  }

  /**
   * Cleans up the TaskScheduler's state for tracking the given task.
   */
  private def cleanupTaskState(tid: Long): Unit = {
    taskIdToTaskSetManager.remove(tid)
    taskIdToExecutorId.remove(tid).foreach { executorId =>
      executorIdToRunningTaskIds.get(executorId).foreach { _.remove(tid) }
    }
  }

  /**
   * Remove an executor from all our data structures and mark it as lost. If the executor's loss
   * reason is not yet known, do not yet remove its association with its host nor update the status
   * of any running tasks, since the loss reason defines whether we'll fail those tasks.
   */
  private def removeExecutor(executorId: String, reason: ExecutorLossReason): Unit = {
    // The tasks on the lost executor may not send any more status updates (because the executor
    // has been lost), so they should be cleaned up here.
    executorIdToRunningTaskIds.remove(executorId).foreach { taskIds =>
      logDebug("Cleaning up TaskScheduler state for tasks " +
        s"${taskIds.mkString("[", ",", "]")} on failed executor $executorId")
      // We do not notify the TaskSetManager of the task failures because that will
      // happen below in the rootPool.executorLost() call.
      taskIds.foreach(cleanupTaskState)
    }

    val host = executorIdToHost(executorId)
    val execs = hostToExecutors.getOrElse(host, new HashSet)
    execs -= executorId
    if (execs.isEmpty) {
      hostToExecutors -= host
      for (rack <- getRackForHost(host); hosts <- hostsByRack.get(rack)) {
        hosts -= host
        if (hosts.isEmpty) {
          hostsByRack -= rack
        }
      }
    }

    executorsPendingDecommission.remove(executorId)

    if (reason != LossReasonPending) {
      executorIdToHost -= executorId
      rootPool.executorLost(executorId, host, reason)
    }
    healthTrackerOpt.foreach(_.handleRemovedExecutor(executorId))
  }

  def executorAdded(execId: String, host: String): Unit = {
    dagScheduler.executorAdded(execId, host)
  }

  def getExecutorsAliveOnHost(host: String): Option[Set[String]] = synchronized {
    hostToExecutors.get(host).map(_.filterNot(isExecutorDecommissioned)).map(_.toSet)
  }

  def hasExecutorsAliveOnHost(host: String): Boolean = synchronized {
    hostToExecutors.get(host)
      .exists(executors => executors.exists(e => !isExecutorDecommissioned(e)))
  }

  def hasHostAliveOnRack(rack: String): Boolean = synchronized {
    hostsByRack.get(rack)
      .exists(hosts => hosts.exists(h => !isHostDecommissioned(h)))
  }

  def isExecutorAlive(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.contains(execId) && !isExecutorDecommissioned(execId)
  }

  def isExecutorBusy(execId: String): Boolean = synchronized {
    executorIdToRunningTaskIds.get(execId).exists(_.nonEmpty)
  }

  // exposed for test
  protected final def isExecutorDecommissioned(execId: String): Boolean =
    getExecutorDecommissionState(execId).isDefined

  // exposed for test
  protected final def isHostDecommissioned(host: String): Boolean = {
    hostToExecutors.get(host).exists { executors =>
      executors.exists(e => getExecutorDecommissionState(e).exists(_.workerHost.isDefined))
    }
  }

  /**
   * Get a snapshot of the currently excluded nodes for the entire application. This is
   * thread-safe -- it can be called without a lock on the TaskScheduler.
   */
  def excludedNodes(): Set[String] = {
    healthTrackerOpt.map(_.excludedNodeList()).getOrElse(Set.empty)
  }

  /**
   * Get the rack for one host.
   *
   * Note that [[getRacksForHosts]] should be preferred when possible as that can be much
   * more efficient.
   */
  def getRackForHost(host: String): Option[String] = {
    getRacksForHosts(Seq(host)).head
  }

  /**
   * Get racks for multiple hosts.
   *
   * The returned Sequence will be the same length as the hosts argument and can be zipped
   * together with the hosts argument.
   */
  def getRacksForHosts(hosts: Seq[String]): Seq[Option[String]] = {
    hosts.map(_ => defaultRackValue)
  }

  private def waitBackendReady(): Unit = {
    if (backend.isReady) {
      return
    }
    while (!backend.isReady) {
      // Might take a while for backend to be ready if it is waiting on resources.
      if (sc.stopped.get) {
        // For example: the master removes the application for some reason
        throw new IllegalStateException("Spark context stopped while waiting for backend")
      }
      synchronized {
        this.wait(100)
      }
    }
  }

  override def applicationId(): String = backend.applicationId()

  override def applicationAttemptId(): Option[String] = backend.applicationAttemptId()

  // exposed for testing
  private[scheduler] def taskSetManagerForAttempt(
      stageId: Int,
      stageAttemptId: Int): Option[TaskSetManager] = synchronized {
    for {
      attempts <- taskSetsByStageIdAndAttempt.get(stageId)
      manager <- attempts.get(stageAttemptId)
    } yield {
      manager
    }
  }
}


private[spark] object TaskSchedulerImpl {

  val SCHEDULER_MODE_PROPERTY = SCHEDULER_MODE.key

  /**
   * Calculate the max available task slots given the `availableCpus` and `availableResources`
   * from a collection of ResourceProfiles. And only those ResourceProfiles who has the
   * same id with the `rpId` can be used to calculate the task slots.
   *
   * @param scheduler the TaskSchedulerImpl instance
   * @param conf SparkConf used to calculate the limiting resource and get the cpu amount per task
   * @param rpId the target ResourceProfile id. Only those ResourceProfiles who has the same id
   *             with it can be used to calculate the task slots.
   * @param availableRPIds an Array of ids of the available ResourceProfiles from the executors.
   * @param availableCpus an Array of the amount of available cpus from the executors.
   * @param availableResources an Array of the resources map from the executors. In the resource
   *                           map, it maps from the resource name to its amount.
   * @return the number of max task slots
   */
  def calculateAvailableSlots(
      scheduler: TaskSchedulerImpl,
      conf: SparkConf,
      rpId: Int,
      availableRPIds: Array[Int],
      availableCpus: Array[Int],
      availableResources: Array[Map[String, Int]]): Int = {
    val resourceProfile = scheduler.sc.resourceProfileManager.resourceProfileFromId(rpId)
    val coresKnown = resourceProfile.isCoresLimitKnown
    val (limitingResource, limitedByCpu) = {
      val limiting = resourceProfile.limitingResource(conf)
      // if limiting resource is empty then we have no other resources, so it has to be CPU
      if (limiting == ResourceProfile.CPUS || limiting.isEmpty) {
        (ResourceProfile.CPUS, true)
      } else {
        (limiting, false)
      }
    }
    val cpusPerTask = ResourceProfile.getTaskCpusOrDefaultForProfile(resourceProfile, conf)
    val taskLimit = resourceProfile.taskResources.get(limitingResource).map(_.amount).get

    availableCpus.zip(availableResources).zip(availableRPIds)
      .filter { case (_, id) => id == rpId }
      .map { case ((cpu, resources), _) =>
        val numTasksPerExecCores = cpu / cpusPerTask
        if (limitedByCpu) {
          numTasksPerExecCores
        } else {
          val availAddrs = resources.getOrElse(limitingResource, 0)
          val resourceLimit = (availAddrs / taskLimit).toInt
          // when executor cores config isn't set, we can't calculate the real limiting resource
          // and number of tasks per executor ahead of time, so calculate it now based on what
          // is available.
          if (!coresKnown && numTasksPerExecCores <= resourceLimit) {
            numTasksPerExecCores
          } else {
            resourceLimit
          }
        }
      }.sum
  }

  /**
   * Used to balance containers across hosts.
   *
   * Accepts a map of hosts to resource offers for that host, and returns a prioritized list of
   * resource offers representing the order in which the offers should be used. The resource
   * offers are ordered such that we'll allocate one container on each host before allocating a
   * second container on any host, and so on, in order to reduce the damage if a host fails.
   *
   * For example, given {@literal <h1, [o1, o2, o3]>}, {@literal <h2, [o4]>} and
   * {@literal <h3, [o5, o6]>}, returns {@literal [o1, o5, o4, o2, o6, o3]}.
   */
  def prioritizeContainers[K, T] (map: HashMap[K, ArrayBuffer[T]]): List[T] = {
    val _keyList = new ArrayBuffer[K](map.size)
    _keyList ++= map.keys

    // order keyList based on population of value in map
    val keyList = _keyList.sortWith(
      (left, right) => map(left).size > map(right).size
    )

    val retval = new ArrayBuffer[T](keyList.size * 2)
    var index = 0
    var found = true

    while (found) {
      found = false
      for (key <- keyList) {
        val containerList: ArrayBuffer[T] = map.getOrElse(key, null)
        assert(containerList != null)
        // Get the index'th entry for this host - if present
        if (index < containerList.size) {
          retval += containerList.apply(index)
          found = true
        }
      }
      index += 1
    }

    retval.toList
  }

  private def maybeCreateHealthTracker(sc: SparkContext): Option[HealthTracker] = {
    if (HealthTracker.isExcludeOnFailureEnabled(sc.conf)) {
      val executorAllocClient: Option[ExecutorAllocationClient] = sc.schedulerBackend match {
        case b: ExecutorAllocationClient => Some(b)
        case _ => None
      }
      Some(new HealthTracker(sc, executorAllocClient))
    } else {
      None
    }
  }

}
