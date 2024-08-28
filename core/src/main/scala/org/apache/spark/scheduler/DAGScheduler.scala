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

import java.io.NotSerializableException
import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, ScheduledFuture, TimeoutException, TimeUnit}
import java.util.concurrent.{Future => JFutrue}
import java.util.concurrent.atomic.AtomicInteger

import scala.annotation.tailrec
import scala.collection.Map
import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.google.common.util.concurrent.{Futures, SettableFuture}

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.executor.{ExecutorMetrics, TaskMetrics}
import org.apache.spark.internal.{config, Logging, LogKeys, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.config.{LEGACY_ABORT_STAGE_AFTER_KILL_TASKS, RDD_CACHE_VISIBILITY_TRACKING_ENABLED}
import org.apache.spark.internal.config.Tests.TEST_NO_STAGE_RETRY
import org.apache.spark.network.shuffle.{BlockStoreClient, MergeFinalizerListener}
import org.apache.spark.network.shuffle.protocol.MergeStatuses
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.{RDD, RDDCheckpointData}
import org.apache.spark.resource.{ResourceProfile, TaskResourceProfile}
import org.apache.spark.resource.ResourceProfile.{DEFAULT_RESOURCE_PROFILE_ID, EXECUTOR_CORES_LOCAL_PROPERTY, PYSPARK_MEMORY_LOCAL_PROPERTY}
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages.BlockManagerHeartbeat
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster. A TaskSet contains fully independent
 * tasks that can run right away based on the data that's already on the cluster (e.g. map output
 * files from previous stages), though it may fail if this data becomes unavailable.
 *
 * Spark stages are created by breaking the RDD graph at shuffle boundaries. RDD operations with
 * "narrow" dependencies, like map() and filter(), are pipelined together into one set of tasks
 * in each stage, but operations with shuffle dependencies require multiple stages (one to write a
 * set of map output files, and another to read those files after a barrier). In the end, every
 * stage will have only shuffle dependencies on other stages, and may compute multiple operations
 * inside it. The actual pipelining of these operations happens in the RDD.compute() functions of
 * various RDDs
 *
 * In addition to coming up with a DAG of stages, the DAGScheduler also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * When looking through this code, there are several key concepts:
 *
 *  - Jobs (represented by [[ActiveJob]]) are the top-level work items submitted to the scheduler.
 *    For example, when the user calls an action, like count(), a job will be submitted through
 *    submitJob. Each Job may require the execution of multiple stages to build intermediate data.
 *
 *  - Stages ([[Stage]]) are sets of tasks that compute intermediate results in jobs, where each
 *    task computes the same function on partitions of the same RDD. Stages are separated at shuffle
 *    boundaries, which introduce a barrier (where we must wait for the previous stage to finish to
 *    fetch outputs). There are two types of stages: [[ResultStage]], for the final stage that
 *    executes an action, and [[ShuffleMapStage]], which writes map output files for a shuffle.
 *    Stages are often shared across multiple jobs, if these jobs reuse the same RDDs.
 *
 *  - Tasks are individual units of work, each sent to one machine.
 *
 *  - Cache tracking: the DAGScheduler figures out which RDDs are cached to avoid recomputing them
 *    and likewise remembers which shuffle map stages have already produced output files to avoid
 *    redoing the map side of a shuffle.
 *
 *  - Preferred locations: the DAGScheduler also computes where to run each task in a stage based
 *    on the preferred locations of its underlying RDDs, or the location of cached or shuffle data.
 *
 *  - Cleanup: all data structures are cleared when the running jobs that depend on them finish,
 *    to prevent memory leaks in a long-running application.
 *
 * To recover from failures, the same stage might need to run multiple times, which are called
 * "attempts". If the TaskScheduler reports that a task failed because a map output file from a
 * previous stage was lost, the DAGScheduler resubmits that lost stage. This is detected through a
 * CompletionEvent with FetchFailed, or an ExecutorLost event. The DAGScheduler will wait a small
 * amount of time to see whether other nodes or tasks fail, then resubmit TaskSets for any lost
 * stage(s) that compute the missing tasks. As part of this process, we might also have to create
 * Stage objects for old (finished) stages where we previously cleaned up the Stage object. Since
 * tasks from the old attempt of a stage could still be running, care must be taken to map any
 * events received in the correct Stage object.
 *
 * Here's a checklist to use when making or reviewing changes to this class:
 *
 *  - All data structures should be cleared when the jobs involving them end to avoid indefinite
 *    accumulation of state in long-running programs.
 *
 *  - When adding a new data structure, update `DAGSchedulerSuite.assertDataStructuresEmpty` to
 *    include the new structure. This will help to catch memory leaks.
 */
private[spark] class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = new SystemClock())
  extends Logging {

  def this(sc: SparkContext, taskScheduler: TaskScheduler) = {
    this(
      sc,
      taskScheduler,
      sc.listenerBus,
      sc.env.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      sc.env.blockManager.master,
      sc.env)
  }

  def this(sc: SparkContext) = this(sc, sc.taskScheduler)

  private[spark] val metricsSource: DAGSchedulerSource = new DAGSchedulerSource(this)

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[spark] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  /**
   * Mapping from shuffle dependency ID to the ShuffleMapStage that will generate the data for
   * that dependency. Only includes stages that are part of currently running job (when the job(s)
   * that require the shuffle stage complete, the mapping will be removed, and the only record of
   * the shuffle data will be in the MapOutputTracker).
   */
  private[scheduler] val shuffleIdToMapStage = new HashMap[Int, ShuffleMapStage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  private[spark] val activeJobs = new HashSet[ActiveJob]

  // Job groups that are cancelled with `cancelFutureJobs` as true, with at most
  // `NUM_CANCELLED_JOB_GROUPS_TO_TRACK` stored. On a new job submission, if its job group is in
  // this set, the job will be immediately cancelled.
  private[scheduler] val cancelledJobGroups =
    new LimitedSizeFIFOSet[String](sc.getConf.get(config.NUM_CANCELLED_JOB_GROUPS_TO_TRACK))

  /**
   * Contains the locations that each RDD's partitions are cached on.  This map's keys are RDD ids
   * and its values are arrays indexed by partition numbers. Each array value is the set of
   * locations where that RDD partition is cached.
   *
   * All accesses to this map should be guarded by synchronizing on it (see SPARK-4454).
   * If you need to access any RDD while synchronizing on the cache locations,
   * first synchronize on the RDD, and then synchronize on this map to avoid deadlocks. The RDD
   * could try to access the cache locations after synchronizing on the RDD.
   */
  private val cacheLocs = new HashMap[Int, IndexedSeq[Seq[TaskLocation]]]

  /**
   * Tracks the latest epoch of a fully processed error related to the given executor. (We use
   * the MapOutputTracker's epoch number, which is sent with every task.)
   *
   * When an executor fails, it can affect the results of many tasks, and we have to deal with
   * all of them consistently. We don't simply ignore all future results from that executor,
   * as the failures may have been transient; but we also don't want to "overreact" to follow-
   * on errors we receive. Furthermore, we might receive notification of a task success, after
   * we find out the executor has actually failed; we'll assume those successes are, in fact,
   * simply delayed notifications and the results have been lost, if the tasks started in the
   * same or an earlier epoch. In particular, we use this to control when we tell the
   * BlockManagerMaster that the BlockManager has been lost.
   */
  private val executorFailureEpoch = new HashMap[String, Long]

  /**
   * Tracks the latest epoch of a fully processed error where shuffle files have been lost from
   * the given executor.
   *
   * This is closely related to executorFailureEpoch. They only differ for the executor when
   * there is an external shuffle service serving shuffle files and we haven't been notified that
   * the entire worker has been lost. In that case, when an executor is lost, we do not update
   * the shuffleFileLostEpoch; we wait for a fetch failure. This way, if only the executor
   * fails, we do not unregister the shuffle data as it can still be served; but if there is
   * a failure in the shuffle service (resulting in fetch failure), we unregister the shuffle
   * data only once, even if we get many fetch failures.
   */
  private val shuffleFileLostEpoch = new HashMap[String, Long]

  private [scheduler] val outputCommitCoordinator = env.outputCommitCoordinator

  // A closure serializer that we reuse.
  // This is only safe because DAGScheduler runs in a single thread.
  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  /** If enabled, FetchFailed will not cause stage retry, in order to surface the problem. */
  private val disallowStageRetryForTest = sc.getConf.get(TEST_NO_STAGE_RETRY)

  private val shouldMergeResourceProfiles = sc.getConf.get(config.RESOURCE_PROFILE_MERGE_CONFLICTS)

  /**
   * Whether to unregister all the outputs on the host in condition that we receive a FetchFailure,
   * this is set default to false, which means, we only unregister the outputs related to the exact
   * executor(instead of the host) on a FetchFailure.
   */
  private[scheduler] val unRegisterOutputOnHostOnFetchFailure =
    sc.getConf.get(config.UNREGISTER_OUTPUT_ON_HOST_ON_FETCH_FAILURE)

  /**
   * Number of consecutive stage attempts allowed before a stage is aborted.
   */
  private[scheduler] val maxConsecutiveStageAttempts =
    sc.getConf.get(config.STAGE_MAX_CONSECUTIVE_ATTEMPTS)

  /**
   * Max stage attempts allowed before a stage is aborted.
   */
  private[scheduler] val maxStageAttempts: Int = {
    Math.max(maxConsecutiveStageAttempts, sc.getConf.get(config.STAGE_MAX_ATTEMPTS))
  }

  /**
   * Whether ignore stage fetch failure caused by executor decommission when
   * count spark.stage.maxConsecutiveAttempts
   */
  private[scheduler] val ignoreDecommissionFetchFailure =
    sc.getConf.get(config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE)

  /**
   * Number of max concurrent tasks check failures for each barrier job.
   */
  private[scheduler] val barrierJobIdToNumTasksCheckFailures = new ConcurrentHashMap[Int, Int]

  /**
   * Time in seconds to wait between a max concurrent tasks check failure and the next check.
   */
  private val timeIntervalNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_INTERVAL)

  /**
   * Max number of max concurrent tasks check failures allowed for a job before fail the job
   * submission.
   */
  private val maxFailureNumTasksCheck = sc.getConf
    .get(config.BARRIER_MAX_CONCURRENT_TASKS_CHECK_MAX_FAILURES)

  private val messageScheduler =
    ThreadUtils.newDaemonSingleThreadScheduledExecutor("dag-scheduler-message")

  private[spark] var eventProcessLoop = new DAGSchedulerEventProcessLoop(this)
  // Used for test only. Some tests uses the same thread of the event poster to
  // process the events to ensure the deterministic behavior during the test.
  private[spark] def setEventProcessLoop(loop: DAGSchedulerEventProcessLoop): Unit = {
    eventProcessLoop = loop
  }

  taskScheduler.setDAGScheduler(this)

  private val pushBasedShuffleEnabled = Utils.isPushBasedShuffleEnabled(sc.getConf, isDriver = true)

  private val blockManagerMasterDriverHeartbeatTimeout =
    sc.getConf.get(config.STORAGE_BLOCKMANAGER_MASTER_DRIVER_HEARTBEAT_TIMEOUT).millis

  private val shuffleMergeResultsTimeoutSec =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_RESULTS_TIMEOUT)

  private val shuffleMergeFinalizeWaitSec =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_FINALIZE_TIMEOUT)

  private val shuffleMergeWaitMinSizeThreshold =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_SIZE_MIN_SHUFFLE_SIZE_TO_WAIT)

  private val shufflePushMinRatio = sc.getConf.get(config.PUSH_BASED_SHUFFLE_MIN_PUSH_RATIO)

  private val shuffleMergeFinalizeNumThreads =
    sc.getConf.get(config.PUSH_BASED_SHUFFLE_MERGE_FINALIZE_THREADS)

  private val shuffleFinalizeRpcThreads = sc.getConf.get(config.PUSH_SHUFFLE_FINALIZE_RPC_THREADS)

  // Since SparkEnv gets initialized after DAGScheduler, externalShuffleClient needs to be
  // initialized lazily
  private lazy val externalShuffleClient: Option[BlockStoreClient] =
    if (pushBasedShuffleEnabled) {
      Some(env.blockManager.blockStoreClient)
    } else {
      None
    }

  // When push-based shuffle is enabled, spark driver will submit a finalize task which will send
  // a finalize rpc to each merger ESS after the shuffle map stage is complete. The merge
  // finalization takes up to PUSH_BASED_SHUFFLE_MERGE_RESULTS_TIMEOUT.
  private val shuffleMergeFinalizeScheduler =
    ThreadUtils.newDaemonThreadPoolScheduledExecutor("shuffle-merge-finalizer",
      shuffleMergeFinalizeNumThreads)

  // Send finalize RPC tasks to merger ESS
  private val shuffleSendFinalizeRpcExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(shuffleFinalizeRpcThreads, "shuffle-merge-finalize-rpc")

  /** Whether rdd cache visibility tracking is enabled. */
  private val trackingCacheVisibility: Boolean =
    sc.getConf.get(RDD_CACHE_VISIBILITY_TRACKING_ENABLED)

  /** Whether to abort a stage after canceling all of its tasks. */
  private val legacyAbortStageAfterKillTasks = sc.getConf.get(LEGACY_ABORT_STAGE_AFTER_KILL_TASKS)

  /**
   * Called by the TaskSetManager to report task's starting.
   */
  def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(BeginEvent(task, taskInfo))
  }

  /**
   * Called by the TaskSetManager to report that a task has completed
   * and results are being fetched remotely.
   */
  def taskGettingResult(taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(GettingResultEvent(taskInfo))
  }

  /**
   * Called by the TaskSetManager to report task completions or failures.
   */
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Seq[AccumulatorV2[_, _]],
      metricPeaks: Array[Long],
      taskInfo: TaskInfo): Unit = {
    eventProcessLoop.post(
      CompletionEvent(task, reason, result, accumUpdates, metricPeaks, taskInfo))
  }

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      // (taskId, stageId, stageAttemptId, accumUpdates)
      accumUpdates: Array[(Long, Int, Int, Seq[AccumulableInfo])],
      blockManagerId: BlockManagerId,
      // (stageId, stageAttemptId) -> metrics
      executorUpdates: mutable.Map[(Int, Int), ExecutorMetrics]): Boolean = {
    listenerBus.post(SparkListenerExecutorMetricsUpdate(execId, accumUpdates.toImmutableArraySeq,
      executorUpdates))
    blockManagerMaster.driverHeartbeatEndPoint.askSync[Boolean](
      BlockManagerHeartbeat(blockManagerId),
      new RpcTimeout(blockManagerMasterDriverHeartbeatTimeout, "BlockManagerHeartbeat"))
  }

  /**
   * Called by TaskScheduler implementation when an executor fails.
   */
  def executorLost(execId: String, reason: ExecutorLossReason): Unit = {
    eventProcessLoop.post(ExecutorLost(execId, reason))
  }

  /**
   * Called by TaskScheduler implementation when a worker is removed.
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit = {
    eventProcessLoop.post(WorkerRemoved(workerId, host, message))
  }

  /**
   * Called by TaskScheduler implementation when a host is added.
   */
  def executorAdded(execId: String, host: String): Unit = {
    eventProcessLoop.post(ExecutorAdded(execId, host))
  }

  /**
   * Called by the TaskSetManager to cancel an entire TaskSet due to either repeated failures or
   * cancellation of the job itself.
   */
  def taskSetFailed(taskSet: TaskSet, reason: String, exception: Option[Throwable]): Unit = {
    eventProcessLoop.post(TaskSetFailed(taskSet, reason, exception))
  }

  /**
   * Called by the TaskSetManager when it decides a speculative task is needed.
   */
  def speculativeTaskSubmitted(task: Task[_], taskIndex: Int): Unit = {
    eventProcessLoop.post(SpeculativeTaskSubmitted(task, taskIndex))
  }

  /**
   * Called by the TaskSetManager when a taskset becomes unschedulable due to executors being
   * excluded because of too many task failures and dynamic allocation is enabled.
   */
  def unschedulableTaskSetAdded(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  /**
   * Called by the TaskSetManager when an unschedulable taskset becomes schedulable and dynamic
   * allocation is enabled.
   */
  def unschedulableTaskSetRemoved(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    eventProcessLoop.post(UnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }

  private[scheduler]
  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = rdd.stateLock.synchronized {
    cacheLocs.synchronized {
      // Note: this doesn't use `getOrElse()` because this method is called O(num tasks) times
      if (!cacheLocs.contains(rdd.id)) {
        // Note: if the storage level is NONE, we don't need to get locations from block manager.
        val locs: IndexedSeq[Seq[TaskLocation]] = if (rdd.getStorageLevel == StorageLevel.NONE) {
          IndexedSeq.fill(rdd.partitions.length)(Nil)
        } else {
          val blockIds =
            rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
          blockManagerMaster.getLocations(blockIds).map { bms =>
            bms.map(bm => TaskLocation(bm.host, bm.executorId))
          }
        }
        cacheLocs(rdd.id) = locs
      }
      cacheLocs(rdd.id)
    }
  }

  private def clearCacheLocs(): Unit = cacheLocs.synchronized {
    cacheLocs.clear()
  }

  /**
   * Gets a shuffle map stage if one exists in shuffleIdToMapStage. Otherwise, if the
   * shuffle map stage doesn't already exist, this method will create the shuffle map stage in
   * addition to any missing ancestor shuffle map stages.
   */
  private def getOrCreateShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleIdToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) =>
        stage

      case None =>
        // Create stages for all missing ancestor shuffle dependencies.
        getMissingAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          // Even though getMissingAncestorShuffleDependencies only returns shuffle dependencies
          // that were not already in shuffleIdToMapStage, it's possible that by the time we
          // get to a particular dependency in the foreach loop, it's been added to
          // shuffleIdToMapStage by the stage creation process for an earlier dependency. See
          // SPARK-13902 for more information.
          if (!shuffleIdToMapStage.contains(dep.shuffleId)) {
            createShuffleMapStage(dep, firstJobId)
          }
        }
        // Finally, create a stage for the given shuffle dependency.
        createShuffleMapStage(shuffleDep, firstJobId)
    }
  }

  /**
   * Check to make sure we don't launch a barrier stage with unsupported RDD chain pattern. The
   * following patterns are not supported:
   * 1. Ancestor RDDs that have different number of partitions from the resulting RDD (e.g.
   * union()/coalesce()/first()/take()/PartitionPruningRDD);
   * 2. An RDD that depends on multiple barrier RDDs (e.g. barrierRdd1.zip(barrierRdd2)).
   */
  private def checkBarrierStageWithRDDChainPattern(rdd: RDD[_], numTasksInStage: Int): Unit = {
    if (rdd.isBarrier() &&
        !traverseParentRDDsWithinStage(rdd, (r: RDD[_]) =>
          r.getNumPartitions == numTasksInStage &&
          r.dependencies.count(_.rdd.isBarrier()) <= 1)) {
      throw SparkCoreErrors.barrierStageWithRDDChainPatternError()
    }
  }

  /**
   * Creates a ShuffleMapStage that generates the given shuffle dependency's partitions. If a
   * previously run stage generated the same shuffle data, this function will copy the output
   * locations that are still available from the previous shuffle to avoid unnecessarily
   * regenerating data.
   */
  def createShuffleMapStage[K, V, C](
      shuffleDep: ShuffleDependency[K, V, C], jobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd)
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd, resourceProfile)
    checkBarrierStageWithRDDChainPattern(rdd, rdd.getNumPartitions)
    val numTasks = rdd.partitions.length
    val parents = getOrCreateParentStages(shuffleDeps, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ShuffleMapStage(
      id, rdd, numTasks, parents, jobId, rdd.creationSite, shuffleDep, mapOutputTracker,
      resourceProfile.id)

    stageIdToStage(id) = stage
    shuffleIdToMapStage(shuffleDep.shuffleId) = stage
    updateJobIdStageIdMaps(jobId, stage)

    if (!mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo(log"Registering RDD ${MDC(RDD_ID, rdd.id)} " +
        log"(${MDC(CREATION_SITE, rdd.getCreationSite)}) as input to " +
        log"shuffle ${MDC(SHUFFLE_ID, shuffleDep.shuffleId)}")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length,
        shuffleDep.partitioner.numPartitions)
    }
    stage
  }

  /**
   * We don't support run a barrier stage with dynamic resource allocation enabled, it shall lead
   * to some confusing behaviors (e.g. with dynamic resource allocation enabled, it may happen that
   * we acquire some executors (but not enough to launch all the tasks in a barrier stage) and
   * later release them due to executor idle time expire, and then acquire again).
   *
   * We perform the check on job submit and fail fast if running a barrier stage with dynamic
   * resource allocation enabled.
   *
   * TODO SPARK-24942 Improve cluster resource management with jobs containing barrier stage
   */
  private def checkBarrierStageWithDynamicAllocation(rdd: RDD[_]): Unit = {
    if (rdd.isBarrier() && Utils.isDynamicAllocationEnabled(sc.getConf)) {
      throw SparkCoreErrors.barrierStageWithDynamicAllocationError()
    }
  }

  /**
   * Check whether the barrier stage requires more slots (to be able to launch all tasks in the
   * barrier stage together) than the total number of active slots currently. Fail current check
   * if trying to submit a barrier stage that requires more slots than current total number. If
   * the check fails consecutively beyond a configured number for a job, then fail current job
   * submission.
   */
  private def checkBarrierStageWithNumSlots(rdd: RDD[_], rp: ResourceProfile): Unit = {
    if (rdd.isBarrier()) {
      val numPartitions = rdd.getNumPartitions
      val maxNumConcurrentTasks = sc.maxNumConcurrentTasks(rp)
      if (numPartitions > maxNumConcurrentTasks) {
        throw SparkCoreErrors.numPartitionsGreaterThanMaxNumConcurrentTasksError(numPartitions,
          maxNumConcurrentTasks)
      }
    }
  }

  private[scheduler] def mergeResourceProfilesForStage(
      stageResourceProfiles: HashSet[ResourceProfile]): ResourceProfile = {
    logDebug(s"Merging stage rdd profiles: $stageResourceProfiles")
    val resourceProfile = if (stageResourceProfiles.size > 1) {
      if (shouldMergeResourceProfiles) {
        val startResourceProfile = stageResourceProfiles.head
        val mergedProfile = stageResourceProfiles.drop(1)
          .foldLeft(startResourceProfile)((a, b) => mergeResourceProfiles(a, b))
        // compared merged profile with existing ones so we don't add it over and over again
        // if the user runs the same operation multiple times
        val resProfile = sc.resourceProfileManager.getEquivalentProfile(mergedProfile)
        resProfile match {
          case Some(existingRp) => existingRp
          case None =>
            // this ResourceProfile could be different if it was merged so we have to add it to
            // our ResourceProfileManager
            sc.resourceProfileManager.addResourceProfile(mergedProfile)
            mergedProfile
        }
      } else {
        throw new IllegalArgumentException("Multiple ResourceProfiles specified in the RDDs for " +
          "this stage, either resolve the conflicting ResourceProfiles yourself or enable " +
          s"${config.RESOURCE_PROFILE_MERGE_CONFLICTS.key} and understand how Spark handles " +
          "the merging them.")
      }
    } else {
      if (stageResourceProfiles.size == 1) {
        stageResourceProfiles.head
      } else {
        sc.resourceProfileManager.defaultResourceProfile
      }
    }
    resourceProfile
  }

  // This is a basic function to merge resource profiles that takes the max
  // value of the profiles. We may want to make this more complex in the future as
  // you may want to sum some resources (like memory).
  private[scheduler] def mergeResourceProfiles(
      r1: ResourceProfile,
      r2: ResourceProfile): ResourceProfile = {
    val mergedExecKeys = r1.executorResources ++ r2.executorResources
    val mergedExecReq = mergedExecKeys.map { case (k, v) =>
        val larger = r1.executorResources.get(k).map( x =>
          if (x.amount > v.amount) x else v).getOrElse(v)
        k -> larger
    }
    val mergedTaskKeys = r1.taskResources ++ r2.taskResources
    val mergedTaskReq = mergedTaskKeys.map { case (k, v) =>
      val larger = r1.taskResources.get(k).map( x =>
        if (x.amount > v.amount) x else v).getOrElse(v)
      k -> larger
    }

    if (mergedExecReq.isEmpty) {
      new TaskResourceProfile(mergedTaskReq)
    } else {
      new ResourceProfile(mergedExecReq, mergedTaskReq)
    }
  }

  /**
   * Create a ResultStage associated with the provided jobId.
   */
  private def createResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val (shuffleDeps, resourceProfiles) = getShuffleDependenciesAndResourceProfiles(rdd)
    val resourceProfile = mergeResourceProfilesForStage(resourceProfiles)
    checkBarrierStageWithDynamicAllocation(rdd)
    checkBarrierStageWithNumSlots(rdd, resourceProfile)
    checkBarrierStageWithRDDChainPattern(rdd, partitions.toSet.size)
    val parents = getOrCreateParentStages(shuffleDeps, jobId)
    val id = nextStageId.getAndIncrement()
    val stage = new ResultStage(id, rdd, func, partitions, parents, jobId,
      callSite, resourceProfile.id)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stage
  }

  /**
   * Get or create the list of parent stages for the given shuffle dependencies. The new
   * Stages will be created with the provided firstJobId.
   */
  private def getOrCreateParentStages(shuffleDeps: HashSet[ShuffleDependency[_, _, _]],
      firstJobId: Int): List[Stage] = {
    shuffleDeps.map { shuffleDep =>
      getOrCreateShuffleMapStage(shuffleDep, firstJobId)
    }.toList
  }

  /** Find ancestor shuffle dependencies that are not registered in shuffleToMapStage yet */
  private def getMissingAncestorShuffleDependencies(
      rdd: RDD[_]): ListBuffer[ShuffleDependency[_, _, _]] = {
    val ancestors = new ListBuffer[ShuffleDependency[_, _, _]]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        val (shuffleDeps, _) = getShuffleDependenciesAndResourceProfiles(toVisit)
        shuffleDeps.foreach { shuffleDep =>
          if (!shuffleIdToMapStage.contains(shuffleDep.shuffleId)) {
            ancestors.prepend(shuffleDep)
            waitingForVisit.prepend(shuffleDep.rdd)
          } // Otherwise, the dependency and its ancestors have already been registered.
        }
      }
    }
    ancestors
  }

  /**
   * Returns shuffle dependencies that are immediate parents of the given RDD and the
   * ResourceProfiles associated with the RDDs for this stage.
   *
   * This function will not return more distant ancestors for shuffle dependencies. For example,
   * if C has a shuffle dependency on B which has a shuffle dependency on A:
   *
   * A <-- B <-- C
   *
   * calling this function with rdd C will only return the B <-- C dependency.
   *
   * This function is scheduler-visible for the purpose of unit testing.
   */
  private[scheduler] def getShuffleDependenciesAndResourceProfiles(
      rdd: RDD[_]): (HashSet[ShuffleDependency[_, _, _]], HashSet[ResourceProfile]) = {
    val parents = new HashSet[ShuffleDependency[_, _, _]]
    val resourceProfiles = new HashSet[ResourceProfile]
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        visited += toVisit
        Option(toVisit.getResourceProfile()).foreach(resourceProfiles += _)
        toVisit.dependencies.foreach {
          case shuffleDep: ShuffleDependency[_, _, _] =>
            parents += shuffleDep
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    (parents, resourceProfiles)
  }

  /**
   * Traverses the given RDD and its ancestors within the same stage and checks whether all of the
   * RDDs satisfy a given predicate.
   */
  private def traverseParentRDDsWithinStage(rdd: RDD[_], predicate: RDD[_] => Boolean): Boolean = {
    val visited = new HashSet[RDD[_]]
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd
    while (waitingForVisit.nonEmpty) {
      val toVisit = waitingForVisit.remove(0)
      if (!visited(toVisit)) {
        if (!predicate(toVisit)) {
          return false
        }
        visited += toVisit
        toVisit.dependencies.foreach {
          case _: ShuffleDependency[_, _, _] =>
            // Not within the same stage with current rdd, do nothing.
          case dependency =>
            waitingForVisit.prepend(dependency.rdd)
        }
      }
    }
    true
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd
    def visit(rdd: RDD[_]): Unit = {
      if (!visited(rdd)) {
        visited += rdd
        val rddHasUncachedPartitions = getCacheLocs(rdd).contains(Nil)
        if (rddHasUncachedPartitions) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
                // Mark mapStage as available with shuffle outputs only after shuffle merge is
                // finalized with push based shuffle. If not, subsequent ShuffleMapStage won't
                // read from merged output as the MergeStatuses are not available.
                if (!mapStage.isAvailable || !mapStage.shuffleDep.shuffleMergeFinalized) {
                  missing += mapStage
                } else {
                  // Forward the nextAttemptId if skipped and get visited for the first time.
                  // Otherwise, once it gets retried,
                  // 1) the stuffs in stage info become distorting, e.g. task num, input byte, e.t.c
                  // 2) the first attempt starts from 0-idx, it will not be marked as a retry
                  mapStage.increaseAttemptIdOnFirstSkip()
                }
              case narrowDep: NarrowDependency[_] =>
                waitingForVisit.prepend(narrowDep.rdd)
            }
          }
        }
      }
    }
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    missing.toList
  }

  /** Invoke `.partitions` on the given RDD and all of its ancestors  */
  private def eagerlyComputePartitionsForRddAndAncestors(rdd: RDD[_]): Unit = {
    val startTime = System.nanoTime
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += rdd

    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd

        // Eagerly compute:
        rdd.partitions

        for (dep <- rdd.dependencies) {
          waitingForVisit.prepend(dep.rdd)
        }
      }
    }

    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    logDebug("eagerlyComputePartitionsForRddAndAncestors for RDD %d took %f seconds"
      .format(rdd.id, (System.nanoTime - startTime) / 1e9))
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage): Unit = {
    @tailrec
    def updateJobIdStageIdMapsList(stages: List[Stage]): Unit = {
      if (stages.nonEmpty) {
        val s = stages.head
        s.jobIds += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parentsWithoutThisJobId = s.parents.filter { ! _.jobIds.contains(jobId) }
        updateJobIdStageIdMapsList(parentsWithoutThisJobId ++ stages.tail)
      }
    }
    updateJobIdStageIdMapsList(List(stage))
  }

  /**
   * Removes state for job and any stages that are not needed by any other job.  Does not
   * handle cancelling tasks or notifying the SparkListener about finished jobs/stages/tasks.
   *
   * @param job The job whose state to cleanup.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob): Unit = {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError(log"No stages registered for job ${MDC(JOB_ID, job.jobId)}")
    } else {
      stageIdToStage.filter {
        case (stageId, _) => registeredStages.get.contains(stageId)
      }.foreach {
        case (stageId, stage) =>
          val jobSet = stage.jobIds
          if (!jobSet.contains(job.jobId)) {
            // scalastyle:off line.size.limit
            logError(log"Job ${MDC(JOB_ID, job.jobId)} not registered for stage ${MDC(STAGE_ID, stageId)} even though that stage was registered for the job")
            // scalastyle:on
          } else {
            def removeStage(stageId: Int): Unit = {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                for ((k, v) <- shuffleIdToMapStage.find(_._2 == stage)) {
                  shuffleIdToMapStage.remove(k)
                }
                if (waitingStages.contains(stage)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waitingStages -= stage
                }
                if (failedStages.contains(stage)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failedStages -= stage
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= job.jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              removeStage(stageId)
            }
          }
      }
    }
    jobIdToStageIds -= job.jobId
    jobIdToActiveJob -= job.jobId
    activeJobs -= job
    job.finalStage match {
      case r: ResultStage => r.removeActiveJob()
      case m: ShuffleMapStage => m.removeActiveJob(job)
    }
  }

  /**
   * Submit an action job to the scheduler.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @return a JobWaiter object that can be used to block until the job finishes executing
   *         or can be used to cancel the job.
   *
   * @throws IllegalArgumentException when partitions ids are illegal
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): JobWaiter[U] = {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    val jobId = nextJobId.getAndIncrement()
    if (partitions.isEmpty) {
      val clonedProperties = Utils.cloneProperties(properties)
      if (sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION) == null) {
        clonedProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, callSite.shortForm)
      }
      val time = clock.getTimeMillis()
      listenerBus.post(
        SparkListenerJobStart(jobId, time, Seq.empty, clonedProperties))
      listenerBus.post(
        SparkListenerJobEnd(jobId, time, JobSucceeded))
      // Return immediately if the job is running 0 tasks
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.nonEmpty)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter[U](this, jobId, partitions.size, resultHandler)
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, partitions.toArray, callSite, waiter,
      JobArtifactSet.getActiveOrDefault(sc),
      Utils.cloneProperties(properties)))
    waiter
  }

  /**
   * Run an action job on the given RDD and pass all the results to the resultHandler function as
   * they arrive.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param partitions set of partitions to run on; some jobs may not want to compute on all
   *   partitions of the target RDD, e.g. for operations like first()
   * @param callSite where in the user program this job was called
   * @param resultHandler callback to pass each result to
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   *
   * @note Throws `Exception` when the job fails
   */
  def runJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      resultHandler: (Int, U) => Unit,
      properties: Properties): Unit = {
    val start = System.nanoTime
    val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties)
    ThreadUtils.awaitReady(waiter.completionFuture, Duration.Inf)
    waiter.completionFuture.value.get match {
      case scala.util.Success(_) =>
        logInfo(log"Job ${MDC(LogKeys.JOB_ID, waiter.jobId)} finished: " +
          log"${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}, took " +
          log"${MDC(LogKeys.TIME, (System.nanoTime - start) / 1e6)} ms")
      case scala.util.Failure(exception) =>
        logInfo(log"Job ${MDC(LogKeys.JOB_ID, waiter.jobId)} failed: " +
          log"${MDC(LogKeys.CALL_SITE_SHORT_FORM, callSite.shortForm)}, took " +
          log"${MDC(LogKeys.TIME, (System.nanoTime - start) / 1e6)} ms")
        // SPARK-8644: Include user stack trace in exceptions coming from DAGScheduler.
        val callerStackTrace = Thread.currentThread().getStackTrace.tail
        exception.setStackTrace(exception.getStackTrace ++ callerStackTrace)
        throw exception
    }
  }

  /**
   * Run an approximate job on the given RDD and pass all the results to an ApproximateEvaluator
   * as they arrive. Returns a partial result object from the evaluator.
   *
   * @param rdd target RDD to run tasks on
   * @param func a function to run on each partition of the RDD
   * @param evaluator `ApproximateEvaluator` to receive the partial results
   * @param callSite where in the user program this job was called
   * @param timeout maximum time to wait for the job, in milliseconds
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties): PartialResult[R] = {
    val jobId = nextJobId.getAndIncrement()
    val clonedProperties = Utils.cloneProperties(properties)
    if (rdd.partitions.isEmpty) {
      // Return immediately if the job is running 0 tasks
      val time = clock.getTimeMillis()
      listenerBus.post(SparkListenerJobStart(jobId, time, Seq[StageInfo](), clonedProperties))
      listenerBus.post(SparkListenerJobEnd(jobId, time, JobSucceeded))
      return new PartialResult(evaluator.currentResult(), true)
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    eventProcessLoop.post(JobSubmitted(
      jobId, rdd, func2, rdd.partitions.indices.toArray, callSite, listener,
      JobArtifactSet.getActiveOrDefault(sc), clonedProperties))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Submit a shuffle map stage to run independently and get a JobWaiter object back. The waiter
   * can be used to block until the job finishes executing or can be used to cancel the job.
   * This method is used for adaptive query planning, to run map stages and look at statistics
   * about their outputs before submitting downstream stages.
   *
   * @param dependency the ShuffleDependency to run a map stage for
   * @param callback function called with the result of the job, which in this case will be a
   *   single MapOutputStatistics object showing how much data was produced for each partition
   * @param callSite where in the user program this job was submitted
   * @param properties scheduler properties to attach to this job, e.g. fair scheduler pool name
   */
  def submitMapStage[K, V, C](
      dependency: ShuffleDependency[K, V, C],
      callback: MapOutputStatistics => Unit,
      callSite: CallSite,
      properties: Properties): JobWaiter[MapOutputStatistics] = {

    val rdd = dependency.rdd
    val jobId = nextJobId.getAndIncrement()
    if (rdd.partitions.length == 0) {
      throw SparkCoreErrors.cannotRunSubmitMapStageOnZeroPartitionRDDError()
    }

    // SPARK-23626: `RDD.getPartitions()` can be slow, so we eagerly compute
    // `.partitions` on every RDD in the DAG to ensure that `getPartitions()`
    // is evaluated outside of the DAGScheduler's single-threaded event loop:
    eagerlyComputePartitionsForRddAndAncestors(rdd)

    // We create a JobWaiter with only one "task", which will be marked as complete when the whole
    // map stage has completed, and will be passed the MapOutputStatistics for that stage.
    // This makes it easier to avoid race conditions between the user code and the map output
    // tracker that might result if we told the user the stage had finished, but then they queries
    // the map output tracker and some node failures had caused the output statistics to be lost.
    val waiter = new JobWaiter[MapOutputStatistics](
      this, jobId, 1,
      (_: Int, r: MapOutputStatistics) => callback(r))
    eventProcessLoop.post(MapStageSubmitted(
      jobId, dependency, callSite, waiter, JobArtifactSet.getActiveOrDefault(sc),
      Utils.cloneProperties(properties)))
    waiter
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int, reason: Option[String]): Unit = {
    logInfo(log"Asked to cancel job ${MDC(JOB_ID, jobId)}")
    eventProcessLoop.post(JobCancelled(jobId, reason))
  }

  /**
   * Cancel all jobs in the given job group ID.
   * @param cancelFutureJobs if true, future submitted jobs in this job group will be cancelled
   */
  def cancelJobGroup(groupId: String, cancelFutureJobs: Boolean = false, reason: Option[String]): Unit = {
    logInfo(log"Asked to cancel job group ${MDC(GROUP_ID, groupId)} with " +
      log"cancelFutureJobs=${MDC(CANCEL_FUTURE_JOBS, cancelFutureJobs)}")
    eventProcessLoop.post(JobGroupCancelled(groupId, cancelFutureJobs, reason))
  }

  /**
   * Cancel all jobs with a given tag.
   *
   * @param tag The tag to be cancelled. Cannot contain ',' (comma) character.
   * @param reason reason for cancellation.
   * @param jobsToBeCancelled a promise to be completed with the set of job ids that are cancelled.
   */
  def cancelJobsWithTag(
      tag: String,
      reason: Option[String],
      jobsToBeCancelled: Option[Promise[Set[Int]]]): Unit = {
    SparkContext.throwIfInvalidTag(tag)
    logInfo(log"Asked to cancel jobs with tag ${MDC(TAG, tag)}")
    eventProcessLoop.post(JobTagCancelled(tag, reason, jobsToBeCancelled))
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs(): Unit = {
    eventProcessLoop.post(AllJobsCancelled)
  }

  private[scheduler] def doCancelAllJobs(): Unit = {
    // Cancel all running jobs.
    runningStages.map(_.firstJobId).foreach(handleJobCancellation(_,
      Option("as part of cancellation of all jobs")))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int, reason: Option[String]): Unit = {
    eventProcessLoop.post(StageCancelled(stageId, reason))
  }

  /**
   * Receives notification about shuffle push for a given shuffle from one map
   * task has completed
   */
  def shufflePushCompleted(shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    eventProcessLoop.post(ShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex))
  }

  /**
   * Kill a given task. It will be retried.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean = {
    taskScheduler.killTaskAttempt(taskId, interruptThread, reason)
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages(): Unit = {
    if (failedStages.nonEmpty) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.firstJobId)) {
        submitStage(stage)
      }
    }
  }

  /**
   * Check for waiting stages which are now eligible for resubmission.
   * Submits stages that depend on the given parent stage. Called when the parent stage completes
   * successfully.
   */
  private def submitWaitingChildStages(parent: Stage): Unit = {
    logTrace(s"Checking if any dependencies of $parent are now runnable")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val childStages = waitingStages.filter(_.parents.contains(parent)).toArray
    waitingStages --= childStages
    for (stage <- childStages.sortBy(_.firstJobId)) {
      submitStage(stage)
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    val jobsThatUseStage: Array[Int] = stage.jobIds.toArray.sorted
    jobsThatUseStage.find(jobIdToActiveJob.contains)
  }

  private[scheduler] def handleJobGroupCancelled(
      groupId: String,
      cancelFutureJobs: Boolean,
      reason: Option[String]): Unit = {
    // If cancelFutureJobs is true, store the cancelled job group id into internal states.
    // When a job belonging to this job group is submitted, skip running it.
    if (cancelFutureJobs) {
      logInfo(log"Add job group ${MDC(GROUP_ID, groupId)} into cancelled job groups")
      cancelledJobGroups.add(groupId)
    }

    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists {
        _.getProperty(SparkContext.SPARK_JOB_GROUP_ID) == groupId
      }
    }
    if (activeInGroup.isEmpty && !cancelFutureJobs) {
      logWarning(log"Failed to cancel job group ${MDC(GROUP_ID, groupId)}. " +
        log"Cannot find active jobs for it.")
    }
    val jobIds = activeInGroup.map(_.jobId)
    val updatedReason = reason.getOrElse("part of cancelled job group %s".format(groupId))
    jobIds.foreach(handleJobCancellation(_, Option(updatedReason)))
  }

  private[scheduler] def handleJobTagCancelled(
      tag: String,
      reason: Option[String],
      jobsToBeCancelled: Option[Promise[Set[Int]]]): Unit = {
    // Cancel all jobs belonging that have this tag.
    // First finds all active jobs with this group id, and then kill stages for them.
    val jobIds = activeJobs.filter { activeJob =>
      Option(activeJob.properties).exists { properties =>
        Option(properties.getProperty(SparkContext.SPARK_JOB_TAGS)).getOrElse("")
          .split(SparkContext.SPARK_JOB_TAGS_SEP).filter(!_.isEmpty).toSet.contains(tag)
      }
    }.map(_.jobId)
    jobsToBeCancelled.foreach(_.success(jobIds.toSet))

    val updatedReason = reason.getOrElse("part of cancelled job tag %s".format(tag))
    jobIds.foreach(handleJobCancellation(_, Option(updatedReason)))
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo): Unit = {
    listenerBus.post(SparkListenerTaskStart(task.stageId, task.stageAttemptId, taskInfo))
  }

  private[scheduler] def handleSpeculativeTaskSubmitted(task: Task[_], taskIndex: Int): Unit = {
    val speculativeTaskSubmittedEvent = new SparkListenerSpeculativeTaskSubmitted(
      task.stageId, task.stageAttemptId, taskIndex, task.partitionId)
    listenerBus.post(speculativeTaskSubmittedEvent)
  }

  private[scheduler] def handleUnschedulableTaskSetAdded(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetAdded(stageId, stageAttemptId))
  }

  private[scheduler] def handleUnschedulableTaskSetRemoved(
      stageId: Int,
      stageAttemptId: Int): Unit = {
    listenerBus.post(SparkListenerUnschedulableTaskSetRemoved(stageId, stageAttemptId))
  }

  private[scheduler] def handleStageFailed(
      stageId: Int,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def handleTaskSetFailed(
      taskSet: TaskSet,
      reason: String,
      exception: Option[Throwable]): Unit = {
    stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason, exception) }
  }

  private[scheduler] def cleanUpAfterSchedulerStop(): Unit = {
    for (job <- activeJobs) {
      val error =
        new SparkException(s"Job ${job.jobId} cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      // The `toArray` here is necessary so that we don't iterate over `runningStages` while
      // mutating it.
      runningStages.toArray.foreach { stage =>
        markStageAsFinished(stage, Some(stageFailedMessage))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo): Unit = {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
  }

  private[scheduler] def handleJobSubmitted(
      jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      artifacts: JobArtifactSet,
      properties: Properties): Unit = {
    // If this job belongs to a cancelled job group, skip running it
    val jobGroupIdOpt = Option(properties).map(_.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    if (jobGroupIdOpt.exists(cancelledJobGroups.contains(_))) {
      listener.jobFailed(
        SparkCoreErrors.sparkJobCancelledAsPartOfJobGroupError(jobId, jobGroupIdOpt.get))
      logInfo(log"Skip running a job that belongs to the cancelled job group ${MDC(GROUP_ID, jobGroupIdOpt.get)}")
      return
    }

    var finalStage: ResultStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = createResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: BarrierJobSlotsNumberCheckFailed =>
        // If jobId doesn't exist in the map, Scala coverts its value null to 0: Int automatically.
        val numCheckFailures = barrierJobIdToNumTasksCheckFailures.compute(jobId,
          (_: Int, value: Int) => value + 1)

        logWarning(log"Barrier stage in job ${MDC(JOB_ID, jobId)} " +
          log"requires ${MDC(NUM_SLOTS, e.requiredConcurrentTasks)} slots, " +
          log"but only ${MDC(MAX_SLOTS, e.maxConcurrentTasks)} are available. " +
          log"Will retry up to ${MDC(NUM_RETRIES, maxFailureNumTasksCheck - numCheckFailures + 1)} " +
          log"more times")

        if (numCheckFailures <= maxFailureNumTasksCheck) {
          messageScheduler.schedule(
            new Runnable {
              override def run(): Unit = eventProcessLoop.post(JobSubmitted(jobId, finalRDD, func,
                partitions, callSite, listener, artifacts, properties))
            },
            timeIntervalNumTasksCheck,
            TimeUnit.SECONDS
          )
          return
        } else {
          // Job failed, clear internal data.
          barrierJobIdToNumTasksCheckFailures.remove(jobId)
          listener.jobFailed(e)
          return
        }

      case e: Exception =>
        logWarning(log"Creating new stage failed due to exception - job: ${MDC(JOB_ID, jobId)}", e)
        listener.jobFailed(e)
        return
    }
    // Job submitted, clear internal data.
    barrierJobIdToNumTasksCheckFailures.remove(jobId)

    val job = new ActiveJob(jobId, finalStage, callSite, listener, artifacts, properties)
    clearCacheLocs()
    logInfo(
      log"Got job ${MDC(JOB_ID, job.jobId)} (${MDC(CALL_SITE_SHORT_FORM, callSite.shortForm)}) " +
      log"with ${MDC(NUM_PARTITIONS, partitions.length)} output partitions")
    logInfo(log"Final stage: ${MDC(STAGE_ID, finalStage)} " +
      log"(${MDC(STAGE_NAME, finalStage.name)})")
    logInfo(log"Parents of final stage: ${MDC(STAGE_ID, finalStage.parents)}")
    logInfo(log"Missing parents: ${MDC(MISSING_PARENT_STAGES, getMissingParentStages(finalStage))}")

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos =
      stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo)).toImmutableArraySeq
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    submitStage(finalStage)
  }

  private[scheduler] def handleMapStageSubmitted(jobId: Int,
      dependency: ShuffleDependency[_, _, _],
      callSite: CallSite,
      listener: JobListener,
      artifacts: JobArtifactSet,
      properties: Properties): Unit = {
    // Submitting this map stage might still require the creation of some parent stages, so make
    // sure that happens.
    var finalStage: ShuffleMapStage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = getOrCreateShuffleMapStage(dependency, jobId)
    } catch {
      case e: Exception =>
        logWarning(log"Creating new stage failed due to exception - job: ${MDC(JOB_ID, jobId)}", e)
        listener.jobFailed(e)
        return
    }

    val job = new ActiveJob(jobId, finalStage, callSite, listener, artifacts, properties)
    clearCacheLocs()
    logInfo(log"Got map stage job ${MDC(JOB_ID, jobId)} " +
      log"(${MDC(CALL_SITE_SHORT_FORM, callSite.shortForm)}) with " +
      log"${MDC(NUM_PARTITIONS, dependency.rdd.partitions.length)} output partitions")
    logInfo(log"Final stage: ${MDC(STAGE_ID, finalStage)} " +
      log"(${MDC(STAGE_NAME, finalStage.name)})")
    logInfo(log"Parents of final stage: ${MDC(PARENT_STAGES, finalStage.parents.toString)}")
    logInfo(log"Missing parents: ${MDC(MISSING_PARENT_STAGES, getMissingParentStages(finalStage))}")

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job
    activeJobs += job
    finalStage.addActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos =
      stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo)).toImmutableArraySeq
    listenerBus.post(
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos,
        Utils.cloneProperties(properties)))
    submitStage(finalStage)

    // If the whole stage has already finished, tell the listener and remove it
    if (finalStage.isAvailable) {
      markMapStageJobAsFinished(job, mapOutputTracker.getStatistics(dependency))
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage): Unit = {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug(s"submitStage($stage (name=${stage.name};" +
        s"jobs=${stage.jobIds.toSeq.sorted.mkString(",")}))")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        if (stage.getNextAttemptId >= maxStageAttempts) {
          val reason = s"$stage (name=${stage.name}) has been resubmitted for the maximum " +
            s"allowable number of times: ${maxStageAttempts}, which is the max value of " +
            s"config `${config.STAGE_MAX_ATTEMPTS.key}` and " +
            s"`${config.STAGE_MAX_CONSECUTIVE_ATTEMPTS.key}`."
          abortStage(stage, reason, None)
        } else {
          val missing = getMissingParentStages(stage).sortBy(_.id)
          logDebug("missing: " + missing)
          if (missing.isEmpty) {
            logInfo(log"Submitting ${MDC(STAGE_ID, stage)} (${MDC(RDD_ID, stage.rdd)}), " +
                    log"which has no missing parents")
            submitMissingTasks(stage, jobId.get)
          } else {
            for (parent <- missing) {
              submitStage(parent)
            }
            waitingStages += stage
          }
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id, None)
    }
  }

  /**
   * `PythonRunner` needs to know what the pyspark memory and cores settings are for the profile
   * being run. Pass them in the local properties of the task if it's set for the stage profile.
   */
  private def addPySparkConfigsToProperties(stage: Stage, properties: Properties): Unit = {
    val rp = sc.resourceProfileManager.resourceProfileFromId(stage.resourceProfileId)
    val pysparkMem = rp.getPySparkMemory
    // use the getOption on EXECUTOR_CORES.key instead of using the EXECUTOR_CORES config reader
    // because the default for this config isn't correct for standalone mode. Here we want
    // to know if it was explicitly set or not. The default profile always has it set to either
    // what user specified or default so special case it here.
    val execCores = if (rp.id == DEFAULT_RESOURCE_PROFILE_ID) {
      sc.conf.getOption(config.EXECUTOR_CORES.key)
    } else {
      val profCores = rp.getExecutorCores.map(_.toString)
      if (profCores.isEmpty) sc.conf.getOption(config.EXECUTOR_CORES.key) else profCores
    }
    pysparkMem.map(mem => properties.setProperty(PYSPARK_MEMORY_LOCAL_PROPERTY, mem.toString))
    execCores.map(cores => properties.setProperty(EXECUTOR_CORES_LOCAL_PROPERTY, cores))
  }

  /**
   * If push based shuffle is enabled, set the shuffle services to be used for the given
   * shuffle map stage for block push/merge.
   *
   * Even with dynamic resource allocation kicking in and significantly reducing the number
   * of available active executors, we would still be able to get sufficient shuffle service
   * locations for block push/merge by getting the historical locations of past executors.
   */
  private def prepareShuffleServicesForShuffleMapStage(stage: ShuffleMapStage): Unit = {
    assert(stage.shuffleDep.shuffleMergeAllowed && !stage.shuffleDep.isShuffleMergeFinalizedMarked)
    configureShufflePushMergerLocations(stage)

    val shuffleId = stage.shuffleDep.shuffleId
    val shuffleMergeId = stage.shuffleDep.shuffleMergeId
    if (stage.shuffleDep.shuffleMergeEnabled) {
      logInfo(log"Shuffle merge enabled before starting the stage for ${MDC(STAGE_ID, stage)}" +
        log" with shuffle ${MDC(SHUFFLE_ID, shuffleId)} and shuffle merge" +
        log" ${MDC(SHUFFLE_MERGE_ID, shuffleMergeId)} with" +
        log" ${MDC(NUM_MERGER_LOCATIONS, stage.shuffleDep.getMergerLocs.size.toString)} merger locations")
    } else {
      logInfo(log"Shuffle merge disabled for ${MDC(STAGE_ID, stage)} with " +
        log"shuffle ${MDC(SHUFFLE_ID, shuffleId)} and " +
        log"shuffle merge ${MDC(SHUFFLE_MERGE_ID, shuffleMergeId)}, " +
        log"but can get enabled later adaptively once enough " +
        log"mergers are available")
    }
  }

  private def configureShufflePushMergerLocations(stage: ShuffleMapStage): Unit = {
    if (stage.shuffleDep.getMergerLocs.nonEmpty) return
    val mergerLocs = sc.schedulerBackend.getShufflePushMergerLocations(
      stage.shuffleDep.partitioner.numPartitions, stage.resourceProfileId)
    if (mergerLocs.nonEmpty) {
      stage.shuffleDep.setMergerLocs(mergerLocs)
      mapOutputTracker.registerShufflePushMergerLocations(stage.shuffleDep.shuffleId, mergerLocs)
      logDebug(s"Shuffle merge locations for shuffle ${stage.shuffleDep.shuffleId} with" +
        s" shuffle merge ${stage.shuffleDep.shuffleMergeId} is" +
        s" ${stage.shuffleDep.getMergerLocs.map(_.host).mkString(", ")}")
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int): Unit = {
    logDebug("submitMissingTasks(" + stage + ")")

    // Before find missing partition, do the intermediate state clean work first.
    // The operation here can make sure for the partially completed intermediate stage,
    // `findMissingPartitions()` returns all partitions every time.
    stage match {
      case sms: ShuffleMapStage if stage.isIndeterminate && !sms.isAvailable =>
        mapOutputTracker.unregisterAllMapAndMergeOutput(sms.shuffleDep.shuffleId)
        sms.shuffleDep.newShuffleMergeState()
      case _ =>
    }

    // Figure out the indexes of partition ids to compute.
    val partitionsToCompute: Seq[Int] = stage.findMissingPartitions()

    // Use the scheduling pool, job group, description, etc. from an ActiveJob associated
    // with this Stage
    val properties = jobIdToActiveJob(jobId).properties
    addPySparkConfigsToProperties(stage, properties)

    runningStages += stage
    // SparkListenerStageSubmitted should be posted before testing whether tasks are
    // serializable. If tasks are not serializable, a SparkListenerStageCompleted event
    // will be posted, which should always come after a corresponding SparkListenerStageSubmitted
    // event.
    stage match {
      case s: ShuffleMapStage =>
        outputCommitCoordinator.stageStart(stage = s.id, maxPartitionId = s.numPartitions - 1)
        // Only generate merger location for a given shuffle dependency once.
        if (s.shuffleDep.shuffleMergeAllowed) {
          if (!s.shuffleDep.isShuffleMergeFinalizedMarked) {
            prepareShuffleServicesForShuffleMapStage(s)
          } else {
            // Disable Shuffle merge for the retry/reuse of the same shuffle dependency if it has
            // already been merge finalized. If the shuffle dependency was previously assigned
            // merger locations but the corresponding shuffle map stage did not complete
            // successfully, we would still enable push for its retry.
            s.shuffleDep.setShuffleMergeAllowed(false)
            logInfo(log"Push-based shuffle disabled for ${MDC(STAGE_ID, stage)} " +
              log"(${MDC(STAGE_NAME, stage.name)}) since it is already shuffle merge finalized")
          }
        }
      case s: ResultStage =>
        outputCommitCoordinator.stageStart(
          stage = s.id, maxPartitionId = s.rdd.partitions.length - 1)
    }
    val taskIdToLocations: Map[Int, Seq[TaskLocation]] = try {
      stage match {
        case s: ShuffleMapStage =>
          partitionsToCompute.map { id => (id, getPreferredLocs(stage.rdd, id))}.toMap
        case s: ResultStage =>
          partitionsToCompute.map { id =>
            val p = s.partitions(id)
            (id, getPreferredLocs(stage.rdd, p))
          }.toMap
      }
    } catch {
      case NonFatal(e) =>
        stage.makeNewStageAttempt(partitionsToCompute.size)
        listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
          Utils.cloneProperties(properties)))
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    stage.makeNewStageAttempt(partitionsToCompute.size, taskIdToLocations.values.toSeq)

    // If there are tasks to execute, record the submission time of the stage. Otherwise,
    // post the even without the submission time, which indicates that this stage was
    // skipped.
    if (partitionsToCompute.nonEmpty) {
      stage.latestInfo.submissionTime = Some(clock.getTimeMillis())
    }
    listenerBus.post(SparkListenerStageSubmitted(stage.latestInfo,
      Utils.cloneProperties(properties)))

    // TODO: Maybe we can keep the taskBinary in Stage to avoid serializing it multiple times.
    // Broadcasted binary for the task, used to dispatch tasks to executors. Note that we broadcast
    // the serialized copy of the RDD and for each task we will deserialize it, which means each
    // task gets a different copy of the RDD. This provides stronger isolation between tasks that
    // might modify state of objects referenced in their closures. This is necessary in Hadoop
    // where the JobConf/Configuration object is not thread-safe.
    var taskBinary: Broadcast[Array[Byte]] = null
    var partitions: Array[Partition] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      var taskBinaryBytes: Array[Byte] = null
      // taskBinaryBytes and partitions are both effected by the checkpoint status. We need
      // this synchronization in case another concurrent job is checkpointing this RDD, so we get a
      // consistent view of both variables.
      RDDCheckpointData.synchronized {
        taskBinaryBytes = stage match {
          case stage: ShuffleMapStage =>
            JavaUtils.bufferToArray(
              closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef))
          case stage: ResultStage =>
            JavaUtils.bufferToArray(closureSerializer.serialize((stage.rdd, stage.func): AnyRef))
        }

        partitions = stage.rdd.partitions
      }

      if (taskBinaryBytes.length > TaskSetManager.TASK_SIZE_TO_WARN_KIB * 1024) {
        logWarning(log"Broadcasting large task binary with size " +
          log"${MDC(NUM_BYTES, Utils.bytesToString(taskBinaryBytes.length))}")
      }
      taskBinary = sc.broadcast(taskBinaryBytes)
    } catch {
      // In the case of a failure during serialization, abort the stage.
      case e: NotSerializableException =>
        abortStage(stage, "Task not serializable: " + e.toString, Some(e))
        runningStages -= stage

        // Abort execution
        return
      case e: Throwable =>
        abortStage(stage, s"Task serialization failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage

        // Abort execution
        return
    }

    val artifacts = jobIdToActiveJob(jobId).artifacts

    val tasks: Seq[Task[_]] = try {
      val serializedTaskMetrics = closureSerializer.serialize(stage.latestInfo.taskMetrics).array()
      stage match {
        case stage: ShuffleMapStage =>
          stage.pendingPartitions.clear()
          partitionsToCompute.map { id =>
            val locs = taskIdToLocations(id)
            val part = partitions(id)
            stage.pendingPartitions += id
            new ShuffleMapTask(stage.id, stage.latestInfo.attemptNumber(), taskBinary,
              part, stage.numPartitions, locs, artifacts, properties, serializedTaskMetrics,
              Option(jobId), Option(sc.applicationId), sc.applicationAttemptId,
              stage.rdd.isBarrier())
          }

        case stage: ResultStage =>
          partitionsToCompute.map { id =>
            val p: Int = stage.partitions(id)
            val part = partitions(p)
            val locs = taskIdToLocations(id)
            new ResultTask(stage.id, stage.latestInfo.attemptNumber(),
              taskBinary, part, stage.numPartitions, locs, id, artifacts, properties,
              serializedTaskMetrics, Option(jobId), Option(sc.applicationId),
              sc.applicationAttemptId, stage.rdd.isBarrier())
          }
      }
    } catch {
      case NonFatal(e) =>
        abortStage(stage, s"Task creation failed: $e\n${Utils.exceptionString(e)}", Some(e))
        runningStages -= stage
        return
    }

    if (tasks.nonEmpty) {
      logInfo(log"Submitting ${MDC(NUM_TASKS, tasks.size)} missing tasks from " +
        log"${MDC(STAGE_ID, stage)} (${MDC(RDD_ID, stage.rdd)}) (first 15 tasks are " +
        log"for partitions ${MDC(PARTITION_IDS, tasks.take(15).map(_.partitionId))})")
      val shuffleId = stage match {
        case s: ShuffleMapStage => Some(s.shuffleDep.shuffleId)
        case _: ResultStage => None
      }

      taskScheduler.submitTasks(new TaskSet(
        tasks.toArray, stage.id, stage.latestInfo.attemptNumber(), jobId, properties,
        stage.resourceProfileId, shuffleId))
    } else {
      // Because we posted SparkListenerStageSubmitted earlier, we should mark
      // the stage as completed here in case there are no tasks to run
      stage match {
        case stage: ShuffleMapStage =>
          logDebug(s"Stage ${stage} is actually done; " +
              s"(available: ${stage.isAvailable}," +
              s"available outputs: ${stage.numAvailableOutputs}," +
              s"partitions: ${stage.numPartitions})")
          if (!stage.shuffleDep.isShuffleMergeFinalizedMarked &&
            stage.shuffleDep.getMergerLocs.nonEmpty) {
            checkAndScheduleShuffleMergeFinalize(stage)
          } else {
            processShuffleMapStageCompletion(stage)
          }
        case stage : ResultStage =>
          logDebug(s"Stage ${stage} is actually done; (partitions: ${stage.numPartitions})")
          markStageAsFinished(stage)
          submitWaitingChildStages(stage)
      }
    }
  }

  /**
   * Merge local values from a task into the corresponding accumulators previously registered
   * here on the driver.
   *
   * Although accumulators themselves are not thread-safe, this method is called only from one
   * thread, the one that runs the scheduling loop. This means we only handle one task
   * completion event at a time so we don't need to worry about locking the accumulators.
   * This still doesn't stop the caller from updating the accumulator outside the scheduler,
   * but that's not our problem since there's nothing we can do about that.
   */
  private def updateAccumulators(event: CompletionEvent): Unit = {
    val task = event.task
    val stage = stageIdToStage(task.stageId)

    event.accumUpdates.foreach { updates =>
      val id = updates.id
      try {
        // Find the corresponding accumulator on the driver and update it
        val acc: AccumulatorV2[Any, Any] = AccumulatorContext.get(id) match {
          case Some(accum) => accum.asInstanceOf[AccumulatorV2[Any, Any]]
          case None =>
            throw SparkCoreErrors.accessNonExistentAccumulatorError(id)
        }
        acc.merge(updates.asInstanceOf[AccumulatorV2[Any, Any]])
        // To avoid UI cruft, ignore cases where value wasn't updated
        if (acc.name.isDefined && !updates.isZero) {
          stage.latestInfo.accumulables(id) = acc.toInfo(None, Some(acc.value))
          event.taskInfo.setAccumulables(
            acc.toInfo(Some(updates.value), Some(acc.value)) +: event.taskInfo.accumulables)
        }
      } catch {
        case NonFatal(e) =>
          // Log the class name to make it easy to find the bad implementation
          val accumClassName = AccumulatorContext.get(id) match {
            case Some(accum) => accum.getClass.getName
            case None => "Unknown class"
          }
              logError(
                log"Failed to update accumulator ${MDC(ACCUMULATOR_ID, id)} " +
                log"(${MDC(CLASS_NAME, accumClassName)}) for task " +
                log"${MDC(PARTITION_ID, task.partitionId)}", e)
      }
    }
  }

  private def postTaskEnd(event: CompletionEvent): Unit = {
    val taskMetrics: TaskMetrics =
      if (event.accumUpdates.nonEmpty) {
        try {
          TaskMetrics.fromAccumulators(event.accumUpdates)
        } catch {
          case NonFatal(e) =>
            val taskId = event.taskInfo.taskId
            logError(
              log"Error when attempting to reconstruct metrics for task ${MDC(TASK_ID, taskId)}",
              e)
            null
        }
      } else {
        null
      }

    listenerBus.post(SparkListenerTaskEnd(event.task.stageId, event.task.stageAttemptId,
      Utils.getFormattedClassName(event.task), event.reason, event.taskInfo,
      new ExecutorMetrics(event.metricPeaks), taskMetrics))
  }

  /**
   * Check [[SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL]] in job properties to see if we should
   * interrupt running tasks. Returns `false` if the property value is not a boolean value
   */
  private def shouldInterruptTaskThread(job: ActiveJob): Boolean = {
    if (job.properties == null) {
      false
    } else {
      val shouldInterruptThread =
        job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
      try {
        shouldInterruptThread.toBoolean
      } catch {
        case e: IllegalArgumentException =>
          logWarning(log"${MDC(CONFIG, SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL)} " +
            log"in Job ${MDC(JOB_ID, job.jobId)} " +
            log"is invalid: ${MDC(CONFIG2, shouldInterruptThread)}. " +
            log"Using 'false' instead", e)
          false
      }
    }
  }

  private[scheduler] def checkAndScheduleShuffleMergeFinalize(
      shuffleStage: ShuffleMapStage): Unit = {
    // Check if a finalize task has already been scheduled. This is to prevent scenarios
    // where we don't schedule multiple shuffle merge finalization which can happen due to
    // stage retry or shufflePushMinRatio is already hit etc.
    if (shuffleStage.shuffleDep.getFinalizeTask.isEmpty) {
      // 1. Stage indeterminate and some map outputs are not available - finalize
      // immediately without registering shuffle merge results.
      // 2. Stage determinate and some map outputs are not available - decide to
      // register merge results based on map outputs size available and
      // shuffleMergeWaitMinSizeThreshold.
      // 3. All shuffle outputs available - decide to register merge results based
      // on map outputs size available and shuffleMergeWaitMinSizeThreshold.
      val totalSize = {
        lazy val computedTotalSize =
          mapOutputTracker.getStatistics(shuffleStage.shuffleDep).
            bytesByPartitionId.filter(_ > 0).sum
        if (shuffleStage.isAvailable) {
          computedTotalSize
        } else {
          if (shuffleStage.isIndeterminate) {
            0L
          } else {
            computedTotalSize
          }
        }
      }

      if (totalSize < shuffleMergeWaitMinSizeThreshold) {
        scheduleShuffleMergeFinalize(shuffleStage, delay = 0, registerMergeResults = false)
      } else {
        scheduleShuffleMergeFinalize(shuffleStage, shuffleMergeFinalizeWaitSec)
      }
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent): Unit = {
    val task = event.task
    val stageId = task.stageId

    outputCommitCoordinator.taskCompleted(
      stageId,
      task.stageAttemptId,
      task.partitionId,
      event.taskInfo.attemptNumber, // this is a task attempt number
      event.reason)

    if (!stageIdToStage.contains(task.stageId)) {
      // The stage may have already finished when we get this event -- e.g. maybe it was a
      // speculative task. It is important that we send the TaskEnd event in any case, so listeners
      // are properly notified and can chose to handle it. For instance, some listeners are
      // doing their own accounting and if they don't get the task end event they think
      // tasks are still running when they really aren't.
      postTaskEnd(event)

      // Skip all the actions if the stage has been cancelled.
      return
    }

    val stage = stageIdToStage(task.stageId)

    // Make sure the task's accumulators are updated before any other processing happens, so that
    // we can post a task end event before any jobs or stages are updated. The accumulators are
    // only updated in certain cases.
    event.reason match {
      case Success =>
        task match {
          case rt: ResultTask[_, _] =>
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                // Only update the accumulator once for each result task.
                if (!job.finished(rt.outputId)) {
                  updateAccumulators(event)
                }
              case None => // Ignore update if task's job has finished.
            }
          case _ =>
            updateAccumulators(event)
        }
      case _: ExceptionFailure | _: TaskKilled => updateAccumulators(event)
      case _ =>
    }
    if (trackingCacheVisibility) {
      // Update rdd blocks' visibility status.
      blockManagerMaster.updateRDDBlockVisibility(
        event.taskInfo.taskId, visible = event.reason == Success)
    }

    postTaskEnd(event)

    event.reason match {
      case Success =>
        // An earlier attempt of a stage (which is zombie) may still have running tasks. If these
        // tasks complete, they still count and we can mark the corresponding partitions as
        // finished if the stage is determinate. Here we notify the task scheduler to skip running
        // tasks for the same partition to save resource.
        if (!stage.isIndeterminate && task.stageAttemptId < stage.latestInfo.attemptNumber()) {
          taskScheduler.notifyPartitionCompletion(stageId, task.partitionId)
        }

        task match {
          case rt: ResultTask[_, _] =>
            // Cast to ResultStage here because it's part of the ResultTask
            // TODO Refactor this out to a function that accepts a ResultStage
            val resultStage = stage.asInstanceOf[ResultStage]
            resultStage.activeJob match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(resultStage)
                    cancelRunningIndependentStages(job, s"Job ${job.jobId} is finished.")
                    cleanupStateForJobAndIndependentStages(job)
                    try {
                      // killAllTaskAttempts will fail if a SchedulerBackend does not implement
                      // killTask.
                      logInfo(log"Job ${MDC(JOB_ID, job.jobId)} is finished. Cancelling " +
                        log"potential speculative or zombie tasks for this job")
                      // ResultStage is only used by this job. It's safe to kill speculative or
                      // zombie tasks in this stage.
                      taskScheduler.killAllTaskAttempts(
                        stageId,
                        shouldInterruptTaskThread(job),
                        reason = "Stage finished")
                    } catch {
                      case e: UnsupportedOperationException =>
                        logWarning(log"Could not cancel tasks " +
                          log"for stage ${MDC(STAGE_ID, stageId)}", e)
                    }
                    listenerBus.post(
                      SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
                  }

                  // taskSucceeded runs some user code that might throw an exception. Make sure
                  // we are resilient against that.
                  try {
                    job.listener.taskSucceeded(rt.outputId, event.result)
                  } catch {
                    case e: Throwable if !Utils.isFatalError(e) =>
                      // TODO: Perhaps we want to mark the resultStage as failed?
                      job.listener.jobFailed(new SparkDriverExecutionException(e))
                  }
                }
              case None =>
                logInfo(log"Ignoring result from ${MDC(RESULT, rt)} because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val shuffleStage = stage.asInstanceOf[ShuffleMapStage]
            // Ignore task completion for old attempt of indeterminate stage
            val ignoreIndeterminate = stage.isIndeterminate &&
              task.stageAttemptId < stage.latestInfo.attemptNumber()
            if (!ignoreIndeterminate) {
              shuffleStage.pendingPartitions -= task.partitionId
              val status = event.result.asInstanceOf[MapStatus]
              val execId = status.location.executorId
              logDebug("ShuffleMapTask finished on " + execId)
              if (executorFailureEpoch.contains(execId) &&
                smt.epoch <= executorFailureEpoch(execId)) {
                logInfo(log"Ignoring possibly bogus ${MDC(STAGE_ID, smt)} completion from " +
                  log"executor ${MDC(EXECUTOR_ID, execId)}")
              } else {
                // The epoch of the task is acceptable (i.e., the task was launched after the most
                // recent failure we're aware of for the executor), so mark the task's output as
                // available.
                mapOutputTracker.registerMapOutput(
                  shuffleStage.shuffleDep.shuffleId, smt.partitionId, status)
              }
            } else {
              logInfo(log"Ignoring ${MDC(TASK_NAME, smt)} completion from an older attempt of indeterminate stage")
            }

            if (runningStages.contains(shuffleStage) && shuffleStage.pendingPartitions.isEmpty) {
              if (!shuffleStage.shuffleDep.isShuffleMergeFinalizedMarked &&
                shuffleStage.shuffleDep.getMergerLocs.nonEmpty) {
                checkAndScheduleShuffleMergeFinalize(shuffleStage)
              } else {
                processShuffleMapStageCompletion(shuffleStage)
              }
            }
        }

      case FetchFailed(bmAddress, shuffleId, _, mapIndex, reduceId, failureMessage) =>
        val failedStage = stageIdToStage(task.stageId)
        val mapStage = shuffleIdToMapStage(shuffleId)

        if (failedStage.latestInfo.attemptNumber() != task.stageAttemptId) {
          logInfo(log"Ignoring fetch failure from " +
            log"${MDC(TASK_ID, task)} as it's from " +
            log"${MDC(STAGE_ID, failedStage)} attempt " +
            log"${MDC(STAGE_ATTEMPT, task.stageAttemptId)} and there is a more recent attempt for " +
            log"that stage (attempt " +
            log"${MDC(NUM_ATTEMPT, failedStage.latestInfo.attemptNumber())}) running")
        } else {
          val ignoreStageFailure = ignoreDecommissionFetchFailure &&
            isExecutorDecommissioningOrDecommissioned(taskScheduler, bmAddress)
          if (ignoreStageFailure) {
            logInfo(log"Ignoring fetch failure from ${MDC(TASK_NAME, task)} of " +
              log"${MDC(STAGE, failedStage)} attempt " +
              log"${MDC(STAGE_ATTEMPT, task.stageAttemptId)} when count " +
              log"${MDC(MAX_ATTEMPTS, config.STAGE_MAX_CONSECUTIVE_ATTEMPTS.key)} " +
              log"as executor ${MDC(EXECUTOR_ID, bmAddress.executorId)} is decommissioned and " +
              log"${MDC(CONFIG, config.STAGE_IGNORE_DECOMMISSION_FETCH_FAILURE.key)}=true")
          } else {
            failedStage.failedAttemptIds.add(task.stageAttemptId)
          }

          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
            disallowStageRetryForTest

          // It is likely that we receive multiple FetchFailed for a single stage (because we have
          // multiple tasks running concurrently on different executors). In that case, it is
          // possible the fetch failure has already been handled by the scheduler.
          if (runningStages.contains(failedStage)) {
            logInfo(log"Marking ${MDC(FAILED_STAGE, failedStage)} " +
              log"(${MDC(FAILED_STAGE_NAME, failedStage.name)}) as failed " +
              log"due to a fetch failure from ${MDC(STAGE, mapStage)} " +
              log"(${MDC(STAGE_NAME, mapStage.name)})")
            markStageAsFinished(failedStage, errorMessage = Some(failureMessage),
              willRetry = !shouldAbortStage)
          } else {
            logDebug(s"Received fetch failure from $task, but it's from $failedStage which is no " +
              "longer running")
          }

          if (mapStage.rdd.isBarrier()) {
            // Mark all the map as broken in the map stage, to ensure retry all the tasks on
            // resubmitted stage attempt.
            // TODO: SPARK-35547: Clean all push-based shuffle metadata like merge enabled and
            // TODO: finalized as we are clearing all the merge results.
            mapOutputTracker.unregisterAllMapAndMergeOutput(shuffleId)
          } else if (mapIndex != -1) {
            // Mark the map whose fetch failed as broken in the map stage
            mapOutputTracker.unregisterMapOutput(shuffleId, mapIndex, bmAddress)
            if (pushBasedShuffleEnabled) {
              // Possibly unregister the merge result <shuffleId, reduceId>, if the FetchFailed
              // mapIndex is part of the merge result of <shuffleId, reduceId>
              mapOutputTracker.
                unregisterMergeResult(shuffleId, reduceId, bmAddress, Option(mapIndex))
            }
          } else {
            // Unregister the merge result of <shuffleId, reduceId> if there is a FetchFailed event
            // and is not a  MetaDataFetchException which is signified by bmAddress being null
            if (bmAddress != null &&
              bmAddress.executorId.equals(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)) {
              assert(pushBasedShuffleEnabled, "Push based shuffle expected to " +
                "be enabled when handling merge block fetch failure.")
              mapOutputTracker.
                unregisterMergeResult(shuffleId, reduceId, bmAddress, None)
            }
          }

          if (failedStage.rdd.isBarrier()) {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapAndMergeOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $failureMessage"
                abortStage(failedResultStage, reason, None)
            }
          }

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Fetch failure will not retry stage due to testing config"
            } else {
              s"$failedStage (${failedStage.name}) has failed the maximum allowable number of " +
                s"times: $maxConsecutiveStageAttempts. Most recent failure reason:\n" +
                failureMessage
            }
            abortStage(failedStage, abortMessage, None)
          } else { // update failedStages and make sure a ResubmitFailedStages event is enqueued
            // TODO: Cancel running tasks in the failed stage -- cf. SPARK-17064
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            failedStages += mapStage
            if (noResubmitEnqueued) {
              // If the map stage is INDETERMINATE, which means the map tasks may return
              // different result when re-try, we need to re-try all the tasks of the failed
              // stage and its succeeding stages, because the input data will be changed after the
              // map tasks are re-tried.
              // Note that, if map stage is UNORDERED, we are fine. The shuffle partitioner is
              // guaranteed to be determinate, so the input data of the reducers will not change
              // even if the map tasks are re-tried.
              if (mapStage.isIndeterminate) {
                // It's a little tricky to find all the succeeding stages of `mapStage`, because
                // each stage only know its parents not children. Here we traverse the stages from
                // the leaf nodes (the result stages of active jobs), and rollback all the stages
                // in the stage chains that connect to the `mapStage`. To speed up the stage
                // traversing, we collect the stages to rollback first. If a stage needs to
                // rollback, all its succeeding stages need to rollback to.
                val stagesToRollback = HashSet[Stage](mapStage)

                def collectStagesToRollback(stageChain: List[Stage]): Unit = {
                  if (stagesToRollback.contains(stageChain.head)) {
                    stageChain.drop(1).foreach(s => stagesToRollback += s)
                  } else {
                    stageChain.head.parents.foreach { s =>
                      collectStagesToRollback(s :: stageChain)
                    }
                  }
                }

                def generateErrorMessage(stage: Stage): String = {
                  "A shuffle map stage with indeterminate output was failed and retried. " +
                    s"However, Spark cannot rollback the $stage to re-process the input data, " +
                    "and has to fail this job. Please eliminate the indeterminacy by " +
                    "checkpointing the RDD before repartition and try again."
                }

                activeJobs.foreach(job => collectStagesToRollback(job.finalStage :: Nil))

                // The stages will be rolled back after checking
                val rollingBackStages = HashSet[Stage](mapStage)
                stagesToRollback.foreach {
                  case mapStage: ShuffleMapStage =>
                    val numMissingPartitions = mapStage.findMissingPartitions().length
                    if (numMissingPartitions < mapStage.numTasks) {
                      if (sc.getConf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
                        val reason = "A shuffle map stage with indeterminate output was failed " +
                          "and retried. However, Spark can only do this while using the new " +
                          "shuffle block fetching protocol. Please check the config " +
                          "'spark.shuffle.useOldFetchProtocol', see more detail in " +
                          "SPARK-27665 and SPARK-25341."
                        abortStage(mapStage, reason, None)
                      } else {
                        rollingBackStages += mapStage
                      }
                    }

                  case resultStage: ResultStage if resultStage.activeJob.isDefined =>
                    val numMissingPartitions = resultStage.findMissingPartitions().length
                    if (numMissingPartitions < resultStage.numTasks) {
                      // TODO: support to rollback result tasks.
                      abortStage(resultStage, generateErrorMessage(resultStage), None)
                    }

                  case _ =>
                }
                logInfo(log"The shuffle map stage ${MDC(SHUFFLE_ID, mapStage)} with indeterminate output was failed, " +
                  log"we will roll back and rerun below stages which include itself and all its " +
                  log"indeterminate child stages: ${MDC(STAGES, rollingBackStages)}")
              }

              // We expect one executor failure to trigger many FetchFailures in rapid succession,
              // but all of those task failures can typically be handled by a single resubmission of
              // the failed stage.  We avoid flooding the scheduler's event queue with resubmit
              // messages by checking whether a resubmit is already in the event queue for the
              // failed stage.  If there is already a resubmit enqueued for a different failed
              // stage, that event would also be sufficient to handle the current failed stage, but
              // producing a resubmit for each failed stage makes debugging and logging a little
              // simpler while not producing an overwhelming number of scheduler events.
              logInfo(
                log"Resubmitting ${MDC(STAGE, mapStage)} " +
                log"(${MDC(STAGE_NAME, mapStage.name)}) and ${MDC(FAILED_STAGE, failedStage)} " +
                log"(${MDC(FAILED_STAGE_NAME, failedStage.name)}) due to fetch failure")
              messageScheduler.schedule(
                new Runnable {
                  override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
                },
                DAGScheduler.RESUBMIT_TIMEOUT,
                TimeUnit.MILLISECONDS
              )
            }
          }

          // TODO: mark the executor as failed only if there were lots of fetch failures on it
          if (bmAddress != null) {
            val externalShuffleServiceEnabled = env.blockManager.externalShuffleServiceEnabled
            val isHostDecommissioned = taskScheduler
              .getExecutorDecommissionState(bmAddress.executorId)
              .exists(_.workerHost.isDefined)

            // Shuffle output of all executors on host `bmAddress.host` may be lost if:
            // - External shuffle service is enabled, so we assume that all shuffle data on node is
            //   bad.
            // - Host is decommissioned, thus all executors on that host will die.
            val shuffleOutputOfEntireHostLost = externalShuffleServiceEnabled ||
              isHostDecommissioned
            val hostToUnregisterOutputs = if (shuffleOutputOfEntireHostLost
              && unRegisterOutputOnHostOnFetchFailure) {
              Some(bmAddress.host)
            } else {
              // Unregister shuffle data just for one executor (we don't have any
              // reason to believe shuffle data has been lost for the entire host).
              None
            }
            removeExecutorAndUnregisterOutputs(
              execId = bmAddress.executorId,
              fileLost = true,
              hostToUnregisterOutputs = hostToUnregisterOutputs,
              maybeEpoch = Some(task.epoch),
              // shuffleFileLostEpoch is ignored when a host is decommissioned because some
              // decommissioned executors on that host might have been removed before this fetch
              // failure and might have bumped up the shuffleFileLostEpoch. We ignore that, and
              // proceed with unconditional removal of shuffle outputs from all executors on that
              // host, including from those that we still haven't confirmed as lost due to heartbeat
              // delays.
              ignoreShuffleFileLostEpoch = isHostDecommissioned)
          }
        }

      case failure: TaskFailedReason if task.isBarrier =>
        // Also handle the task failed reasons here.
        failure match {
          case Resubmitted =>
            handleResubmittedFailure(task, stage)

          case _ => // Do nothing.
        }

        // Always fail the current stage and retry all the tasks when a barrier task fail.
        val failedStage = stageIdToStage(task.stageId)
        if (failedStage.latestInfo.attemptNumber() != task.stageAttemptId) {
          logInfo(log"Ignoring task failure from ${MDC(TASK_NAME, task)} as it's from " +
            log"${MDC(FAILED_STAGE, failedStage)} attempt ${MDC(STAGE_ATTEMPT, task.stageAttemptId)} " +
            log"and there is a more recent attempt for that stage (attempt " +
            log"${MDC(NUM_ATTEMPT, failedStage.latestInfo.attemptNumber())}) running")
        } else {
              logInfo(log"Marking ${MDC(STAGE_ID, failedStage.id)} (${MDC(STAGE_NAME, failedStage.name)}) " +
                log"as failed due to a barrier task failed.")
          val message = s"Stage failed because barrier task $task finished unsuccessfully.\n" +
            failure.toErrorString
          try {
            // killAllTaskAttempts will fail if a SchedulerBackend does not implement killTask.
            val reason = s"Task $task from barrier stage $failedStage (${failedStage.name}) " +
              "failed."
            val job = jobIdToActiveJob.get(failedStage.firstJobId)
            val shouldInterrupt = job.exists(j => shouldInterruptTaskThread(j))
            taskScheduler.killAllTaskAttempts(stageId, shouldInterrupt, reason)
          } catch {
            case e: UnsupportedOperationException =>
              // Cannot continue with barrier stage if failed to cancel zombie barrier tasks.
              // TODO SPARK-24877 leave the zombie tasks and ignore their completion events.
              logWarning(log"Could not kill all tasks for stage ${MDC(STAGE_ID, stageId)}", e)
              abortStage(failedStage, "Could not kill zombie barrier tasks for stage " +
                s"$failedStage (${failedStage.name})", Some(e))
          }
          markStageAsFinished(failedStage, Some(message))

          failedStage.failedAttemptIds.add(task.stageAttemptId)
          // TODO Refactor the failure handling logic to combine similar code with that of
          // FetchFailed.
          val shouldAbortStage =
            failedStage.failedAttemptIds.size >= maxConsecutiveStageAttempts ||
              disallowStageRetryForTest

          if (shouldAbortStage) {
            val abortMessage = if (disallowStageRetryForTest) {
              "Barrier stage will not retry stage due to testing config. Most recent failure " +
                s"reason: $message"
            } else {
              s"$failedStage (${failedStage.name}) has failed the maximum allowable number of " +
                s"times: $maxConsecutiveStageAttempts. Most recent failure reason: $message"
            }
            abortStage(failedStage, abortMessage, None)
          } else {
            failedStage match {
              case failedMapStage: ShuffleMapStage =>
                // Mark all the map as broken in the map stage, to ensure retry all the tasks on
                // resubmitted stage attempt.
                mapOutputTracker.unregisterAllMapAndMergeOutput(failedMapStage.shuffleDep.shuffleId)

              case failedResultStage: ResultStage =>
                // Abort the failed result stage since we may have committed output for some
                // partitions.
                val reason = "Could not recover from a failed barrier ResultStage. Most recent " +
                  s"failure reason: $message"
                abortStage(failedResultStage, reason, None)
            }
            // In case multiple task failures triggered for a single stage attempt, ensure we only
            // resubmit the failed stage once.
            val noResubmitEnqueued = !failedStages.contains(failedStage)
            failedStages += failedStage
            if (noResubmitEnqueued) {
              logInfo(log"Resubmitting ${MDC(FAILED_STAGE, failedStage)} " +
                log"(${MDC(FAILED_STAGE_NAME, failedStage.name)}) due to barrier stage failure.")
              messageScheduler.schedule(new Runnable {
                override def run(): Unit = eventProcessLoop.post(ResubmitFailedStages)
              }, DAGScheduler.RESUBMIT_TIMEOUT, TimeUnit.MILLISECONDS)
            }
          }
        }

      case Resubmitted =>
        handleResubmittedFailure(task, stage)

      case _: TaskCommitDenied =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle denied commits

      case _: ExceptionFailure | _: TaskKilled =>
        // Nothing left to do, already handled above for accumulator updates.

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case _: ExecutorLostFailure | UnknownReason =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
  }

  /**
   * Whether executor is decommissioning or decommissioned.
   * Return true when:
   *  1. Waiting for decommission start
   *  2. Under decommission process
   *  3. Stopped or terminated after finishing decommission
   *  4. Under decommission process, then removed by driver with other reasons
   * Return false in case 3 and 4 when removed executors info are not retained.
   * The max size of removed executors is controlled by
   * spark.scheduler.maxRetainedRemovedExecutors
   */
  private[scheduler] def isExecutorDecommissioningOrDecommissioned(
      taskScheduler: TaskScheduler, bmAddress: BlockManagerId): Boolean = {
    if (bmAddress != null) {
      taskScheduler
        .getExecutorDecommissionState(bmAddress.executorId)
        .nonEmpty
    } else {
      false
    }
  }

  /**
   *
   * Schedules shuffle merge finalization.
   *
   * @param stage the stage to finalize shuffle merge
   * @param delay how long to wait before finalizing shuffle merge
   * @param registerMergeResults indicate whether DAGScheduler would register the received
   *                             MergeStatus with MapOutputTracker and wait to schedule the reduce
   *                             stage until MergeStatus have been received from all mergers or
   *                             reaches timeout. For very small shuffle, this could be set to
   *                             false to avoid impact to job runtime.
   */
  private[scheduler] def scheduleShuffleMergeFinalize(
      stage: ShuffleMapStage,
      delay: Long,
      registerMergeResults: Boolean = true): Unit = {
    val shuffleDep = stage.shuffleDep
    val scheduledTask: Option[ScheduledFuture[_]] = shuffleDep.getFinalizeTask
    scheduledTask match {
      case Some(task) =>
        // If we find an already scheduled task, check if the task has been triggered yet.
        // If it's already triggered, do nothing. Otherwise, cancel it and schedule a new
        // one for immediate execution. Note that we should get here only when
        // handleShufflePushCompleted schedules a finalize task after the shuffle map stage
        // completed earlier and scheduled a task with default delay.
        // The current task should be coming from handleShufflePushCompleted, thus the
        // delay should be 0 and registerMergeResults should be true.
        assert(delay == 0 && registerMergeResults)
        if (task.getDelay(TimeUnit.NANOSECONDS) > 0 && task.cancel(false)) {
          logInfo(log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) scheduled " +
            log"for finalizing shuffle merge immediately after cancelling previously scheduled task.")
          shuffleDep.setFinalizeTask(
            shuffleMergeFinalizeScheduler.schedule(
              new Runnable {
                override def run(): Unit = finalizeShuffleMerge(stage, registerMergeResults)
              },
              0,
              TimeUnit.SECONDS
            )
          )
        } else {
          logInfo(
            log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) existing scheduled task " +
            log"for finalizing shuffle merge would either be in-progress or finished. " +
            log"No need to schedule shuffle merge finalization again.")
        }
      case None =>
        // If no previous finalization task is scheduled, schedule the finalization task.
        logInfo(log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) scheduled for " +
          log"finalizing shuffle merge in ${MDC(DELAY, delay * 1000L)} ms")
        shuffleDep.setFinalizeTask(
          shuffleMergeFinalizeScheduler.schedule(
            new Runnable {
              override def run(): Unit = finalizeShuffleMerge(stage, registerMergeResults)
            },
            delay,
            TimeUnit.SECONDS
          )
        )
    }
  }

  /**
   * DAGScheduler notifies all the remote shuffle services chosen to serve shuffle merge request for
   * the given shuffle map stage to finalize the shuffle merge process for this shuffle. This is
   * invoked in a separate thread to reduce the impact on the DAGScheduler main thread, as the
   * scheduler might need to talk to 1000s of shuffle services to finalize shuffle merge.
   *
   * @param stage ShuffleMapStage to finalize shuffle merge for
   * @param registerMergeResults indicate whether DAGScheduler would register the received
   *                             MergeStatus with MapOutputTracker and wait to schedule the reduce
   *                             stage until MergeStatus have been received from all mergers or
   *                             reaches timeout. For very small shuffle, this could be set to
   *                             false to avoid impact to job runtime.
   */
  private[scheduler] def finalizeShuffleMerge(
      stage: ShuffleMapStage,
      registerMergeResults: Boolean = true): Unit = {
    logInfo(
      log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) finalizing the shuffle merge with" +
      log" registering merge results set to ${MDC(REGISTER_MERGE_RESULTS, registerMergeResults)}")
    val shuffleId = stage.shuffleDep.shuffleId
    val shuffleMergeId = stage.shuffleDep.shuffleMergeId
    val numMergers = stage.shuffleDep.getMergerLocs.length
    val results = (0 until numMergers).map(_ => SettableFuture.create[Boolean]())
    externalShuffleClient.foreach { shuffleClient =>
      val scheduledFutures =
        if (!registerMergeResults) {
          results.foreach(_.set(true))
          // Finalize in separate thread as shuffle merge is a no-op in this case
          stage.shuffleDep.getMergerLocs.map {
            case shuffleServiceLoc =>
              // Sends async request to shuffle service to finalize shuffle merge on that host.
              // Since merge statuses will not be registered in this case,
              // we pass a no-op listener.
              shuffleSendFinalizeRpcExecutor.submit(new Runnable() {
                override def run(): Unit = {
                  shuffleClient.finalizeShuffleMerge(shuffleServiceLoc.host,
                    shuffleServiceLoc.port, shuffleId, shuffleMergeId,
                    new MergeFinalizerListener {
                      override def onShuffleMergeSuccess(statuses: MergeStatuses): Unit = {
                      }

                      override def onShuffleMergeFailure(e: Throwable): Unit = {
                      }
                    })
                }
              })
          }
        } else {
          stage.shuffleDep.getMergerLocs.zipWithIndex.map {
            case (shuffleServiceLoc, index) =>
              // Sends async request to shuffle service to finalize shuffle merge on that host
              // TODO: SPARK-35536: Cancel finalizeShuffleMerge if the stage is cancelled
              // TODO: during shuffleMergeFinalizeWaitSec
              shuffleSendFinalizeRpcExecutor.submit(new Runnable() {
                override def run(): Unit = {
                  shuffleClient.finalizeShuffleMerge(shuffleServiceLoc.host,
                    shuffleServiceLoc.port, shuffleId, shuffleMergeId,
                    new MergeFinalizerListener {
                      override def onShuffleMergeSuccess(statuses: MergeStatuses): Unit = {
                        assert(shuffleId == statuses.shuffleId)
                        eventProcessLoop.post(RegisterMergeStatuses(stage, MergeStatus.
                          convertMergeStatusesToMergeStatusArr(statuses, shuffleServiceLoc)))
                        results(index).set(true)
                      }

                      override def onShuffleMergeFailure(e: Throwable): Unit = {
                        logWarning(log"Exception encountered when trying to finalize shuffle " +
                          log"merge on ${MDC(HOST_PORT, shuffleServiceLoc.host)} " +
                          log"for shuffle ${MDC(SHUFFLE_ID, shuffleId)}", e)
                        // Do not fail the future as this would cause dag scheduler to prematurely
                        // give up on waiting for merge results from the remaining shuffle services
                        // if one fails
                        results(index).set(false)
                      }
                    })
                }
              })
          }
        }
      // DAGScheduler only waits for a limited amount of time for the merge results.
      // It will attempt to submit the next stage(s) irrespective of whether merge results
      // from all shuffle services are received or not.
      var timedOut = false
      try {
        Futures.allAsList(results: _*).get(shuffleMergeResultsTimeoutSec, TimeUnit.SECONDS)
      } catch {
        case _: TimeoutException =>
          timedOut = true
              logInfo(log"Timed out on waiting for merge results from all " +
                log"${MDC(NUM_MERGERS, numMergers)} mergers for " +
                log"shuffle ${MDC(SHUFFLE_ID, shuffleId)}")
      } finally {
        if (timedOut || !registerMergeResults) {
          cancelFinalizeShuffleMergeFutures(scheduledFutures,
            if (timedOut) 0L else shuffleMergeResultsTimeoutSec)
        }
        eventProcessLoop.post(ShuffleMergeFinalized(stage))
      }
    }
  }

  private def cancelFinalizeShuffleMergeFutures(
      futures: Seq[JFutrue[_]],
      delayInSecs: Long): Unit = {

    def cancelFutures(): Unit = futures.foreach(_.cancel(true))

    if (delayInSecs > 0) {
      shuffleMergeFinalizeScheduler.schedule(new Runnable {
        override def run(): Unit = {
          cancelFutures()
        }
      }, delayInSecs, TimeUnit.SECONDS)
    } else {
      cancelFutures()
    }
  }

  private def processShuffleMapStageCompletion(shuffleStage: ShuffleMapStage): Unit = {
    markStageAsFinished(shuffleStage)
    logInfo("looking for newly runnable stages")
    logInfo(log"running: ${MDC(STAGES, runningStages)}")
    logInfo(log"waiting: ${MDC(STAGES, waitingStages)}")
    logInfo(log"failed: ${MDC(STAGES, failedStages)}")

    // This call to increment the epoch may not be strictly necessary, but it is retained
    // for now in order to minimize the changes in behavior from an earlier version of the
    // code. This existing behavior of always incrementing the epoch following any
    // successful shuffle map stage completion may have benefits by causing unneeded
    // cached map outputs to be cleaned up earlier on executors. In the future we can
    // consider removing this call, but this will require some extra investigation.
    // See https://github.com/apache/spark/pull/17955/files#r117385673 for more details.
    mapOutputTracker.incrementEpoch()

    clearCacheLocs()

    if (!shuffleStage.isAvailable) {
      // Some tasks had failed; let's resubmit this shuffleStage.
      // TODO: Lower-level scheduler should also deal with this
      logInfo(log"Resubmitting ${MDC(STAGE, shuffleStage)} " +
        log"(${MDC(STAGE_NAME, shuffleStage.name)}) " +
        log"because some of its tasks had failed: " +
        log"${MDC(PARTITION_IDS, shuffleStage.findMissingPartitions().mkString(", "))}")
      submitStage(shuffleStage)
    } else {
      markMapStageJobsAsFinished(shuffleStage)
      submitWaitingChildStages(shuffleStage)
    }
  }

  private[scheduler] def handleRegisterMergeStatuses(
      stage: ShuffleMapStage,
      mergeStatuses: Seq[(Int, MergeStatus)]): Unit = {
    // Register merge statuses if the stage is still running and shuffle merge is not finalized yet.
    // TODO: SPARK-35549: Currently merge statuses results which come after shuffle merge
    // TODO: is finalized is not registered.
    if (runningStages.contains(stage) && !stage.shuffleDep.isShuffleMergeFinalizedMarked) {
      mapOutputTracker.registerMergeResults(stage.shuffleDep.shuffleId, mergeStatuses)
    }
  }

  private[scheduler] def handleShuffleMergeFinalized(stage: ShuffleMapStage,
        shuffleMergeId: Int): Unit = {
    // Check if update is for the same merge id - finalization might have completed for an earlier
    // adaptive attempt while the stage might have failed/killed and shuffle id is getting
    // re-executing now.
    if (stage.shuffleDep.shuffleMergeId == shuffleMergeId) {
      // When it reaches here, there is a possibility that the stage will be resubmitted again
      // because of various reasons. Some of these could be:
      // a) Stage results are not available. All the tasks completed once so the
      // pendingPartitions is empty but due to an executor failure some of the map outputs are not
      // available any more, so the stage will be re-submitted.
      // b) Stage failed due to a task failure.
      // We should mark the stage as merged finalized irrespective of what state it is in.
      // This will prevent the push being enabled for the re-attempt.
      // Note: for indeterminate stages, this doesn't matter at all, since the merge finalization
      // related state is reset during the stage submission.
      stage.shuffleDep.markShuffleMergeFinalized()
      if (stage.pendingPartitions.isEmpty)
        if (runningStages.contains(stage)) {
          processShuffleMapStageCompletion(stage)
        } else if (stage.isIndeterminate) {
          // There are 2 possibilities here - stage is either cancelled or it will be resubmitted.
          // If this is an indeterminate stage which is cancelled, we unregister all its merge
          // results here just to free up some memory. If the indeterminate stage is resubmitted,
          // merge results are cleared again when the newer attempt is submitted.
          mapOutputTracker.unregisterAllMergeResult(stage.shuffleDep.shuffleId)
          // For determinate stages, which have completed merge finalization, we don't need to
          // unregister merge results - since the stage retry, or any other stage computing the
          // same shuffle id, can use it.
        }
    }
  }

  private[scheduler] def handleShufflePushCompleted(
      shuffleId: Int, shuffleMergeId: Int, mapIndex: Int): Unit = {
    shuffleIdToMapStage.get(shuffleId) match {
      case Some(mapStage) =>
        val shuffleDep = mapStage.shuffleDep
        // Only update shufflePushCompleted events for the current active stage map tasks.
        // This is required to prevent shuffle merge finalization by dangling tasks of a
        // previous attempt in the case of indeterminate stage.
        if (shuffleDep.shuffleMergeId == shuffleMergeId) {
          if (!shuffleDep.isShuffleMergeFinalizedMarked &&
            shuffleDep.incPushCompleted(mapIndex).toDouble / shuffleDep.rdd.partitions.length
              >= shufflePushMinRatio) {
            scheduleShuffleMergeFinalize(mapStage, delay = 0)
          }
        }
      case None =>
    }
  }

  private def handleResubmittedFailure(task: Task[_], stage: Stage): Unit = {
              logInfo(log"Resubmitted ${MDC(TASK_NAME, task)}, so marking it as still running.")
    stage match {
      case sms: ShuffleMapStage =>
        sms.pendingPartitions += task.partitionId

      case _ =>
        throw SparkCoreErrors.sendResubmittedTaskStatusForShuffleMapStagesOnlyError()
    }
  }

  private[scheduler] def markMapStageJobsAsFinished(shuffleStage: ShuffleMapStage): Unit = {
    // Mark any map-stage jobs waiting on this stage as finished
    if (shuffleStage.isAvailable && shuffleStage.mapStageJobs.nonEmpty) {
      val stats = mapOutputTracker.getStatistics(shuffleStage.shuffleDep)
      for (job <- shuffleStage.mapStageJobs) {
        markMapStageJobAsFinished(job, stats)
      }
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * We will also assume that we've lost all shuffle blocks associated with the executor if the
   * executor serves its own blocks (i.e., we're not using an external shuffle service), or the
   * entire Standalone worker is lost.
   */
  private[scheduler] def handleExecutorLost(
      execId: String,
      workerHost: Option[String]): Unit = {
    // if the cluster manager explicitly tells us that the entire worker was lost, then
    // we know to unregister shuffle output.  (Note that "worker" specifically refers to the process
    // from a Standalone cluster, where the shuffle service lives in the Worker.)
    val fileLost = !sc.shuffleDriverComponents.supportsReliableStorage() &&
      (workerHost.isDefined || !env.blockManager.externalShuffleServiceEnabled)
    removeExecutorAndUnregisterOutputs(
      execId = execId,
      fileLost = fileLost,
      hostToUnregisterOutputs = workerHost,
      maybeEpoch = None)
  }

  /**
   * Handles removing an executor from the BlockManagerMaster as well as unregistering shuffle
   * outputs for the executor or optionally its host.
   *
   * @param execId executor to be removed
   * @param fileLost If true, indicates that we assume we've lost all shuffle blocks associated
   *   with the executor; this happens if the executor serves its own blocks (i.e., we're not
   *   using an external shuffle service), the entire Standalone worker is lost, or a FetchFailed
   *   occurred (in which case we presume all shuffle data related to this executor to be lost).
   * @param hostToUnregisterOutputs (optional) executor host if we're unregistering all the
   *   outputs on the host
   * @param maybeEpoch (optional) the epoch during which the failure was caught (this prevents
   *   reprocessing for follow-on fetch failures)
   */
  private def removeExecutorAndUnregisterOutputs(
      execId: String,
      fileLost: Boolean,
      hostToUnregisterOutputs: Option[String],
      maybeEpoch: Option[Long] = None,
      ignoreShuffleFileLostEpoch: Boolean = false): Unit = {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    logDebug(s"Considering removal of executor $execId; " +
      s"fileLost: $fileLost, currentEpoch: $currentEpoch")
    // Check if the execId is a shuffle push merger. We do not remove the executor if it is,
    // and only remove the outputs on the host.
    val isShuffleMerger = execId.equals(BlockManagerId.SHUFFLE_MERGER_IDENTIFIER)
    if (isShuffleMerger && pushBasedShuffleEnabled) {
      hostToUnregisterOutputs.foreach(
        host => blockManagerMaster.removeShufflePushMergerLocation(host))
    }
    if (!isShuffleMerger &&
      (!executorFailureEpoch.contains(execId) || executorFailureEpoch(execId) < currentEpoch)) {
      executorFailureEpoch(execId) = currentEpoch
      logInfo(log"Executor lost: ${MDC(EXECUTOR_ID, execId)} (epoch ${MDC(EPOCH, currentEpoch)})")
      if (pushBasedShuffleEnabled) {
        // Remove fetchFailed host in the shuffle push merger list for push based shuffle
        hostToUnregisterOutputs.foreach(
          host => blockManagerMaster.removeShufflePushMergerLocation(host))
      }
      blockManagerMaster.removeExecutor(execId)
      clearCacheLocs()
    }
    if (fileLost) {
      // When the fetch failure is for a merged shuffle chunk, ignoreShuffleFileLostEpoch is true
      // and so all the files will be removed.
      val remove = if (ignoreShuffleFileLostEpoch) {
        true
      } else if (!shuffleFileLostEpoch.contains(execId) ||
        shuffleFileLostEpoch(execId) < currentEpoch) {
        shuffleFileLostEpoch(execId) = currentEpoch
        true
      } else {
        false
      }
      if (remove) {
        hostToUnregisterOutputs match {
          case Some(host) =>
            logInfo(log"Shuffle files lost for host: ${MDC(HOST, host)} (epoch " +
              log"${MDC(EPOCH, currentEpoch)}")
            mapOutputTracker.removeOutputsOnHost(host)
          case None =>
              logInfo(log"Shuffle files lost for executor: ${MDC(EXECUTOR_ID, execId)} " +
                log"(epoch ${MDC(EPOCH, currentEpoch)})")
            mapOutputTracker.removeOutputsOnExecutor(execId)
        }
      }
    }
  }

  /**
   * Responds to a worker being removed. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use workerRemoved() to post a loss event from outside.
   *
   * We will assume that we've lost all shuffle blocks associated with the host if a worker is
   * removed, so we will remove them all from MapStatus.
   *
   * @param workerId identifier of the worker that is removed.
   * @param host host of the worker that is removed.
   * @param message the reason why the worker is removed.
   */
  private[scheduler] def handleWorkerRemoved(
      workerId: String,
      host: String,
      message: String): Unit = {
    logInfo(log"Shuffle files lost for worker ${MDC(WORKER_ID, workerId)} " +
      log"on host ${MDC(HOST, host)}")
    mapOutputTracker.removeOutputsOnHost(host)
    clearCacheLocs()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String): Unit = {
    // remove from executorFailureEpoch(execId) ?
    if (executorFailureEpoch.contains(execId)) {
      logInfo(log"Host added was in lost list earlier: ${MDC(HOST, host)}")
      executorFailureEpoch -= execId
    }
    shuffleFileLostEpoch -= execId

    if (pushBasedShuffleEnabled) {
      // Only set merger locations for stages that are not yet finished and have empty mergers
      shuffleIdToMapStage.filter { case (_, stage) =>
        stage.shuffleDep.shuffleMergeAllowed && stage.shuffleDep.getMergerLocs.isEmpty &&
          runningStages.contains(stage)
      }.foreach { case (_, stage: ShuffleMapStage) =>
        configureShufflePushMergerLocations(stage)
        if (stage.shuffleDep.getMergerLocs.nonEmpty) {
          logInfo(log"Shuffle merge enabled adaptively for ${MDC(STAGE, stage)} with shuffle" +
            log" ${MDC(SHUFFLE_ID, stage.shuffleDep.shuffleId)} and shuffle merge" +
            log" ${MDC(SHUFFLE_MERGE_ID, stage.shuffleDep.shuffleMergeId)} with " +
            log"${MDC(NUM_MERGER_LOCATIONS, stage.shuffleDep.getMergerLocs.size)} merger locations")
        }
      }
    }
  }

  private[scheduler] def handleStageCancellation(stageId: Int, reason: Option[String]): Unit = {
    stageIdToStage.get(stageId) match {
      case Some(stage) =>
        val jobsThatUseStage: Array[Int] = stage.jobIds.toArray
        jobsThatUseStage.foreach { jobId =>
          val reasonStr = reason match {
            case Some(originalReason) =>
              s"because $originalReason"
            case None =>
              s"because Stage $stageId was cancelled"
          }
          handleJobCancellation(jobId, Option(reasonStr))
        }
      case None =>
        logInfo(log"No active jobs to kill for Stage ${MDC(STAGE_ID, stageId)}")
    }
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: Option[String]): Unit = {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(
        job = jobIdToActiveJob(jobId),
        error = SparkCoreErrors.sparkJobCancelled(jobId, reason.getOrElse(""), null)
      )
    }
  }

  /**
   * Marks a stage as finished and removes it from the list of running stages.
   */
  private def markStageAsFinished(
      stage: Stage,
      errorMessage: Option[String] = None,
      willRetry: Boolean = false): Unit = {
    val serviceTime = stage.latestInfo.submissionTime match {
      case Some(t) => clock.getTimeMillis() - t
      case _ => "Unknown"
    }
    if (errorMessage.isEmpty) {
      logInfo(log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) " +
        log"finished in ${MDC(TIME_UNITS, serviceTime)} ms")
      stage.latestInfo.completionTime = Some(clock.getTimeMillis())

      // Clear failure count for this stage, now that it's succeeded.
      // We only limit consecutive failures of stage attempts,so that if a stage is
      // re-used many times in a long-running job, unrelated failures don't eventually cause the
      // stage to be aborted.
      stage.clearFailures()
    } else {
      stage.latestInfo.stageFailed(errorMessage.get)
      logInfo(log"${MDC(STAGE, stage)} (${MDC(STAGE_NAME, stage.name)}) failed in " +
        log"${MDC(TIME_UNITS, serviceTime)} ms due to ${MDC(ERROR, errorMessage.get)}")
    }
    updateStageInfoForPushBasedShuffle(stage)
    if (!willRetry) {
      outputCommitCoordinator.stageEnd(stage.id)
    }
    listenerBus.post(SparkListenerStageCompleted(stage.latestInfo))
    runningStages -= stage
  }

  /**
   * Called by the OutputCommitCoordinator to cancel stage due to data duplication may happen.
   */
  private[scheduler] def stageFailed(stageId: Int, reason: String): Unit = {
    eventProcessLoop.post(StageFailed(stageId, reason, None))
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(
      failedStage: Stage,
      reason: String,
      exception: Option[Throwable]): Unit = {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentJobs: Seq[ActiveJob] =
      activeJobs.filter(job => stageDependsOn(job.finalStage, failedStage)).toSeq
    failedStage.latestInfo.completionTime = Some(clock.getTimeMillis())
    updateStageInfoForPushBasedShuffle(failedStage)
    for (job <- dependentJobs) {
      val finalException = exception.collect {
        // If the error is user-facing (defines error class and is not internal error), we don't
        // wrap it with "Job aborted" and expose this error to the end users directly.
        case st: Exception with SparkThrowable if st.getErrorClass != null &&
            !SparkThrowableHelper.isInternalError(st.getErrorClass) =>
          st
      }.getOrElse {
        new SparkException(s"Job aborted due to stage failure: $reason", cause = exception.orNull)
      }
      failJobAndIndependentStages(job, finalException)
    }
    if (dependentJobs.isEmpty) {
      logInfo(log"Ignoring failure of ${MDC(FAILED_STAGE, failedStage)} because all jobs " +
        log"depending on it are done")
    }
  }

  private def updateStageInfoForPushBasedShuffle(stage: Stage): Unit = {
    // With adaptive shuffle mergers, StageInfo's
    // isShufflePushEnabled and shuffleMergers need to be updated at the end.
    stage match {
      case s: ShuffleMapStage =>
        stage.latestInfo.setPushBasedShuffleEnabled(s.shuffleDep.shuffleMergeEnabled)
        if (s.shuffleDep.shuffleMergeEnabled) {
          stage.latestInfo.setShuffleMergerCount(s.shuffleDep.getMergerLocs.size)
        }
      case _ =>
    }
  }

  /** Cancel all independent, running stages that are only used by this job. */
  private def cancelRunningIndependentStages(job: ActiveJob, reason: String): Boolean = {
    var ableToCancelStages = true
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError(log"No stages registered for job ${MDC(JOB_ID, job.jobId)}")
    }
    stages.foreach { stageId =>
      val jobsForStage: Option[HashSet[Int]] = stageIdToStage.get(stageId).map(_.jobIds)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(log"Job ${MDC(JOB_ID, job.jobId)} not registered for stage " +
            log"${MDC(STAGE_ID, stageId)} even though that stage was registered for the job")
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError(log"Missing Stage for stage with id ${MDC(STAGE_ID, stageId)}")
        } else {
          // This stage is only used by the job, so finish the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            try { // killAllTaskAttempts will fail if a SchedulerBackend does not implement killTask
              taskScheduler.killAllTaskAttempts(stageId, shouldInterruptTaskThread(job), reason)
              if (legacyAbortStageAfterKillTasks) {
                stageFailed(stageId, reason)
              }
              markStageAsFinished(stage, Some(reason))
            } catch {
              case e: UnsupportedOperationException =>
                logWarning(log"Could not cancel tasks for stage ${MDC(STAGE_ID, stageId)}", e)
                ableToCancelStages = false
            }
          }
        }
      }
    }
    ableToCancelStages
  }

  /** Fails a job and all stages that are only used by that job, and cleans up relevant state. */
  private def failJobAndIndependentStages(
      job: ActiveJob,
      error: Exception): Unit = {
    if (cancelRunningIndependentStages(job, error.getMessage)) {
      // SPARK-15783 important to cleanup state first, just for tests where we have some asserts
      // against the state.  Otherwise we have a *little* bit of flakiness in the tests.
      cleanupStateForJobAndIndependentStages(job)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobFailed(error)))
    }
  }

  /** Return true if one of stage's ancestors is target. */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new ListBuffer[RDD[_]]
    waitingForVisit += stage.rdd
    def visit(rdd: RDD[_]): Unit = {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getOrCreateShuffleMapStage(shufDep, stage.firstJobId)
              if (!mapStage.isAvailable) {
                waitingForVisit.prepend(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              waitingForVisit.prepend(narrowDep.rdd)
          }
        }
      }
    }
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.remove(0))
    }
    visitedRdds.contains(target.rdd)
  }

  /**
   * Gets the locality information associated with a partition of a particular RDD.
   *
   * This method is thread-safe and is called from both DAGScheduler and SparkContext.
   *
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = {
    getPreferredLocsInternal(rdd, partition, new HashSet)
  }

  /**
   * Recursive implementation for getPreferredLocs.
   *
   * This method is thread-safe because it only accesses DAGScheduler state through thread-safe
   * methods (getCacheLocs()); please be careful when modifying this method, because any new
   * DAGScheduler state accessed by it may require additional synchronization.
   */
  private def getPreferredLocsInternal(
      rdd: RDD[_],
      partition: Int,
      visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
    // If the partition has already been visited, no need to re-visit.
    // This avoids exponential path exploration.  SPARK-695
    if (!visited.add((rdd, partition))) {
      // Nil has already been returned for previously visited partitions.
      return Nil
    }
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached.nonEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (rddPrefs.nonEmpty) {
      return rddPrefs.filter(_ != null).map(TaskLocation(_))
    }

    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
          if (locs != Nil) {
            return locs
          }
        }

      case _ =>
    }

    Nil
  }

  /** Mark a map stage job as finished with the given output stats, and report to its listener. */
  def markMapStageJobAsFinished(job: ActiveJob, stats: MapOutputStatistics): Unit = {
    // In map stage jobs, we only create a single "task", which is to finish all of the stage
    // (including reusing any previous map outputs, etc); so we just mark task 0 as done
    job.finished(0) = true
    job.numFinished += 1
    job.listener.taskSucceeded(0, stats)
    cleanupStateForJobAndIndependentStages(job)
    listenerBus.post(SparkListenerJobEnd(job.jobId, clock.getTimeMillis(), JobSucceeded))
  }

  def stop(exitCode: Int = 0): Unit = {
    Utils.tryLogNonFatalError {
      messageScheduler.shutdownNow()
    }
    Utils.tryLogNonFatalError {
      shuffleMergeFinalizeScheduler.shutdownNow()
    }
    Utils.tryLogNonFatalError {
      eventProcessLoop.stop()
    }
    Utils.tryLogNonFatalError {
      taskScheduler.stop(exitCode)
    }
  }

  eventProcessLoop.start()
}

private[scheduler] class DAGSchedulerEventProcessLoop(dagScheduler: DAGScheduler)
  extends EventLoop[DAGSchedulerEvent]("dag-scheduler-event-loop") with Logging {

  private[this] val timer = dagScheduler.metricsSource.messageProcessingTimer

  /**
   * The main event loop of the DAG scheduler.
   */
  override def onReceive(event: DAGSchedulerEvent): Unit = {
    val timerContext = timer.time()
    try {
      doOnReceive(event)
    } finally {
      timerContext.stop()
    }
  }

  private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, artifacts, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, artifacts,
        properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, artifacts, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, artifacts,
        properties)

    case StageCancelled(stageId, reason) =>
      dagScheduler.handleStageCancellation(stageId, reason)

    case JobCancelled(jobId, reason) =>
      dagScheduler.handleJobCancellation(jobId, reason)

    case JobGroupCancelled(groupId, cancelFutureJobs, reason) =>
      dagScheduler.handleJobGroupCancelled(groupId, cancelFutureJobs, reason)

    case JobTagCancelled(tag, reason, jobsToBeCancelled) =>
      dagScheduler.handleJobTagCancelled(tag, reason, jobsToBeCancelled)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId, reason) =>
      val workerHost = reason match {
        case ExecutorProcessLost(_, workerHost, _) => workerHost
        case ExecutorDecommission(workerHost, _) => workerHost
        case _ => None
      }
      dagScheduler.handleExecutorLost(execId, workerHost)

    case WorkerRemoved(workerId, host, message) =>
      dagScheduler.handleWorkerRemoved(workerId, host, message)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case SpeculativeTaskSubmitted(task, taskIndex) =>
      dagScheduler.handleSpeculativeTaskSubmitted(task, taskIndex)

    case UnschedulableTaskSetAdded(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetAdded(stageId, stageAttemptId)

    case UnschedulableTaskSetRemoved(stageId, stageAttemptId) =>
      dagScheduler.handleUnschedulableTaskSetRemoved(stageId, stageAttemptId)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion: CompletionEvent =>
      dagScheduler.handleTaskCompletion(completion)

    case StageFailed(stageId, reason, exception) =>
      dagScheduler.handleStageFailed(stageId, reason, exception)

    case TaskSetFailed(taskSet, reason, exception) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason, exception)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()

    case RegisterMergeStatuses(stage, mergeStatuses) =>
      dagScheduler.handleRegisterMergeStatuses(stage, mergeStatuses)

    case ShuffleMergeFinalized(stage) =>
      dagScheduler.handleShuffleMergeFinalized(stage, stage.shuffleDep.shuffleMergeId)

    case ShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex) =>
      dagScheduler.handleShufflePushCompleted(shuffleId, shuffleMergeId, mapIndex)
  }

  override def onError(e: Throwable): Unit = {
    logError("DAGSchedulerEventProcessLoop failed; shutting down SparkContext", e)
    try {
      dagScheduler.doCancelAllJobs()
    } catch {
      case t: Throwable => logError("DAGScheduler failed to cancel all jobs.", t)
    }
    dagScheduler.sc.stopInNewThread()
  }

  override def onStop(): Unit = {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200
}

/**
 * A NOT thread-safe set that only keeps the last `capacity` elements added to it.
 */
private[scheduler] class LimitedSizeFIFOSet[T](val capacity: Int) {
  private val set = scala.collection.mutable.LinkedHashSet[T]()
  def add(t: T): Unit = {
    set += t
    if (set.size > capacity) {
      set -= set.head
    }
  }
  def contains(t: T): Boolean = set.contains(t)
}
