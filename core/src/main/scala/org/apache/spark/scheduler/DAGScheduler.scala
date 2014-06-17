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

import java.io.{NotSerializableException, PrintWriter, StringWriter}
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag

import akka.actor._
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Stop
import akka.pattern.ask
import akka.util.Timeout

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerMaster, RDDBlockId}
import org.apache.spark.util.{CallSite, SystemClock, Clock, Utils}

/**
 * The high-level scheduling layer that implements stage-oriented scheduling. It computes a DAG of
 * stages for each job, keeps track of which RDDs and stage outputs are materialized, and finds a
 * minimal schedule to run the job. It then submits stages as TaskSets to an underlying
 * TaskScheduler implementation that runs them on the cluster.
 *
 * In addition to coming up with a DAG of stages, this class also determines the preferred
 * locations to run each task on, based on the current cache status, and passes these to the
 * low-level TaskScheduler. Furthermore, it handles failures due to shuffle output files being
 * lost, in which case old stages may need to be resubmitted. Failures *within* a stage that are
 * not caused by shuffle file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 */
private[spark]
class DAGScheduler(
    private[scheduler] val sc: SparkContext,
    private[scheduler] val taskScheduler: TaskScheduler,
    listenerBus: LiveListenerBus,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv,
    clock: Clock = SystemClock)
  extends Logging {

  import DAGScheduler._

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

  private[scheduler] val nextJobId = new AtomicInteger(0)
  private[scheduler] def numTotalJobs: Int = nextJobId.get()
  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToJobIds = new HashMap[Int, HashSet[Int]]
  private[scheduler] val stageIdToStage = new HashMap[Int, Stage]
  private[scheduler] val shuffleToMapStage = new HashMap[Int, Stage]
  private[scheduler] val jobIdToActiveJob = new HashMap[Int, ActiveJob]
  private[scheduler] val resultStageToJob = new HashMap[Stage, ActiveJob]
  private[scheduler] val stageToInfos = new HashMap[Stage, StageInfo]

  // Stages we need to run whose parents aren't done
  private[scheduler] val waitingStages = new HashSet[Stage]

  // Stages we are running right now
  private[scheduler] val runningStages = new HashSet[Stage]

  // Stages that must be resubmitted due to fetch failures
  private[scheduler] val failedStages = new HashSet[Stage]

  // Missing tasks from each stage
  private[scheduler] val pendingTasks = new HashMap[Stage, HashSet[Task[_]]]

  private[scheduler] val activeJobs = new HashSet[ActiveJob]

  // Contains the locations that each RDD's partitions are cached on
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  private val failedEpoch = new HashMap[String, Long]

  private val dagSchedulerActorSupervisor =
    env.actorSystem.actorOf(Props(new DAGSchedulerActorSupervisor(this)))

  private[scheduler] var eventProcessActor: ActorRef = _

  private def initializeEventProcessActor() {
    // blocking the thread until supervisor is started, which ensures eventProcessActor is
    // not null before any job is submitted
    implicit val timeout = Timeout(30 seconds)
    val initEventActorReply =
      dagSchedulerActorSupervisor ? Props(new DAGSchedulerEventProcessActor(this))
    eventProcessActor = Await.result(initEventActorReply, timeout.duration).
      asInstanceOf[ActorRef]
  }

  initializeEventProcessActor()

  // Called by TaskScheduler to report task's starting.
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessActor ! BeginEvent(task, taskInfo)
  }

  // Called to report that a task has completed and results are being fetched remotely.
  def taskGettingResult(taskInfo: TaskInfo) {
    eventProcessActor ! GettingResultEvent(taskInfo)
  }

  // Called by TaskScheduler to report task completions or failures.
  def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics) {
    eventProcessActor ! CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics)
  }

  // Called by TaskScheduler when an executor fails.
  def executorLost(execId: String) {
    eventProcessActor ! ExecutorLost(execId)
  }

  // Called by TaskScheduler when a host is added
  def executorAdded(execId: String, host: String) {
    eventProcessActor ! ExecutorAdded(execId, host)
  }

  // Called by TaskScheduler to cancel an entire TaskSet due to either repeated failures or
  // cancellation of the job itself.
  def taskSetFailed(taskSet: TaskSet, reason: String) {
    eventProcessActor ! TaskSetFailed(taskSet, reason)
  }

  private def getCacheLocs(rdd: RDD[_]): Array[Seq[TaskLocation]] = {
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
      val locs = BlockManager.blockIdsToBlockManagers(blockIds, env, blockManagerMaster)
      cacheLocs(rdd.id) = blockIds.map { id =>
        locs.getOrElse(id, Nil).map(bm => TaskLocation(bm.host, bm.executorId))
      }
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs() {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   * The jobId value passed in will be used if the stage doesn't already exist with
   * a lower jobId (jobId always increases across jobs.)
   */
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_, _, _], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage =
          newOrUsedStage(
            shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId,
            shuffleDep.rdd.creationSite)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  /**
   * Create a Stage -- either directly for use as a result stage, or as part of the (re)-creation
   * of a shuffle map stage in newOrUsedStage.  The stage will be associated with the provided
   * jobId. Production of shuffle map stages should always use newOrUsedStage, not newStage
   * directly.
   */
  private def newStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: Option[ShuffleDependency[_, _, _]],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val id = nextStageId.getAndIncrement()
    val stage =
      new Stage(id, rdd, numTasks, shuffleDep, getParentStages(rdd, jobId), jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stageToInfos(stage) = StageInfo.fromStage(stage)
    stage
  }

  /**
   * Create a shuffle map Stage for the given RDD.  The stage will also be associated with the
   * provided jobId.  If a stage for the shuffleId existed previously so that the shuffleId is
   * present in the MapOutputTracker, then the number and location of available outputs are
   * recovered from the MapOutputTracker
   */
  private def newOrUsedStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      jobId: Int,
      callSite: CallSite)
    : Stage =
  {
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) {
        stage.outputLocs(i) = Option(locs(i)).toList   // locs(i) will be null if missing
      }
      stage.numAvailableOutputs = locs.count(_ != null)
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.size)
    }
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD. The stages will be assigned the
   * provided jobId if they haven't already been created with a lower jobId.
   */
  private def getParentStages(rdd: RDD[_], jobId: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              parents += getShuffleMapStage(shufDep, jobId)
            case _ =>
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    parents.toList
  }

  private def getMissingParentStages(stage: Stage): List[Stage] = {
    val missing = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(rdd: RDD[_]) {
      if (!visited(rdd)) {
        visited += rdd
        if (getCacheLocs(rdd).contains(Nil)) {
          for (dep <- rdd.dependencies) {
            dep match {
              case shufDep: ShuffleDependency[_, _, _] =>
                val mapStage = getShuffleMapStage(shufDep, stage.jobId)
                if (!mapStage.isAvailable) {
                  missing += mapStage
                }
              case narrowDep: NarrowDependency[_] =>
                visit(narrowDep.rdd)
            }
          }
        }
      }
    }
    visit(stage.rdd)
    missing.toList
  }

  /**
   * Registers the given jobId among the jobs that need the given stage and
   * all of that stage's ancestors.
   */
  private def updateJobIdStageIdMaps(jobId: Int, stage: Stage) {
    def updateJobIdStageIdMapsList(stages: List[Stage]) {
      if (!stages.isEmpty) {
        val s = stages.head
        stageIdToJobIds.getOrElseUpdate(s.id, new HashSet[Int]()) += jobId
        jobIdToStageIds.getOrElseUpdate(jobId, new HashSet[Int]()) += s.id
        val parents = getParentStages(s.rdd, jobId)
        val parentsWithoutThisJobId = parents.filter(p =>
          !stageIdToJobIds.get(p.id).exists(_.contains(jobId)))
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
   * @param resultStage Specifies the result stage for the job; if set to None, this method
   *                    searches resultStagesToJob to find and cleanup the appropriate result stage.
   */
  private def cleanupStateForJobAndIndependentStages(job: ActiveJob, resultStage: Option[Stage]) {
    val registeredStages = jobIdToStageIds.get(job.jobId)
    if (registeredStages.isEmpty || registeredStages.get.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    } else {
      stageIdToJobIds.filterKeys(stageId => registeredStages.get.contains(stageId)).foreach {
        case (stageId, jobSet) =>
          if (!jobSet.contains(job.jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(job.jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              for (stage <- stageIdToStage.get(stageId)) {
                if (runningStages.contains(stage)) {
                  logDebug("Removing running stage %d".format(stageId))
                  runningStages -= stage
                }
                stageToInfos -= stage
                for ((k, v) <- shuffleToMapStage.find(_._2 == stage)) {
                  shuffleToMapStage.remove(k)
                }
                if (pendingTasks.contains(stage) && !pendingTasks(stage).isEmpty) {
                  logDebug("Removing pending status for stage %d".format(stageId))
                }
                pendingTasks -= stage
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
              stageIdToJobIds -= stageId

              ShuffleMapTask.removeStage(stageId)
              ResultTask.removeStage(stageId)

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

    if (resultStage.isEmpty) {
      // Clean up result stages.
      val resultStagesForJob = resultStageToJob.keySet.filter(
        stage => resultStageToJob(stage).jobId == job.jobId)
      if (resultStagesForJob.size != 1) {
        logWarning(
          s"${resultStagesForJob.size} result stages for job ${job.jobId} (expect exactly 1)")
      }
      resultStageToJob --= resultStagesForJob
    } else {
      resultStageToJob -= resultStage.get
    }
  }

  /**
   * Submit a job to the job scheduler and get a JobWaiter object back. The JobWaiter object
   * can be used to block until the the job finishes executing or can be used to cancel the job.
   */
  def submitJob[T, U](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null): JobWaiter[U] =
  {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions || p < 0).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
          "Total number of partitions: " + maxPartitions)
    }

    val jobId = nextJobId.getAndIncrement()
    if (partitions.size == 0) {
      return new JobWaiter[U](this, jobId, 0, resultHandler)
    }

    assert(partitions.size > 0)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val waiter = new JobWaiter(this, jobId, partitions.size, resultHandler)
    eventProcessActor ! JobSubmitted(
      jobId, rdd, func2, partitions.toArray, allowLocal, callSite, waiter, properties)
    waiter
  }

  def runJob[T, U: ClassTag](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: CallSite,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
  {
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded => {}
      case JobFailed(exception: Exception) =>
        logInfo("Failed to run " + callSite.short)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: CallSite,
      timeout: Long,
      properties: Properties = null)
    : PartialResult[R] =
  {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.partitions.size).toArray
    val jobId = nextJobId.getAndIncrement()
    eventProcessActor ! JobSubmitted(
      jobId, rdd, func2, partitions, allowLocal = false, callSite, listener, properties)
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Cancel a job that is running or waiting in the queue.
   */
  def cancelJob(jobId: Int) {
    logInfo("Asked to cancel job " + jobId)
    eventProcessActor ! JobCancelled(jobId)
  }

  def cancelJobGroup(groupId: String) {
    logInfo("Asked to cancel job group " + groupId)
    eventProcessActor ! JobGroupCancelled(groupId)
  }

  /**
   * Cancel all jobs that are running or waiting in the queue.
   */
  def cancelAllJobs() {
    eventProcessActor ! AllJobsCancelled
  }

  private[scheduler] def doCancelAllJobs() {
    // Cancel all running jobs.
    runningStages.map(_.jobId).foreach(handleJobCancellation(_,
      reason = "as part of cancellation of all jobs"))
    activeJobs.clear() // These should already be empty by this point,
    jobIdToActiveJob.clear() // but just in case we lost track of some jobs...
    submitWaitingStages()
  }

  /**
   * Cancel all jobs associated with a running or scheduled stage.
   */
  def cancelStage(stageId: Int) {
    eventProcessActor ! StageCancelled(stageId)
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    if (failedStages.size > 0) {
      // Failed stages may be removed by job cancellation, so failed might be empty even if
      // the ResubmitFailedStages event has been scheduled.
      logInfo("Resubmitting failed stages")
      clearCacheLocs()
      val failedStagesCopy = failedStages.toArray
      failedStages.clear()
      for (stage <- failedStagesCopy.sortBy(_.jobId)) {
        submitStage(stage)
      }
    }
    submitWaitingStages()
  }

  /**
   * Check for waiting or failed stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   */
  private def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + runningStages)
    logTrace("waiting: " + waitingStages)
    logTrace("failed: " + failedStages)
    val waitingStagesCopy = waitingStages.toArray
    waitingStages.clear()
    for (stage <- waitingStagesCopy.sortBy(_.jobId)) {
      submitStage(stage)
    }
  }

  /**
   * Run a job on an RDD locally, assuming it has only a single partition and no dependencies.
   * We run the operation in a separate thread just in case it takes a bunch of time, so that we
   * don't block the DAGScheduler event loop or other concurrent jobs.
   */
  protected def runLocally(job: ActiveJob) {
    logInfo("Computing the requested partition locally")
    new Thread("Local computation of job " + job.jobId) {
      override def run() {
        runLocallyWithinThread(job)
      }
    }.start()
  }

  // Broken out for easier testing in DAGSchedulerSuite.
  protected def runLocallyWithinThread(job: ActiveJob) {
    var jobResult: JobResult = JobSucceeded
    try {
      SparkEnv.set(env)
      val rdd = job.finalStage.rdd
      val split = rdd.partitions(job.partitions(0))
      val taskContext =
        new TaskContext(job.finalStage.id, job.partitions(0), 0, runningLocally = true)
      try {
        val result = job.func(taskContext, rdd.iterator(split, taskContext))
        job.listener.taskSucceeded(0, result)
      } finally {
        taskContext.executeOnCompleteCallbacks()
      }
    } catch {
      case e: Exception =>
        jobResult = JobFailed(e)
        job.listener.jobFailed(e)
      case oom: OutOfMemoryError =>
        val exception = new SparkException("Local job aborted due to out of memory error", oom)
        jobResult = JobFailed(exception)
        job.listener.jobFailed(exception)
    } finally {
      val s = job.finalStage
      stageIdToJobIds -= s.id    // clean up data structures that were populated for a local job,
      stageIdToStage -= s.id     // but that won't get cleaned up via the normal paths through
      stageToInfos -= s          // completion events or stage abort
      jobIdToStageIds -= job.jobId
      listenerBus.post(SparkListenerJobEnd(job.jobId, jobResult))
    }
  }

  /** Finds the earliest-created active job that needs the stage */
  // TODO: Probably should actually find among the active jobs that need this
  // stage the one with the highest priority (highest-priority pool, earliest created).
  // That should take care of at least part of the priority inversion problem with
  // cross-job dependencies.
  private def activeJobForStage(stage: Stage): Option[Int] = {
    if (stageIdToJobIds.contains(stage.id)) {
      val jobsThatUseStage: Array[Int] = stageIdToJobIds(stage.id).toArray.sorted
      jobsThatUseStage.find(jobIdToActiveJob.contains)
    } else {
      None
    }
  }

  private[scheduler] def handleJobGroupCancelled(groupId: String) {
    // Cancel all jobs belonging to this job group.
    // First finds all active jobs with this group id, and then kill stages for them.
    val activeInGroup = activeJobs.filter(activeJob =>
      groupId == activeJob.properties.get(SparkContext.SPARK_JOB_GROUP_ID))
    val jobIds = activeInGroup.map(_.jobId)
    jobIds.foreach(handleJobCancellation(_, "part of cancelled job group %s".format(groupId)))
    submitWaitingStages()
  }

  private[scheduler] def handleBeginEvent(task: Task[_], taskInfo: TaskInfo) {
    for (stage <- stageIdToStage.get(task.stageId); stageInfo <- stageToInfos.get(stage)) {
      if (taskInfo.serializedSize > DAGScheduler.TASK_SIZE_TO_WARN * 1024 &&
        !stageInfo.emittedTaskSizeWarning) {
        stageInfo.emittedTaskSizeWarning = true
        logWarning(("Stage %d (%s) contains a task of very large " +
          "size (%d KB). The maximum recommended task size is %d KB.").format(
            task.stageId, stageInfo.name, taskInfo.serializedSize / 1024,
            DAGScheduler.TASK_SIZE_TO_WARN))
      }
    }
    listenerBus.post(SparkListenerTaskStart(task.stageId, taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleTaskSetFailed(taskSet: TaskSet, reason: String) {
    stageIdToStage.get(taskSet.stageId).foreach {abortStage(_, reason) }
    submitWaitingStages()
  }

  private[scheduler] def cleanUpAfterSchedulerStop() {
    for (job <- activeJobs) {
      val error = new SparkException("Job cancelled because SparkContext was shut down")
      job.listener.jobFailed(error)
      // Tell the listeners that all of the running stages have ended.  Don't bother
      // cancelling the stages because if the DAG scheduler is stopped, the entire application
      // is in the process of getting stopped.
      val stageFailedMessage = "Stage cancelled because SparkContext was shut down"
      runningStages.foreach { stage =>
        val info = stageToInfos(stage)
        info.stageFailed(stageFailedMessage)
        listenerBus.post(SparkListenerStageCompleted(info))
      }
      listenerBus.post(SparkListenerJobEnd(job.jobId, JobFailed(error)))
    }
  }

  private[scheduler] def handleGetTaskResult(taskInfo: TaskInfo) {
    listenerBus.post(SparkListenerTaskGettingResult(taskInfo))
    submitWaitingStages()
  }

  private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      allowLocal: Boolean,
      callSite: CallSite,
      listener: JobListener,
      properties: Properties = null)
  {
    var finalStage: Stage = null
    try {
      // New stage creation may throw an exception if, for example, jobs are run on a
      // HadoopRDD whose underlying HDFS files have been deleted.
      finalStage = newStage(finalRDD, partitions.size, None, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    if (finalStage != null) {
      val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
      clearCacheLocs()
      logInfo("Got job %s (%s) with %d output partitions (allowLocal=%s)".format(
        job.jobId, callSite.short, partitions.length, allowLocal))
      logInfo("Final stage: " + finalStage + "(" + finalStage.name + ")")
      logInfo("Parents of final stage: " + finalStage.parents)
      logInfo("Missing parents: " + getMissingParentStages(finalStage))
      if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
        // Compute very short actions like first() or take() with no parent stages locally.
        listenerBus.post(SparkListenerJobStart(job.jobId, Array[Int](), properties))
        runLocally(job)
      } else {
        jobIdToActiveJob(jobId) = job
        activeJobs += job
        resultStageToJob(finalStage) = job
        listenerBus.post(SparkListenerJobStart(job.jobId, jobIdToStageIds(jobId).toArray,
          properties))
        submitStage(finalStage)
      }
    }
    submitWaitingStages()
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waitingStages(stage) && !runningStages(stage) && !failedStages(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing == Nil) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
          runningStages += stage
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waitingStages += stage
        }
      }
    } else {
      abortStage(stage, "No active job for stage " + stage.id)
    }
  }


  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage, jobId: Int) {
    logDebug("submitMissingTasks(" + stage + ")")
    // Get our pending tasks and remember them in our pendingTasks entry
    val myPending = pendingTasks.getOrElseUpdate(stage, new HashSet)
    myPending.clear()
    var tasks = ArrayBuffer[Task[_]]()
    if (stage.isShuffleMap) {
      for (p <- 0 until stage.numPartitions if stage.outputLocs(p) == Nil) {
        val locs = getPreferredLocs(stage.rdd, p)
        tasks += new ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep.get, p, locs)
      }
    } else {
      // This is a final stage; figure out its job's missing partitions
      val job = resultStageToJob(stage)
      for (id <- 0 until job.numPartitions if !job.finished(id)) {
        val partition = job.partitions(id)
        val locs = getPreferredLocs(stage.rdd, partition)
        tasks += new ResultTask(stage.id, stage.rdd, job.func, partition, locs, id)
      }
    }

    val properties = if (jobIdToActiveJob.contains(jobId)) {
      jobIdToActiveJob(stage.jobId).properties
    } else {
      // this stage will be assigned to "default" pool
      null
    }

    // must be run listener before possible NotSerializableException
    // should be "StageSubmitted" first and then "JobEnded"
    listenerBus.post(SparkListenerStageSubmitted(stageToInfos(stage), properties))

    if (tasks.size > 0) {
      // Preemptively serialize a task to make sure it can be serialized. We are catching this
      // exception here because it would be fairly hard to catch the non-serializable exception
      // down the road, where we have several different implementations for local scheduler and
      // cluster schedulers.
      try {
        SparkEnv.get.closureSerializer.newInstance().serialize(tasks.head)
      } catch {
        case e: NotSerializableException =>
          abortStage(stage, "Task not serializable: " + e.toString)
          runningStages -= stage
          return
      }

      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      myPending ++= tasks
      logDebug("New pending tasks: " + myPending)
      taskScheduler.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      stageToInfos(stage).submissionTime = Some(clock.getTime())
    } else {
      logDebug("Stage " + stage + " is actually done; %b %d %d".format(
        stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
      runningStages -= stage
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private[scheduler] def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task
    val stageId = task.stageId
    val taskType = Utils.getFormattedClassName(task)
    listenerBus.post(SparkListenerTaskEnd(stageId, taskType, event.reason, event.taskInfo,
      event.taskMetrics))
    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }
    val stage = stageIdToStage(task.stageId)

    def markStageAsFinished(stage: Stage) = {
      val serviceTime = stageToInfos(stage).submissionTime match {
        case Some(t) => "%.03f".format((clock.getTime() - t) / 1000.0)
        case _ => "Unknown"
      }
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stageToInfos(stage).completionTime = Some(clock.getTime())
      listenerBus.post(SparkListenerStageCompleted(stageToInfos(stage)))
      runningStages -= stage
    }
    event.reason match {
      case Success =>
        logInfo("Completed " + task)
        if (event.accumUpdates != null) {
          Accumulators.add(event.accumUpdates) // TODO: do this only if task wasn't resubmitted
        }
        pendingTasks(stage) -= task
        task match {
          case rt: ResultTask[_, _] =>
            resultStageToJob.get(stage) match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    markStageAsFinished(stage)
                    cleanupStateForJobAndIndependentStages(job, Some(stage))
                    listenerBus.post(SparkListenerJobEnd(job.jobId, JobSucceeded))
                  }
                  job.listener.taskSucceeded(rt.outputId, event.result)
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedEpoch.contains(execId) && smt.epoch <= failedEpoch(execId)) {
              logInfo("Ignoring possibly bogus ShuffleMapTask completion from " + execId)
            } else {
              stage.addOutputLoc(smt.partitionId, status)
            }
            if (runningStages.contains(stage) && pendingTasks(stage).isEmpty) {
              markStageAsFinished(stage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + runningStages)
              logInfo("waiting: " + waitingStages)
              logInfo("failed: " + failedStages)
              if (stage.shuffleDep.isDefined) {
                // We supply true to increment the epoch number here in case this is a
                // recomputation of the map outputs. In that case, some nodes may have cached
                // locations with holes (from when we detected the error) and will need the
                // epoch incremented to refetch them.
                // TODO: Only increment the epoch number if this is not the first time
                //       we registered these map outputs.
                mapOutputTracker.registerMapOutputs(
                  stage.shuffleDep.get.shuffleId,
                  stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
                  changeEpoch = true)
              }
              clearCacheLocs()
              if (stage.outputLocs.exists(_ == Nil)) {
                // Some tasks had failed; let's resubmit this stage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + stage + " (" + stage.name +
                  ") because some of its tasks had failed: " +
                  stage.outputLocs.zipWithIndex.filter(_._1 == Nil).map(_._2).mkString(", "))
                submitStage(stage)
              } else {
                val newlyRunnable = new ArrayBuffer[Stage]
                for (stage <- waitingStages) {
                  logInfo("Missing parents for " + stage + ": " + getMissingParentStages(stage))
                }
                for (stage <- waitingStages if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waitingStages --= newlyRunnable
                runningStages ++= newlyRunnable
                for {
                  stage <- newlyRunnable.sortBy(_.id)
                  jobId <- activeJobForStage(stage)
                } {
                  logInfo("Submitting " + stage + " (" + stage.rdd + "), which is now runnable")
                  submitMissingTasks(stage, jobId)
                }
              }
            }
          }

      case Resubmitted =>
        logInfo("Resubmitted " + task + ", so marking it as still running")
        pendingTasks(stage) += task

      case FetchFailed(bmAddress, shuffleId, mapId, reduceId) =>
        // Mark the stage that the reducer was in as unrunnable
        val failedStage = stageIdToStage(task.stageId)
        runningStages -= failedStage
        // TODO: Cancel running tasks in the stage
        logInfo("Marking " + failedStage + " (" + failedStage.name +
          ") for resubmision due to a fetch failure")
        // Mark the map whose fetch failed as broken in the map stage
        val mapStage = shuffleToMapStage(shuffleId)
        if (mapId != -1) {
          mapStage.removeOutputLoc(mapId, bmAddress)
          mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
        }
        logInfo("The failed fetch was from " + mapStage + " (" + mapStage.name +
          "); marking it for resubmission")
        if (failedStages.isEmpty && eventProcessActor != null) {
          // Don't schedule an event to resubmit failed stages if failed isn't empty, because
          // in that case the event will already have been scheduled. eventProcessActor may be
          // null during unit tests.
          import env.actorSystem.dispatcher
          env.actorSystem.scheduler.scheduleOnce(
            RESUBMIT_TIMEOUT, eventProcessActor, ResubmitFailedStages)
        }
        failedStages += failedStage
        failedStages += mapStage
        // TODO: mark the executor as failed only if there were lots of fetch failures on it
        if (bmAddress != null) {
          handleExecutorLost(bmAddress.executorId, Some(task.epoch))
        }

      case ExceptionFailure(className, description, stackTrace, metrics) =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
        // Unrecognized failure - also do nothing. If the task fails repeatedly, the TaskScheduler
        // will abort the job.
    }
    submitWaitingStages()
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private[scheduler] def handleExecutorLost(execId: String, maybeEpoch: Option[Long] = None) {
    val currentEpoch = maybeEpoch.getOrElse(mapOutputTracker.getEpoch)
    if (!failedEpoch.contains(execId) || failedEpoch(execId) < currentEpoch) {
      failedEpoch(execId) = currentEpoch
      logInfo("Executor lost: %s (epoch %d)".format(execId, currentEpoch))
      blockManagerMaster.removeExecutor(execId)
      // TODO: This will be really slow if we keep accumulating shuffle map stages
      for ((shuffleId, stage) <- shuffleToMapStage) {
        stage.removeOutputsOnExecutor(execId)
        val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray
        mapOutputTracker.registerMapOutputs(shuffleId, locs, changeEpoch = true)
      }
      if (shuffleToMapStage.isEmpty) {
        mapOutputTracker.incrementEpoch()
      }
      clearCacheLocs()
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(epoch " + currentEpoch + ")")
    }
    submitWaitingStages()
  }

  private[scheduler] def handleExecutorAdded(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host added was in lost list earlier: " + host)
      failedEpoch -= execId
    }
    submitWaitingStages()
  }

  private[scheduler] def handleStageCancellation(stageId: Int) {
    if (stageIdToJobIds.contains(stageId)) {
      val jobsThatUseStage: Array[Int] = stageIdToJobIds(stageId).toArray
      jobsThatUseStage.foreach(jobId => {
        handleJobCancellation(jobId, "because Stage %s was cancelled".format(stageId))
      })
    } else {
      logInfo("No active jobs to kill for Stage " + stageId)
    }
    submitWaitingStages()
  }

  private[scheduler] def handleJobCancellation(jobId: Int, reason: String = "") {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      failJobAndIndependentStages(jobIdToActiveJob(jobId),
        "Job %d cancelled %s".format(jobId, reason), None)
    }
    submitWaitingStages()
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private[scheduler] def abortStage(failedStage: Stage, reason: String) {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentStages = resultStageToJob.keys.filter(x => stageDependsOn(x, failedStage)).toSeq
    stageToInfos(failedStage).completionTime = Some(clock.getTime())
    for (resultStage <- dependentStages) {
      val job = resultStageToJob(resultStage)
      failJobAndIndependentStages(job, s"Job aborted due to stage failure: $reason",
        Some(resultStage))
    }
    if (dependentStages.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
  }

  /**
   * Fails a job and all stages that are only used by that job, and cleans up relevant state.
   *
   * @param resultStage The result stage for the job, if known. Used to cleanup state for the job
   *                    slightly more efficiently than when not specified.
   */
  private def failJobAndIndependentStages(job: ActiveJob, failureReason: String,
      resultStage: Option[Stage]) {
    val error = new SparkException(failureReason)
    job.listener.jobFailed(error)

    val shouldInterruptThread =
      if (job.properties == null) false
      else job.properties.getProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false").toBoolean

    // Cancel all independent, running stages.
    val stages = jobIdToStageIds(job.jobId)
    if (stages.isEmpty) {
      logError("No stages registered for job " + job.jobId)
    }
    stages.foreach { stageId =>
      val jobsForStage = stageIdToJobIds.get(stageId)
      if (jobsForStage.isEmpty || !jobsForStage.get.contains(job.jobId)) {
        logError(
          "Job %d not registered for stage %d even though that stage was registered for the job"
            .format(job.jobId, stageId))
      } else if (jobsForStage.get.size == 1) {
        if (!stageIdToStage.contains(stageId)) {
          logError("Missing Stage for stage with id $stageId")
        } else {
          // This is the only job that uses this stage, so fail the stage if it is running.
          val stage = stageIdToStage(stageId)
          if (runningStages.contains(stage)) {
            taskScheduler.cancelTasks(stageId, shouldInterruptThread)
            val stageInfo = stageToInfos(stage)
            stageInfo.stageFailed(failureReason)
            listenerBus.post(SparkListenerStageCompleted(stageToInfos(stage)))
          }
        }
      }
    }

    cleanupStateForJobAndIndependentStages(job, resultStage)

    listenerBus.post(SparkListenerJobEnd(job.jobId, JobFailed(error)))
  }

  /**
   * Return true if one of stage's ancestors is target.
   */
  private def stageDependsOn(stage: Stage, target: Stage): Boolean = {
    if (stage == target) {
      return true
    }
    val visitedRdds = new HashSet[RDD[_]]
    val visitedStages = new HashSet[Stage]
    def visit(rdd: RDD[_]) {
      if (!visitedRdds(rdd)) {
        visitedRdds += rdd
        for (dep <- rdd.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_, _, _] =>
              val mapStage = getShuffleMapStage(shufDep, stage.jobId)
              if (!mapStage.isAvailable) {
                visitedStages += mapStage
                visit(mapStage.rdd)
              }  // Otherwise there's no need to follow the dependency back
            case narrowDep: NarrowDependency[_] =>
              visit(narrowDep.rdd)
          }
        }
      }
    }
    visit(stage.rdd)
    visitedRdds.contains(target.rdd)
  }

  /**
   * Synchronized method that might be called from other threads.
   * @param rdd whose partitions are to be looked at
   * @param partition to lookup locality information for
   * @return list of machines that are preferred by the partition
   */
  private[spark]
  def getPreferredLocs(rdd: RDD[_], partition: Int): Seq[TaskLocation] = synchronized {
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (!cached.isEmpty) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
    if (!rddPrefs.isEmpty) {
      return rddPrefs.map(host => TaskLocation(host))
    }
    // If the RDD has narrow dependencies, pick the first partition of the first narrow dep
    // that has any placement preferences. Ideally we would choose based on transfer sizes,
    // but this will do for now.
    rdd.dependencies.foreach {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocs(n.rdd, inPart)
          if (locs != Nil) {
            return locs
          }
        }
      case _ =>
    }
    Nil
  }

  def stop() {
    logInfo("Stopping DAGScheduler")
    dagSchedulerActorSupervisor ! PoisonPill
    taskScheduler.stop()
  }
}

private[scheduler] class DAGSchedulerActorSupervisor(dagScheduler: DAGScheduler)
  extends Actor with Logging {

  override val supervisorStrategy =
    OneForOneStrategy() {
      case x: Exception =>
        logError("eventProcesserActor failed due to the error %s; shutting down SparkContext"
          .format(x.getMessage))
        dagScheduler.doCancelAllJobs()
        dagScheduler.sc.stop()
        Stop
    }

  def receive = {
    case p: Props => sender ! context.actorOf(p)
    case _ => logWarning("received unknown message in DAGSchedulerActorSupervisor")
  }
}

private[scheduler] class DAGSchedulerEventProcessActor(dagScheduler: DAGScheduler)
  extends Actor with Logging {

  override def preStart() {
    // set DAGScheduler for taskScheduler to ensure eventProcessActor is always
    // valid when the messages arrive
    dagScheduler.taskScheduler.setDAGScheduler(dagScheduler)
  }

  /**
   * The main event loop of the DAG scheduler.
   */
  def receive = {
    case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite,
        listener, properties)

    case StageCancelled(stageId) =>
      dagScheduler.handleStageCancellation(stageId)

    case JobCancelled(jobId) =>
      dagScheduler.handleJobCancellation(jobId)

    case JobGroupCancelled(groupId) =>
      dagScheduler.handleJobGroupCancelled(groupId)

    case AllJobsCancelled =>
      dagScheduler.doCancelAllJobs()

    case ExecutorAdded(execId, host) =>
      dagScheduler.handleExecutorAdded(execId, host)

    case ExecutorLost(execId) =>
      dagScheduler.handleExecutorLost(execId)

    case BeginEvent(task, taskInfo) =>
      dagScheduler.handleBeginEvent(task, taskInfo)

    case GettingResultEvent(taskInfo) =>
      dagScheduler.handleGetTaskResult(taskInfo)

    case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
      dagScheduler.handleTaskCompletion(completion)

    case TaskSetFailed(taskSet, reason) =>
      dagScheduler.handleTaskSetFailed(taskSet, reason)

    case ResubmitFailedStages =>
      dagScheduler.resubmitFailedStages()
  }

  override def postStop() {
    // Cancel any active jobs in postStop hook
    dagScheduler.cleanUpAfterSchedulerStop()
  }
}

private[spark] object DAGScheduler {
  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200.milliseconds

  // The time, in millis, to wake up between polls of the completion queue in order to potentially
  // resubmit failed stages
  val POLL_TIMEOUT = 10L

  // Warns the user if a stage contains a task with size greater than this value (in KB)
  val TASK_SIZE_TO_WARN = 100
}
