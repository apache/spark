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
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.concurrent.duration._
import scala.reflect.ClassTag

import akka.actor._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerMaster, RDDBlockId}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}

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
 * THREADING: This class runs all its logic in a single thread executing the run() method, to which
 * events are submitted using a synchronized queue (eventQueue). The public API methods, such as
 * runJob, taskEnded and executorLost, post events asynchronously to this queue. All other methods
 * should be private.
 */
private[spark]
class DAGScheduler(
    taskSched: TaskScheduler,
    mapOutputTracker: MapOutputTrackerMaster,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv)
  extends Logging {

  def this(taskSched: TaskScheduler) {
    this(taskSched, SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster],
      SparkEnv.get.blockManager.master, SparkEnv.get)
  }
  taskSched.setDAGScheduler(this)

  // Called by TaskScheduler to report task's starting.
  def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventProcessActor ! BeginEvent(task, taskInfo)
  }

  // Called to report that a task has completed and results are being fetched remotely.
  def taskGettingResult(task: Task[_], taskInfo: TaskInfo) {
    eventProcessActor ! GettingResultEvent(task, taskInfo)
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
  def executorGained(execId: String, host: String) {
    eventProcessActor ! ExecutorGained(execId, host)
  }

  // Called by TaskScheduler to cancel an entire TaskSet due to either repeated failures or
  // cancellation of the job itself.
  def taskSetFailed(taskSet: TaskSet, reason: String) {
    eventProcessActor ! TaskSetFailed(taskSet, reason)
  }

  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 200.milliseconds

  // The time, in millis, to wake up between polls of the completion queue in order to potentially
  // resubmit failed stages
  val POLL_TIMEOUT = 10L

  // Warns the user if a stage contains a task with size greater than this value (in KB)
  val TASK_SIZE_TO_WARN = 100

  private var eventProcessActor: ActorRef = _

  private[scheduler] val nextJobId = new AtomicInteger(0)

  def numTotalJobs: Int = nextJobId.get()

  private val nextStageId = new AtomicInteger(0)

  private[scheduler] val jobIdToStageIds = new TimeStampedHashMap[Int, HashSet[Int]]

  private[scheduler] val stageIdToJobIds = new TimeStampedHashMap[Int, HashSet[Int]]

  private[scheduler] val stageIdToStage = new TimeStampedHashMap[Int, Stage]

  private[scheduler] val shuffleToMapStage = new TimeStampedHashMap[Int, Stage]

  private[spark] val stageToInfos = new TimeStampedHashMap[Stage, StageInfo]

  // An async scheduler event bus. The bus should be stopped when DAGSCheduler is stopped.
  private[spark] val listenerBus = new SparkListenerBus

  // Contains the locations that each RDD's partitions are cached on
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  val failedEpoch = new HashMap[String, Long]

  // stage id to the active job
  val idToActiveJob = new HashMap[Int, ActiveJob]

  val waiting = new HashSet[Stage] // Stages we need to run whose parents aren't done
  val running = new HashSet[Stage] // Stages we are running right now
  val failed = new HashSet[Stage]  // Stages that must be resubmitted due to fetch failures
  // Missing tasks from each stage
  val pendingTasks = new TimeStampedHashMap[Stage, HashSet[Task[_]]]
  var lastFetchFailureTime: Long = 0  // Used to wait a bit to avoid repeated resubmits

  val activeJobs = new HashSet[ActiveJob]
  val resultStageToJob = new HashMap[Stage, ActiveJob]

  val metadataCleaner = new MetadataCleaner(
    MetadataCleanerType.DAG_SCHEDULER, this.cleanup, env.conf)

  /**
   * Starts the event processing actor.  The actor has two responsibilities:
   *
   * 1. Waits for events like job submission, task finished, task failure etc., and calls
   *    [[org.apache.spark.scheduler.DAGScheduler.processEvent()]] to process them.
   * 2. Schedules a periodical task to resubmit failed stages.
   *
   * NOTE: the actor cannot be started in the constructor, because the periodical task references
   * some internal states of the enclosing [[org.apache.spark.scheduler.DAGScheduler]] object, thus
   * cannot be scheduled until the [[org.apache.spark.scheduler.DAGScheduler]] is fully constructed.
   */
  def start() {
    eventProcessActor = env.actorSystem.actorOf(Props(new Actor {
      /**
       * A handle to the periodical task, used to cancel the task when the actor is stopped.
       */
      var resubmissionTask: Cancellable = _

      override def preStart() {
        import context.dispatcher
        /**
         * A message is sent to the actor itself periodically to remind the actor to resubmit failed
         * stages.  In this way, stage resubmission can be done within the same thread context of
         * other event processing logic to avoid unnecessary synchronization overhead.
         */
        resubmissionTask = context.system.scheduler.schedule(
          RESUBMIT_TIMEOUT, RESUBMIT_TIMEOUT, self, ResubmitFailedStages)
      }

      /**
       * The main event loop of the DAG scheduler.
       */
      def receive = {
        case event: DAGSchedulerEvent =>
          logTrace("Got event of type " + event.getClass.getName)

          /**
           * All events are forwarded to `processEvent()`, so that the event processing logic can
           * easily tested without starting a dedicated actor.  Please refer to `DAGSchedulerSuite`
           * for details.
           */
          if (!processEvent(event)) {
            submitWaitingStages()
          } else {
            resubmissionTask.cancel()
            context.stop(self)
          }
      }
    }))
  }

  def addSparkListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  private def getCacheLocs(rdd: RDD[_]): Array[Seq[TaskLocation]] = {
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.partitions.indices.map(index=> RDDBlockId(rdd.id, index)).toArray[BlockId]
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
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_,_], jobId: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage =
          newOrUsedStage(shuffleDep.rdd, shuffleDep.rdd.partitions.size, shuffleDep, jobId)
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
      shuffleDep: Option[ShuffleDependency[_,_]],
      jobId: Int,
      callSite: Option[String] = None)
    : Stage =
  {
    val id = nextStageId.getAndIncrement()
    val stage =
      new Stage(id, rdd, numTasks, shuffleDep, getParentStages(rdd, jobId), jobId, callSite)
    stageIdToStage(id) = stage
    updateJobIdStageIdMaps(jobId, stage)
    stageToInfos(stage) = new StageInfo(stage)
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
      shuffleDep: ShuffleDependency[_,_],
      jobId: Int,
      callSite: Option[String] = None)
    : Stage =
  {
    val stage = newStage(rdd, numTasks, Some(shuffleDep), jobId, callSite)
    if (mapOutputTracker.has(shuffleDep.shuffleId)) {
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      for (i <- 0 until locs.size) stage.outputLocs(i) = List(locs(i))
      stage.numAvailableOutputs = locs.size
    } else {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.origin + ")")
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
            case shufDep: ShuffleDependency[_,_] =>
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
              case shufDep: ShuffleDependency[_,_] =>
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
   * Removes job and any stages that are not needed by any other job.  Returns the set of ids for
   * stages that were removed.  The associated tasks for those stages need to be cancelled if we
   * got here via job cancellation.
   */
  private def removeJobAndIndependentStages(jobId: Int): Set[Int] = {
    val registeredStages = jobIdToStageIds(jobId)
    val independentStages = new HashSet[Int]()
    if (registeredStages.isEmpty) {
      logError("No stages registered for job " + jobId)
    } else {
      stageIdToJobIds.filterKeys(stageId => registeredStages.contains(stageId)).foreach {
        case (stageId, jobSet) =>
          if (!jobSet.contains(jobId)) {
            logError(
              "Job %d not registered for stage %d even though that stage was registered for the job"
              .format(jobId, stageId))
          } else {
            def removeStage(stageId: Int) {
              // data structures based on Stage
              stageIdToStage.get(stageId).foreach { s =>
                if (running.contains(s)) {
                  logDebug("Removing running stage %d".format(stageId))
                  running -= s
                }
                stageToInfos -= s
                shuffleToMapStage.keys.filter(shuffleToMapStage(_) == s).foreach(shuffleId =>
                  shuffleToMapStage.remove(shuffleId))
                if (pendingTasks.contains(s) && !pendingTasks(s).isEmpty) {
                  logDebug("Removing pending status for stage %d".format(stageId))
                }
                pendingTasks -= s
                if (waiting.contains(s)) {
                  logDebug("Removing stage %d from waiting set.".format(stageId))
                  waiting -= s
                }
                if (failed.contains(s)) {
                  logDebug("Removing stage %d from failed set.".format(stageId))
                  failed -= s
                }
              }
              // data structures based on StageId
              stageIdToStage -= stageId
              stageIdToJobIds -= stageId

              logDebug("After removal of stage %d, remaining stages = %d"
                .format(stageId, stageIdToStage.size))
            }

            jobSet -= jobId
            if (jobSet.isEmpty) { // no other job needs this stage
              independentStages += stageId
              removeStage(stageId)
            }
          }
      }
    }
    independentStages.toSet
  }

  private def jobIdToStageIdsRemove(jobId: Int) {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to remove unregistered job " + jobId)
    } else {
      removeJobAndIndependentStages(jobId)
      jobIdToStageIds -= jobId
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
      callSite: String,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null): JobWaiter[U] =
  {
    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = rdd.partitions.length
    partitions.find(p => p >= maxPartitions).foreach { p =>
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
      callSite: String,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
  {
    val waiter = submitJob(rdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    waiter.awaitResult() match {
      case JobSucceeded => {}
      case JobFailed(exception: Exception, _) =>
        logInfo("Failed to run " + callSite)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: String,
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

  /**
   * Process one event retrieved from the event processing actor.
   *
   * @param event The event to be processed.
   * @return `true` if we should stop the event loop.
   */
  private[scheduler] def processEvent(event: DAGSchedulerEvent): Boolean = {
    event match {
      case JobSubmitted(jobId, rdd, func, partitions, allowLocal, callSite, listener, properties) =>
        var finalStage: Stage = null
        try {
          // New stage creation may throw an exception if, for example, jobs are run on a HadoopRDD
          // whose underlying HDFS files have been deleted.
          finalStage = newStage(rdd, partitions.size, None, jobId, Some(callSite))
        } catch {
          case e: Exception =>
            logWarning("Creating new stage failed due to exception - job: " + jobId, e)
            listener.jobFailed(e)
            return false
        }
        val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
        clearCacheLocs()
        logInfo("Got job " + job.jobId + " (" + callSite + ") with " + partitions.length +
                " output partitions (allowLocal=" + allowLocal + ")")
        logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
        logInfo("Parents of final stage: " + finalStage.parents)
        logInfo("Missing parents: " + getMissingParentStages(finalStage))
        if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
          // Compute very short actions like first() or take() with no parent stages locally.
          listenerBus.post(SparkListenerJobStart(job, Array(), properties))
          runLocally(job)
        } else {
          idToActiveJob(jobId) = job
          activeJobs += job
          resultStageToJob(finalStage) = job
          listenerBus.post(SparkListenerJobStart(job, jobIdToStageIds(jobId).toArray, properties))
          submitStage(finalStage)
        }

      case JobCancelled(jobId) =>
        handleJobCancellation(jobId)

      case JobGroupCancelled(groupId) =>
        // Cancel all jobs belonging to this job group.
        // First finds all active jobs with this group id, and then kill stages for them.
        val activeInGroup = activeJobs.filter(activeJob =>
          groupId == activeJob.properties.get(SparkContext.SPARK_JOB_GROUP_ID))
        val jobIds = activeInGroup.map(_.jobId)
        jobIds.foreach { handleJobCancellation }

      case AllJobsCancelled =>
        // Cancel all running jobs.
        running.map(_.jobId).foreach { handleJobCancellation }
        activeJobs.clear()      // These should already be empty by this point,
        idToActiveJob.clear()   // but just in case we lost track of some jobs...

      case ExecutorGained(execId, host) =>
        handleExecutorGained(execId, host)

      case ExecutorLost(execId) =>
        handleExecutorLost(execId)

      case BeginEvent(task, taskInfo) =>
        for (
          job <- idToActiveJob.get(task.stageId);
          stage <- stageIdToStage.get(task.stageId);
          stageInfo <- stageToInfos.get(stage)
        ) {
          if (taskInfo.serializedSize > TASK_SIZE_TO_WARN * 1024 &&
              !stageInfo.emittedTaskSizeWarning) {
            stageInfo.emittedTaskSizeWarning = true
            logWarning(("Stage %d (%s) contains a task of very large " +
              "size (%d KB). The maximum recommended task size is %d KB.").format(
              task.stageId, stageInfo.name, taskInfo.serializedSize / 1024, TASK_SIZE_TO_WARN))
          }
        }
        listenerBus.post(SparkListenerTaskStart(task, taskInfo))

      case GettingResultEvent(task, taskInfo) =>
        listenerBus.post(SparkListenerTaskGettingResult(task, taskInfo))

      case completion @ CompletionEvent(task, reason, _, _, taskInfo, taskMetrics) =>
        listenerBus.post(SparkListenerTaskEnd(task, reason, taskInfo, taskMetrics))
        handleTaskCompletion(completion)

      case TaskSetFailed(taskSet, reason) =>
        stageIdToStage.get(taskSet.stageId).foreach { abortStage(_, reason) }

      case ResubmitFailedStages =>
        if (failed.size > 0) {
          resubmitFailedStages()
        }

      case StopDAGScheduler =>
        // Cancel any active jobs
        for (job <- activeJobs) {
          val error = new SparkException("Job cancelled because SparkContext was shut down")
          job.listener.jobFailed(error)
          listenerBus.post(SparkListenerJobEnd(job, JobFailed(error, None)))
        }
        return true
    }
    false
  }

  /**
   * Resubmit any failed stages. Ordinarily called after a small amount of time has passed since
   * the last fetch failure.
   */
  private[scheduler] def resubmitFailedStages() {
    logInfo("Resubmitting failed stages")
    clearCacheLocs()
    val failed2 = failed.toArray
    failed.clear()
    for (stage <- failed2.sortBy(_.jobId)) {
      submitStage(stage)
    }
  }

  /**
   * Check for waiting or failed stages which are now eligible for resubmission.
   * Ordinarily run on every iteration of the event loop.
   */
  private[scheduler] def submitWaitingStages() {
    // TODO: We might want to run this less often, when we are sure that something has become
    // runnable that wasn't before.
    logTrace("Checking for newly runnable parent stages")
    logTrace("running: " + running)
    logTrace("waiting: " + waiting)
    logTrace("failed: " + failed)
    val waiting2 = waiting.toArray
    waiting.clear()
    for (stage <- waiting2.sortBy(_.jobId)) {
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
        jobResult = JobFailed(e, Some(job.finalStage))
        job.listener.jobFailed(e)
    } finally {
      val s = job.finalStage
      stageIdToJobIds -= s.id    // clean up data structures that were populated for a local job,
      stageIdToStage -= s.id     // but that won't get cleaned up via the normal paths through
      stageToInfos -= s          // completion events or stage abort
      jobIdToStageIds -= job.jobId
      listenerBus.post(SparkListenerJobEnd(job, jobResult))
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
      jobsThatUseStage.find(idToActiveJob.contains(_))
    } else {
      None
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    val jobId = activeJobForStage(stage)
    if (jobId.isDefined) {
      logDebug("submitStage(" + stage + ")")
      if (!waiting(stage) && !running(stage) && !failed(stage)) {
        val missing = getMissingParentStages(stage).sortBy(_.id)
        logDebug("missing: " + missing)
        if (missing == Nil) {
          logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
          submitMissingTasks(stage, jobId.get)
          running += stage
        } else {
          for (parent <- missing) {
            submitStage(parent)
          }
          waiting += stage
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

    val properties = if (idToActiveJob.contains(jobId)) {
      idToActiveJob(stage.jobId).properties
    } else {
      //this stage will be assigned to "default" pool
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
          running -= stage
          return
      }

      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      myPending ++= tasks
      logDebug("New pending tasks: " + myPending)
      taskSched.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      stageToInfos(stage).submissionTime = Some(System.currentTimeMillis())
    } else {
      logDebug("Stage " + stage + " is actually done; %b %d %d".format(
        stage.isAvailable, stage.numAvailableOutputs, stage.numPartitions))
      running -= stage
    }
  }

  /**
   * Responds to a task finishing. This is called inside the event loop so it assumes that it can
   * modify the scheduler's internal state. Use taskEnded() to post a task end event from outside.
   */
  private def handleTaskCompletion(event: CompletionEvent) {
    val task = event.task

    if (!stageIdToStage.contains(task.stageId)) {
      // Skip all the actions if the stage has been cancelled.
      return
    }
    val stage = stageIdToStage(task.stageId)

    def markStageAsFinished(stage: Stage) = {
      val serviceTime = stageToInfos(stage).submissionTime match {
        case Some(t) => "%.03f".format((System.currentTimeMillis() - t) / 1000.0)
        case _ => "Unknown"
      }
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stageToInfos(stage).completionTime = Some(System.currentTimeMillis())
      listenerBus.post(SparkListenerStageCompleted(stageToInfos(stage)))
      running -= stage
    }
    event.reason match {
      case Success =>
        logInfo("Completed " + task)
        if (event.accumUpdates != null) {
          Accumulators.add(event.accumUpdates) // TODO: do this only if task wasn't resubmitted
        }
        pendingTasks(stage) -= task
        stageToInfos(stage).taskInfos += event.taskInfo -> event.taskMetrics
        task match {
          case rt: ResultTask[_, _] =>
            resultStageToJob.get(stage) match {
              case Some(job) =>
                if (!job.finished(rt.outputId)) {
                  job.finished(rt.outputId) = true
                  job.numFinished += 1
                  // If the whole job has finished, remove it
                  if (job.numFinished == job.numPartitions) {
                    idToActiveJob -= stage.jobId
                    activeJobs -= job
                    resultStageToJob -= stage
                    markStageAsFinished(stage)
                    jobIdToStageIdsRemove(job.jobId)
                    listenerBus.post(SparkListenerJobEnd(job, JobSucceeded))
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
            if (running.contains(stage) && pendingTasks(stage).isEmpty) {
              markStageAsFinished(stage)
              logInfo("looking for newly runnable stages")
              logInfo("running: " + running)
              logInfo("waiting: " + waiting)
              logInfo("failed: " + failed)
              if (stage.shuffleDep != None) {
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
                for (stage <- waiting) {
                  logInfo("Missing parents for " + stage + ": " + getMissingParentStages(stage))
                }
                for (stage <- waiting if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waiting --= newlyRunnable
                running ++= newlyRunnable
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
        running -= failedStage
        failed += failedStage
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
        failed += mapStage
        // Remember that a fetch failed now; this is used to resubmit the broken
        // stages later, after a small wait (to give other tasks the chance to fail)
        lastFetchFailureTime = System.currentTimeMillis() // TODO: Use pluggable clock
        // TODO: mark the executor as failed only if there were lots of fetch failures on it
        if (bmAddress != null) {
          handleExecutorLost(bmAddress.executorId, Some(task.epoch))
        }

      case ExceptionFailure(className, description, stackTrace, metrics) =>
        // Do nothing here, left up to the TaskScheduler to decide how to handle user failures

      case TaskResultLost =>
        // Do nothing here; the TaskScheduler handles these failures and resubmits the task.

      case other =>
        // Unrecognized failure - abort all jobs depending on this stage
        abortStage(stageIdToStage(task.stageId), task + " failed: " + other)
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * Optionally the epoch during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private def handleExecutorLost(execId: String, maybeEpoch: Option[Long] = None) {
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
  }

  private def handleExecutorGained(execId: String, host: String) {
    // remove from failedEpoch(execId) ?
    if (failedEpoch.contains(execId)) {
      logInfo("Host gained which was in lost list earlier: " + host)
      failedEpoch -= execId
    }
  }

  private def handleJobCancellation(jobId: Int) {
    if (!jobIdToStageIds.contains(jobId)) {
      logDebug("Trying to cancel unregistered job " + jobId)
    } else {
      val independentStages = removeJobAndIndependentStages(jobId)
      independentStages.foreach { taskSched.cancelTasks }
      val error = new SparkException("Job %d cancelled".format(jobId))
      val job = idToActiveJob(jobId)
      job.listener.jobFailed(error)
      jobIdToStageIds -= jobId
      activeJobs -= job
      idToActiveJob -= jobId
      listenerBus.post(SparkListenerJobEnd(job, JobFailed(error, Some(job.finalStage))))
    }
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being canceled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private def abortStage(failedStage: Stage, reason: String) {
    if (!stageIdToStage.contains(failedStage.id)) {
      // Skip all the actions if the stage has been removed.
      return
    }
    val dependentStages = resultStageToJob.keys.filter(x => stageDependsOn(x, failedStage)).toSeq
    stageToInfos(failedStage).completionTime = Some(System.currentTimeMillis())
    for (resultStage <- dependentStages) {
      val job = resultStageToJob(resultStage)
      val error = new SparkException("Job aborted: " + reason)
      job.listener.jobFailed(error)
      jobIdToStageIdsRemove(job.jobId)
      idToActiveJob -= resultStage.jobId
      activeJobs -= job
      resultStageToJob -= resultStage
      listenerBus.post(SparkListenerJobEnd(job, JobFailed(error, Some(failedStage))))
    }
    if (dependentStages.isEmpty) {
      logInfo("Ignoring failure of " + failedStage + " because all jobs depending on it are done")
    }
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
            case shufDep: ShuffleDependency[_,_] =>
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
          if (locs != Nil)
            return locs
        }
      case _ =>
    }
    Nil
  }

  private def cleanup(cleanupTime: Long) {
    Map(
      "stageIdToStage" -> stageIdToStage,
      "shuffleToMapStage" -> shuffleToMapStage,
      "pendingTasks" -> pendingTasks,
      "stageToInfos" -> stageToInfos,
      "jobIdToStageIds" -> jobIdToStageIds,
      "stageIdToJobIds" -> stageIdToJobIds).
      foreach { case(s, t) => {
      val sizeBefore = t.size
      t.clearOldValues(cleanupTime)
      logInfo("%s %d --> %d".format(s, sizeBefore, t.size))
    }}
  }

  def stop() {
    if (eventProcessActor != null) {
      eventProcessActor ! StopDAGScheduler
    }
    metadataCleaner.cancel()
    taskSched.stop()
    listenerBus.stop()
  }
}
