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
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.partial.{ApproximateActionListener, ApproximateEvaluator, PartialResult}
import org.apache.spark.scheduler.cluster.TaskInfo
import org.apache.spark.storage.{BlockManager, BlockManagerMaster}
import org.apache.spark.util.{MetadataCleaner, TimeStampedHashMap}

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
 * not caused by shuffie file loss are handled by the TaskScheduler, which will retry each task
 * a small number of times before cancelling the whole stage.
 *
 * THREADING: This class runs all its logic in a single thread executing the run() method, to which
 * events are submitted using a synchonized queue (eventQueue). The public API methods, such as
 * runJob, taskEnded and executorLost, post events asynchronously to this queue. All other methods
 * should be private.
 */
private[spark]
class DAGScheduler(
    taskSched: TaskScheduler,
    mapOutputTracker: MapOutputTracker,
    blockManagerMaster: BlockManagerMaster,
    env: SparkEnv)
  extends TaskSchedulerListener with Logging {

  def this(taskSched: TaskScheduler) {
    this(taskSched, SparkEnv.get.mapOutputTracker, SparkEnv.get.blockManager.master, SparkEnv.get)
  }
  taskSched.setListener(this)

  // Called by TaskScheduler to report task's starting.
  override def taskStarted(task: Task[_], taskInfo: TaskInfo) {
    eventQueue.put(BeginEvent(task, taskInfo))
  }

  // Called by TaskScheduler to report task completions or failures.
  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any],
      taskInfo: TaskInfo,
      taskMetrics: TaskMetrics) {
    eventQueue.put(CompletionEvent(task, reason, result, accumUpdates, taskInfo, taskMetrics))
  }

  // Called by TaskScheduler when an executor fails.
  override def executorLost(execId: String) {
    eventQueue.put(ExecutorLost(execId))
  }

  // Called by TaskScheduler when a host is added
  override def executorGained(execId: String, host: String) {
    eventQueue.put(ExecutorGained(execId, host))
  }

  // Called by TaskScheduler to cancel an entire TaskSet due to repeated failures.
  override def taskSetFailed(taskSet: TaskSet, reason: String) {
    eventQueue.put(TaskSetFailed(taskSet, reason))
  }

  // The time, in millis, to wait for fetch failure events to stop coming in after one is detected;
  // this is a simplistic way to avoid resubmitting tasks in the non-fetchable map stage one by one
  // as more failure events come in
  val RESUBMIT_TIMEOUT = 50L

  // The time, in millis, to wake up between polls of the completion queue in order to potentially
  // resubmit failed stages
  val POLL_TIMEOUT = 10L

  private val eventQueue = new LinkedBlockingQueue[DAGSchedulerEvent]

  val nextJobId = new AtomicInteger(0)

  val nextStageId = new AtomicInteger(0)

  val stageIdToStage = new TimeStampedHashMap[Int, Stage]

  val shuffleToMapStage = new TimeStampedHashMap[Int, Stage]

  private[spark] val stageToInfos = new TimeStampedHashMap[Stage, StageInfo]

  private val listenerBus = new SparkListenerBus()

  // Contains the locations that each RDD's partitions are cached on
  private val cacheLocs = new HashMap[Int, Array[Seq[TaskLocation]]]

  // For tracking failed nodes, we use the MapOutputTracker's epoch number, which is sent with
  // every task. When we detect a node failing, we note the current epoch number and failed
  // executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask results.
  //
  // TODO: Garbage collect information about failure epochs when we know there are no more
  //       stray messages to detect.
  val failedEpoch = new HashMap[String, Long]

  val idToActiveJob = new HashMap[Int, ActiveJob]

  val waiting = new HashSet[Stage] // Stages we need to run whose parents aren't done
  val running = new HashSet[Stage] // Stages we are running right now
  val failed = new HashSet[Stage]  // Stages that must be resubmitted due to fetch failures
  val pendingTasks = new TimeStampedHashMap[Stage, HashSet[Task[_]]] // Missing tasks from each stage
  var lastFetchFailureTime: Long = 0  // Used to wait a bit to avoid repeated resubmits

  val activeJobs = new HashSet[ActiveJob]
  val resultStageToJob = new HashMap[Stage, ActiveJob]

  val metadataCleaner = new MetadataCleaner("DAGScheduler", this.cleanup)

  // Start a thread to run the DAGScheduler event loop
  def start() {
    new Thread("DAGScheduler") {
      setDaemon(true)
      override def run() {
        DAGScheduler.this.run()
      }
    }.start()
  }

  def addSparkListener(listener: SparkListener) {
    listenerBus.addListener(listener)
  }

  private def getCacheLocs(rdd: RDD[_]): Array[Seq[TaskLocation]] = {
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.partitions.indices.map(index=> "rdd_%d_%d".format(rdd.id, index)).toArray
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
        val stage = newStage(shuffleDep.rdd, Some(shuffleDep), jobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  /**
   * Create a Stage for the given RDD, either as a shuffle map stage (for a ShuffleDependency) or
   * as a result stage for the final RDD used directly in an action. The stage will also be
   * associated with the provided jobId.
   */
  private def newStage(
      rdd: RDD[_],
      shuffleDep: Option[ShuffleDependency[_,_]],
      jobId: Int,
      callSite: Option[String] = None)
    : Stage =
  {
    if (shuffleDep != None) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.origin + ")")
      mapOutputTracker.registerShuffle(shuffleDep.get.shuffleId, rdd.partitions.size)
    }
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, shuffleDep, getParentStages(rdd, jobId), jobId, callSite)
    stageIdToStage(id) = stage
    stageToInfos(stage) = StageInfo(stage)
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
   * Returns (and does not submit) a JobSubmitted event suitable to run a given job, and a
   * JobWaiter whose getResult() method will return the result of the job when it is complete.
   *
   * The job is assumed to have at least one partition; zero partition jobs should be handled
   * without a JobSubmitted event.
   */
  private[scheduler] def prepareJob[T, U: ClassManifest](
      finalRdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: String,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
    : (JobSubmitted, JobWaiter[U]) =
  {
    assert(partitions.size > 0)
    val waiter = new JobWaiter(partitions.size, resultHandler)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val toSubmit = JobSubmitted(finalRdd, func2, partitions.toArray, allowLocal, callSite, waiter,
      properties)
    (toSubmit, waiter)
  }

  def runJob[T, U: ClassManifest](
      finalRdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: String,
      allowLocal: Boolean,
      resultHandler: (Int, U) => Unit,
      properties: Properties = null)
  {
    if (partitions.size == 0) {
      return
    }

    // Check to make sure we are not launching a task on a partition that does not exist.
    val maxPartitions = finalRdd.partitions.length
    partitions.find(p => p >= maxPartitions).foreach { p =>
      throw new IllegalArgumentException(
        "Attempting to access a non-existent partition: " + p + ". " +
        "Total number of partitions: " + maxPartitions)
    }

    val (toSubmit: JobSubmitted, waiter: JobWaiter[_]) = prepareJob(
        finalRdd, func, partitions, callSite, allowLocal, resultHandler, properties)
    eventQueue.put(toSubmit)
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
    eventQueue.put(JobSubmitted(rdd, func2, partitions, allowLocal = false, callSite, listener, properties))
    listener.awaitResult()    // Will throw an exception if the job fails
  }

  /**
   * Process one event retrieved from the event queue.
   * Returns true if we should stop the event loop.
   */
  private[scheduler] def processEvent(event: DAGSchedulerEvent): Boolean = {
    event match {
      case JobSubmitted(finalRDD, func, partitions, allowLocal, callSite, listener, properties) =>
        val jobId = nextJobId.getAndIncrement()
        val finalStage = newStage(finalRDD, None, jobId, Some(callSite))
        val job = new ActiveJob(jobId, finalStage, func, partitions, callSite, listener, properties)
        clearCacheLocs()
        logInfo("Got job " + job.jobId + " (" + callSite + ") with " + partitions.length +
                " output partitions (allowLocal=" + allowLocal + ")")
        logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
        logInfo("Parents of final stage: " + finalStage.parents)
        logInfo("Missing parents: " + getMissingParentStages(finalStage))
        if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
          // Compute very short actions like first() or take() with no parent stages locally.
          runLocally(job)
        } else {
          listenerBus.post(SparkListenerJobStart(job, properties))
          idToActiveJob(jobId) = job
          activeJobs += job
          resultStageToJob(finalStage) = job
          submitStage(finalStage)
        }

      case ExecutorGained(execId, host) =>
        handleExecutorGained(execId, host)

      case ExecutorLost(execId) =>
        handleExecutorLost(execId)

      case begin: BeginEvent =>
        listenerBus.post(SparkListenerTaskStart(begin.task, begin.taskInfo))

      case completion: CompletionEvent =>
        listenerBus.post(SparkListenerTaskEnd(
          completion.task, completion.reason, completion.taskInfo, completion.taskMetrics))
        handleTaskCompletion(completion)

      case TaskSetFailed(taskSet, reason) =>
        abortStage(stageIdToStage(taskSet.stageId), reason)

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
   * The main event loop of the DAG scheduler, which waits for new-job / task-finished / failure
   * events and responds by launching tasks. This runs in a dedicated thread and receives events
   * via the eventQueue.
   */
  private def run() {
    SparkEnv.set(env)

    while (true) {
      val event = eventQueue.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS)
      if (event != null) {
        logDebug("Got event of type " + event.getClass.getName)
      }
      this.synchronized { // needed in case other threads makes calls into methods of this class
        if (event != null) {
          if (processEvent(event)) {
            return
          }
        }

        val time = System.currentTimeMillis() // TODO: use a pluggable clock for testability
        // Periodically resubmit failed stages if some map output fetches have failed and we have
        // waited at least RESUBMIT_TIMEOUT. We wait for this short time because when a node fails,
        // tasks on many other nodes are bound to get a fetch failure, and they won't all get it at
        // the same time, so we want to make sure we've identified all the reduce tasks that depend
        // on the failed node.
        if (failed.size > 0 && time > lastFetchFailureTime + RESUBMIT_TIMEOUT) {
          resubmitFailedStages()
        } else {
          submitWaitingStages()
        }
      }
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
        job.listener.jobFailed(e)
    }
  }

  /** Submits stage, but first recursively submits any missing parents. */
  private def submitStage(stage: Stage) {
    logDebug("submitStage(" + stage + ")")
    if (!waiting(stage) && !running(stage) && !failed(stage)) {
      val missing = getMissingParentStages(stage).sortBy(_.id)
      logDebug("missing: " + missing)
      if (missing == Nil) {
        logInfo("Submitting " + stage + " (" + stage.rdd + "), which has no missing parents")
        submitMissingTasks(stage)
        running += stage
      } else {
        for (parent <- missing) {
          submitStage(parent)
        }
        waiting += stage
      }
    }
  }

  /** Called when stage's parents are available and we can now do its task. */
  private def submitMissingTasks(stage: Stage) {
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

    val properties = if (idToActiveJob.contains(stage.jobId)) {
      idToActiveJob(stage.jobId).properties
    } else {
      //this stage will be assigned to "default" pool
      null
    }

    // must be run listener before possible NotSerializableException
    // should be "StageSubmitted" first and then "JobEnded"
    listenerBus.post(SparkListenerStageSubmitted(stage, tasks.size, properties))

    if (tasks.size > 0) {
      // Preemptively serialize a task to make sure it can be serialized. We are catching this
      // exception here because it would be fairly hard to catch the non-serializable exception
      // down the road, where we have several different implementations for local scheduler and
      // cluster schedulers.
      try {
        SparkEnv.get.closureSerializer.newInstance().serialize(tasks.head)
      } catch {
        case e: NotSerializableException =>
          abortStage(stage, e.toString)
          running -= stage
          return
      }

      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      myPending ++= tasks
      logDebug("New pending tasks: " + myPending)
      taskSched.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.jobId, properties))
      if (!stage.submissionTime.isDefined) {
        stage.submissionTime = Some(System.currentTimeMillis())
      }
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
    val stage = stageIdToStage(task.stageId)

    def markStageAsFinished(stage: Stage) = {
      val serviceTime = stage.submissionTime match {
        case Some(t) => "%.03f".format((System.currentTimeMillis() - t) / 1000.0)
        case _ => "Unkown"
      }
      logInfo("%s (%s) finished in %s s".format(stage, stage.name, serviceTime))
      stage.completionTime = Some(System.currentTimeMillis)
      listenerBus.post(StageCompleted(stageToInfos(stage)))
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
              stage.addOutputLoc(smt.partition, status)
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
              if (stage.outputLocs.count(_ == Nil) != 0) {
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
                for (stage <- newlyRunnable.sortBy(_.id)) {
                  logInfo("Submitting " + stage + " (" + stage.rdd + "), which is now runnable")
                  submitMissingTasks(stage)
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

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being cancelled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private def abortStage(failedStage: Stage, reason: String) {
    val dependentStages = resultStageToJob.keys.filter(x => stageDependsOn(x, failedStage)).toSeq
    failedStage.completionTime = Some(System.currentTimeMillis())
    for (resultStage <- dependentStages) {
      val job = resultStageToJob(resultStage)
      val error = new SparkException("Job failed: " + reason)
      job.listener.jobFailed(error)
      listenerBus.post(SparkListenerJobEnd(job, JobFailed(error, Some(failedStage))))
      idToActiveJob -= resultStage.jobId
      activeJobs -= job
      resultStageToJob -= resultStage
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
    rdd.dependencies.foreach(_ match {
      case n: NarrowDependency[_] =>
        for (inPart <- n.getParents(partition)) {
          val locs = getPreferredLocs(n.rdd, inPart)
          if (locs != Nil)
            return locs
        }
      case _ =>
    })
    Nil
  }

  private def cleanup(cleanupTime: Long) {
    var sizeBefore = stageIdToStage.size
    stageIdToStage.clearOldValues(cleanupTime)
    logInfo("stageIdToStage " + sizeBefore + " --> " + stageIdToStage.size)

    sizeBefore = shuffleToMapStage.size
    shuffleToMapStage.clearOldValues(cleanupTime)
    logInfo("shuffleToMapStage " + sizeBefore + " --> " + shuffleToMapStage.size)

    sizeBefore = pendingTasks.size
    pendingTasks.clearOldValues(cleanupTime)
    logInfo("pendingTasks " + sizeBefore + " --> " + pendingTasks.size)

    sizeBefore = stageToInfos.size
    stageToInfos.clearOldValues(cleanupTime)
    logInfo("stageToInfos " + sizeBefore + " --> " + stageToInfos.size)
  }

  def stop() {
    eventQueue.put(StopDAGScheduler)
    metadataCleaner.cancel()
    taskSched.stop()
  }
}
