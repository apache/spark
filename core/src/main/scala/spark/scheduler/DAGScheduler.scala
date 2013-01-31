package spark.scheduler

import java.net.URI
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Queue, Map}

import spark._
import spark.partial.ApproximateActionListener
import spark.partial.ApproximateEvaluator
import spark.partial.PartialResult
import spark.storage.BlockManagerMaster
import spark.storage.BlockManagerId
import util.{MetadataCleaner, TimeStampedHashMap}

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes a DAG of stages for
 * each job, keeps track of which RDDs and stage outputs are materialized, and computes a minimal
 * schedule to run the job. Subclasses only need to implement the code to send a task to the cluster
 * and to report fetch failures (the submitTasks method, and code to add CompletionEvents).
 */
private[spark]
class DAGScheduler(taskSched: TaskScheduler) extends TaskSchedulerListener with Logging {
  taskSched.setListener(this)

  // Called by TaskScheduler to report task completions or failures.
  override def taskEnded(
      task: Task[_],
      reason: TaskEndReason,
      result: Any,
      accumUpdates: Map[Long, Any]) {
    eventQueue.put(CompletionEvent(task, reason, result, accumUpdates))
  }

  // Called by TaskScheduler when an executor fails.
  override def executorLost(execId: String) {
    eventQueue.put(ExecutorLost(execId))
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

  val nextRunId = new AtomicInteger(0)

  val nextStageId = new AtomicInteger(0)

  val idToStage = new TimeStampedHashMap[Int, Stage]

  val shuffleToMapStage = new TimeStampedHashMap[Int, Stage]

  var cacheLocs = new HashMap[Int, Array[List[String]]]

  val env = SparkEnv.get
  val mapOutputTracker = env.mapOutputTracker
  val blockManagerMaster = env.blockManager.master

  // For tracking failed nodes, we use the MapOutputTracker's generation number, which is
  // sent with every task. When we detect a node failing, we note the current generation number
  // and failed executor, increment it for new tasks, and use this to ignore stray ShuffleMapTask
  // results.
  // TODO: Garbage collect information about failure generations when we know there are no more
  //       stray messages to detect.
  val failedGeneration = new HashMap[String, Long]

  val waiting = new HashSet[Stage] // Stages we need to run whose parents aren't done
  val running = new HashSet[Stage] // Stages we are running right now
  val failed = new HashSet[Stage]  // Stages that must be resubmitted due to fetch failures
  val pendingTasks = new TimeStampedHashMap[Stage, HashSet[Task[_]]] // Missing tasks from each stage
  var lastFetchFailureTime: Long = 0  // Used to wait a bit to avoid repeated resubmits

  val activeJobs = new HashSet[ActiveJob]
  val resultStageToJob = new HashMap[Stage, ActiveJob]

  val metadataCleaner = new MetadataCleaner("DAGScheduler", this.cleanup)

  // Start a thread to run the DAGScheduler event loop
  new Thread("DAGScheduler") {
    setDaemon(true)
    override def run() {
      DAGScheduler.this.run()
    }
  }.start()

  private def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    if (!cacheLocs.contains(rdd.id)) {
      val blockIds = rdd.splits.indices.map(index=> "rdd_%d_%d".format(rdd.id, index)).toArray
      cacheLocs(rdd.id) = blockManagerMaster.getLocations(blockIds).map {
        locations => locations.map(_.ip).toList
      }.toArray
    }
    cacheLocs(rdd.id)
  }

  private def clearCacheLocs() {
    cacheLocs.clear()
  }

  /**
   * Get or create a shuffle map stage for the given shuffle dependency's map side.
   * The priority value passed in will be used if the stage doesn't already exist with
   * a lower priority (we assume that priorities always increase across jobs for now).
   */
  private def getShuffleMapStage(shuffleDep: ShuffleDependency[_,_], priority: Int): Stage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(shuffleDep.rdd, Some(shuffleDep), priority)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }

  /**
   * Create a Stage for the given RDD, either as a shuffle map stage (for a ShuffleDependency) or
   * as a result stage for the final RDD used directly in an action. The stage will also be given
   * the provided priority.
   */
  private def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_,_]], priority: Int): Stage = {
    if (shuffleDep != None) {
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of splits is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.origin + ")")
      mapOutputTracker.registerShuffle(shuffleDep.get.shuffleId, rdd.splits.size)
    }
    val id = nextStageId.getAndIncrement()
    val stage = new Stage(id, rdd, shuffleDep, getParentStages(rdd, priority), priority)
    idToStage(id) = stage
    stage
  }

  /**
   * Get or create the list of parent stages for a given RDD. The stages will be assigned the
   * provided priority if they haven't already been created with a lower priority.
   */
  private def getParentStages(rdd: RDD[_], priority: Int): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of splits is unknown
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_] =>
              parents += getShuffleMapStage(shufDep, priority)
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
        val locs = getCacheLocs(rdd)
        for (p <- 0 until rdd.splits.size) {
          if (locs(p) == Nil) {
            for (dep <- rdd.dependencies) {
              dep match {
                case shufDep: ShuffleDependency[_,_] =>
                  val mapStage = getShuffleMapStage(shufDep, stage.priority)
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
    }
    visit(stage.rdd)
    missing.toList
  }

  def runJob[T, U: ClassManifest](
      finalRdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      partitions: Seq[Int],
      callSite: String,
      allowLocal: Boolean)
    : Array[U] =
  {
    if (partitions.size == 0) {
      return new Array[U](0)
    }
    val waiter = new JobWaiter(partitions.size)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    eventQueue.put(JobSubmitted(finalRdd, func2, partitions.toArray, allowLocal, callSite, waiter))
    waiter.getResult() match {
      case JobSucceeded(results: Seq[_]) =>
        return results.asInstanceOf[Seq[U]].toArray
      case JobFailed(exception: Exception) =>
        logInfo("Failed to run " + callSite)
        throw exception
    }
  }

  def runApproximateJob[T, U, R](
      rdd: RDD[T],
      func: (TaskContext, Iterator[T]) => U,
      evaluator: ApproximateEvaluator[U, R],
      callSite: String,
      timeout: Long)
    : PartialResult[R] =
  {
    val listener = new ApproximateActionListener(rdd, func, evaluator, timeout)
    val func2 = func.asInstanceOf[(TaskContext, Iterator[_]) => _]
    val partitions = (0 until rdd.splits.size).toArray
    eventQueue.put(JobSubmitted(rdd, func2, partitions, false, callSite, listener))
    return listener.getResult()    // Will throw an exception if the job fails
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
      val time = System.currentTimeMillis() // TODO: use a pluggable clock for testability
      if (event != null) {
        logDebug("Got event of type " + event.getClass.getName)
      }

      event match {
        case JobSubmitted(finalRDD, func, partitions, allowLocal, callSite, listener) =>
          val runId = nextRunId.getAndIncrement()
          val finalStage = newStage(finalRDD, None, runId)
          val job = new ActiveJob(runId, finalStage, func, partitions, callSite, listener)
          clearCacheLocs()
          logInfo("Got job " + job.runId + " (" + callSite + ") with " + partitions.length +
                  " output partitions")
          logInfo("Final stage: " + finalStage + " (" + finalStage.origin + ")")
          logInfo("Parents of final stage: " + finalStage.parents)
          logInfo("Missing parents: " + getMissingParentStages(finalStage))
          if (allowLocal && finalStage.parents.size == 0 && partitions.length == 1) {
            // Compute very short actions like first() or take() with no parent stages locally.
            runLocally(job)
          } else {
            activeJobs += job
            resultStageToJob(finalStage) = job
            submitStage(finalStage)
          }

        case ExecutorLost(execId) =>
          handleExecutorLost(execId)

        case completion: CompletionEvent =>
          handleTaskCompletion(completion)

        case TaskSetFailed(taskSet, reason) =>
          abortStage(idToStage(taskSet.stageId), reason)

        case StopDAGScheduler =>
          // Cancel any active jobs
          for (job <- activeJobs) {
            val error = new SparkException("Job cancelled because SparkContext was shut down")
            job.listener.jobFailed(error)
          }
          return

        case null =>
          // queue.poll() timed out, ignore it
      }

      // Periodically resubmit failed stages if some map output fetches have failed and we have
      // waited at least RESUBMIT_TIMEOUT. We wait for this short time because when a node fails,
      // tasks on many other nodes are bound to get a fetch failure, and they won't all get it at
      // the same time, so we want to make sure we've identified all the reduce tasks that depend
      // on the failed node.
      if (failed.size > 0 && time > lastFetchFailureTime + RESUBMIT_TIMEOUT) {
        logInfo("Resubmitting failed stages")
        clearCacheLocs()
        val failed2 = failed.toArray
        failed.clear()
        for (stage <- failed2.sortBy(_.priority)) {
          submitStage(stage)
        }
      } else {
        // TODO: We might want to run this less often, when we are sure that something has become
        // runnable that wasn't before.
        logTrace("Checking for newly runnable parent stages")
        logTrace("running: " + running)
        logTrace("waiting: " + waiting)
        logTrace("failed: " + failed)
        val waiting2 = waiting.toArray
        waiting.clear()
        for (stage <- waiting2.sortBy(_.priority)) {
          submitStage(stage)
        }
      }
    }
  }

  /**
   * Run a job on an RDD locally, assuming it has only a single partition and no dependencies.
   * We run the operation in a separate thread just in case it takes a bunch of time, so that we
   * don't block the DAGScheduler event loop or other concurrent jobs.
   */
  private def runLocally(job: ActiveJob) {
    logInfo("Computing the requested partition locally")
    new Thread("Local computation of job " + job.runId) {
      override def run() {
        try {
          SparkEnv.set(env)
          val rdd = job.finalStage.rdd
          val split = rdd.splits(job.partitions(0))
          val taskContext = new TaskContext(job.finalStage.id, job.partitions(0), 0)
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
    }.start()
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
      for (id <- 0 until job.numPartitions if (!job.finished(id))) {
        val partition = job.partitions(id)
        val locs = getPreferredLocs(stage.rdd, partition)
        tasks += new ResultTask(stage.id, stage.rdd, job.func, partition, locs, id)
      }
    }
    if (tasks.size > 0) {
      logInfo("Submitting " + tasks.size + " missing tasks from " + stage + " (" + stage.rdd + ")")
      myPending ++= tasks
      logDebug("New pending tasks: " + myPending)
      taskSched.submitTasks(
        new TaskSet(tasks.toArray, stage.id, stage.newAttemptId(), stage.priority))
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
    val stage = idToStage(task.stageId)

    def markStageAsFinished(stage: Stage) = {
      val serviceTime = stage.submissionTime match {
        case Some(t) => "%.03f".format((System.currentTimeMillis() - t) / 1000.0)
        case _ => "Unkown"
      }
      logInfo("%s (%s) finished in %s s".format(stage, stage.origin, serviceTime))
      running -= stage
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
                    activeJobs -= job
                    resultStageToJob -= stage
                    markStageAsFinished(stage)
                  }
                  job.listener.taskSucceeded(rt.outputId, event.result)
                }
              case None =>
                logInfo("Ignoring result from " + rt + " because its job has finished")
            }

          case smt: ShuffleMapTask =>
            val stage = idToStage(smt.stageId)
            val status = event.result.asInstanceOf[MapStatus]
            val execId = status.location.executorId
            logDebug("ShuffleMapTask finished on " + execId)
            if (failedGeneration.contains(execId) && smt.generation <= failedGeneration(execId)) {
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
                // We supply true to increment the generation number here in case this is a
                // recomputation of the map outputs. In that case, some nodes may have cached
                // locations with holes (from when we detected the error) and will need the
                // generation incremented to refetch them.
                // TODO: Only increment the generation number if this is not the first time
                //       we registered these map outputs.
                mapOutputTracker.registerMapOutputs(
                  stage.shuffleDep.get.shuffleId,
                  stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray,
                  true)
              }
              clearCacheLocs()
              if (stage.outputLocs.count(_ == Nil) != 0) {
                // Some tasks had failed; let's resubmit this stage
                // TODO: Lower-level scheduler should also deal with this
                logInfo("Resubmitting " + stage + " (" + stage.origin +
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
        val failedStage = idToStage(task.stageId)
        running -= failedStage
        failed += failedStage
        // TODO: Cancel running tasks in the stage
        logInfo("Marking " + failedStage + " (" + failedStage.origin +
          ") for resubmision due to a fetch failure")
        // Mark the map whose fetch failed as broken in the map stage
        val mapStage = shuffleToMapStage(shuffleId)
        if (mapId != -1) {
          mapStage.removeOutputLoc(mapId, bmAddress)
          mapOutputTracker.unregisterMapOutput(shuffleId, mapId, bmAddress)
        }
        logInfo("The failed fetch was from " + mapStage + " (" + mapStage.origin +
          "); marking it for resubmission")
        failed += mapStage
        // Remember that a fetch failed now; this is used to resubmit the broken
        // stages later, after a small wait (to give other tasks the chance to fail)
        lastFetchFailureTime = System.currentTimeMillis() // TODO: Use pluggable clock
        // TODO: mark the executor as failed only if there were lots of fetch failures on it
        if (bmAddress != null) {
          handleExecutorLost(bmAddress.executorId, Some(task.generation))
        }

      case other =>
        // Non-fetch failure -- probably a bug in user code; abort all jobs depending on this stage
        abortStage(idToStage(task.stageId), task + " failed: " + other)
    }
  }

  /**
   * Responds to an executor being lost. This is called inside the event loop, so it assumes it can
   * modify the scheduler's internal state. Use executorLost() to post a loss event from outside.
   *
   * Optionally the generation during which the failure was caught can be passed to avoid allowing
   * stray fetch failures from possibly retriggering the detection of a node as lost.
   */
  private def handleExecutorLost(execId: String, maybeGeneration: Option[Long] = None) {
    val currentGeneration = maybeGeneration.getOrElse(mapOutputTracker.getGeneration)
    if (!failedGeneration.contains(execId) || failedGeneration(execId) < currentGeneration) {
      failedGeneration(execId) = currentGeneration
      logInfo("Executor lost: %s (generation %d)".format(execId, currentGeneration))
      env.blockManager.master.removeExecutor(execId)
      // TODO: This will be really slow if we keep accumulating shuffle map stages
      for ((shuffleId, stage) <- shuffleToMapStage) {
        stage.removeOutputsOnExecutor(execId)
        val locs = stage.outputLocs.map(list => if (list.isEmpty) null else list.head).toArray
        mapOutputTracker.registerMapOutputs(shuffleId, locs, true)
      }
      if (shuffleToMapStage.isEmpty) {
        mapOutputTracker.incrementGeneration()
      }
      clearCacheLocs()
    } else {
      logDebug("Additional executor lost message for " + execId +
               "(generation " + currentGeneration + ")")
    }
  }

  /**
   * Aborts all jobs depending on a particular Stage. This is called in response to a task set
   * being cancelled by the TaskScheduler. Use taskSetFailed() to inject this event from outside.
   */
  private def abortStage(failedStage: Stage, reason: String) {
    val dependentStages = resultStageToJob.keys.filter(x => stageDependsOn(x, failedStage)).toSeq
    for (resultStage <- dependentStages) {
      val job = resultStageToJob(resultStage)
      job.listener.jobFailed(new SparkException("Job failed: " + reason))
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
              val mapStage = getShuffleMapStage(shufDep, stage.priority)
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

  private def getPreferredLocs(rdd: RDD[_], partition: Int): List[String] = {
    // If the partition is cached, return the cache locations
    val cached = getCacheLocs(rdd)(partition)
    if (cached != Nil) {
      return cached
    }
    // If the RDD has some placement preferences (as is the case for input RDDs), get those
    val rddPrefs = rdd.preferredLocations(rdd.splits(partition)).toList
    if (rddPrefs != Nil) {
      return rddPrefs
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
    return Nil
  }

  private def cleanup(cleanupTime: Long) {
    var sizeBefore = idToStage.size
    idToStage.clearOldValues(cleanupTime)
    logInfo("idToStage " + sizeBefore + " --> " + idToStage.size)

    sizeBefore = shuffleToMapStage.size
    shuffleToMapStage.clearOldValues(cleanupTime)
    logInfo("shuffleToMapStage " + sizeBefore + " --> " + shuffleToMapStage.size)
    
    sizeBefore = pendingTasks.size
    pendingTasks.clearOldValues(cleanupTime)
    logInfo("pendingTasks " + sizeBefore + " --> " + pendingTasks.size)
  }

  def stop() {
    eventQueue.put(StopDAGScheduler)
    metadataCleaner.cancel()
    taskSched.stop()
  }
}
