package spark

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

// A task created by the DAG scheduler. Knows its stage ID and map ouput tracker generation.
abstract class DAGTask[T](val stageId: Int) extends Task[T] {
  val gen = SparkEnv.get.mapOutputTracker.getGeneration
  override def generation: Option[Long] = Some(gen)
}

// A completion event passed by the underlying task scheduler to the DAG scheduler
case class CompletionEvent(task: DAGTask[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any])

// Various possible reasons why a DAG task ended. The underlying scheduler is supposed
// to retry tasks several times for "ephemeral" failures, and only report back failures
// that require some old stages to be resubmitted, such as shuffle map fetch failures.
sealed trait TaskEndReason
case object Success extends TaskEndReason
case class FetchFailed(serverUri: String, shuffleId: Int, mapId: Int, reduceId: Int) extends TaskEndReason
case class OtherFailure(message: String) extends TaskEndReason

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes
 * a DAG of stages for each job, keeps track of which RDDs and stage outputs
 * are materialized, and computes a minimal schedule to run the job. Subclasses
 * only need to implement the code to send a task to the cluster and to report
 * fetch failures (the submitTasks method, and code to add CompletionEvents).
 */
private trait DAGScheduler extends Scheduler with Logging {
  // Must be implemented by subclasses to start running a set of tasks
  def submitTasks(tasks: Seq[Task[_]]): Unit

  // Must be called by subclasses to report task completions or failures
  def taskEnded(task: Task[_], reason: TaskEndReason, result: Any, accumUpdates: Map[Long, Any]) {
    val dagTask = task.asInstanceOf[DAGTask[_]]
    completionEvents.put(CompletionEvent(dagTask, reason, result, accumUpdates))
  }

  // The time, in millis, to wait for fetch failure events to stop coming in after
  // one is detected; this is a simplistic way to avoid resubmitting tasks in the
  // non-fetchable map stage one by one as more failure events come in
  val RESUBMIT_TIMEOUT = 2000L

  // The time, in millis, to wake up between polls of the completion queue
  // in order to potentially resubmit failed stages
  val POLL_TIMEOUT = 500L


  private val completionEvents = new LinkedBlockingQueue[CompletionEvent]

  var nextStageId = 0

  def newStageId() = {
    var res = nextStageId
    nextStageId += 1
    res
  }

  val idToStage = new HashMap[Int, Stage]

  val shuffleToMapStage = new HashMap[Int, Stage]

  var cacheLocs = new HashMap[Int, Array[List[String]]]

  val cacheTracker = SparkEnv.get.cacheTracker
  val mapOutputTracker = SparkEnv.get.mapOutputTracker

  def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    cacheLocs(rdd.id)
  }
  
  def updateCacheLocs() {
    cacheLocs = cacheTracker.getLocationsSnapshot()
  }

  def getShuffleMapStage(shuf: ShuffleDependency[_,_,_]): Stage = {
    shuffleToMapStage.get(shuf.shuffleId) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(shuf.rdd, Some(shuf))
        shuffleToMapStage(shuf.shuffleId) = stage
        stage
    }
  }

  def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_,_,_]]): Stage = {
    // Kind of ugly: need to register RDDs with the cache here since
    // we can't do it in its constructor because # of splits is unknown
    cacheTracker.registerRDD(rdd.id, rdd.splits.size)
    val id = newStageId()
    val stage = new Stage(id, rdd, shuffleDep, getParentStages(rdd))
    idToStage(id) = stage
    stage
  }

  def getParentStages(rdd: RDD[_]): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of splits is unknown
        cacheTracker.registerRDD(r.id, r.splits.size)
        for (dep <- r.dependencies) {
          dep match {
            case shufDep: ShuffleDependency[_,_,_] =>
              parents += getShuffleMapStage(shufDep)
            case _ =>
              visit(dep.rdd)
          }
        }
      }
    }
    visit(rdd)
    parents.toList
  }

  def getMissingParentStages(stage: Stage): List[Stage] = {
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
                case shufDep: ShuffleDependency[_,_,_] =>
                  val stage = getShuffleMapStage(shufDep)
                  if (!stage.isAvailable)
                    missing += stage
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

  override def runJob[T, U](finalRdd: RDD[T], func: (TaskContext, Iterator[T]) => U,
                            partitions: Seq[Int], allowLocal: Boolean)
                           (implicit m: ClassManifest[U])
      : Array[U] = {
    val outputParts = partitions.toArray
    val numOutputParts: Int = partitions.size
    val finalStage = newStage(finalRdd, None)
    val results = new Array[U](numOutputParts)
    val finished = new Array[Boolean](numOutputParts)
    var numFinished = 0

    val waiting = new HashSet[Stage] // stages we need to run whose parents aren't done
    val running = new HashSet[Stage] // stages we are running right now
    val failed = new HashSet[Stage]  // stages that must be resubmitted due to fetch failures
    val pendingTasks = new HashMap[Stage, HashSet[Task[_]]] // missing tasks from each stage
    var lastFetchFailureTime: Long = 0  // used to wait a bit to avoid repeated resubmits

    updateCacheLocs()
    
    logInfo("Final stage: " + finalStage)
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    // Optimization for short actions like first() and take() that can be computed locally
    // without shipping tasks to the cluster.
    if (allowLocal && finalStage.parents.size == 0 && numOutputParts == 1) {
      logInfo("Computing the requested partition locally")
      val split = finalRdd.splits(outputParts(0))
      val taskContext = new TaskContext(finalStage.id, outputParts(0), 0)
      return Array(func(taskContext, finalRdd.iterator(split)))
    }

    def submitStage(stage: Stage) {
      if (!waiting(stage) && !running(stage)) {
        val missing = getMissingParentStages(stage)
        if (missing == Nil) {
          logInfo("Submitting " + stage + ", which has no missing parents")
          submitMissingTasks(stage)
          running += stage
        } else {
          for (parent <- missing)
            submitStage(parent)
          waiting += stage
        }
      }
    }

    def submitMissingTasks(stage: Stage) {
      // Get our pending tasks and remember them in our pendingTasks entry
      val myPending = pendingTasks.getOrElseUpdate(stage, new HashSet)
      var tasks = ArrayBuffer[Task[_]]()
      if (stage == finalStage) {
        for (id <- 0 until numOutputParts if (!finished(id))) {
          val part = outputParts(id)
          val locs = getPreferredLocs(finalRdd, part)
          tasks += new ResultTask(finalStage.id, finalRdd, func, part, locs, id)
        }
      } else {
        for (p <- 0 until stage.numPartitions if stage.outputLocs(p) == Nil) {
          val locs = getPreferredLocs(stage.rdd, p)
          tasks += new ShuffleMapTask(stage.id, stage.rdd, stage.shuffleDep.get, p, locs)
        }
      }
      myPending ++= tasks
      submitTasks(tasks)
    }

    submitStage(finalStage)

    while (numFinished != numOutputParts) {
      val evt = completionEvents.poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS)
      val time = System.currentTimeMillis // TODO: use a pluggable clock for testability

      // If we got an event off the queue, mark the task done or react to a fetch failure
      if (evt != null) {
        val stage = idToStage(evt.task.stageId)
        pendingTasks(stage) -= evt.task
        if (evt.reason == Success) {
          // A task ended
          logInfo("Completed " + evt.task)
          Accumulators.add(evt.accumUpdates)
          evt.task match {
            case rt: ResultTask[_, _] =>
              results(rt.outputId) = evt.result.asInstanceOf[U]
              finished(rt.outputId) = true
              numFinished += 1
            case smt: ShuffleMapTask =>
              val stage = idToStage(smt.stageId)
              stage.addOutputLoc(smt.partition, evt.result.asInstanceOf[String])
              if (pendingTasks(stage).isEmpty) {
                logInfo(stage + " finished; looking for newly runnable stages")
                running -= stage
                if (stage.shuffleDep != None) {
                  mapOutputTracker.registerMapOutputs(
                    stage.shuffleDep.get.shuffleId,
                    stage.outputLocs.map(_.head).toArray)
                }
                updateCacheLocs()
                val newlyRunnable = new ArrayBuffer[Stage]
                for (stage <- waiting if getMissingParentStages(stage) == Nil) {
                  newlyRunnable += stage
                }
                waiting --= newlyRunnable
                running ++= newlyRunnable
                for (stage <- newlyRunnable) {
                  submitMissingTasks(stage)
                }
              }
          }
        } else {
          evt.reason match {
            case FetchFailed(serverUri, shuffleId, mapId, reduceId) =>
              // Mark the stage that the reducer was in as unrunnable
              val failedStage = idToStage(evt.task.stageId)
              running -= failedStage
              failed += failedStage
              // TODO: Cancel running tasks in the stage
              logInfo("Marking " + failedStage + " for resubmision due to a fetch failure")
              // Mark the map whose fetch failed as broken in the map stage
              val mapStage = shuffleToMapStage(shuffleId)
              mapStage.removeOutputLoc(mapId, serverUri)
              mapOutputTracker.unregisterMapOutput(shuffleId, mapId, serverUri)
              logInfo("The failed fetch was from " + mapStage + "; marking it for resubmission")
              failed += mapStage
              // Remember that a fetch failed now; this is used to resubmit the broken
              // stages later, after a small wait (to give other tasks the chance to fail)
              lastFetchFailureTime = time
              // TODO: If there are a lot of fetch failures on the same node, maybe mark all
              // outputs on the node as dead.
            case _ =>
              // Non-fetch failure -- probably a bug in the job, so bail out
              throw new SparkException("Task failed: " + evt.task + ", reason: " + evt.reason)
              // TODO: Cancel all tasks that are still running
          }
        }
      } // end if (evt != null)

      // If fetches have failed recently and we've waited for the right timeout,
      // resubmit all the failed stages
      if (failed.size > 0 && time > lastFetchFailureTime + RESUBMIT_TIMEOUT) {
        logInfo("Resubmitting failed stages")
        updateCacheLocs()
        for (stage <- failed) {
          submitStage(stage)
        }
        failed.clear()
      }
    }

    return results
  }

  def getPreferredLocs(rdd: RDD[_], partition: Int): List[String] = {
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
            return locs;
        }
      case _ =>
    })
    return Nil
  }
}
