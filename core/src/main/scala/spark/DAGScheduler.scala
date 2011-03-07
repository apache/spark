package spark

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes
 * a DAG of stages for each job, keeps track of which RDDs and stage outputs
 * are materialized, and computes a minimal schedule to run the job. Subclasses
 * only need to implement the code to send a task to the cluster and to report
 * failures from it (the submitTasks method, and code to add completionEvents).
 */
private trait DAGScheduler extends Scheduler with Logging {
  // Must be implemented by subclasses to start running a set of tasks
  def submitTasks(tasks: Seq[Task[_]]): Unit

  // Must be called by subclasses to report task completions or failures
  def taskEnded(task: Task[_], successful: Boolean, result: Any, accumUpdates: Map[Long, Any]) {
    completionEvents.put(CompletionEvent(task, successful, result, accumUpdates))
  }

  private val completionEvents = new LinkedBlockingQueue[CompletionEvent]

  var nextStageId = 0

  def newStageId() = {
    var res = nextStageId
    nextStageId += 1
    res
  }

  val idToStage = new HashMap[Int, Stage]

  val shuffleToMapStage = new HashMap[ShuffleDependency[_,_,_], Stage]

  var cacheLocs = new HashMap[Int, Array[List[String]]]

  def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    cacheLocs(rdd.id)
  }
  
  def updateCacheLocs() {
    cacheLocs = RDDCache.getLocationsSnapshot()
  }

  def getShuffleMapStage(shuf: ShuffleDependency[_,_,_]): Stage = {
    shuffleToMapStage.get(shuf) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(shuf.rdd, Some(shuf))
        shuffleToMapStage(shuf) = stage
        stage
    }
  }

  def newStage(rdd: RDD[_], shuffleDep: Option[ShuffleDependency[_,_,_]]): Stage = {
    // Kind of ugly: need to register RDDs with the cache here since
    // we can't do it in its constructor because # of splits is unknown
    RDDCache.registerRDD(rdd.id, rdd.splits.size)
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
        RDDCache.registerRDD(r.id, r.splits.size)
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

  override def runJob[T, U](finalRdd: RDD[T], func: Iterator[T] => U)(implicit m: ClassManifest[U])
      : Array[U] = {
    val numOutputParts: Int = finalRdd.splits.size
    val finalStage = newStage(finalRdd, None)
    val results = new Array[U](numOutputParts)
    val finished = new Array[Boolean](numOutputParts)
    var numFinished = 0

    val waiting = new HashSet[Stage]
    val running = new HashSet[Stage]
    val pendingTasks = new HashMap[Stage, HashSet[Task[_]]]

    updateCacheLocs()
    
    logInfo("Final stage: " + finalStage)
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

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
        for (p <- 0 until numOutputParts if (!finished(p))) {
          val locs = getPreferredLocs(finalRdd, p)
          tasks += new ResultTask(finalStage.id, finalRdd, func, p, locs)
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
      val evt = completionEvents.take()
      if (evt.successful) {
        logInfo("Completed " + evt.task)
        Accumulators.add(currentThread, evt.accumUpdates)
        evt.task match {
          case rt: ResultTask[_, _] =>
            results(rt.partition) = evt.result.asInstanceOf[U]
            finished(rt.partition) = true
            numFinished += 1
            pendingTasks(finalStage) -= rt
          case smt: ShuffleMapTask =>
            val stage = idToStage(smt.stageId)
            stage.addOutputLoc(smt.partition, evt.result.asInstanceOf[String])
            val pending = pendingTasks(stage)
            pending -= smt
            if (pending.isEmpty) {
              logInfo(stage + " finished; looking for newly runnable stages")
              running -= stage
              if (stage.shuffleDep != None) {
                MapOutputTracker.registerMapOutputs(
                  stage.shuffleDep.get.shuffleId,
                  stage.outputLocs.map(_.first).toArray)
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
        throw new SparkException("Task failed: " + evt.task)
        // TODO: Kill the running job
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

case class CompletionEvent(task: Task[_], successful: Boolean, result: Any, accumUpdates: Map[Long, Any])
