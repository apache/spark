package spark

import java.util.concurrent._

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.mutable.Map

/**
 * A Scheduler subclass that implements stage-oriented scheduling. It computes
 * a DAG of stages for each job, keeps track of which RDDs and stage outputs
 * are materialized, and computes a minimal schedule to run the job. Subclasses
 * only need to implement the code to send a task to the cluster and to report
 * failures from it (the submitTasks method, and code to add completionEvents).
 */
private abstract class DAGScheduler extends Scheduler with Logging {
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

  val cacheLocs = new HashMap[RDD[_], Array[List[String]]]

  def getCacheLocs(rdd: RDD[_]): Array[List[String]] = {
    cacheLocs.getOrElseUpdate(rdd, Array.fill[List[String]](rdd.splits.size)(Nil))
  }

  def addCacheLoc(rdd: RDD[_], partition: Int, host: String) {
    val locs = getCacheLocs(rdd)
    locs(partition) = host :: locs(partition)
  }

  def removeCacheLoc(rdd: RDD[_], partition: Int, host: String) {
    val locs = getCacheLocs(rdd)
    locs(partition) -= host
  }

  def getShuffleMapStage(shuf: ShuffleDependency[_,_,_]): Stage = {
    shuffleToMapStage.get(shuf) match {
      case Some(stage) => stage
      case None =>
        val stage = newStage(
          true, shuf.rdd, shuf.spec.partitioner.numPartitions)
        shuffleToMapStage(shuf) = stage
        stage
    }
  }

  def newStage(isShuffleMap: Boolean, rdd: RDD[_], numPartitions: Int): Stage = {
    val id = newStageId()
    val parents = getParentStages(rdd)
    val stage = new Stage(id, isShuffleMap, rdd, parents, numPartitions)
    idToStage(id) = stage
    stage
  }

  def getParentStages(rdd: RDD[_]): List[Stage] = {
    val parents = new HashSet[Stage]
    val visited = new HashSet[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) {
        visited += r
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

  override def runJob[T, U](rdd: RDD[T], func: Iterator[T] => U)(implicit m: ClassManifest[U])
      : Array[U] = {
    val numOutputParts: Int = rdd.splits.size
    val finalStage = newStage(false, rdd, numOutputParts)
    val results = new Array[U](numOutputParts)
    val finished = new Array[Boolean](numOutputParts)
    var numFinished = 0

    val waiting = new HashSet[Stage]
    val running = new HashSet[Stage]
    val pendingTasks = new HashMap[Stage, HashSet[Task[_]]]

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
      var tasks: List[Task[_]] = Nil
      if (stage == finalStage) {
        for (p <- 0 until numOutputParts if (!finished(p))) {
          val locs = getPreferredLocs(rdd, p)
          tasks = new ResultTask(rdd, func, p, locs) :: tasks
        }
      }
      submitTasks(tasks)
    }

    submitStage(finalStage)

    while (numFinished != numOutputParts) {
      val evt = completionEvents.take()
      if (evt.successful) {
        Accumulators.add(currentThread, evt.accumUpdates)
        evt.task match {
          case rt: ResultTask[_, _] =>
            results(rt.partition) = evt.result.asInstanceOf[U]
            finished(rt.partition) = true
            numFinished += 1
          // case smt: ShuffleMapTask
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
    })
    return Nil
  }
}

case class CompletionEvent(task: Task[_], successful: Boolean, result: Any, accumUpdates: Map[Long, Any])

class Stage(val id: Int, val isShuffleMap: Boolean, val rdd: RDD[_], val parents: List[Stage], val numPartitions: Int) {
  val outputLocs = Array.fill[List[String]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  def isAvailable: Boolean = {
    if (parents.size == 0 && !isShuffleMap)
      true
    else
      numAvailableOutputs == numPartitions
  }

  def addOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = host :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  def removeOutputLoc(partition: Int, host: String) {
    val prevList = outputLocs(partition)
    val newList = prevList - host
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil)
      numAvailableOutputs -= 1
  }

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}

class ResultTask[T, U](rdd: RDD[T], func: Iterator[T] => U, val partition: Int, locs: Seq[String])
extends Task[U] {
  val split = rdd.splits(partition)

  override def run: U = {
    func(rdd.iterator(split))
  }

  override def preferredLocations: Seq[String] = locs

  override def toString = "ResultTask " + partition
}