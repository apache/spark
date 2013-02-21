package spark.scheduler

import java.net.URI

import spark._
import spark.storage.BlockManagerId

/**
 * A stage is a set of independent tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * another stage, or a result stage, in which case its tasks directly compute the action that
 * initiated a job (e.g. count(), save(), etc). For shuffle map stages, we also track the nodes
 * that each output partition is on.
 *
 * Each Stage also has a priority, which is (by default) based on the job it was submitted in.
 * This allows Stages from earlier jobs to be computed first or recovered faster on failure.
 */
private[spark] class Stage(
    val id: Int,
    val rdd: RDD[_],
    val shuffleDep: Option[ShuffleDependency[_,_]],  // Output shuffle if stage is a map stage
    val parents: List[Stage],
    val priority: Int)
  extends Logging {
  
  val isShuffleMap = shuffleDep != None
  val numPartitions = rdd.partitions.size
  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
  var numAvailableOutputs = 0

  /** When first task was submitted to scheduler. */
  var submissionTime: Option[Long] = None

  private var nextAttemptId = 0

  def isAvailable: Boolean = {
    if (!isShuffleMap) {
      true
    } else {
      numAvailableOutputs == numPartitions
    }
  }

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil)
      numAvailableOutputs += 1
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }
 
  def removeOutputsOnExecutor(execId: String) {
    var becameUnavailable = false
    for (partition <- 0 until numPartitions) {
      val prevList = outputLocs(partition)
      val newList = prevList.filterNot(_.location.executorId == execId)
      outputLocs(partition) = newList
      if (prevList != Nil && newList == Nil) {
        becameUnavailable = true
        numAvailableOutputs -= 1
      }
    }
    if (becameUnavailable) {
      logInfo("%s is now unavailable on executor %s (%d/%d, %s)".format(
        this, execId, numAvailableOutputs, numPartitions, isAvailable))
    }
  }

  def newAttemptId(): Int = {
    val id = nextAttemptId
    nextAttemptId += 1
    return id
  }

  def origin: String = rdd.origin

  override def toString = "Stage " + id

  override def hashCode(): Int = id
}
