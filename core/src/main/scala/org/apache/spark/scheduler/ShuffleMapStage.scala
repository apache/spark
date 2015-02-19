package org.apache.spark.scheduler

import org.apache.spark.rdd.RDD
import org.apache.spark.ShuffleDependency
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.CallSite

/**
 * Define a class that represents the ShuffleMapStage to help clean up the DAGScheduler class 
 */
private[spark] class ShuffleMapStage(override val id: Int,
                                     override val rdd: RDD[_],
                                     override val numTasks: Int,
                                     override val parents: List[Stage],
                                     override val jobId: Int,
                                     override val callSite: CallSite,
                                     val shuffleDep: ShuffleDependency[_, _, _])
  extends Stage(id, rdd, numTasks, parents, jobId, callSite) {

  override def toString = "ShuffleMapStage " + id

  val outputLocs = Array.fill[List[MapStatus]](numPartitions)(Nil)
 

  def addOutputLoc(partition: Int, status: MapStatus) {
    val prevList = outputLocs(partition)
    outputLocs(partition) = status :: prevList
    if (prevList == Nil) {
      numAvailableOutputs += 1
    }
  }

  def removeOutputLoc(partition: Int, bmAddress: BlockManagerId) {
    val prevList = outputLocs(partition)
    val newList = prevList.filterNot(_.location == bmAddress)
    outputLocs(partition) = newList
    if (prevList != Nil && newList == Nil) {
      numAvailableOutputs -= 1
    }
  }

  /**
   * Removes all shuffle outputs associated with this executor. Note that this will also remove
   * outputs which are served by an external shuffle server (if one exists), as they are still
   * registered with this execId.
   */
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
}

