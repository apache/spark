package spark

import org.apache.hadoop.fs.Path
import rdd.CoalescedRDD
import scheduler.{ResultTask, ShuffleMapTask}

/**
 * This class contains all the information of the regarding RDD checkpointing.
 */

private[spark] class RDDCheckpointData[T: ClassManifest](rdd: RDD[T])
extends Logging with Serializable {

  /**
   * This class manages the state transition of an RDD through checkpointing
   * [ Not checkpointed --> marked for checkpointing --> checkpointing in progress --> checkpointed ]
   */
  class CheckpointState extends Serializable {
    var state = 0

    def mark()    { if (state == 0) state = 1 }
    def start()   { assert(state == 1); state = 2 }
    def finish()  { assert(state == 2); state = 3 }

    def isMarked() =     { state == 1 }
    def isInProgress =   { state == 2 }
    def isCheckpointed = { state == 3 }
  }

  val cpState = new CheckpointState()
  @transient var cpFile: Option[String] = None
  @transient var cpRDD: Option[RDD[T]] = None
  @transient var cpRDDSplits: Seq[Split] = Nil

  // Mark the RDD for checkpointing
  def markForCheckpoint() = {
    RDDCheckpointData.synchronized { cpState.mark() }
  }

  // Is the RDD already checkpointed
  def isCheckpointed() = {
    RDDCheckpointData.synchronized { cpState.isCheckpointed }
  }

  // Get the file to which this RDD was checkpointed to as a Option
  def getCheckpointFile() = {
    RDDCheckpointData.synchronized { cpFile }
  }

  // Do the checkpointing of the RDD. Called after the first job using that RDD is over.
  def doCheckpoint() {
    // If it is marked for checkpointing AND checkpointing is not already in progress,
    // then set it to be in progress, else return
    RDDCheckpointData.synchronized {
      if (cpState.isMarked && !cpState.isInProgress) {
        cpState.start()
      } else {
        return
      }
    }

    // Save to file, and reload it as an RDD
    val file = new Path(rdd.context.checkpointDir, "rdd-" + rdd.id).toString
    rdd.saveAsObjectFile(file)

    val newRDD = {
      val hadoopRDD = rdd.context.objectFile[T](file, rdd.splits.size)

      val oldSplits = rdd.splits.size
      val newSplits = hadoopRDD.splits.size

      logDebug("RDD splits = " + oldSplits + " --> " + newSplits)
      if (newSplits < oldSplits) {
        throw new Exception("# splits after checkpointing is less than before " +
          "[" + oldSplits + " --> " + newSplits)
      } else if (newSplits > oldSplits) {
        new CoalescedRDD(hadoopRDD, rdd.splits.size)
      } else {
        hadoopRDD
      }
    }
    logDebug("New RDD has " + newRDD.splits.size + " splits")

    // Change the dependencies and splits of the RDD
    RDDCheckpointData.synchronized {
      cpFile = Some(file)
      cpRDD = Some(newRDD)
      cpRDDSplits = newRDD.splits
      rdd.changeDependencies(newRDD)
      cpState.finish()
      RDDCheckpointData.checkpointCompleted()
      logInfo("Done checkpointing RDD " + rdd.id + ", new parent is RDD " + newRDD.id)
    }
  }

  // Get preferred location of a split after checkpointing
  def preferredLocations(split: Split) = {
    RDDCheckpointData.synchronized {
      cpRDD.get.preferredLocations(split)
    }
  }

  // Get iterator. This is called at the worker nodes.
  def iterator(split: Split): Iterator[T] = {
    rdd.firstParent[T].iterator(split)
  }
}

private[spark] object RDDCheckpointData {
  def checkpointCompleted() {
    ShuffleMapTask.clearCache()
    ResultTask.clearCache()
  }
}
