package spark

import org.apache.hadoop.fs.Path
import rdd.CoalescedRDD
import scheduler.{ResultTask, ShuffleMapTask}

/**
 * Enumeration to manage state transitions of an RDD through checkpointing
 * [ Initialized --> marked for checkpointing --> checkpointing in progress --> checkpointed ]
 */
private[spark] object CheckpointState extends Enumeration {
  type CheckpointState = Value
  val Initialized, MarkedForCheckpoint, CheckpointingInProgress, Checkpointed = Value
}

/**
 * This class contains all the information of the regarding RDD checkpointing.
 */
private[spark] class RDDCheckpointData[T: ClassManifest](rdd: RDD[T])
extends Logging with Serializable {

  import CheckpointState._

  var cpState = Initialized
  @transient var cpFile: Option[String] = None
  @transient var cpRDD: Option[RDD[T]] = None
  @transient var cpRDDSplits: Seq[Split] = Nil

  // Mark the RDD for checkpointing
  def markForCheckpoint() {
    RDDCheckpointData.synchronized {
      if (cpState == Initialized) cpState = MarkedForCheckpoint
    }
  }

  // Is the RDD already checkpointed
  def isCheckpointed(): Boolean = {
    RDDCheckpointData.synchronized { cpState == Checkpointed }
  }

  // Get the file to which this RDD was checkpointed to as an Option
  def getCheckpointFile(): Option[String] = {
    RDDCheckpointData.synchronized { cpFile }
  }

  // Do the checkpointing of the RDD. Called after the first job using that RDD is over.
  def doCheckpoint() {
    // If it is marked for checkpointing AND checkpointing is not already in progress,
    // then set it to be in progress, else return
    RDDCheckpointData.synchronized {
      if (cpState == MarkedForCheckpoint) {
        cpState = CheckpointingInProgress
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
      cpState = Checkpointed
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
