package spark

import org.apache.hadoop.fs.Path



private[spark] class RDDCheckpointData[T: ClassManifest](rdd: RDD[T])
extends Serializable {

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
  var cpFile: Option[String] = None
  var cpRDD: Option[RDD[T]] = None
  var cpRDDSplits: Seq[Split] = Nil

  def markForCheckpoint() = {
    rdd.synchronized { cpState.mark() }
  }

  def isCheckpointed() = {
    rdd.synchronized { cpState.isCheckpointed }
  }

  def getCheckpointFile() = {
    rdd.synchronized { cpFile }
  }

  def doCheckpoint() {
    rdd.synchronized {
      if (cpState.isMarked && !cpState.isInProgress) {
        cpState.start()
      } else {
        return
      }
    }

    val file = new Path(rdd.context.checkpointDir, "rdd-" + rdd.id).toString
    rdd.saveAsObjectFile(file)
    val newRDD = rdd.context.objectFile[T](file, rdd.splits.size)

    rdd.synchronized {
      rdd.changeDependencies(newRDD)
      cpFile = Some(file)
      cpRDD = Some(newRDD)
      cpRDDSplits = newRDD.splits
      cpState.finish()
    }
  }

  def preferredLocations(split: Split) = {
    cpRDD.get.preferredLocations(split)
  }

  def iterator(splitIndex: Int): Iterator[T] = {
    cpRDD.get.iterator(cpRDDSplits(splitIndex))
  }
}
