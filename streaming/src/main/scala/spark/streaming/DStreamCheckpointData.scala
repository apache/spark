package spark.streaming

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration
import collection.mutable.HashMap
import spark.Logging



private[streaming]
class DStreamCheckpointData[T: ClassManifest] (dstream: DStream[T])
  extends Serializable with Logging {
  private[streaming] val checkpointFiles = new HashMap[Time, String]()
  @transient private lazy val fileSystem =
    new Path(dstream.context.checkpointDir).getFileSystem(new Configuration())
  @transient private var lastCheckpointFiles: HashMap[Time, String] = null

  /**
   * Update the checkpoint data of the DStream. Default implementation records the checkpoint files to
   * which the generate RDDs of the DStream has been saved.
   */
  def update() {

    // Get the checkpointed RDDs from the generated RDDs
    val newCheckpointFiles = dstream.generatedRDDs.filter(_._2.getCheckpointFile.isDefined)
                                       .map(x => (x._1, x._2.getCheckpointFile.get))

    // Make a copy of the existing checkpoint data (checkpointed RDDs)
    lastCheckpointFiles = checkpointFiles.clone()

    // If the new checkpoint data has checkpoints then replace existing with the new one
    if (newCheckpointFiles.size > 0) {
      checkpointFiles.clear()
      checkpointFiles ++= newCheckpointFiles
    }

    // TODO: remove this, this is just for debugging
    newCheckpointFiles.foreach {
      case (time, data) => { logInfo("Added checkpointed RDD for time " + time + " to stream checkpoint") }
    }
  }

  /**
   * Cleanup old checkpoint data. Default implementation, cleans up old checkpoint files.
   */
  def cleanup() {
    // If there is at least on checkpoint file in the current checkpoint files,
    // then delete the old checkpoint files.
    if (checkpointFiles.size > 0 && lastCheckpointFiles != null) {
      (lastCheckpointFiles -- checkpointFiles.keySet).foreach {
        case (time, file) => {
          try {
            val path = new Path(file)
            fileSystem.delete(path, true)
            logInfo("Deleted checkpoint file '" + file + "' for time " + time)
          } catch {
            case e: Exception =>
              logWarning("Error deleting old checkpoint file '" + file + "' for time " + time, e)
          }
        }
      }
    }
  }

  /**
   * Restore the checkpoint data. Default implementation restores the RDDs from their
   * checkpoint files.
   */
  def restore() {
    // Create RDDs from the checkpoint data
    checkpointFiles.foreach {
      case(time, file) => {
        logInfo("Restoring checkpointed RDD for time " + time + " from file '" + file + "'")
        dstream.generatedRDDs += ((time, dstream.context.sc.checkpointFile[T](file)))
      }
    }
  }

  override def toString() = {
    "[\n" + checkpointFiles.size + "\n" + checkpointFiles.mkString("\n") + "\n]"
  }
}

