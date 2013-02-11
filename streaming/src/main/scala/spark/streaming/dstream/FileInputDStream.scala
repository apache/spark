package spark.streaming.dstream

import spark.RDD
import spark.rdd.UnionRDD
import spark.streaming.{DStreamCheckpointData, StreamingContext, Time}

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import scala.collection.mutable.{HashSet, HashMap}
import java.io.{ObjectInputStream, IOException}

private[streaming]
class FileInputDStream[K: ClassManifest, V: ClassManifest, F <: NewInputFormat[K,V] : ClassManifest](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: Path => Boolean = FileInputDStream.defaultFilter,
    newFilesOnly: Boolean = true) 
  extends InputDStream[(K, V)](ssc_) {

  protected[streaming] override val checkpointData = new FileInputDStreamCheckpointData

  private val lastModTimeFiles = new HashSet[String]()
  private var lastModTime = 0L

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null
  @transient private var files = new HashMap[Time, Array[String]]

  override def start() {
    if (newFilesOnly) {
      lastModTime = System.currentTimeMillis()
    } else {
      lastModTime = 0
    }
  }
  
  override def stop() { }

  /**
   * Finds the files that were modified since the last time this method was called and makes
   * a union RDD out of them. Note that this maintains the list of files that were processed
   * in the latest modification time in the previous call to this method. This is because the
   * modification time returned by the FileStatus API seems to return times only at the
   * granularity of seconds. Hence, new files may have the same modification time as the
   * latest modification time in the previous call to this method and the list of files
   * maintained is used to filter the one that have been processed.
   */
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    // Create the filter for selecting new files
    val newFilter = new PathFilter() {
      var latestModTime = 0L
      val latestModTimeFiles = new HashSet[String]()

      def accept(path: Path): Boolean = {
        if (!filter(path)) {
          return false
        } else {
          val modTime = fs.getFileStatus(path).getModificationTime()
          if (modTime < lastModTime){
            return false
          } else if (modTime == lastModTime && lastModTimeFiles.contains(path.toString)) {
            return false
          }
          if (modTime > latestModTime) {
            latestModTime = modTime
            latestModTimeFiles.clear()
          }
          latestModTimeFiles += path.toString
          return true
        }        
      }
    }

    val newFiles = fs.listStatus(path, newFilter).map(_.getPath.toString)
    logInfo("New files: " + newFiles.mkString(", "))
    if (newFiles.length > 0) {
      // Update the modification time and the files processed for that modification time
      if (lastModTime != newFilter.latestModTime) {
        lastModTime = newFilter.latestModTime
        lastModTimeFiles.clear()
      }
      lastModTimeFiles ++= newFilter.latestModTimeFiles
    }
    files += ((validTime, newFiles))
    Some(filesToRDD(newFiles))
  }

  /** Forget the old time-to-files mappings along with old RDDs */
  protected[streaming] override def forgetOldMetadata(time: Time) {
    super.forgetOldMetadata(time)
    val filesToBeRemoved = files.filter(_._1 <= (time - rememberDuration))
    files --= filesToBeRemoved.keys
    logInfo("Forgot " + filesToBeRemoved.size + " files from " + this)
  }

  /** Generate one RDD from an array of files */
  protected[streaming] def filesToRDD(files: Seq[String]): RDD[(K, V)] = {
    new UnionRDD(
      context.sparkContext,
      files.map(file => context.sparkContext.newAPIHadoopFile[K, V, F](file))
    )
  }

  private def path: Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  private def fs: FileSystem = {
    if (fs_ == null) fs_ = path.getFileSystem(new Configuration())
    fs_
  }

  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream) {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new HashMap[Time, RDD[(K,V)]] ()
    files = new HashMap[Time, Array[String]]
  }

  /**
   * A custom version of the DStreamCheckpointData that stores names of
   * Hadoop files as checkpoint data.
   */
  private[streaming]
  class FileInputDStreamCheckpointData extends DStreamCheckpointData(this) {

    def hadoopFiles = data.asInstanceOf[HashMap[Time, Array[String]]]

    override def update() {
      hadoopFiles.clear()
      hadoopFiles ++= files
    }

    override def cleanup() { }

    override def restore() {
      hadoopFiles.foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]") )
          files += ((t, f))
          generatedRDDs += ((t, filesToRDD(f)))
        }
      }
    }
  }
}

private[streaming]
object FileInputDStream {
  def defaultFilter(path: Path): Boolean = !path.getName().startsWith(".")
}


