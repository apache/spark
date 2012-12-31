package spark.streaming.dstream

import spark.RDD
import spark.rdd.UnionRDD
import spark.streaming.{StreamingContext, Time}

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import scala.collection.mutable.HashSet

private[streaming]
class FileInputDStream[K: ClassManifest, V: ClassManifest, F <: NewInputFormat[K,V] : ClassManifest](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: PathFilter = FileInputDStream.defaultPathFilter,
    newFilesOnly: Boolean = true) 
  extends InputDStream[(K, V)](ssc_) {

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  var lastModTime = 0L
  val lastModTimeFiles = new HashSet[String]()

  def path(): Path = {
    if (path_ == null) path_ = new Path(directory)
    path_
  }

  def fs(): FileSystem = {
    if (fs_ == null) fs_ = path.getFileSystem(new Configuration())
    fs_
  }

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
        if (!filter.accept(path)) {
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

    val newFiles = fs.listStatus(path, newFilter)
    logInfo("New files: " + newFiles.map(_.getPath).mkString(", "))
    if (newFiles.length > 0) {
      // Update the modification time and the files processed for that modification time
      if (lastModTime != newFilter.latestModTime) {
        lastModTime = newFilter.latestModTime
        lastModTimeFiles.clear()
      }
      lastModTimeFiles ++= newFilter.latestModTimeFiles
    }
    val newRDD = new UnionRDD(ssc.sc, newFiles.map(
      file => ssc.sc.newAPIHadoopFile[K, V, F](file.getPath.toString)))
    Some(newRDD)
  }
}

object FileInputDStream {
  val defaultPathFilter = new PathFilter with Serializable {
    def accept(path: Path): Boolean = {
      val file = path.getName()
      if (file.startsWith(".") || file.endsWith("_tmp")) {
        return false
      } else {
        return true
      }
    }
  }
}

