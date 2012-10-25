package spark.streaming

import spark.RDD
import spark.UnionRDD

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import java.io.{ObjectInputStream, IOException}


class FileInputDStream[K: ClassManifest, V: ClassManifest, F <: NewInputFormat[K,V] : ClassManifest](
    @transient ssc_ : StreamingContext,
    directory: String,
    filter: PathFilter = FileInputDStream.defaultPathFilter,
    newFilesOnly: Boolean = true) 
  extends InputDStream[(K, V)](ssc_) {

  @transient private var path_ : Path = null
  @transient private var fs_ : FileSystem = null

  var lastModTime: Long = 0

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
  
  override def compute(validTime: Time): Option[RDD[(K, V)]] = {
    val newFilter = new PathFilter() {
      var latestModTime = 0L
      
      def accept(path: Path): Boolean = {
        if (!filter.accept(path)) {
          return false
        } else {
          val modTime = fs.getFileStatus(path).getModificationTime()
          if (modTime <= lastModTime) {
            return false
          }
          if (modTime > latestModTime) {
            latestModTime = modTime
          }
          return true
        }        
      }
    }

    val newFiles = fs.listStatus(path, newFilter)
    logInfo("New files: " + newFiles.map(_.getPath).mkString(", "))
    if (newFiles.length > 0) {
      lastModTime = newFilter.latestModTime
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

