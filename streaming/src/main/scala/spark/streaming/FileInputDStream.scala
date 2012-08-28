package spark.streaming

import spark.SparkContext
import spark.RDD
import spark.BlockRDD
import spark.UnionRDD
import spark.storage.StorageLevel
import spark.streaming._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import java.net.InetSocketAddress

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}


class FileInputDStream[K: ClassManifest, V: ClassManifest, F <: NewInputFormat[K,V] : ClassManifest](
    ssc: StreamingContext,
    directory: Path,
    filter: PathFilter = FileInputDStream.defaultPathFilter,
    newFilesOnly: Boolean = true) 
  extends InputDStream[(K, V)](ssc) {
  
  val fs = directory.getFileSystem(new Configuration()) 
  var lastModTime: Long = 0
  
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
    
    val newFiles = fs.listStatus(directory, newFilter)
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
  val defaultPathFilter = new PathFilter {
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

