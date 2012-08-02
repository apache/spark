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
    ssc: SparkStreamContext,
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

/*
class NetworkInputDStream[T: ClassManifest](
    val networkInputName: String,
    val addresses: Array[InetSocketAddress],
    batchDuration: Time,
    ssc: SparkStreamContext) 
extends InputDStream[T](networkInputName, batchDuration, ssc) {

 
  // TODO(Haoyuan): This is for the performance test.
  @transient var rdd: RDD[T] = null

  if (System.getProperty("spark.fake", "false") == "true") {
    logInfo("Running initial count to cache fake RDD")
    rdd = ssc.sc.textFile(SparkContext.inputFile, 
        SparkContext.idealPartitions).asInstanceOf[RDD[T]]
    val fakeCacheLevel = System.getProperty("spark.fake.cache", "")
    if (fakeCacheLevel == "MEMORY_ONLY_2") {
      rdd.persist(StorageLevel.MEMORY_ONLY_2)
    } else if (fakeCacheLevel == "MEMORY_ONLY_DESER_2") {
      rdd.persist(StorageLevel.MEMORY_ONLY_2)
    } else if (fakeCacheLevel != "") {
      logError("Invalid fake cache level: " + fakeCacheLevel)
      System.exit(1)
    }
    rdd.count()
  }
  
  @transient val references = new HashMap[Time,String]
 
  override def compute(validTime: Time): Option[RDD[T]] = {
    if (System.getProperty("spark.fake", "false") == "true") {
      logInfo("Returning fake RDD at " + validTime)
      return Some(rdd)
    } 
    references.get(validTime) match {
      case Some(reference) => 
        if (reference.startsWith("file") || reference.startsWith("hdfs")) {
          logInfo("Reading from file " + reference  + " for time " + validTime)
          Some(ssc.sc.textFile(reference).asInstanceOf[RDD[T]])
        } else {
          logInfo("Getting from BlockManager " + reference + " for time " + validTime)
          Some(new BlockRDD(ssc.sc, Array(reference)))
        }
      case None =>
        throw new Exception(this.toString + ": Reference missing for time " + validTime + "!!!")
        None
    }
  }

  def setReference(time: Time, reference: AnyRef) {
    references += ((time, reference.toString))
    logInfo("Reference added for time " + time + " - " + reference.toString)
  }
}


class TestInputDStream(
    val testInputName: String,
    batchDuration: Time,
    ssc: SparkStreamContext) 
extends InputDStream[String](testInputName, batchDuration, ssc) {
  
  @transient val references = new HashMap[Time,Array[String]]
 
  override def compute(validTime: Time): Option[RDD[String]] = {
    references.get(validTime) match {
      case Some(reference) =>
        Some(new BlockRDD[String](ssc.sc, reference))
      case None =>
        throw new Exception(this.toString + ": Reference missing for time " + validTime + "!!!")
        None
    }
  }

  def setReference(time: Time, reference: AnyRef) {
    references += ((time, reference.asInstanceOf[Array[String]]))
  }
}
*/
