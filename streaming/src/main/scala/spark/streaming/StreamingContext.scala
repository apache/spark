package spark.streaming

import spark.RDD
import spark.Logging
import spark.SparkEnv
import spark.SparkContext
import spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Queue

import java.io.InputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

class StreamingContext (@transient val sc: SparkContext) extends Logging {

  def this(master: String, frameworkName: String, sparkHome: String = null, jars: Seq[String] = Nil) =
    this(new SparkContext(master, frameworkName, sparkHome, jars))

  initLogging()

  val env = SparkEnv.get
  
  val inputStreams = new ArrayBuffer[InputDStream[_]]()
  val outputStreams = new ArrayBuffer[DStream[_]]()
  val nextNetworkInputStreamId = new AtomicInteger(0)
  
  var batchDuration: Time = null 
  var scheduler: Scheduler = null
  var networkInputTracker: NetworkInputTracker = null
  var receiverJobThread: Thread = null 
  
  def setBatchDuration(duration: Long) {
    setBatchDuration(Time(duration))
  }
  
  def setBatchDuration(duration: Time) {
    batchDuration = duration
  }
  
  private[streaming] def getNewNetworkStreamId() = nextNetworkInputStreamId.getAndIncrement()
  
  def createNetworkTextStream(hostname: String, port: Int): DStream[String] = {
    createNetworkObjectStream[String](hostname, port, ObjectInputReceiver.bytesToLines)
  }
  
  def createNetworkObjectStream[T: ClassManifest](
      hostname: String, 
      port: Int, 
      converter: (InputStream) => Iterator[T]
    ): DStream[T] = {
    val inputStream = new ObjectInputDStream[T](this, hostname, port, converter)
    inputStreams += inputStream
    inputStream
  }
  
  def createRawNetworkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_2
    ): DStream[T] = {
    val inputStream = new RawInputDStream[T](this, hostname, port, storageLevel)
    inputStreams += inputStream
    inputStream
  }
 
  /*
  def createHttpTextStream(url: String): DStream[String] = {
    createHttpStream(url, ObjectInputReceiver.bytesToLines)
  }
  
  def createHttpStream[T: ClassManifest](
      url: String, 
      converter: (InputStream) => Iterator[T]
    ): DStream[T] = {
  }
  */

  /** 
   * This function creates a input stream that monitors a Hadoop-compatible
   * for new files and executes the necessary processing on them.
   */  
  def createFileStream[
    K: ClassManifest, 
    V: ClassManifest, 
    F <: NewInputFormat[K, V]: ClassManifest
  ](directory: String): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, new Path(directory))
    inputStreams += inputStream
    inputStream
  }

  def createTextFileStream(directory: String): DStream[String] = {
    createFileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }
  
  /**
   * This function create a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue 
   */
  def createQueueStream[T: ClassManifest](
      queue: Queue[RDD[T]],      
      oneAtATime: Boolean = true,
      defaultRDD: RDD[T] = null
    ): DStream[T] = {
    val inputStream = new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
    inputStreams += inputStream
    inputStream
  }
  
  def createQueueStream[T: ClassManifest](iterator: Iterator[RDD[T]]): DStream[T] = {
    val queue = new Queue[RDD[T]]
    val inputStream = createQueueStream(queue, true, null)
    queue ++= iterator
    inputStream
  } 

  
  /**
   * This function registers a DStream as an output stream that will be
   * computed every interval.
   */  
  def registerOutputStream (outputStream: DStream[_]) {
    outputStreams += outputStream
  }
  
  /**
   * This function verify whether the stream computation is eligible to be executed.
   */
  private def verify() {
    if (batchDuration == null) {
      throw new Exception("Batch duration has not been set")
    }
    if (batchDuration < Milliseconds(100)) {
      logWarning("Batch duration of " + batchDuration + " is very low")
    }
    if (inputStreams.size == 0) {
      throw new Exception("No input streams created, so nothing to take input from")
    }
    if (outputStreams.size == 0) {
      throw new Exception("No output streams registered, so nothing to execute")
    }
    
  }
  
  /**
   * This function starts the execution of the streams. 
   */  
  def start() {
    verify()
    val networkInputStreams = inputStreams.filter(s => s match {
        case n: NetworkInputDStream[_] => true 
        case _ => false
      }).map(_.asInstanceOf[NetworkInputDStream[_]]).toArray
     
    if (networkInputStreams.length > 0) {
      // Start the network input tracker (must start before receivers)
      networkInputTracker = new NetworkInputTracker(this, networkInputStreams)
      networkInputTracker.start()
    }

    Thread.sleep(1000)
    // Start the scheduler 
    scheduler = new Scheduler(this, inputStreams.toArray, outputStreams.toArray)
    scheduler.start()    
  }
  
  /**
   * This function starts the execution of the streams. 
   */
  def stop() {
    try {
      if (scheduler != null) scheduler.stop()
      if (networkInputTracker != null) networkInputTracker.stop()
      if (receiverJobThread != null) receiverJobThread.interrupt()
      sc.stop() 
    } catch {
      case e: Exception => logWarning("Error while stopping", e)
    }
    
    logInfo("StreamingContext stopped")
  }
}


object StreamingContext {
  implicit def toPairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)]) = {
    new PairDStreamFunctions[K, V](stream)
  }
}

