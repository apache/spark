package spark.streaming

import spark.RDD
import spark.Logging
import spark.SparkEnv
import spark.SparkContext
import spark.storage.StorageLevel

import scala.collection.mutable.Queue

import java.io.InputStream
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

class StreamingContext (
    sc_ : SparkContext,
    cp_ : Checkpoint
  ) extends Logging {

  def this(sparkContext: SparkContext) = this(sparkContext, null)

  def this(master: String, frameworkName: String, sparkHome: String = null, jars: Seq[String] = Nil) =
    this(new SparkContext(master, frameworkName, sparkHome, jars), null)

  def this(file: String) = this(null, Checkpoint.loadFromFile(file))

  def this(cp_ : Checkpoint) = this(null, cp_)

  initLogging()

  if (sc_ == null && cp_ == null) {
    throw new Exception("Streaming Context cannot be initilalized with " +
      "both SparkContext and checkpoint as null")
  }

  val isCheckpointPresent = (cp_ != null)

  val sc: SparkContext = {
    if (isCheckpointPresent) {
      new SparkContext(cp_.master, cp_.framework, cp_.sparkHome, cp_.jars)
    } else {
      sc_
    }
  }

  val env = SparkEnv.get

  val graph: DStreamGraph = {
    if (isCheckpointPresent) {

      cp_.graph.setContext(this)
      cp_.graph
    } else {
      new DStreamGraph()
    }
  }

  val nextNetworkInputStreamId = new AtomicInteger(0)
  var networkInputTracker: NetworkInputTracker = null

  private[streaming] var checkpointFile: String = if (isCheckpointPresent) cp_.checkpointFile else null
  private[streaming] var checkpointInterval: Time = if (isCheckpointPresent) cp_.checkpointInterval else null
  private[streaming] var receiverJobThread: Thread = null
  private[streaming] var scheduler: Scheduler = null

  def setBatchDuration(duration: Time) {
    graph.setBatchDuration(duration)
  }

  def setCheckpointDetails(file: String, interval: Time) {
    checkpointFile = file
    checkpointInterval = interval
  }

  private[streaming] def getInitialCheckpoint(): Checkpoint = {
    if (isCheckpointPresent) cp_ else null
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
    graph.addInputStream(inputStream)
    inputStream
  }
  
  def createRawNetworkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_2
    ): DStream[T] = {
    val inputStream = new RawInputDStream[T](this, hostname, port, storageLevel)
    graph.addInputStream(inputStream)
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
    val inputStream = new FileInputDStream[K, V, F](this, directory)
    graph.addInputStream(inputStream)
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
    graph.addInputStream(inputStream)
    inputStream
  }
  
  def createQueueStream[T: ClassManifest](array: Array[RDD[T]]): DStream[T] = {
    val queue = new Queue[RDD[T]]
    val inputStream = createQueueStream(queue, true, null)
    queue ++= array
    inputStream
  }

  /**
   * This function registers a InputDStream as an input stream that will be
   * started (InputDStream.start() called) to get the input data streams.
   */
  def registerInputStream(inputStream: InputDStream[_]) {
    graph.addInputStream(inputStream)
  }

  /**
   * This function registers a DStream as an output stream that will be
   * computed every interval.
   */  
  def registerOutputStream(outputStream: DStream[_]) {
    graph.addOutputStream(outputStream)
  }
  
  def validate() {
    assert(graph != null, "Graph is null")
    graph.validate()
  }

  /**
   * This function starts the execution of the streams. 
   */  
  def start() {
    validate()

    val networkInputStreams = graph.getInputStreams().filter(s => s match {
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
    scheduler = new Scheduler(this)
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

  def doCheckpoint(currentTime: Time) {
    new Checkpoint(this, currentTime).saveToFile(checkpointFile)
  }
}


object StreamingContext {
  implicit def toPairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)]) = {
    new PairDStreamFunctions[K, V](stream)
  }
}

