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
import org.apache.hadoop.fs.Path
import java.util.UUID

final class StreamingContext (
    sc_ : SparkContext,
    cp_ : Checkpoint
  ) extends Logging {

  def this(sparkContext: SparkContext) = this(sparkContext, null)

  def this(master: String, frameworkName: String, sparkHome: String = null, jars: Seq[String] = Nil) =
    this(new SparkContext(master, frameworkName, sparkHome, jars), null)

  def this(path: String) = this(null, Checkpoint.load(path))

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
      cp_.graph.restoreCheckpointData()
      cp_.graph
    } else {
      new DStreamGraph()
    }
  }

  private[streaming] val nextNetworkInputStreamId = new AtomicInteger(0)
  private[streaming] var networkInputTracker: NetworkInputTracker = null

  private[streaming] var checkpointDir: String = {
    if (isCheckpointPresent) {
      sc.setCheckpointDir(StreamingContext.getSparkCheckpointDir(cp_.checkpointDir), true)
      cp_.checkpointDir
    } else {
      null
    }
  }

  private[streaming] var checkpointInterval: Time = if (isCheckpointPresent) cp_.checkpointInterval else null
  private[streaming] var receiverJobThread: Thread = null
  private[streaming] var scheduler: Scheduler = null

  def setBatchDuration(duration: Time) {
    graph.setBatchDuration(duration)
  }

  def setRememberDuration(duration: Time) {
    graph.setRememberDuration(duration)
  }

  def checkpoint(dir: String, interval: Time) {
    if (dir != null) {
      sc.setCheckpointDir(StreamingContext.getSparkCheckpointDir(dir))
      checkpointDir = dir
      checkpointInterval = interval
    } else {
      checkpointDir = null
      checkpointInterval = null
    }
  }

  private[streaming] def getInitialCheckpoint(): Checkpoint = {
    if (isCheckpointPresent) cp_ else null
  }

  private[streaming] def getNewNetworkStreamId() = nextNetworkInputStreamId.getAndIncrement()

  def networkTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[String] = {
    networkStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }

  def networkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      converter: (InputStream) => Iterator[T],
      storageLevel: StorageLevel
    ): DStream[T] = {
    val inputStream = new SocketInputDStream[T](this, hostname, port, converter, storageLevel)
    graph.addInputStream(inputStream)
    inputStream
  }

  def rawNetworkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2
    ): DStream[T] = {
    val inputStream = new RawInputDStream[T](this, hostname, port, storageLevel)
    graph.addInputStream(inputStream)
    inputStream
  }

  /**
   * This function creates a input stream that monitors a Hadoop-compatible
   * for new files and executes the necessary processing on them.
   */
  def fileStream[
    K: ClassManifest,
    V: ClassManifest,
    F <: NewInputFormat[K, V]: ClassManifest
  ](directory: String): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, directory)
    graph.addInputStream(inputStream)
    inputStream
  }

  def textFileStream(directory: String): DStream[String] = {
    fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }

  /**
   * This function create a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue
   */
  def queueStream[T: ClassManifest](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true,
      defaultRDD: RDD[T] = null
    ): DStream[T] = {
    val inputStream = new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
    graph.addInputStream(inputStream)
    inputStream
  }

  def queueStream[T: ClassManifest](array: Array[RDD[T]]): DStream[T] = {
    val queue = new Queue[RDD[T]]
    val inputStream = queueStream(queue, true, null)
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

  /**
   * This function starts the execution of the streams.
   */
  def start() {
    assert(graph != null, "Graph is null")
    graph.validate()

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
   * This function stops the execution of the streams.
   */
  def stop() {
    try {
      if (scheduler != null) scheduler.stop()
      if (networkInputTracker != null) networkInputTracker.stop()
      if (receiverJobThread != null) receiverJobThread.interrupt()
      sc.stop()
      logInfo("StreamingContext stopped successfully")
    } catch {
      case e: Exception => logWarning("Error while stopping", e)
    }
  }

  def doCheckpoint(currentTime: Time) {
    val startTime = System.currentTimeMillis()
    graph.updateCheckpointData(currentTime)
    new Checkpoint(this, currentTime).save(checkpointDir)
    val stopTime = System.currentTimeMillis()
    logInfo("Checkpointing the graph took " + (stopTime - startTime) + " ms")
  }
}


object StreamingContext {
  implicit def toPairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)]) = {
    new PairDStreamFunctions[K, V](stream)
  }

  def rddToFileName[T](prefix: String, suffix: String, time: Time): String = {
    if (prefix == null) {
      time.millis.toString
    } else if (suffix == null || suffix.length ==0) {
      prefix + "-" + time.milliseconds
    } else {
      prefix + "-" + time.milliseconds + "." + suffix
    }
  }

  def getSparkCheckpointDir(sscCheckpointDir: String): String = {
    new Path(sscCheckpointDir, UUID.randomUUID.toString).toString
  }
}

