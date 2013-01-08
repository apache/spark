package spark.streaming

import spark.streaming.dstream._

import spark.{RDD, Logging, SparkEnv, SparkContext}
import spark.storage.StorageLevel
import spark.util.MetadataCleaner

import scala.collection.mutable.Queue

import java.io.InputStream
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import java.util.UUID

/**
 * A StreamingContext is the main entry point for Spark Streaming functionality. Besides the basic
 * information (such as, cluster URL and job name) to internally create a SparkContext, it provides
 * methods used to create DStream from various input sources.
 */
class StreamingContext private (
    sc_ : SparkContext,
    cp_ : Checkpoint,
    batchDur_ : Time
  ) extends Logging {

  /**
   * Creates a StreamingContext using an existing SparkContext.
   * @param sparkContext Existing SparkContext
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: SparkContext, batchDuration: Time) = this(sparkContext, null, batchDuration)

  /**
   * Creates a StreamingContext by providing the details necessary for creating a new SparkContext.
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param frameworkName A name for your job, to display on the cluster web UI
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(master: String, frameworkName: String, batchDuration: Time) =
    this(StreamingContext.createNewSparkContext(master, frameworkName), null, batchDuration)

  /**
   * Re-creates a StreamingContext from a checkpoint file.
   * @param path Path either to the directory that was specified as the checkpoint directory, or
   *             to the checkpoint file 'graph' or 'graph.bk'.
   */
  def this(path: String) = this(null, CheckpointReader.read(path), null)

  initLogging()

  if (sc_ == null && cp_ == null) {
    throw new Exception("Streaming Context cannot be initilalized with " +
      "both SparkContext and checkpoint as null")
  }

  protected[streaming] val isCheckpointPresent = (cp_ != null)

  val sc: SparkContext = {
    if (isCheckpointPresent) {
      new SparkContext(cp_.master, cp_.framework, cp_.sparkHome, cp_.jars)
    } else {
      sc_
    }
  }

  protected[streaming] val env = SparkEnv.get

  protected[streaming] val graph: DStreamGraph = {
    if (isCheckpointPresent) {
      cp_.graph.setContext(this)
      cp_.graph.restoreCheckpointData()
      cp_.graph
    } else {
      assert(batchDur_ != null, "Batch duration for streaming context cannot be null")
      val newGraph = new DStreamGraph()
      newGraph.setBatchDuration(batchDur_)
      newGraph
    }
  }

  protected[streaming] val nextNetworkInputStreamId = new AtomicInteger(0)
  protected[streaming] var networkInputTracker: NetworkInputTracker = null

  protected[streaming] var checkpointDir: String = {
    if (isCheckpointPresent) {
      sc.setCheckpointDir(StreamingContext.getSparkCheckpointDir(cp_.checkpointDir), true)
      cp_.checkpointDir
    } else {
      null
    }
  }

  protected[streaming] var checkpointInterval: Time = if (isCheckpointPresent) cp_.checkpointInterval else null
  protected[streaming] var receiverJobThread: Thread = null
  protected[streaming] var scheduler: Scheduler = null

  /**
   * Sets each DStreams in this context to remember RDDs it generated in the last given duration.
   * DStreams remember RDDs only for a limited duration of time and releases them for garbage
   * collection. This method allows the developer to specify how to long to remember the RDDs (
   * if the developer wishes to query old data outside the DStream computation).
   * @param duration Minimum duration that each DStream should remember its RDDs
   */
  def remember(duration: Time) {
    graph.remember(duration)
  }

  /**
   * Sets the context to periodically checkpoint the DStream operations for master
   * fault-tolerance. By default, the graph will be checkpointed every batch interval.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored
   * @param interval checkpoint interval
   */
  def checkpoint(directory: String, interval: Time = null) {
    if (directory != null) {
      sc.setCheckpointDir(StreamingContext.getSparkCheckpointDir(directory))
      checkpointDir = directory
      checkpointInterval = interval
    } else {
      checkpointDir = null
      checkpointInterval = null
    }
  }

  protected[streaming] def getInitialCheckpoint(): Checkpoint = {
    if (isCheckpointPresent) cp_ else null
  }

  protected[streaming] def getNewNetworkStreamId() = nextNetworkInputStreamId.getAndIncrement()

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param hostname Zookeper hostname.
   * @param port Zookeper port.
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   * @param initialOffsets Optional initial offsets for each of the partitions to consume.
   * By default the value is pulled from zookeper.
   * @param storageLevel RDD storage level. Defaults to memory-only.
   */
  def kafkaStream[T: ClassManifest](
      hostname: String,
      port: Int,
      groupId: String,
      topics: Map[String, Int],
      initialOffsets: Map[KafkaPartitionKey, Long] = Map[KafkaPartitionKey, Long](),
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2
    ): DStream[T] = {
    val inputStream = new KafkaInputDStream[T](this, hostname, port, groupId, topics, initialOffsets, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def networkTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[String] = {
    networkStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interepreted as object using the given
   * converter.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param converter     Function to convert the byte stream to objects
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects received (after converting bytes to objects)
   */
  def networkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      converter: (InputStream) => Iterator[T],
      storageLevel: StorageLevel
    ): DStream[T] = {
    val inputStream = new SocketInputDStream[T](this, hostname, port, converter, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def flumeStream (
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[SparkFlumeEvent] = {
    val inputStream = new FlumeInputDStream(this, hostname, port, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects in the received blocks
   */
  def rawNetworkStream[T: ClassManifest](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[T] = {
    val inputStream = new RawInputDStream[T](this, hostname, port, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassManifest,
    V: ClassManifest,
    F <: NewInputFormat[K, V]: ClassManifest
  ] (directory: String): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, directory)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * @param directory HDFS directory to monitor for new file
   * @param filter Function to filter paths to process
   * @param newFilesOnly Should process only new files and ignore existing files in the directory
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassManifest,
    V: ClassManifest,
    F <: NewInputFormat[K, V]: ClassManifest
  ] (directory: String, filter: Path => Boolean, newFilesOnly: Boolean): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, directory, filter, newFilesOnly)
    registerInputStream(inputStream)
    inputStream
  }


  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): DStream[String] = {
    fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }

  /**
   * Creates a input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @param defaultRDD Default RDD is returned by the DStream when the queue is empty
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T: ClassManifest](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true,
      defaultRDD: RDD[T] = null
    ): DStream[T] = {
    val inputStream = new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same interval
   */
  def union[T: ClassManifest](streams: Seq[DStream[T]]): DStream[T] = {
    new UnionDStream[T](streams.toArray)
  }

  /**
   * Registers an input stream that will be started (InputDStream.start() called) to get the
   * input data.
   */
  def registerInputStream(inputStream: InputDStream[_]) {
    graph.addInputStream(inputStream)
  }

  /**
   * Registers an output stream that will be computed every interval
   */
  def registerOutputStream(outputStream: DStream[_]) {
    graph.addOutputStream(outputStream)
  }

  protected def validate() {
    assert(graph != null, "Graph is null")
    graph.validate()

    assert(
      checkpointDir == null || checkpointInterval != null,
      "Checkpoint directory has been set, but the graph checkpointing interval has " +
        "not been set. Please use StreamingContext.checkpoint() to set the interval."
    )
  }

  /**
   * Starts the execution of the streams.
   */
  def start() {
    if (checkpointDir != null && checkpointInterval == null && graph != null) {
      checkpointInterval = graph.batchDuration
    }

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
   * Sstops the execution of the streams.
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
}


object StreamingContext {

  implicit def toPairDStreamFunctions[K: ClassManifest, V: ClassManifest](stream: DStream[(K,V)]) = {
    new PairDStreamFunctions[K, V](stream)
  }

  protected[streaming] def createNewSparkContext(master: String, frameworkName: String): SparkContext = {

    // Set the default cleaner delay to an hour if not already set.
    // This should be sufficient for even 1 second interval.
    if (MetadataCleaner.getDelaySeconds < 0) {
      MetadataCleaner.setDelaySeconds(60)
    }
    new SparkContext(master, frameworkName)
  }

  protected[streaming] def rddToFileName[T](prefix: String, suffix: String, time: Time): String = {
    if (prefix == null) {
      time.milliseconds.toString
    } else if (suffix == null || suffix.length ==0) {
      prefix + "-" + time.milliseconds
    } else {
      prefix + "-" + time.milliseconds + "." + suffix
    }
  }

  protected[streaming] def getSparkCheckpointDir(sscCheckpointDir: String): String = {
    new Path(sscCheckpointDir, UUID.randomUUID.toString).toString
  }
}

