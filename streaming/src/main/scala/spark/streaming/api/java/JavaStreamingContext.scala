package spark.streaming.api.java

import scala.collection.JavaConversions._
import java.util.{List => JList}
import java.lang.{Long => JLong, Integer => JInt}

import spark.streaming._
import dstream._
import spark.storage.StorageLevel
import spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import java.io.InputStream
import java.util.{Map => JMap}

/**
 * A StreamingContext is the main entry point for Spark Streaming functionality. Besides the basic
 * information (such as, cluster URL and job name) to internally create a SparkContext, it provides
 * methods used to create DStream from various input sources.
 */
class JavaStreamingContext(val ssc: StreamingContext) {

  // TODOs:
  // - Test to/from Hadoop functions
  // - Support creating and registering InputStreams


  /**
   * Creates a StreamingContext.
   * @param master Name of the Spark Master
   * @param frameworkName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(master: String, frameworkName: String, batchDuration: Duration) =
    this(new StreamingContext(master, frameworkName, batchDuration))

  /**
   * Re-creates a StreamingContext from a checkpoint file.
   * @param path Path either to the directory that was specified as the checkpoint directory, or
   *             to the checkpoint file 'graph' or 'graph.bk'.
   */
  def this(path: String) = this (new StreamingContext(path))

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param hostname Zookeper hostname.
   * @param port Zookeper port.
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   */
  def kafkaStream[T](
    hostname: String,
    port: Int,
    groupId: String,
    topics: JMap[String, JInt])
  : JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    ssc.kafkaStream[T](hostname, port, groupId, Map(topics.mapValues(_.intValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param hostname Zookeper hostname.
   * @param port Zookeper port.
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   * @param initialOffsets Optional initial offsets for each of the partitions to consume.
   * By default the value is pulled from zookeper.
   */
  def kafkaStream[T](
    hostname: String,
    port: Int,
    groupId: String,
    topics: JMap[String, JInt],
    initialOffsets: JMap[KafkaPartitionKey, JLong])
  : JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    ssc.kafkaStream[T](
      hostname,
      port,
      groupId,
      Map(topics.mapValues(_.intValue()).toSeq: _*),
      Map(initialOffsets.mapValues(_.longValue()).toSeq: _*))
  }

  /**
   * Create an input stream that pulls messages form a Kafka Broker.
   * @param hostname Zookeper hostname.
   * @param port Zookeper port.
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   * in its own thread.
   * @param initialOffsets Optional initial offsets for each of the partitions to consume.
   * By default the value is pulled from zookeper.
   * @param storageLevel RDD storage level. Defaults to memory-only
   */
  def kafkaStream[T](
    hostname: String,
    port: Int,
    groupId: String,
    topics: JMap[String, JInt],
    initialOffsets: JMap[KafkaPartitionKey, JLong],
    storageLevel: StorageLevel)
  : JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    ssc.kafkaStream[T](
      hostname,
      port,
      groupId,
      Map(topics.mapValues(_.intValue()).toSeq: _*),
      Map(initialOffsets.mapValues(_.longValue()).toSeq: _*),
      storageLevel)
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
  def networkTextStream(hostname: String, port: Int, storageLevel: StorageLevel)
  : JavaDStream[String] = {
    ssc.networkTextStream(hostname, port, storageLevel)
  }

  /**
   * Create a input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   */
  def networkTextStream(hostname: String, port: Int): JavaDStream[String] = {
    ssc.networkTextStream(hostname, port)
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
  def networkStream[T](
      hostname: String,
      port: Int,
      converter: JFunction[InputStream, java.lang.Iterable[T]],
      storageLevel: StorageLevel)
  : JavaDStream[T] = {
    def fn = (x: InputStream) => converter.apply(x).toIterator
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    ssc.networkStream(hostname, port, fn, storageLevel)
  }

  /**
   * Creates a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): JavaDStream[String] = {
    ssc.textFileStream(directory)
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
  def rawNetworkStream[T](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel): JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    JavaDStream.fromDStream(ssc.rawNetworkStream(hostname, port, storageLevel))
  }

  /**
   * Create a input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @tparam T            Type of the objects in the received blocks
   */
  def rawNetworkStream[T](hostname: String, port: Int): JavaDStream[T] = {
    implicit val cmt: ClassManifest[T] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[T]]
    JavaDStream.fromDStream(ssc.rawNetworkStream(hostname, port))
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
  def fileStream[K, V, F <: NewInputFormat[K, V]](directory: String): JavaPairDStream[K, V] = {
    implicit val cmk: ClassManifest[K] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[K]]
    implicit val cmv: ClassManifest[V] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[V]]
    implicit val cmf: ClassManifest[F] =
      implicitly[ClassManifest[AnyRef]].asInstanceOf[ClassManifest[F]]
    ssc.fileStream[K, V, F](directory);
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def flumeStream(hostname: String, port: Int, storageLevel: StorageLevel):
    JavaDStream[SparkFlumeEvent] = {
    ssc.flumeStream(hostname, port, storageLevel)
  }


  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   */
  def flumeStream(hostname: String, port: Int):
  JavaDStream[SparkFlumeEvent] = {
    ssc.flumeStream(hostname, port)
  }

  /**
   * Registers an output stream that will be computed every interval
   */
  def registerOutputStream(outputStream: JavaDStreamLike[_, _]) {
    ssc.registerOutputStream(outputStream.dstream)
  }

  /**
   * Sets the context to periodically checkpoint the DStream operations for master
   * fault-tolerance. By default, the graph will be checkpointed every batch interval.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored
   * @param interval checkpoint interval
   */
  def checkpoint(directory: String, interval: Duration = null) {
    ssc.checkpoint(directory, interval)
  }

  /**
   * Sets each DStreams in this context to remember RDDs it generated in the last given duration.
   * DStreams remember RDDs only for a limited duration of duration and releases them for garbage
   * collection. This method allows the developer to specify how to long to remember the RDDs (
   * if the developer wishes to query old data outside the DStream computation).
   * @param duration Minimum duration that each DStream should remember its RDDs
   */
  def remember(duration: Duration) {
    ssc.remember(duration)
  }

  /**
   * Starts the execution of the streams.
   */
  def start() = ssc.start()

  /**
   * Sstops the execution of the streams.
   */
  def stop() = ssc.stop()

}
