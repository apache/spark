/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming

import akka.actor.Props
import akka.actor.SupervisorStrategy
import akka.zeromq.Subscribe

import org.apache.spark.streaming.dstream._

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.receivers.ActorReceiver
import org.apache.spark.streaming.receivers.ReceiverSupervisorStrategy
import org.apache.spark.streaming.receivers.ZeroMQReceiver
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.MetadataCleaner
import org.apache.spark.streaming.receivers.ActorReceiver

import scala.collection.mutable.Queue
import scala.collection.Map
import scala.reflect.ClassTag

import java.io.InputStream
import java.util.concurrent.atomic.AtomicInteger
import java.util.UUID

import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.fs.Path
import twitter4j.Status
import twitter4j.auth.Authorization
import akka.util.ByteString


/**
 * A StreamingContext is the main entry point for Spark Streaming functionality. Besides the basic
 * information (such as, cluster URL and job name) to internally create a SparkContext, it provides
 * methods used to create DStream from various input sources.
 */
class StreamingContext private (
    sc_ : SparkContext,
    cp_ : Checkpoint,
    batchDur_ : Duration
  ) extends Logging {

  /**
   * Create a StreamingContext using an existing SparkContext.
   * @param sparkContext Existing SparkContext
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
  }

  /**
   * Create a StreamingContext by providing the details necessary for creating a new SparkContext.
   * @param master Cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName A name for your job, to display on the cluster web UI
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String = null,
      jars: Seq[String] = Nil,
      environment: Map[String, String] = Map()) = {
    this(StreamingContext.createNewSparkContext(master, appName, sparkHome, jars, environment),
         null, batchDuration)
  }


  /**
   * Re-create a StreamingContext from a checkpoint file.
   * @param path Path either to the directory that was specified as the checkpoint directory, or
   *             to the checkpoint file 'graph' or 'graph.bk'.
   */
  def this(path: String) = this(null, CheckpointReader.read(path), null)

  initLogging()

  if (sc_ == null && cp_ == null) {
    throw new Exception("Spark Streaming cannot be initialized with " +
      "both SparkContext and checkpoint as null")
  }

  if(cp_ != null && cp_.delaySeconds >= 0 && MetadataCleaner.getDelaySeconds < 0) {
    MetadataCleaner.setDelaySeconds(cp_.delaySeconds)
  }

  if (MetadataCleaner.getDelaySeconds < 0) {
    throw new SparkException("Spark Streaming cannot be used without setting spark.cleaner.ttl; "
      + "set this property before creating a SparkContext (use SPARK_JAVA_OPTS for the shell)")
  }

  protected[streaming] val isCheckpointPresent = (cp_ != null)

  protected[streaming] val sc: SparkContext = {
    if (isCheckpointPresent) {
      new SparkContext(cp_.master, cp_.framework, cp_.sparkHome, cp_.jars, cp_.environment)
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

  protected[streaming] var checkpointDuration: Duration = if (isCheckpointPresent) cp_.checkpointDuration else null
  protected[streaming] var receiverJobThread: Thread = null
  protected[streaming] var scheduler: Scheduler = null

  /**
   * Return the associated Spark context
   */
  def sparkContext = sc

  /**
   * Set each DStreams in this context to remember RDDs it generated in the last given duration.
   * DStreams remember RDDs only for a limited duration of time and releases them for garbage
   * collection. This method allows the developer to specify how to long to remember the RDDs (
   * if the developer wishes to query old data outside the DStream computation).
   * @param duration Minimum duration that each DStream should remember its RDDs
   */
  def remember(duration: Duration) {
    graph.remember(duration)
  }

  /**
   * Set the context to periodically checkpoint the DStream operations for master
   * fault-tolerance. The graph will be checkpointed every batch interval.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored
   */
  def checkpoint(directory: String) {
    if (directory != null) {
      sc.setCheckpointDir(StreamingContext.getSparkCheckpointDir(directory))
      checkpointDir = directory
    } else {
      checkpointDir = null
    }
  }

  protected[streaming] def initialCheckpoint: Checkpoint = {
    if (isCheckpointPresent) cp_ else null
  }

  protected[streaming] def getNewNetworkStreamId() = nextNetworkInputStreamId.getAndIncrement()

  /**
   * Create an input stream with any arbitrary user implemented network receiver.
   * Find more details at: http://spark-project.org/docs/latest/streaming-custom-receivers.html
   * @param receiver Custom implementation of NetworkReceiver
   */
  def networkStream[T: ClassTag](
    receiver: NetworkReceiver[T]): DStream[T] = {
    val inputStream = new PluggableInputDStream[T](this,
      receiver)
    graph.addInputStream(inputStream)
    inputStream
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * Find more details at: http://spark-project.org/docs/latest/streaming-custom-receivers.html
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel RDD storage level. Defaults to memory-only.
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T: ClassTag](
      props: Props,
      name: String,
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2,
      supervisorStrategy: SupervisorStrategy = ReceiverSupervisorStrategy.defaultStrategy
    ): DStream[T] = {
    networkStream(new ActorReceiver[T](props, name, storageLevel, supervisorStrategy))
  }

  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic
   *                       and each frame has sequence of byte thus it needs the converter
   *                       (which might be deserializer of bytes) to translate from sequence
   *                       of sequence of bytes, where sequence refer to a frame
   *                       and sub sequence refer to its payload.
   * @param storageLevel RDD storage level. Defaults to memory-only.
   */
  def zeroMQStream[T: ClassTag](
      publisherUrl:String,
      subscribe: Subscribe,
      bytesToObjects: Seq[ByteString] ⇒ Iterator[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2,
      supervisorStrategy: SupervisorStrategy = ReceiverSupervisorStrategy.defaultStrategy
    ): DStream[T] = {
    actorStream(Props(new ZeroMQReceiver(publisherUrl, subscribe, bytesToObjects)),
        "ZeroMQReceiver", storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param zkQuorum Zookeper quorum (hostname:port,hostname:port,..).
   * @param groupId The group id for this consumer.
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def kafkaStream(
      zkQuorum: String,
      groupId: String,
      topics: Map[String, Int],
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2
    ): DStream[(String, String)] = {
    val kafkaParams = Map[String, String](
      "zookeeper.connect" -> zkQuorum, "group.id" -> groupId,
      "zookeeper.connection.timeout.ms" -> "10000")
    kafkaStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      kafkaParams,
      topics,
      storageLevel)
  }

  /**
   * Create an input stream that pulls messages from a Kafka Broker.
   * @param kafkaParams Map of kafka configuration paramaters.
   *                    See: http://kafka.apache.org/configuration.html
   * @param topics Map of (topic_name -> numPartitions) to consume. Each partition is consumed
   *               in its own thread.
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def kafkaStream[
    K: ClassTag,
    V: ClassTag,
    U <: kafka.serializer.Decoder[_]: Manifest,
    T <: kafka.serializer.Decoder[_]: Manifest](
      kafkaParams: Map[String, String],
      topics: Map[String, Int],
      storageLevel: StorageLevel
    ): DStream[(K, V)] = {
    val inputStream = new KafkaInputDStream[K, V, U, T](this, kafkaParams, topics, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream from TCP source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded `\n` delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   */
  def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[String] = {
    socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }

  /**
   * Create a input stream from TCP source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interepreted as object using the given
   * converter.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param converter     Function to convert the byte stream to objects
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects received (after converting bytes to objects)
   */
  def socketStream[T: ClassTag](
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
   * Create a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def flumeStream (
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[SparkFlumeEvent] = {
    val inputStream = new FlumeInputDStream[SparkFlumeEvent](this, hostname, port, storageLevel)
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
  def rawSocketStream[T: ClassTag](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[T] = {
    val inputStream = new RawInputDStream[T](this, hostname, port, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  ] (directory: String): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, directory)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * @param directory HDFS directory to monitor for new file
   * @param filter Function to filter paths to process
   * @param newFilesOnly Should process only new files and ignore existing files in the directory
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  ] (directory: String, filter: Path => Boolean, newFilesOnly: Boolean): DStream[(K, V)] = {
    val inputStream = new FileInputDStream[K, V, F](this, directory, filter, newFilesOnly)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create a input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): DStream[String] = {
    fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }

  /**
   * Create a input stream that returns tweets received from Twitter.
   * @param twitterAuth Twitter4J authentication, or None to use Twitter4J's default OAuth
   *        authorization; this uses the system properties twitter4j.oauth.consumerKey,
   *        .consumerSecret, .accessToken and .accessTokenSecret.
   * @param filters Set of filter strings to get only those tweets that match them
   * @param storageLevel Storage level to use for storing the received objects
   */
  def twitterStream(
      twitterAuth: Option[Authorization] = None,
      filters: Seq[String] = Nil,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[Status] = {
    val inputStream = new TwitterInputDStream(this, twitterAuth, filters, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }

  /**
   * Create an input stream from a queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true
    ): DStream[T] = {
    queueStream(queue, oneAtATime, sc.makeRDD(Seq[T](), 1))
  }

  /**
   * Create an input stream from a queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @param defaultRDD Default RDD is returned by the DStream when the queue is empty.
   *                   Set as null if no RDD should be returned when empty
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean,
      defaultRDD: RDD[T]
    ): DStream[T] = {
    val inputStream = new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
    registerInputStream(inputStream)
    inputStream
  }

/**
   * Create an input stream that receives messages pushed by a mqtt publisher.
   * @param brokerUrl Url of remote mqtt publisher
   * @param topic topic name to subscribe to
   * @param storageLevel RDD storage level. Defaults to memory-only.
   */

  def mqttStream(
    brokerUrl: String,
    topic: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2): DStream[String] = {
    val inputStream = new MQTTInputDStream[String](this, brokerUrl, topic, storageLevel)
    registerInputStream(inputStream)
    inputStream
  }
  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[T: ClassTag](streams: Seq[DStream[T]]): DStream[T] = {
    new UnionDStream[T](streams.toArray)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams.
   */
  def transform[T: ClassTag](
      dstreams: Seq[DStream[_]],
      transformFunc: (Seq[RDD[_]], Time) => RDD[T]
    ): DStream[T] = {
    new TransformedDStream[T](dstreams, sparkContext.clean(transformFunc))
  }

  /**
   * Register an input stream that will be started (InputDStream.start() called) to get the
   * input data.
   */
  def registerInputStream(inputStream: InputDStream[_]) {
    graph.addInputStream(inputStream)
  }

  /**
   * Register an output stream that will be computed every interval
   */
  def registerOutputStream(outputStream: DStream[_]) {
    graph.addOutputStream(outputStream)
  }

  protected def validate() {
    assert(graph != null, "Graph is null")
    graph.validate()

    assert(
      checkpointDir == null || checkpointDuration != null,
      "Checkpoint directory has been set, but the graph checkpointing interval has " +
        "not been set. Please use StreamingContext.checkpoint() to set the interval."
    )
  }

  /**
   * Start the execution of the streams.
   */
  def start() {
    if (checkpointDir != null && checkpointDuration == null && graph != null) {
      checkpointDuration = graph.batchDuration
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
   * Stop the execution of the streams.
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

  implicit def toPairDStreamFunctions[K: ClassTag, V: ClassTag](stream: DStream[(K,V)]) = {
    new PairDStreamFunctions[K, V](stream)
  }

  protected[streaming] def createNewSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: Map[String, String]): SparkContext = {
    // Set the default cleaner delay to an hour if not already set.
    // This should be sufficient for even 1 second interval.
    if (MetadataCleaner.getDelaySeconds < 0) {
      MetadataCleaner.setDelaySeconds(3600)
    }
    new SparkContext(master, appName, sparkHome, jars, environment)
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
