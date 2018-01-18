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

import java.io.{InputStream, NotSerializableException}
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

import scala.collection.Map
import scala.collection.mutable.Queue
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.commons.lang3.SerializationUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BytesWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat

import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.input.FixedLengthBinaryInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, RDDOperationScope}
import org.apache.spark.scheduler.LiveListenerBus
import org.apache.spark.serializer.SerializationDebugger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContextState._
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.
    {ExecutorAllocationManager, JobScheduler, StreamingListener, StreamingListenerStreamingStarted}
import org.apache.spark.streaming.ui.{StreamingJobProgressListener, StreamingTab}
import org.apache.spark.util.{CallSite, ShutdownHookManager, ThreadUtils, Utils}

/**
 * Main entry point for Spark Streaming functionality. It provides methods used to create
 * [[org.apache.spark.streaming.dstream.DStream]]s from various input sources. It can be either
 * created by providing a Spark master URL and an appName, or from a org.apache.spark.SparkConf
 * configuration (see core Spark documentation), or from an existing org.apache.spark.SparkContext.
 * The associated SparkContext can be accessed using `context.sparkContext`. After
 * creating and transforming DStreams, the streaming computation can be started and stopped
 * using `context.start()` and `context.stop()`, respectively.
 * `context.awaitTermination()` allows the current thread to wait for the termination
 * of the context by `stop()` or by an exception.
 */
class StreamingContext private[streaming] (
    _sc: SparkContext,
    _cp: Checkpoint,
    _batchDur: Duration
  ) extends Logging {

  /**
   * Create a StreamingContext using an existing SparkContext.
   * @param sparkContext existing SparkContext
   * @param batchDuration the time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: SparkContext, batchDuration: Duration) = {
    this(sparkContext, null, batchDuration)
  }

  /**
   * Create a StreamingContext by providing the configuration necessary for a new SparkContext.
   * @param conf a org.apache.spark.SparkConf object specifying Spark parameters
   * @param batchDuration the time interval at which streaming data will be divided into batches
   */
  def this(conf: SparkConf, batchDuration: Duration) = {
    this(StreamingContext.createNewSparkContext(conf), null, batchDuration)
  }

  /**
   * Create a StreamingContext by providing the details necessary for creating a new SparkContext.
   * @param master cluster URL to connect to (e.g. mesos://host:port, spark://host:port, local[4]).
   * @param appName a name for your job, to display on the cluster web UI
   * @param batchDuration the time interval at which streaming data will be divided into batches
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
   * Recreate a StreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   * @param hadoopConf Optional, configuration object if necessary for reading from
   *                   HDFS compatible filesystems
   */
  def this(path: String, hadoopConf: Configuration) =
    this(null, CheckpointReader.read(path, new SparkConf(), hadoopConf).orNull, null)

  /**
   * Recreate a StreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   */
  def this(path: String) = this(path, SparkHadoopUtil.get.conf)

  /**
   * Recreate a StreamingContext from a checkpoint file using an existing SparkContext.
   * @param path Path to the directory that was specified as the checkpoint directory
   * @param sparkContext Existing SparkContext
   */
  def this(path: String, sparkContext: SparkContext) = {
    this(
      sparkContext,
      CheckpointReader.read(path, sparkContext.conf, sparkContext.hadoopConfiguration).orNull,
      null)
  }

  require(_sc != null || _cp != null,
    "Spark Streaming cannot be initialized with both SparkContext and checkpoint as null")

  private[streaming] val isCheckpointPresent: Boolean = _cp != null

  private[streaming] val sc: SparkContext = {
    if (_sc != null) {
      _sc
    } else if (isCheckpointPresent) {
      SparkContext.getOrCreate(_cp.createSparkConf())
    } else {
      throw new SparkException("Cannot create StreamingContext without a SparkContext")
    }
  }

  if (sc.conf.get("spark.master") == "local" || sc.conf.get("spark.master") == "local[1]") {
    logWarning("spark.master should be set as local[n], n > 1 in local mode if you have receivers" +
      " to get data, otherwise Spark jobs will not get resources to process the received data.")
  }

  private[streaming] val conf = sc.conf

  private[streaming] val env = sc.env

  private[streaming] val graph: DStreamGraph = {
    if (isCheckpointPresent) {
      _cp.graph.setContext(this)
      _cp.graph.restoreCheckpointData()
      _cp.graph
    } else {
      require(_batchDur != null, "Batch duration for StreamingContext cannot be null")
      val newGraph = new DStreamGraph()
      newGraph.setBatchDuration(_batchDur)
      newGraph
    }
  }

  private val nextInputStreamId = new AtomicInteger(0)

  private[streaming] var checkpointDir: String = {
    if (isCheckpointPresent) {
      sc.setCheckpointDir(_cp.checkpointDir)
      _cp.checkpointDir
    } else {
      null
    }
  }

  private[streaming] val checkpointDuration: Duration = {
    if (isCheckpointPresent) _cp.checkpointDuration else graph.batchDuration
  }

  private[streaming] val scheduler = new JobScheduler(this)

  private[streaming] val waiter = new ContextWaiter

  private[streaming] val progressListener = new StreamingJobProgressListener(this)

  private[streaming] val uiTab: Option[StreamingTab] =
    if (conf.getBoolean("spark.ui.enabled", true)) {
      Some(new StreamingTab(this))
    } else {
      None
    }

  /* Initializing a streamingSource to register metrics */
  private val streamingSource = new StreamingSource(this)

  private var state: StreamingContextState = INITIALIZED

  private val startSite = new AtomicReference[CallSite](null)

  // Copy of thread-local properties from SparkContext. These properties will be set in all tasks
  // submitted by this StreamingContext after start.
  private[streaming] val savedProperties = new AtomicReference[Properties](new Properties)

  private[streaming] def getStartSite(): CallSite = startSite.get()

  private var shutdownHookRef: AnyRef = _

  conf.getOption("spark.streaming.checkpoint.directory").foreach(checkpoint)

  /**
   * Return the associated Spark context
   */
  def sparkContext: SparkContext = sc

  /**
   * Set each DStream in this context to remember RDDs it generated in the last given duration.
   * DStreams remember RDDs only for a limited duration of time and release them for garbage
   * collection. This method allows the developer to specify how long to remember the RDDs (
   * if the developer wishes to query old data outside the DStream computation).
   * @param duration Minimum duration that each DStream should remember its RDDs
   */
  def remember(duration: Duration) {
    graph.remember(duration)
  }

  /**
   * Set the context to periodically checkpoint the DStream operations for driver
   * fault-tolerance.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored.
   *                  Note that this must be a fault-tolerant file system like HDFS.
   */
  def checkpoint(directory: String) {
    if (directory != null) {
      val path = new Path(directory)
      val fs = path.getFileSystem(sparkContext.hadoopConfiguration)
      fs.mkdirs(path)
      val fullPath = fs.getFileStatus(path).getPath().toString
      sc.setCheckpointDir(fullPath)
      checkpointDir = fullPath
    } else {
      checkpointDir = null
    }
  }

  private[streaming] def isCheckpointingEnabled: Boolean = {
    checkpointDir != null
  }

  private[streaming] def initialCheckpoint: Checkpoint = {
    if (isCheckpointPresent) _cp else null
  }

  private[streaming] def getNewInputStreamId() = nextInputStreamId.getAndIncrement()

  /**
   * Execute a block of code in a scope such that all new DStreams created in this body will
   * be part of the same scope. For more detail, see the comments in `doCompute`.
   *
   * Note: Return statements are NOT allowed in the given body.
   */
  private[streaming] def withScope[U](body: => U): U = sparkContext.withScope(body)

  /**
   * Execute a block of code in a scope such that all new DStreams created in this body will
   * be part of the same scope. For more detail, see the comments in `doCompute`.
   *
   * Note: Return statements are NOT allowed in the given body.
   */
  private[streaming] def withNamedScope[U](name: String)(body: => U): U = {
    RDDOperationScope.withScope(sc, name, allowNesting = false, ignoreParent = false)(body)
  }

  /**
   * Create an input stream with any arbitrary user implemented receiver.
   * Find more details at http://spark.apache.org/docs/latest/streaming-custom-receivers.html
   * @param receiver Custom implementation of Receiver
   */
  def receiverStream[T: ClassTag](receiver: Receiver[T]): ReceiverInputDStream[T] = {
    withNamedScope("receiver stream") {
      new PluggableInputDStream[T](this, receiver)
    }
  }

  /**
   * Creates an input stream from TCP source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded `\n` delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   * @see [[socketStream]]
   */
  def socketTextStream(
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = withNamedScope("socket text stream") {
    socketStream[String](hostname, port, SocketReceiver.bytesToLines, storageLevel)
  }

  /**
   * Creates an input stream from TCP source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interpreted as object using the given
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
    ): ReceiverInputDStream[T] = {
    new SocketInputDStream[T](this, hostname, port, converter, storageLevel)
  }

  /**
   * Create an input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   *                      (default: StorageLevel.MEMORY_AND_DISK_SER_2)
   * @tparam T            Type of the objects in the received blocks
   */
  def rawSocketStream[T: ClassTag](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[T] = withNamedScope("raw socket stream") {
    new RawInputDStream[T](this, hostname, port, storageLevel)
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * Files must be written to the monitored directory by "moving" them from another
   * location within the same file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  ] (directory: String): InputDStream[(K, V)] = {
    new FileInputDStream[K, V, F](this, directory)
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * Files must be written to the monitored directory by "moving" them from another
   * location within the same file system.
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
  ] (directory: String, filter: Path => Boolean, newFilesOnly: Boolean): InputDStream[(K, V)] = {
    new FileInputDStream[K, V, F](this, directory, filter, newFilesOnly)
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them using the given key-value types and input format.
   * Files must be written to the monitored directory by "moving" them from another
   * location within the same file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   * @param filter Function to filter paths to process
   * @param newFilesOnly Should process only new files and ignore existing files in the directory
   * @param conf Hadoop configuration
   * @tparam K Key type for reading HDFS file
   * @tparam V Value type for reading HDFS file
   * @tparam F Input format for reading HDFS file
   */
  def fileStream[
    K: ClassTag,
    V: ClassTag,
    F <: NewInputFormat[K, V]: ClassTag
  ] (directory: String,
     filter: Path => Boolean,
     newFilesOnly: Boolean,
     conf: Configuration): InputDStream[(K, V)] = {
    new FileInputDStream[K, V, F](this, directory, filter, newFilesOnly, Option(conf))
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). Files must be written to the
   * monitored directory by "moving" them from another location within the same
   * file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): DStream[String] = withNamedScope("text file stream") {
    fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as flat binary files, assuming a fixed length per record,
   * generating one byte array per record. Files must be written to the monitored directory
   * by "moving" them from another location within the same file system. File names
   * starting with . are ignored.
   *
   * @param directory HDFS directory to monitor for new file
   * @param recordLength length of each record in bytes
   *
   * @note We ensure that the byte array for each record in the
   * resulting RDDs of the DStream has the provided record length.
   */
  def binaryRecordsStream(
      directory: String,
      recordLength: Int): DStream[Array[Byte]] = withNamedScope("binary records stream") {
    val conf = _sc.hadoopConfiguration
    conf.setInt(FixedLengthBinaryInputFormat.RECORD_LENGTH_PROPERTY, recordLength)
    val br = fileStream[LongWritable, BytesWritable, FixedLengthBinaryInputFormat](
      directory, FileInputDStream.defaultFilter: Path => Boolean, newFilesOnly = true, conf)
    br.map { case (k, v) =>
      val bytes = v.copyBytes()
      require(bytes.length == recordLength, "Byte array does not have correct length. " +
        s"${bytes.length} did not equal recordLength: $recordLength")
      bytes
    }
  }

  /**
   * Create an input stream from a queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * @param queue      Queue of RDDs. Modifications to this data structure must be synchronized.
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @tparam T         Type of objects in the RDD
   *
   * @note Arbitrary RDDs can be added to `queueStream`, there is no way to recover data of
   * those RDDs, so `queueStream` doesn't support checkpointing.
   */
  def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean = true
    ): InputDStream[T] = {
    queueStream(queue, oneAtATime, sc.makeRDD(Seq.empty[T], 1))
  }

  /**
   * Create an input stream from a queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * @param queue      Queue of RDDs. Modifications to this data structure must be synchronized.
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @param defaultRDD Default RDD is returned by the DStream when the queue is empty.
   *                   Set as null if no RDD should be returned when empty
   * @tparam T         Type of objects in the RDD
   *
   * @note Arbitrary RDDs can be added to `queueStream`, there is no way to recover data of
   * those RDDs, so `queueStream` doesn't support checkpointing.
   */
  def queueStream[T: ClassTag](
      queue: Queue[RDD[T]],
      oneAtATime: Boolean,
      defaultRDD: RDD[T]
    ): InputDStream[T] = {
    new QueueInputDStream(this, queue, oneAtATime, defaultRDD)
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[T: ClassTag](streams: Seq[DStream[T]]): DStream[T] = withScope {
    new UnionDStream[T](streams.toArray)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams.
   */
  def transform[T: ClassTag](
      dstreams: Seq[DStream[_]],
      transformFunc: (Seq[RDD[_]], Time) => RDD[T]
    ): DStream[T] = withScope {
    new TransformedDStream[T](dstreams, sparkContext.clean(transformFunc))
  }

  /**
   * Add a [[org.apache.spark.streaming.scheduler.StreamingListener]] object for
   * receiving system events related to streaming.
   */
  def addStreamingListener(streamingListener: StreamingListener) {
    scheduler.listenerBus.addListener(streamingListener)
  }

  private def validate() {
    assert(graph != null, "Graph is null")
    graph.validate()

    require(
      !isCheckpointingEnabled || checkpointDuration != null,
      "Checkpoint directory has been set, but the graph checkpointing interval has " +
        "not been set. Please use StreamingContext.checkpoint() to set the interval."
    )

    // Verify whether the DStream checkpoint is serializable
    if (isCheckpointingEnabled) {
      val checkpoint = new Checkpoint(this, Time(0))
      try {
        Checkpoint.serialize(checkpoint, conf)
      } catch {
        case e: NotSerializableException =>
          throw new NotSerializableException(
            "DStream checkpointing has been enabled but the DStreams with their functions " +
              "are not serializable\n" +
              SerializationDebugger.improveException(checkpoint, e).getMessage()
          )
      }
    }

    if (Utils.isDynamicAllocationEnabled(sc.conf) ||
        ExecutorAllocationManager.isDynamicAllocationEnabled(conf)) {
      logWarning("Dynamic Allocation is enabled for this application. " +
        "Enabling Dynamic allocation for Spark Streaming applications can cause data loss if " +
        "Write Ahead Log is not enabled for non-replayable sources like Flume. " +
        "See the programming guide for details on how to enable the Write Ahead Log.")
    }
  }

  /**
   * :: DeveloperApi ::
   *
   * Return the current state of the context. The context can be in three possible states -
   *
   *  - StreamingContextState.INITIALIZED - The context has been created, but not started yet.
   *    Input DStreams, transformations and output operations can be created on the context.
   *  - StreamingContextState.ACTIVE - The context has been started, and not stopped.
   *    Input DStreams, transformations and output operations cannot be created on the context.
   *  - StreamingContextState.STOPPED - The context has been stopped and cannot be used any more.
   */
  @DeveloperApi
  def getState(): StreamingContextState = synchronized {
    state
  }

  /**
   * Start the execution of the streams.
   *
   * @throws IllegalStateException if the StreamingContext is already stopped.
   */
  def start(): Unit = synchronized {
    state match {
      case INITIALIZED =>
        startSite.set(DStream.getCreationSite())
        StreamingContext.ACTIVATION_LOCK.synchronized {
          StreamingContext.assertNoOtherContextIsActive()
          try {
            validate()

            // Start the streaming scheduler in a new thread, so that thread local properties
            // like call sites and job groups can be reset without affecting those of the
            // current thread.
            ThreadUtils.runInNewThread("streaming-start") {
              sparkContext.setCallSite(startSite.get)
              sparkContext.clearJobGroup()
              sparkContext.setLocalProperty(SparkContext.SPARK_JOB_INTERRUPT_ON_CANCEL, "false")
              savedProperties.set(SerializationUtils.clone(sparkContext.localProperties.get()))
              scheduler.start()
            }
            state = StreamingContextState.ACTIVE
            scheduler.listenerBus.post(
              StreamingListenerStreamingStarted(System.currentTimeMillis()))
          } catch {
            case NonFatal(e) =>
              logError("Error starting the context, marking it as stopped", e)
              scheduler.stop(false)
              state = StreamingContextState.STOPPED
              throw e
          }
          StreamingContext.setActiveContext(this)
        }
        logDebug("Adding shutdown hook") // force eager creation of logger
        shutdownHookRef = ShutdownHookManager.addShutdownHook(
          StreamingContext.SHUTDOWN_HOOK_PRIORITY)(() => stopOnShutdown())
        // Registering Streaming Metrics at the start of the StreamingContext
        assert(env.metricsSystem != null)
        env.metricsSystem.registerSource(streamingSource)
        uiTab.foreach(_.attach())
        logInfo("StreamingContext started")
      case ACTIVE =>
        logWarning("StreamingContext has already been started")
      case STOPPED =>
        throw new IllegalStateException("StreamingContext has already been stopped")
    }
  }


  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   */
  def awaitTermination() {
    waiter.waitForStopOrError()
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   *
   * @param timeout time to wait in milliseconds
   * @return `true` if it's stopped; or throw the reported error during the execution; or `false`
   *         if the waiting time elapsed before returning from the method.
   */
  def awaitTerminationOrTimeout(timeout: Long): Boolean = {
    waiter.waitForStopOrError(timeout)
  }

  /**
   * Stop the execution of the streams immediately (does not wait for all received data
   * to be processed). By default, if `stopSparkContext` is not specified, the underlying
   * SparkContext will also be stopped. This implicit behavior can be configured using the
   * SparkConf configuration spark.streaming.stopSparkContextByDefault.
   *
   * @param stopSparkContext If true, stops the associated SparkContext. The underlying SparkContext
   *                         will be stopped regardless of whether this StreamingContext has been
   *                         started.
   */
  def stop(
      stopSparkContext: Boolean = conf.getBoolean("spark.streaming.stopSparkContextByDefault", true)
     ): Unit = synchronized {
    stop(stopSparkContext, false)
  }

  /**
   * Stop the execution of the streams, with option of ensuring all received data
   * has been processed.
   *
   * @param stopSparkContext if true, stops the associated SparkContext. The underlying SparkContext
   *                         will be stopped regardless of whether this StreamingContext has been
   *                         started.
   * @param stopGracefully if true, stops gracefully by waiting for the processing of all
   *                       received data to be completed
   */
  def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    var shutdownHookRefToRemove: AnyRef = null
    if (LiveListenerBus.withinListenerThread.value) {
      throw new SparkException(s"Cannot stop StreamingContext within listener bus thread.")
    }
    synchronized {
      // The state should always be Stopped after calling `stop()`, even if we haven't started yet
      state match {
        case INITIALIZED =>
          logWarning("StreamingContext has not been started yet")
          state = STOPPED
        case STOPPED =>
          logWarning("StreamingContext has already been stopped")
          state = STOPPED
        case ACTIVE =>
          // It's important that we don't set state = STOPPED until the very end of this case,
          // since we need to ensure that we're still able to call `stop()` to recover from
          // a partially-stopped StreamingContext which resulted from this `stop()` call being
          // interrupted. See SPARK-12001 for more details. Because the body of this case can be
          // executed twice in the case of a partial stop, all methods called here need to be
          // idempotent.
          Utils.tryLogNonFatalError {
            scheduler.stop(stopGracefully)
          }
          // Removing the streamingSource to de-register the metrics on stop()
          Utils.tryLogNonFatalError {
            env.metricsSystem.removeSource(streamingSource)
          }
          Utils.tryLogNonFatalError {
            uiTab.foreach(_.detach())
          }
          StreamingContext.setActiveContext(null)
          Utils.tryLogNonFatalError {
            waiter.notifyStop()
          }
          if (shutdownHookRef != null) {
            shutdownHookRefToRemove = shutdownHookRef
            shutdownHookRef = null
          }
          logInfo("StreamingContext stopped successfully")
          state = STOPPED
      }
    }
    if (shutdownHookRefToRemove != null) {
      ShutdownHookManager.removeShutdownHook(shutdownHookRefToRemove)
    }
    // Even if we have already stopped, we still need to attempt to stop the SparkContext because
    // a user might stop(stopSparkContext = false) and then call stop(stopSparkContext = true).
    if (stopSparkContext) sc.stop()
  }

  private def stopOnShutdown(): Unit = {
    val stopGracefully = conf.getBoolean("spark.streaming.stopGracefullyOnShutdown", false)
    logInfo(s"Invoking stop(stopGracefully=$stopGracefully) from shutdown hook")
    // Do not stop SparkContext, let its own shutdown hook stop it
    stop(stopSparkContext = false, stopGracefully = stopGracefully)
  }
}

/**
 * StreamingContext object contains a number of utility functions related to the
 * StreamingContext class.
 */

object StreamingContext extends Logging {

  /**
   * Lock that guards activation of a StreamingContext as well as access to the singleton active
   * StreamingContext in getActiveOrCreate().
   */
  private val ACTIVATION_LOCK = new Object()

  private val SHUTDOWN_HOOK_PRIORITY = ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY + 1

  private val activeContext = new AtomicReference[StreamingContext](null)

  private def assertNoOtherContextIsActive(): Unit = {
    ACTIVATION_LOCK.synchronized {
      if (activeContext.get() != null) {
        throw new IllegalStateException(
          "Only one StreamingContext may be started in this JVM. " +
            "Currently running StreamingContext was started at" +
            activeContext.get.getStartSite().longForm)
      }
    }
  }

  private def setActiveContext(ssc: StreamingContext): Unit = {
    ACTIVATION_LOCK.synchronized {
      activeContext.set(ssc)
    }
  }

  /**
   * :: Experimental ::
   *
   * Get the currently active context, if there is one. Active means started but not stopped.
   */
  @Experimental
  def getActive(): Option[StreamingContext] = {
    ACTIVATION_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  /**
   * :: Experimental ::
   *
   * Either return the "active" StreamingContext (that is, started but not stopped), or create a
   * new StreamingContext that is
   * @param creatingFunc   Function to create a new StreamingContext
   */
  @Experimental
  def getActiveOrCreate(creatingFunc: () => StreamingContext): StreamingContext = {
    ACTIVATION_LOCK.synchronized {
      getActive().getOrElse { creatingFunc() }
    }
  }

  /**
   * :: Experimental ::
   *
   * Either get the currently active StreamingContext (that is, started but not stopped),
   * OR recreate a StreamingContext from checkpoint data in the given path. If checkpoint data
   * does not exist in the provided, then create a new StreamingContext by calling the provided
   * `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param creatingFunc   Function to create a new StreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new StreamingContext if there is an
   *                       error in reading checkpoint data. By default, an exception will be
   *                       thrown on error.
   */
  @Experimental
  def getActiveOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext = {
    ACTIVATION_LOCK.synchronized {
      getActive().getOrElse { getOrCreate(checkpointPath, creatingFunc, hadoopConf, createOnError) }
    }
  }

  /**
   * Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
   * If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
   * recreated from the checkpoint data. If the data does not exist, then the StreamingContext
   * will be created by called the provided `creatingFunc`.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param creatingFunc   Function to create a new StreamingContext
   * @param hadoopConf     Optional Hadoop configuration if necessary for reading from the
   *                       file system
   * @param createOnError  Optional, whether to create a new StreamingContext if there is an
   *                       error in reading checkpoint data. By default, an exception will be
   *                       thrown on error.
   */
  def getOrCreate(
      checkpointPath: String,
      creatingFunc: () => StreamingContext,
      hadoopConf: Configuration = SparkHadoopUtil.get.conf,
      createOnError: Boolean = false
    ): StreamingContext = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), hadoopConf, createOnError)
    checkpointOption.map(new StreamingContext(null, _, null)).getOrElse(creatingFunc())
  }

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to StreamingContext.
   */
  def jarOfClass(cls: Class[_]): Option[String] = SparkContext.jarOfClass(cls)

  private[streaming] def createNewSparkContext(conf: SparkConf): SparkContext = {
    new SparkContext(conf)
  }

  private[streaming] def createNewSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Seq[String],
      environment: Map[String, String]
    ): SparkContext = {
    val conf = SparkContext.updatedConf(
      new SparkConf(), master, appName, sparkHome, jars, environment)
    new SparkContext(conf)
  }

  private[streaming] def rddToFileName[T](prefix: String, suffix: String, time: Time): String = {
    var result = time.milliseconds.toString
    if (prefix != null && prefix.length > 0) {
      result = s"$prefix-$result"
    }
    if (suffix != null && suffix.length > 0) {
      result = s"$result.$suffix"
    }
    result
  }
}

private class StreamingContextPythonHelper {

  /**
   * This is a private method only for Python to implement `getOrCreate`.
   */
  def tryRecoverFromCheckpoint(checkpointPath: String): Option[StreamingContext] = {
    val checkpointOption = CheckpointReader.read(
      checkpointPath, new SparkConf(), SparkHadoopUtil.get.conf, ignoreReadError = false)
    checkpointOption.map(new StreamingContext(null, _, null))
  }
}
