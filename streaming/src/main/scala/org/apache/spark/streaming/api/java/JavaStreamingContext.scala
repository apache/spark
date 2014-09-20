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

package org.apache.spark.streaming.api.java


import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import java.io.{Closeable, InputStream}
import java.util.{List => JList, Map => JMap}

import akka.actor.{Props, SupervisorStrategy}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.java.function.{Function => JFunction, Function2 => JFunction2}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.StreamingListener
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.dstream.{PluggableInputDStream, ReceiverInputDStream, DStream}
import org.apache.spark.streaming.receiver.Receiver

/**
 * A Java-friendly version of [[org.apache.spark.streaming.StreamingContext]] which is the main
 * entry point for Spark Streaming functionality. It provides methods to create
 * [[org.apache.spark.streaming.api.java.JavaDStream]] and
 * [[org.apache.spark.streaming.api.java.JavaPairDStream.]] from input sources. The internal
 * org.apache.spark.api.java.JavaSparkContext (see core Spark documentation) can be accessed
 * using `context.sparkContext`. After creating and transforming DStreams, the streaming
 * computation can be started and stopped using `context.start()` and `context.stop()`,
 * respectively. `context.awaitTransformation()` allows the current thread to wait for the
 * termination of a context by `stop()` or by an exception.
 */
class JavaStreamingContext(val ssc: StreamingContext) extends Closeable {

  /**
   * Create a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(master: String, appName: String, batchDuration: Duration) =
    this(new StreamingContext(master, appName, batchDuration, null, Nil, Map()))

  /**
   * Create a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jarFile JAR file containing job code, to ship to cluster. This can be a path on the
   *                local file system or an HDFS, HTTP, HTTPS, or FTP URL.
   */
  def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jarFile: String) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, Seq(jarFile), Map()))

  /**
   * Create a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   */
  def this(
      master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jars: Array[String]) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, jars, Map()))

  /**
   * Create a StreamingContext.
   * @param master Name of the Spark Master
   * @param appName Name to be used when registering with the scheduler
   * @param batchDuration The time interval at which streaming data will be divided into batches
   * @param sparkHome The SPARK_HOME directory on the slave nodes
   * @param jars Collection of JARs to send to the cluster. These can be paths on the local file
   *             system or HDFS, HTTP, HTTPS, or FTP URLs.
   * @param environment Environment variables to set on worker nodes
   */
  def this(
    master: String,
    appName: String,
    batchDuration: Duration,
    sparkHome: String,
    jars: Array[String],
    environment: JMap[String, String]) =
    this(new StreamingContext(master, appName, batchDuration, sparkHome, jars, environment))

  /**
   * Create a JavaStreamingContext using an existing JavaSparkContext.
   * @param sparkContext The underlying JavaSparkContext to use
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(sparkContext: JavaSparkContext, batchDuration: Duration) =
    this(new StreamingContext(sparkContext.sc, batchDuration))

  /**
   * Create a JavaStreamingContext using a SparkConf configuration.
   * @param conf A Spark application configuration
   * @param batchDuration The time interval at which streaming data will be divided into batches
   */
  def this(conf: SparkConf, batchDuration: Duration) =
    this(new StreamingContext(conf, batchDuration))

  /**
   * Recreate a JavaStreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   */
  def this(path: String) = this(new StreamingContext(path, new Configuration))

  /**
   * Re-creates a JavaStreamingContext from a checkpoint file.
   * @param path Path to the directory that was specified as the checkpoint directory
   *
   */
  def this(path: String, hadoopConf: Configuration) = this(new StreamingContext(path, hadoopConf))

  /** The underlying SparkContext */
  val sparkContext = new JavaSparkContext(ssc.sc)

  @deprecated("use sparkContext", "0.9.0")
  val sc: JavaSparkContext = sparkContext

  /**
   * Create an input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def socketTextStream(
      hostname: String, port: Int,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[String] = {
    ssc.socketTextStream(hostname, port, storageLevel)
  }

  /**
   * Create an input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes is interpreted as UTF8 encoded \n delimited
   * lines. Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   */
  def socketTextStream(hostname: String, port: Int): JavaReceiverInputDStream[String] = {
    ssc.socketTextStream(hostname, port)
  }

  /**
   * Create an input stream from network source hostname:port. Data is received using
   * a TCP socket and the receive bytes it interepreted as object using the given
   * converter.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param converter     Function to convert the byte stream to objects
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects received (after converting bytes to objects)
   */
  def socketStream[T](
      hostname: String,
      port: Int,
      converter: JFunction[InputStream, java.lang.Iterable[T]],
      storageLevel: StorageLevel)
  : JavaReceiverInputDStream[T] = {
    def fn = (x: InputStream) => converter.call(x).toIterator
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.socketStream(hostname, port, fn, storageLevel)
  }

  /**
   * Create an input stream that monitors a Hadoop-compatible filesystem
   * for new files and reads them as text files (using key as LongWritable, value
   * as Text and input format as TextInputFormat). Files must be written to the
   * monitored directory by "moving" them from another location within the same
   * file system. File names starting with . are ignored.
   * @param directory HDFS directory to monitor for new file
   */
  def textFileStream(directory: String): JavaDStream[String] = {
    ssc.textFileStream(directory)
  }

  /**
   * Create an input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @param storageLevel  Storage level to use for storing the received objects
   * @tparam T            Type of the objects in the received blocks
   */
  def rawSocketStream[T](
      hostname: String,
      port: Int,
      storageLevel: StorageLevel): JavaReceiverInputDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    JavaReceiverInputDStream.fromReceiverInputDStream(
      ssc.rawSocketStream(hostname, port, storageLevel))
  }

  /**
   * Create an input stream from network source hostname:port, where data is received
   * as serialized blocks (serialized using the Spark's serializer) that can be directly
   * pushed into the block manager without deserializing them. This is the most efficient
   * way to receive data.
   * @param hostname      Hostname to connect to for receiving data
   * @param port          Port to connect to for receiving data
   * @tparam T            Type of the objects in the received blocks
   */
  def rawSocketStream[T](hostname: String, port: Int): JavaReceiverInputDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    JavaReceiverInputDStream.fromReceiverInputDStream(
      ssc.rawSocketStream(hostname, port))
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
  def fileStream[K, V, F <: NewInputFormat[K, V]](
      directory: String): JavaPairInputDStream[K, V] = {
    implicit val cmk: ClassTag[K] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val cmv: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    implicit val cmf: ClassTag[F] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[F]]
    ssc.fileStream[K, V, F](directory)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String,
      storageLevel: StorageLevel,
      supervisorStrategy: SupervisorStrategy
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name, storageLevel, supervisorStrategy)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   * @param storageLevel Storage level to use for storing the received objects
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String,
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name, storageLevel)
  }

  /**
   * Create an input stream with any arbitrary user implemented actor receiver.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param props Props object defining creation of the actor
   * @param name Name of the actor
   *
   * @note An important point to note:
   *       Since Actor may exist outside the spark framework, It is thus user's responsibility
   *       to ensure the type safety, i.e parametrized type of data received and actorStream
   *       should be same.
   */
  def actorStream[T](
      props: Props,
      name: String
    ): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.actorStream[T](props, name)
  }

  /**
   * Create an input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](queue: java.util.Queue[JavaRDD[T]]): JavaDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue)
  }

  /**
   * Create an input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](
      queue: java.util.Queue[JavaRDD[T]],
      oneAtATime: Boolean
    ): JavaInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue, oneAtATime)
  }

  /**
   * Create an input stream from an queue of RDDs. In each batch,
   * it will process either one or all of the RDDs returned by the queue.
   *
   * NOTE: changes to the queue after the stream is created will not be recognized.
   * @param queue      Queue of RDDs
   * @param oneAtATime Whether only one RDD should be consumed from the queue in every interval
   * @param defaultRDD Default RDD is returned by the DStream when the queue is empty
   * @tparam T         Type of objects in the RDD
   */
  def queueStream[T](
      queue: java.util.Queue[JavaRDD[T]],
      oneAtATime: Boolean,
      defaultRDD: JavaRDD[T]): JavaInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val sQueue = new scala.collection.mutable.Queue[RDD[T]]
    sQueue.enqueue(queue.map(_.rdd).toSeq: _*)
    ssc.queueStream(sQueue, oneAtATime, defaultRDD.rdd)
  }

  /**
     * Create an input stream with any arbitrary user implemented receiver.
     * Find more details at: http://spark.apache.org/docs/latest/streaming-custom-receivers.html
     * @param receiver Custom implementation of Receiver
     */
  def receiverStream[T](receiver: Receiver[T]): JavaReceiverInputDStream[T] = {
    implicit val cm: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    ssc.receiverStream(receiver)
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[T](first: JavaDStream[T], rest: JList[JavaDStream[T]]): JavaDStream[T] = {
    val dstreams: Seq[DStream[T]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.dstream)
    implicit val cm: ClassTag[T] = first.classTag
    ssc.union(dstreams)(cm)
  }

  /**
   * Create a unified DStream from multiple DStreams of the same type and same slide duration.
   */
  def union[K, V](
      first: JavaPairDStream[K, V],
      rest: JList[JavaPairDStream[K, V]]
    ): JavaPairDStream[K, V] = {
    val dstreams: Seq[DStream[(K, V)]] = (Seq(first) ++ asScalaBuffer(rest)).map(_.dstream)
    implicit val cm: ClassTag[(K, V)] = first.classTag
    implicit val kcm: ClassTag[K] = first.kManifest
    implicit val vcm: ClassTag[V] = first.vManifest
    new JavaPairDStream[K, V](ssc.union(dstreams)(cm))(kcm, vcm)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams. The order of the JavaRDDs in the transform function parameter will be the
   * same as the order of corresponding DStreams in the list. Note that for adding a
   * JavaPairDStream in the list of JavaDStreams, convert it to a JavaDStream using
   * [[org.apache.spark.streaming.api.java.JavaPairDStream]].toJavaDStream().
   * In the transform function, convert the JavaRDD corresponding to that JavaDStream to
   * a JavaPairRDD using org.apache.spark.api.java.JavaPairRDD.fromJavaRDD().
   */
  def transform[T](
      dstreams: JList[JavaDStream[_]],
      transformFunc: JFunction2[JList[JavaRDD[_]], Time, JavaRDD[T]]
    ): JavaDStream[T] = {
    implicit val cmt: ClassTag[T] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[T]]
    val scalaDStreams = dstreams.map(_.dstream).toSeq
    val scalaTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
      val jrdds = rdds.map(rdd => JavaRDD.fromRDD[AnyRef](rdd.asInstanceOf[RDD[AnyRef]])).toList
      transformFunc.call(jrdds, time).rdd
    }
    ssc.transform(scalaDStreams, scalaTransformFunc)
  }

  /**
   * Create a new DStream in which each RDD is generated by applying a function on RDDs of
   * the DStreams. The order of the JavaRDDs in the transform function parameter will be the
   * same as the order of corresponding DStreams in the list. Note that for adding a
   * JavaPairDStream in the list of JavaDStreams, convert it to a JavaDStream using
   * [[org.apache.spark.streaming.api.java.JavaPairDStream]].toJavaDStream().
   * In the transform function, convert the JavaRDD corresponding to that JavaDStream to
   * a JavaPairRDD using org.apache.spark.api.java.JavaPairRDD.fromJavaRDD().
   */
  def transformToPair[K, V](
      dstreams: JList[JavaDStream[_]],
      transformFunc: JFunction2[JList[JavaRDD[_]], Time, JavaPairRDD[K, V]]
    ): JavaPairDStream[K, V] = {
    implicit val cmk: ClassTag[K] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[K]]
    implicit val cmv: ClassTag[V] =
      implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[V]]
    val scalaDStreams = dstreams.map(_.dstream).toSeq
    val scalaTransformFunc = (rdds: Seq[RDD[_]], time: Time) => {
      val jrdds = rdds.map(rdd => JavaRDD.fromRDD[AnyRef](rdd.asInstanceOf[RDD[AnyRef]])).toList
      transformFunc.call(jrdds, time).rdd
    }
    ssc.transform(scalaDStreams, scalaTransformFunc)
  }

  /**
   * Sets the context to periodically checkpoint the DStream operations for master
   * fault-tolerance. The graph will be checkpointed every batch interval.
   * @param directory HDFS-compatible directory where the checkpoint data will be reliably stored
   */
  def checkpoint(directory: String) {
    ssc.checkpoint(directory)
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

  /** Add a [[org.apache.spark.streaming.scheduler.StreamingListener]] object for
    * receiving system events related to streaming.
    */
  def addStreamingListener(streamingListener: StreamingListener) {
    ssc.addStreamingListener(streamingListener)
  }

  /**
   * Start the execution of the streams.
   */
  def start(): Unit = {
    ssc.start()
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   */
  def awaitTermination(): Unit = {
    ssc.awaitTermination()
  }

  /**
   * Wait for the execution to stop. Any exceptions that occurs during the execution
   * will be thrown in this thread.
   * @param timeout time to wait in milliseconds
   */
  def awaitTermination(timeout: Long): Unit = {
    ssc.awaitTermination(timeout)
  }

  /**
   * Stop the execution of the streams. Will stop the associated JavaSparkContext as well.
   */
  def stop(): Unit = {
    ssc.stop()
  }

  /**
   * Stop the execution of the streams.
   * @param stopSparkContext Stop the associated SparkContext or not
   */
  def stop(stopSparkContext: Boolean) = ssc.stop(stopSparkContext)

  /**
   * Stop the execution of the streams.
   * @param stopSparkContext Stop the associated SparkContext or not
   * @param stopGracefully Stop gracefully by waiting for the processing of all
   *                       received data to be completed
   */
  def stop(stopSparkContext: Boolean, stopGracefully: Boolean) = {
    ssc.stop(stopSparkContext, stopGracefully)
  }

  override def close(): Unit = stop()

}

/**
 * JavaStreamingContext object contains a number of utility functions.
 */
object JavaStreamingContext {
  implicit def fromStreamingContext(ssc: StreamingContext):
    JavaStreamingContext = new JavaStreamingContext(ssc)

  implicit def toStreamingContext(jssc: JavaStreamingContext): StreamingContext = jssc.ssc

  /**
   * Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
   * If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
   * recreated from the checkpoint data. If the data does not exist, then the provided factory
   * will be used to create a JavaStreamingContext.
   *
   * @param checkpointPath Checkpoint directory used in an earlier JavaStreamingContext program
   * @param factory        JavaStreamingContextFactory object to create a new JavaStreamingContext
   */
  def getOrCreate(
      checkpointPath: String,
      factory: JavaStreamingContextFactory
    ): JavaStreamingContext = {
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      factory.create.ssc
    })
    new JavaStreamingContext(ssc)
  }

  /**
   * Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
   * If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
   * recreated from the checkpoint data. If the data does not exist, then the provided factory
   * will be used to create a JavaStreamingContext.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param factory        JavaStreamingContextFactory object to create a new JavaStreamingContext
   * @param hadoopConf     Hadoop configuration if necessary for reading from any HDFS compatible
   *                       file system
   */
  def getOrCreate(
      checkpointPath: String,
      hadoopConf: Configuration,
      factory: JavaStreamingContextFactory
    ): JavaStreamingContext = {
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      factory.create.ssc
    }, hadoopConf)
    new JavaStreamingContext(ssc)
  }

  /**
   * Either recreate a StreamingContext from checkpoint data or create a new StreamingContext.
   * If checkpoint data exists in the provided `checkpointPath`, then StreamingContext will be
   * recreated from the checkpoint data. If the data does not exist, then the provided factory
   * will be used to create a JavaStreamingContext.
   *
   * @param checkpointPath Checkpoint directory used in an earlier StreamingContext program
   * @param factory        JavaStreamingContextFactory object to create a new JavaStreamingContext
   * @param hadoopConf     Hadoop configuration if necessary for reading from any HDFS compatible
   *                       file system
   * @param createOnError  Whether to create a new JavaStreamingContext if there is an
   *                       error in reading checkpoint data.
   */
  def getOrCreate(
      checkpointPath: String,
      hadoopConf: Configuration,
      factory: JavaStreamingContextFactory,
      createOnError: Boolean
    ): JavaStreamingContext = {
    val ssc = StreamingContext.getOrCreate(checkpointPath, () => {
      factory.create.ssc
    }, hadoopConf, createOnError)
    new JavaStreamingContext(ssc)
  }

  /**
   * Find the JAR from which a given class was loaded, to make it easy for users to pass
   * their JARs to StreamingContext.
   */
  def jarOfClass(cls: Class[_]): Array[String] = SparkContext.jarOfClass(cls).toArray
}

/**
 * Factory interface for creating a new JavaStreamingContext
 */
trait JavaStreamingContextFactory {
  def create(): JavaStreamingContext
}
