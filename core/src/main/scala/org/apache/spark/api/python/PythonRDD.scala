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

package org.apache.spark.api.python

import java.io._
import java.net._
import java.nio.charset.StandardCharsets
import java.util.{ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.PythonFunction.PythonAccumulator
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{HOST, PORT}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.security.{SocketAuthHelper, SocketAuthServer, SocketFuncServer}
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util._
import org.apache.spark.util.ArrayImplicits._


private[spark] class PythonRDD(
    parent: RDD[_],
    func: PythonFunction,
    preservePartitioning: Boolean,
    isFromBarrier: Boolean = false)
  extends RDD[Array[Byte]](parent) {

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitioning) firstParent.partitioner else None
  }

  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val runner = PythonRunner(func, jobArtifactUUID)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }

  @transient protected lazy override val isBarrier_ : Boolean =
    isFromBarrier || dependencies.exists(_.rdd.isBarrier())
}

/**
 * A wrapper for a Python function, contains all necessary context to run the function in Python
 * runner.
 */
private[spark] trait PythonFunction {
  def command: Seq[Byte]
  def envVars: JMap[String, String]
  def pythonIncludes: JList[String]
  def pythonExec: String
  def pythonVer: String
  def broadcastVars: JList[Broadcast[PythonBroadcast]]
  def accumulator: PythonAccumulator
}

private[spark] object PythonFunction {
  type PythonAccumulator = CollectionAccumulator[Array[Byte]]
}

/**
 * A simple wrapper for a Python function created via pyspark.
 */
private[spark] case class SimplePythonFunction(
    command: Seq[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: PythonAccumulator) extends PythonFunction {

  def this(
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      pythonExec: String,
      pythonVer: String,
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: PythonAccumulator) = {
    this(command.toImmutableArraySeq,
      envVars, pythonIncludes, pythonExec, pythonVer, broadcastVars, accumulator)
  }
}

/**
 * A wrapper for chained Python functions (from bottom to top).
 * @param funcs
 */
private[spark] case class ChainedPythonFunctions(funcs: Seq[PythonFunction])

/** Thrown for exceptions in user Python code. */
private[spark] class PythonException(msg: String, cause: Throwable)
  extends RuntimeException(msg, cause)

/**
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from Python.
 * This is used by PySpark's shuffle operations.
 */
private class PairwiseRDD(prev: RDD[Array[Byte]]) extends RDD[(Long, Array[Byte])](prev) {
  override def getPartitions: Array[Partition] = prev.partitions
  override val partitioner: Option[Partitioner] = prev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Array[Byte])] =
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (Utils.deserializeLongValue(a), b)
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

private[spark] object PythonRDD extends Logging {

  // remember the broadcasts sent to each worker
  private val workerBroadcasts = new mutable.WeakHashMap[PythonWorker, mutable.Set[Long]]()

  // Authentication helper used when serving iterator data.
  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SocketAuthHelper(conf)
  }

  def getWorkerBroadcasts(worker: PythonWorker): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
  }

  /**
   * Return an RDD of values from an RDD of (Long, Array[Byte]), with preservePartitions=true
   *
   * This is useful for PySpark to have the partitioner after partitionBy()
   */
  def valueOfPair(pair: JavaPairRDD[Long, Array[Byte]]): JavaRDD[Array[Byte]] = {
    pair.rdd.mapPartitions(it => it.map(_._2), true)
  }

  /**
   * Adapter for calling SparkContext#runJob from Python.
   *
   * This method will serve an iterator of an array that contains all elements in the RDD
   * (effectively a collect()), but allows you to run on a certain subset of partitions,
   * or to enable local execution.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int]): Array[Any] = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala.toSeq)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions.toImmutableArraySeq: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def collectAndServe[T](rdd: RDD[T]): Array[Any] = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   * This method is similar with `PythonRDD.collectAndServe`, but user can specify job group id,
   * job description, and interruptOnCancel option.
   */
  def collectAndServeWithJobGroup[T](
      rdd: RDD[T],
      groupId: String,
      description: String,
      interruptOnCancel: Boolean): Array[Any] = {
    val sc = rdd.sparkContext
    sc.setJobGroup(groupId, description, interruptOnCancel)
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  /**
   * A helper function to create a local RDD iterator and serve it via socket. Partitions are
   * are collected as separate jobs, by order of index. Partition data is first requested by a
   * non-zero integer to start a collection job. The response is prefaced by an integer with 1
   * meaning partition data will be served, 0 meaning the local iterator has been consumed,
   * and -1 meaning an error occurred during collection. This function is used by
   * pyspark.rdd._local_iterator_from_socket().
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def toLocalIteratorAndServe[T](rdd: RDD[T], prefetchPartitions: Boolean = false): Array[Any] = {
    val handleFunc = (sock: Socket) => {
      val out = new DataOutputStream(sock.getOutputStream)
      val in = new DataInputStream(sock.getInputStream)
      Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
        // Collects a partition on each iteration
        val collectPartitionIter = rdd.partitions.indices.iterator.map { i =>
          var result: Array[Any] = null
          rdd.sparkContext.submitJob(
            rdd,
            (iter: Iterator[Any]) => iter.toArray,
            Seq(i), // The partition we are evaluating
            (_, res: Array[Any]) => result = res,
            result)
        }
        val prefetchIter = collectPartitionIter.buffered

        // Write data until iteration is complete, client stops iteration, or error occurs
        var complete = false
        while (!complete) {

          // Read request for data, value of zero will stop iteration or non-zero to continue
          if (in.readInt() == 0) {
            complete = true
          } else if (prefetchIter.hasNext) {

            // Client requested more data, attempt to collect the next partition
            val partitionFuture = prefetchIter.next()
            // Cause the next job to be submitted if prefetchPartitions is enabled.
            if (prefetchPartitions) {
              prefetchIter.headOption
            }
            val partitionArray = ThreadUtils.awaitResult(partitionFuture, Duration.Inf)

            // Send response there is a partition to read
            out.writeInt(1)

            // Write the next object and signal end of data for this iteration
            writeIteratorToStream(partitionArray.iterator, out)
            out.writeInt(SpecialLengths.END_OF_DATA_SECTION)
            out.flush()
          } else {
            // Send response there are no more partitions to read and close
            out.writeInt(0)
            complete = true
          }
        }
      })(catchBlock = {
        // Send response that an error occurred, original exception is re-thrown
        out.writeInt(-1)
      }, finallyBlock = {
        out.close()
        in.close()
      })
    }

    val server = new SocketFuncServer(authHelper, "serve toLocalIterator", handleFunc)
    Array(server.port, server.secret, server)
  }

  def readRDDFromFile(
      sc: JavaSparkContext,
      filename: String,
      parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromFile(sc, filename, parallelism)
  }

  def readRDDFromInputStream(
      sc: SparkContext,
      in: InputStream,
      parallelism: Int): JavaRDD[Array[Byte]] = {
    JavaRDD.readRDDFromInputStream(sc, in, parallelism)
  }

  def setupBroadcast(path: String): PythonBroadcast = {
    new PythonBroadcast(path)
  }

  /**
   * Writes the next element of the iterator `iter` to `dataOut`. Returns true if any data was
   * written to the stream. Returns false if no data was written as the iterator has been exhausted.
   */
  def writeNextElementToStream[T](iter: Iterator[T], dataOut: DataOutputStream): Boolean = {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }
    if (iter.hasNext) {
      write(iter.next())
      true
    } else {
      false
    }
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream): Unit = {
    while (writeNextElementToStream(iter, dataOut)) {
      // Nothing.
    }
  }

  /**
   * Create an RDD from a path using [[org.apache.hadoop.mapred.SequenceFileInputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def sequenceFile[K, V](
      sc: JavaSparkContext,
      path: String,
      keyClassMaybeNull: String,
      valueClassMaybeNull: String,
      keyConverterClass: String,
      valueConverterClass: String,
      minSplits: Int,
      batchSize: Int): JavaRDD[Array[Byte]] = {
    val keyClass = Option(keyClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    val valueClass = Option(valueClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val rdd = sc.sc.sequenceFile[K, V](path, kc, vc, minSplits)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(sc.hadoopConfiguration()))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a file path, using an arbitrary [[org.apache.hadoop.mapreduce.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
      sc: JavaSparkContext,
      path: String,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String],
      batchSize: Int): JavaRDD[Array[Byte]] = {
    val mergedConf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(mergedConf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a [[org.apache.hadoop.conf.Configuration]] converted from a map that is
   * passed in from Python, using an arbitrary [[org.apache.hadoop.mapreduce.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      sc: JavaSparkContext,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String],
      batchSize: Int): JavaRDD[Array[Byte]] = {
    val conf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(conf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  private def newAPIHadoopRDDFromClassNames[K, V, F <: NewInputFormat[K, V]](
      sc: JavaSparkContext,
      path: Option[String] = None,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      conf: Configuration): RDD[(K, V)] = {
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val fc = Utils.classForName[F](inputFormatClass)
    if (path.isDefined) {
      sc.sc.newAPIHadoopFile[K, V, F](path.get, fc, kc, vc, conf)
    } else {
      sc.sc.newAPIHadoopRDD[K, V, F](conf, fc, kc, vc)
    }
  }

  /**
   * Create an RDD from a file path, using an arbitrary [[org.apache.hadoop.mapred.InputFormat]],
   * key and value class.
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def hadoopFile[K, V, F <: InputFormat[K, V]](
      sc: JavaSparkContext,
      path: String,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String],
      batchSize: Int): JavaRDD[Array[Byte]] = {
    val mergedConf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(mergedConf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  /**
   * Create an RDD from a [[org.apache.hadoop.conf.Configuration]] converted from a map
   * that is passed in from Python, using an arbitrary [[org.apache.hadoop.mapred.InputFormat]],
   * key and value class
   * A key and/or value converter class can optionally be passed in
   * (see [[org.apache.spark.api.python.Converter]])
   */
  def hadoopRDD[K, V, F <: InputFormat[K, V]](
      sc: JavaSparkContext,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String],
      batchSize: Int): JavaRDD[Array[Byte]] = {
    val conf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(conf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPython(converted, batchSize))
  }

  private def hadoopRDDFromClassNames[K, V, F <: InputFormat[K, V]](
      sc: JavaSparkContext,
      path: Option[String] = None,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      conf: Configuration) = {
    val kc = Utils.classForName[K](keyClass)
    val vc = Utils.classForName[V](valueClass)
    val fc = Utils.classForName[F](inputFormatClass)
    if (path.isDefined) {
      sc.sc.hadoopFile(path.get, fc, kc, vc)
    } else {
      sc.sc.hadoopRDD(new JobConf(conf), fc, kc, vc)
    }
  }

  def writeUTF(str: String, dataOut: DataOutputStream): Unit = {
    PythonWorkerUtils.writeUTF(str, dataOut)
  }

  /**
   * Create a socket server and a background thread to serve the data in `items`,
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * Once a connection comes in, it tries to serialize all the data in `items`
   * and send them into this connection.
   *
   * The thread will terminate after all the data are sent or any exceptions happen.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  def serveIterator(items: Iterator[_], threadName: String): Array[Any] = {
    serveToStream(threadName) { out =>
      writeIteratorToStream(items, new DataOutputStream(out))
    }
  }

  /**
   * Create a socket server and background thread to execute the writeFunc
   * with the given OutputStream.
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * Once a connection comes in, it will execute the block of code and pass in
   * the socket output stream.
   *
   * The thread will terminate after the block of code is executed or any
   * exceptions happen.
   *
   * @return 3-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, the secret for authentication, and a socket auth
   *         server object that can be used to join the JVM serving thread in Python.
   */
  private[spark] def serveToStream(
      threadName: String)(writeFunc: OutputStream => Unit): Array[Any] = {
    SocketAuthServer.serveToStream(threadName, authHelper)(writeFunc)
  }

  private def getMergedConf(confAsMap: java.util.HashMap[String, String],
      baseConf: Configuration): Configuration = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    PythonHadoopUtil.mergeConfs(baseConf, conf)
  }

  private def inferKeyValueTypes[K, V, KK, VV](rdd: RDD[(K, V)], keyConverterClass: String = null,
      valueConverterClass: String = null): (Class[_ <: KK], Class[_ <: VV]) = {
    // Peek at an element to figure out key/value types. Since Writables are not serializable,
    // we cannot call first() on the converted RDD. Instead, we call first() on the original RDD
    // and then convert locally.
    val (key, value) = rdd.first()
    val (kc, vc) = getKeyValueConverters[K, V, KK, VV](
      keyConverterClass, valueConverterClass, new JavaToWritableConverter)
    (kc.convert(key).getClass, vc.convert(value).getClass)
  }

  private def getKeyValueTypes[K, V](keyClass: String, valueClass: String):
      Option[(Class[K], Class[V])] = {
    for {
      k <- Option(keyClass)
      v <- Option(valueClass)
    } yield (Utils.classForName(k), Utils.classForName(v))
  }

  private def getKeyValueConverters[K, V, KK, VV](
      keyConverterClass: String,
      valueConverterClass: String,
      defaultConverter: Converter[_, _]): (Converter[K, KK], Converter[V, VV]) = {
    val keyConverter = Converter.getInstance(Option(keyConverterClass),
      defaultConverter.asInstanceOf[Converter[K, KK]])
    val valueConverter = Converter.getInstance(Option(valueConverterClass),
      defaultConverter.asInstanceOf[Converter[V, VV]])
    (keyConverter, valueConverter)
  }

  /**
   * Convert an RDD of key-value pairs from internal types to serializable types suitable for
   * output, or vice versa.
   */
  private def convertRDD[K, V](rdd: RDD[(K, V)],
                               keyConverterClass: String,
                               valueConverterClass: String,
                               defaultConverter: Converter[Any, Any]): RDD[(Any, Any)] = {
    val (kc, vc) = getKeyValueConverters[K, V, Any, Any](keyConverterClass, valueConverterClass,
      defaultConverter)
    PythonHadoopUtil.convertRDD(rdd, kc, vc)
  }

  /**
   * Output a Python RDD of key-value pairs as a Hadoop SequenceFile using the Writable types
   * we convert from the RDD's key and value types. Note that keys and values can't be
   * [[org.apache.hadoop.io.Writable]] types already, since Writables are not Java
   * `Serializable` and we can't peek at them. The `path` can be on any Hadoop file system.
   */
  def saveAsSequenceFile[C <: CompressionCodec](
      pyRDD: JavaRDD[Array[Byte]],
      batchSerialized: Boolean,
      path: String,
      compressionCodecClass: String): Unit = {
    saveAsHadoopFile(
      pyRDD, batchSerialized, path, "org.apache.hadoop.mapred.SequenceFileOutputFormat",
      null, null, null, null, new java.util.HashMap(), compressionCodecClass)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using old Hadoop
   * `OutputFormat` in mapred package. Keys and values are converted to suitable output
   * types using either user specified converters or, if not specified,
   * [[org.apache.spark.api.python.JavaToWritableConverter]]. Post-conversion types
   * `keyClass` and `valueClass` are automatically inferred if not specified. The passed-in
   * `confAsMap` is merged with the default Hadoop conf associated with the SparkContext of
   * this RDD.
   */
  def saveAsHadoopFile[F <: OutputFormat[_, _], C <: CompressionCodec](
      pyRDD: JavaRDD[Array[Byte]],
      batchSerialized: Boolean,
      path: String,
      outputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String],
      compressionCodecClass: String): Unit = {
    val rdd = SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized)
    val (kc, vc) = getKeyValueTypes(keyClass, valueClass).getOrElse(
      inferKeyValueTypes(rdd, keyConverterClass, valueConverterClass))
    val mergedConf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val codec = Option(compressionCodecClass).map(Utils.classForName(_).asInstanceOf[Class[C]])
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new JavaToWritableConverter)
    val fc = Utils.classForName[F](outputFormatClass)
    converted.saveAsHadoopFile(path, kc, vc, fc, new JobConf(mergedConf), codec = codec)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using new Hadoop
   * `OutputFormat` in mapreduce package. Keys and values are converted to suitable output
   * types using either user specified converters or, if not specified,
   * [[org.apache.spark.api.python.JavaToWritableConverter]]. Post-conversion types
   * `keyClass` and `valueClass` are automatically inferred if not specified. The passed-in
   * `confAsMap` is merged with the default Hadoop conf associated with the SparkContext of
   * this RDD.
   */
  def saveAsNewAPIHadoopFile[F <: NewOutputFormat[_, _]](
      pyRDD: JavaRDD[Array[Byte]],
      batchSerialized: Boolean,
      path: String,
      outputFormatClass: String,
      keyClass: String,
      valueClass: String,
      keyConverterClass: String,
      valueConverterClass: String,
      confAsMap: java.util.HashMap[String, String]): Unit = {
    val rdd = SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized)
    val (kc, vc) = getKeyValueTypes(keyClass, valueClass).getOrElse(
      inferKeyValueTypes(rdd, keyConverterClass, valueConverterClass))
    val mergedConf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new JavaToWritableConverter)
    val fc = Utils.classForName(outputFormatClass).asInstanceOf[Class[F]]
    converted.saveAsNewAPIHadoopFile(path, kc, vc, fc, mergedConf)
  }

  /**
   * Output a Python RDD of key-value pairs to any Hadoop file system, using a Hadoop conf
   * converted from the passed-in `confAsMap`. The conf should set relevant output params (
   * e.g., output path, output format, etc), in the same way as it would be configured for
   * a Hadoop MapReduce job. Both old and new Hadoop OutputFormat APIs are supported
   * (mapred vs. mapreduce). Keys/values are converted for output using either user specified
   * converters or, by default, [[org.apache.spark.api.python.JavaToWritableConverter]].
   */
  def saveAsHadoopDataset(
      pyRDD: JavaRDD[Array[Byte]],
      batchSerialized: Boolean,
      confAsMap: java.util.HashMap[String, String],
      keyConverterClass: String,
      valueConverterClass: String,
      useNewAPI: Boolean): Unit = {
    val conf = getMergedConf(confAsMap, pyRDD.context.hadoopConfiguration)
    val converted = convertRDD(SerDeUtil.pythonToPairRDD(pyRDD, batchSerialized),
      keyConverterClass, valueConverterClass, new JavaToWritableConverter)
    if (useNewAPI) {
      converted.saveAsNewAPIHadoopDataset(conf)
    } else {
      converted.saveAsHadoopDataset(new JobConf(conf))
    }
  }
}

private
class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, StandardCharsets.UTF_8)
}

/**
 * Internal class that acts as an `AccumulatorV2` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
private[spark] class PythonAccumulatorV2(
    @transient private val serverHost: String,
    private val serverPort: Int,
    private val secretToken: String)
  extends CollectionAccumulator[Array[Byte]] with Logging {

  Utils.checkHost(serverHost)

  val bufferSize = SparkEnv.get.conf.get(BUFFER_SIZE)

  /**
   * We try to reuse a single Socket to transfer accumulator updates, as they are all added
   * by the DAGScheduler's single-threaded RpcEndpoint anyway.
   */
  @transient private var socket: Socket = _

  private def openSocket(): Socket = synchronized {
    if (socket == null || socket.isClosed) {
      socket = new Socket(serverHost, serverPort)
      logInfo(log"Connected to AccumulatorServer at host: ${MDC(HOST, serverHost)}" +
        log" port: ${MDC(PORT, serverPort)}")
      // send the secret just for the initial authentication when opening a new connection
      socket.getOutputStream.write(secretToken.getBytes(StandardCharsets.UTF_8))
    }
    socket
  }

  // Need to override so the types match with PythonFunction
  override def copyAndReset(): PythonAccumulatorV2 = {
    new PythonAccumulatorV2(serverHost, serverPort, secretToken)
  }

  override def merge(other: AccumulatorV2[Array[Byte], JList[Array[Byte]]]): Unit = synchronized {
    val otherPythonAccumulator = other.asInstanceOf[PythonAccumulatorV2]
    // This conditional isn't strictly speaking needed - merging only currently happens on the
    // driver program - but that isn't guaranteed so incase this changes.
    if (serverHost == null) {
      // We are on the worker
      super.merge(otherPythonAccumulator)
    } else {
      // This happens on the master, where we pass the updates to Python through a socket
      val socket = openSocket()
      val in = socket.getInputStream
      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      val values = other.value
      out.writeInt(values.size)
      for (array <- values.asScala) {
        out.writeInt(array.length)
        out.write(array)
      }
      out.flush()
      // Wait for a byte from the Python side as an acknowledgement
      val byteRead = in.read()
      if (byteRead == -1) {
        throw new SparkException("EOF reached before Python server acknowledged")
      }
    }
  }
}

private[spark] class PythonBroadcast(@transient var path: String) extends Serializable
    with Logging {

  // id of the Broadcast variable which wrapped this PythonBroadcast
  private var broadcastId: Long = _
  private var encryptionServer: SocketAuthServer[Unit] = null
  private var decryptionServer: SocketAuthServer[Unit] = null

  /**
   * Read data from disks, then copy it to `out`
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    out.writeLong(broadcastId)
    val in = new FileInputStream(new File(path))
    try {
      Utils.copyStream(in, out)
    } finally {
      in.close()
    }
  }

  /**
   * Write data into disk and map it to a broadcast block.
   */
  private def readObject(in: ObjectInputStream): Unit = {
    broadcastId = in.readLong()
    val blockId = BroadcastBlockId(broadcastId, "python")
    val blockManager = SparkEnv.get.blockManager
    val diskBlockManager = blockManager.diskBlockManager
    if (!diskBlockManager.containsBlock(blockId)) {
      Utils.tryOrIOException {
        val dir = new File(Utils.getLocalDir(SparkEnv.get.conf))
        val file = File.createTempFile("broadcast", "", dir)
        val out = new FileOutputStream(file)
        Utils.tryWithSafeFinally {
          val size = Utils.copyStream(in, out)
          val ct = implicitly[ClassTag[Object]]
          // SPARK-28486: map broadcast file to a broadcast block, so that it could be
          // cleared by unpersist/destroy rather than gc(previously).
          val blockStoreUpdater = blockManager.
            TempFileBasedBlockStoreUpdater(blockId, StorageLevel.DISK_ONLY, ct, file, size)
          blockStoreUpdater.save()
        } {
          out.close()
        }
      }
    }
    path = diskBlockManager.getFile(blockId).getAbsolutePath
  }

  def setBroadcastId(bid: Long): Unit = {
    this.broadcastId = bid
  }

  def setupEncryptionServer(): Array[Any] = {
    encryptionServer = new SocketAuthServer[Unit]("broadcast-encrypt-server") {
      override def handleConnection(sock: Socket): Unit = {
        val env = SparkEnv.get
        val in = sock.getInputStream()
        val abspath = new File(path).getAbsolutePath
        val out = env.serializerManager.wrapForEncryption(new FileOutputStream(abspath))
        DechunkedInputStream.dechunkAndCopyToOutput(in, out)
      }
    }
    Array(encryptionServer.port, encryptionServer.secret)
  }

  def setupDecryptionServer(): Array[Any] = {
    decryptionServer = new SocketAuthServer[Unit]("broadcast-decrypt-server-for-driver") {
      override def handleConnection(sock: Socket): Unit = {
        val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()))
        Utils.tryWithSafeFinally {
          val in = SparkEnv.get.serializerManager.wrapForEncryption(new FileInputStream(path))
          Utils.tryWithSafeFinally {
            Utils.copyStream(in, out, false)
          } {
            in.close()
          }
          out.flush()
        } {
          JavaUtils.closeQuietly(out)
        }
      }
    }
    Array(decryptionServer.port, decryptionServer.secret)
  }

  def waitTillBroadcastDataSent(): Unit = decryptionServer.getResult()

  def waitTillDataReceived(): Unit = encryptionServer.getResult()
}

/**
 * The inverse of pyspark's ChunkedStream for sending data of unknown size.
 *
 * We might be serializing a really large object from python -- we don't want
 * python to buffer the whole thing in memory, nor can it write to a file,
 * so we don't know the length in advance.  So python writes it in chunks, each chunk
 * preceded by a length, till we get a "length" of -1 which serves as EOF.
 *
 * Tested from python tests.
 */
private[spark] class DechunkedInputStream(wrapped: InputStream) extends InputStream with Logging {
  private val din = new DataInputStream(wrapped)
  private var remainingInChunk = din.readInt()

  override def read(): Int = {
    val into = new Array[Byte](1)
    val n = read(into, 0, 1)
    if (n == -1) {
      -1
    } else {
      // if you just cast a byte to an int, then anything > 127 is negative, which is interpreted
      // as an EOF
      val b = into(0)
      if (b < 0) {
        256 + b
      } else {
        b
      }
    }
  }

  override def read(dest: Array[Byte], off: Int, len: Int): Int = {
    if (remainingInChunk == -1) {
      return -1
    }
    var destSpace = len
    var destPos = off
    while (destSpace > 0 && remainingInChunk != -1) {
      val toCopy = math.min(remainingInChunk, destSpace)
      val read = din.read(dest, destPos, toCopy)
      destPos += read
      destSpace -= read
      remainingInChunk -= read
      if (remainingInChunk == 0) {
        remainingInChunk = din.readInt()
      }
    }
    assert(destSpace == 0 || remainingInChunk == -1)
    destPos - off
  }

  override def close(): Unit = wrapped.close()
}

private[spark] object DechunkedInputStream {

  /**
   * Dechunks the input, copies to output, and closes both input and the output safely.
   */
  def dechunkAndCopyToOutput(chunked: InputStream, out: OutputStream): Unit = {
    val dechunked = new DechunkedInputStream(chunked)
    Utils.tryWithSafeFinally {
      Utils.copyStream(dechunked, out)
    } {
      JavaUtils.closeQuietly(out)
      JavaUtils.closeQuietly(dechunked)
    }
  }
}

/**
 * Sends decrypted broadcast data to python worker.  See [[PythonRunner]] for entire protocol.
 */
private[spark] class EncryptedPythonBroadcastServer(
    val env: SparkEnv,
    val idsAndFiles: Seq[(Long, String)])
    extends SocketAuthServer[Unit]("broadcast-decrypt-server") with Logging {

  override def handleConnection(socket: Socket): Unit = {
    val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))
    var socketIn: InputStream = null
    // send the broadcast id, then the decrypted data.  We don't need to send the length, the
    // the python pickle module just needs a stream.
    Utils.tryWithSafeFinally {
      (idsAndFiles).foreach { case (id, path) =>
        out.writeLong(id)
        val in = env.serializerManager.wrapForEncryption(new FileInputStream(path))
        Utils.tryWithSafeFinally {
          Utils.copyStream(in, out, false)
        } {
          in.close()
        }
      }
      logTrace("waiting for python to accept broadcast data over socket")
      out.flush()
      socketIn = socket.getInputStream()
      socketIn.read()
      logTrace("done serving broadcast data")
    } {
      JavaUtils.closeQuietly(socketIn)
      JavaUtils.closeQuietly(out)
    }
  }

  def waitTillBroadcastDataSent(): Unit = {
    getResult()
  }
}

/**
 * Helper for making RDD[Array[Byte]] from some python data, by reading the data from python
 * over a socket.  This is used in preference to writing data to a file when encryption is enabled.
 */
private[spark] abstract class PythonRDDServer
    extends SocketAuthServer[JavaRDD[Array[Byte]]]("pyspark-parallelize-server") {

  def handleConnection(sock: Socket): JavaRDD[Array[Byte]] = {
    val in = sock.getInputStream()
    val dechunkedInput: InputStream = new DechunkedInputStream(in)
    streamToRDD(dechunkedInput)
  }

  protected def streamToRDD(input: InputStream): RDD[Array[Byte]]

}

private[spark] class PythonParallelizeServer(sc: SparkContext, parallelism: Int)
    extends PythonRDDServer {

  override protected def streamToRDD(input: InputStream): RDD[Array[Byte]] = {
    PythonRDD.readRDDFromInputStream(sc, input, parallelism)
  }
}
