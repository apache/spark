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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util._


private[spark] class PythonRDD(
    parent: RDD[_],
    func: PythonFunction,
    preservePartitoning: Boolean)
  extends RDD[Array[Byte]](parent) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuseWorker = conf.getBoolean("spark.python.worker.reuse", true)

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitoning) firstParent.partitioner else None
  }

  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val runner = PythonRunner(func, bufferSize, reuseWorker)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }
}

/**
 * A wrapper for a Python function, contains all necessary context to run the function in Python
 * runner.
 */
private[spark] case class PythonFunction(
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    pythonExec: String,
    pythonVer: String,
    broadcastVars: JList[Broadcast[PythonBroadcast]],
    accumulator: PythonAccumulatorV2)

/**
 * A wrapper for chained Python functions (from bottom to top).
 * @param funcs
 */
private[spark] case class ChainedPythonFunctions(funcs: Seq[PythonFunction])

/** Thrown for exceptions in user Python code. */
private[spark] class PythonException(msg: String, cause: Exception)
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
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()

  def getWorkerBroadcasts(worker: Socket): mutable.Set[Long] = {
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
   * @return the port number of a local socket which serves the data collected from this job.
   */
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int]): Int = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }

  /**
   * A helper function to collect an RDD as an iterator, then serve it via socket.
   *
   * @return the port number of a local socket which serves the data collected from this job.
   */
  def collectAndServe[T](rdd: RDD[T]): Int = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  def toLocalIteratorAndServe[T](rdd: RDD[T]): Int = {
    serveIterator(rdd.toLocalIterator, s"serve toLocalIterator")
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new FileInputStream(filename))
    try {
      val objs = new mutable.ArrayBuffer[Array[Byte]]
      try {
        while (true) {
          val length = file.readInt()
          val obj = new Array[Byte](length)
          file.readFully(obj)
          objs += obj
        }
      } catch {
        case eof: EOFException => // No-op
      }
      JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
    } finally {
      file.close()
    }
  }

  def readBroadcastFromFile(sc: JavaSparkContext, path: String): Broadcast[PythonBroadcast] = {
    sc.broadcast(new PythonBroadcast(path))
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

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

    iter.foreach(write)
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
    val kc = Utils.classForName(keyClass).asInstanceOf[Class[K]]
    val vc = Utils.classForName(valueClass).asInstanceOf[Class[V]]
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
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
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
    val kc = Utils.classForName(keyClass).asInstanceOf[Class[K]]
    val vc = Utils.classForName(valueClass).asInstanceOf[Class[V]]
    val fc = Utils.classForName(inputFormatClass).asInstanceOf[Class[F]]
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
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
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
    val kc = Utils.classForName(keyClass).asInstanceOf[Class[K]]
    val vc = Utils.classForName(valueClass).asInstanceOf[Class[V]]
    val fc = Utils.classForName(inputFormatClass).asInstanceOf[Class[F]]
    if (path.isDefined) {
      sc.sc.hadoopFile(path.get, fc, kc, vc)
    } else {
      sc.sc.hadoopRDD(new JobConf(conf), fc, kc, vc)
    }
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(StandardCharsets.UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
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
   */
  def serveIterator[T](items: Iterator[T], threadName: String): Int = {
    val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
    // Close the socket if no connection in 15 seconds
    serverSocket.setSoTimeout(15000)

    new Thread(threadName) {
      setDaemon(true)
      override def run() {
        try {
          val sock = serverSocket.accept()
          val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
          Utils.tryWithSafeFinally {
            writeIteratorToStream(items, out)
          } {
            out.close()
          }
        } catch {
          case NonFatal(e) =>
            logError(s"Error while sending iterator", e)
        } finally {
          serverSocket.close()
        }
      }
    }.start()

    serverSocket.getLocalPort
  }

  private def getMergedConf(confAsMap: java.util.HashMap[String, String],
      baseConf: Configuration): Configuration = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    PythonHadoopUtil.mergeConfs(baseConf, conf)
  }

  private def inferKeyValueTypes[K, V](rdd: RDD[(K, V)], keyConverterClass: String = null,
      valueConverterClass: String = null): (Class[_], Class[_]) = {
    // Peek at an element to figure out key/value types. Since Writables are not serializable,
    // we cannot call first() on the converted RDD. Instead, we call first() on the original RDD
    // and then convert locally.
    val (key, value) = rdd.first()
    val (kc, vc) = getKeyValueConverters(keyConverterClass, valueConverterClass,
      new JavaToWritableConverter)
    (kc.convert(key).getClass, vc.convert(value).getClass)
  }

  private def getKeyValueTypes(keyClass: String, valueClass: String):
      Option[(Class[_], Class[_])] = {
    for {
      k <- Option(keyClass)
      v <- Option(valueClass)
    } yield (Utils.classForName(k), Utils.classForName(v))
  }

  private def getKeyValueConverters(keyConverterClass: String, valueConverterClass: String,
      defaultConverter: Converter[Any, Any]): (Converter[Any, Any], Converter[Any, Any]) = {
    val keyConverter = Converter.getInstance(Option(keyConverterClass), defaultConverter)
    val valueConverter = Converter.getInstance(Option(valueConverterClass), defaultConverter)
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
    val (kc, vc) = getKeyValueConverters(keyConverterClass, valueConverterClass,
      defaultConverter)
    PythonHadoopUtil.convertRDD(rdd, kc, vc)
  }

  /**
   * Output a Python RDD of key-value pairs as a Hadoop SequenceFile using the Writable types
   * we convert from the RDD's key and value types. Note that keys and values can't be
   * [[org.apache.hadoop.io.Writable]] types already, since Writables are not Java
   * `Serializable` and we can't peek at them. The `path` can be on any Hadoop file system.
   */
  def saveAsSequenceFile[K, V, C <: CompressionCodec](
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
  def saveAsHadoopFile[K, V, F <: OutputFormat[_, _], C <: CompressionCodec](
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
    val fc = Utils.classForName(outputFormatClass).asInstanceOf[Class[F]]
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
  def saveAsNewAPIHadoopFile[K, V, F <: NewOutputFormat[_, _]](
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
  def saveAsHadoopDataset[K, V](
      pyRDD: JavaRDD[Array[Byte]],
      batchSerialized: Boolean,
      confAsMap: java.util.HashMap[String, String],
      keyConverterClass: String,
      valueConverterClass: String,
      useNewAPI: Boolean): Unit = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
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
    private val serverPort: Int)
  extends CollectionAccumulator[Array[Byte]] {

  Utils.checkHost(serverHost)

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  /**
   * We try to reuse a single Socket to transfer accumulator updates, as they are all added
   * by the DAGScheduler's single-threaded RpcEndpoint anyway.
   */
  @transient private var socket: Socket = _

  private def openSocket(): Socket = synchronized {
    if (socket == null || socket.isClosed) {
      socket = new Socket(serverHost, serverPort)
    }
    socket
  }

  // Need to override so the types match with PythonFunction
  override def copyAndReset(): PythonAccumulatorV2 = new PythonAccumulatorV2(serverHost, serverPort)

  override def merge(other: AccumulatorV2[Array[Byte], JList[Array[Byte]]]): Unit = synchronized {
    val otherPythonAccumulator = other.asInstanceOf[PythonAccumulatorV2]
    // This conditional isn't strictly speaking needed - merging only currently happens on the
    // driver program - but that isn't gauranteed so incase this changes.
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

/**
 * A Wrapper for Python Broadcast, which is written into disk by Python. It also will
 * write the data into disk after deserialization, then Python can read it from disks.
 */
// scalastyle:off no.finalize
private[spark] class PythonBroadcast(@transient var path: String) extends Serializable
  with Logging {

  /**
   * Read data from disks, then copy it to `out`
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val in = new FileInputStream(new File(path))
    try {
      Utils.copyStream(in, out)
    } finally {
      in.close()
    }
  }

  /**
   * Write data into disk, using randomly generated name.
   */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val dir = new File(Utils.getLocalDir(SparkEnv.get.conf))
    val file = File.createTempFile("broadcast", "", dir)
    path = file.getAbsolutePath
    val out = new FileOutputStream(file)
    Utils.tryWithSafeFinally {
      Utils.copyStream(in, out)
    } {
      out.close()
    }
  }

  /**
   * Delete the file once the object is GCed.
   */
  override def finalize() {
    if (!path.isEmpty) {
      val file = new File(path)
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file.getPath}")
        }
      }
    }
    super.finalize()
  }
}
// scalastyle:on no.finalize
