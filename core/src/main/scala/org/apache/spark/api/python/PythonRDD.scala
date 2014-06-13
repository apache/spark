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
import java.nio.charset.Charset
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.util.Try

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.{InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.spark._
import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

private[spark] class PythonRDD[T: ClassTag](
    parent: RDD[T],
    command: Array[Byte],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends RDD[Array[Byte]](parent) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)

  override def getPartitions = parent.partitions

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.toMap)

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(env, worker, split, context)

    context.addOnCompleteCallback { () =>
      writerThread.shutdownOnTaskCompletion()

      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception => logWarning("Failed to close worker socket", e)
      }
    }

    writerThread.start()
    new MonitorThread(env, worker, context).start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = new Iterator[Array[Byte]] {
      def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.TIMING_DATA =>
              // Timing data from worker
              val bootTime = stream.readLong()
              val initTime = stream.readLong()
              val finishTime = stream.readLong()
              val boot = bootTime - startTime
              val init = initTime - bootTime
              val finish = finishTime - initTime
              val total = finishTime - startTime
              logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
                init, finish))
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj, "utf-8"),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
              }
              null
          }
        } catch {

          case e: Exception if context.interrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException

          case e: Exception if writerThread.exception.isDefined =>
            logError("Python worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      def hasNext = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  /**
   * The thread responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  class WriterThread(env: SparkEnv, worker: Socket, split: Partition, context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Exception = null

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.completed)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        SparkEnv.set(env)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(split.index)
        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory, dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.length)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        dataOut.writeInt(broadcastVars.length)
        for (broadcast <- broadcastVars) {
          dataOut.writeLong(broadcast.id)
          dataOut.writeInt(broadcast.value.length)
          dataOut.write(broadcast.value)
        }
        dataOut.flush()
        // Serialized command:
        dataOut.writeInt(command.length)
        dataOut.write(command)
        // Data values
        PythonRDD.writeIteratorToStream(parent.iterator(split, context), dataOut)
        dataOut.flush()
      } catch {
        case e: Exception if context.completed || context.interrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
      } finally {
        Try(worker.shutdownOutput()) // kill Python worker process
      }
    }
  }

  /**
   * It is necessary to have a monitor thread for python workers if the user cancels with
   * interrupts disabled. In that case we will need to explicitly kill the worker, otherwise the
   * threads can block indefinitely.
   */
  class MonitorThread(env: SparkEnv, worker: Socket, context: TaskContext)
    extends Thread(s"Worker Monitor for $pythonExec") {

    setDaemon(true)

    override def run() {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.interrupted && !context.completed) {
        Thread.sleep(2000)
      }
      if (!context.completed) {
        try {
          logWarning("Incomplete task interrupted: Attempting to kill Python Worker")
          env.destroyPythonWorker(pythonExec, envVars.toMap)
        } catch {
          case e: Exception =>
            logError("Exception when trying to kill worker", e)
        }
      }
    }
  }
}

/** Thrown for exceptions in user Python code. */
private class PythonException(msg: String, cause: Exception) extends RuntimeException(msg, cause)

/**
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from Python.
 * This is used by PySpark's shuffle operations.
 */
private class PairwiseRDD(prev: RDD[Array[Byte]]) extends
  RDD[(Long, Array[Byte])](prev) {
  override def getPartitions = prev.partitions
  override def compute(split: Partition, context: TaskContext) =
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (Utils.deserializeLongValue(a), b)
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
}

private[spark] object PythonRDD extends Logging {
  val UTF8 = Charset.forName("UTF-8")

  /**
   * Adapter for calling SparkContext#runJob from Python.
   *
   * This method will return an iterator of an array that contains all elements in the RDD
   * (effectively a collect()), but allows you to run on a certain subset of partitions,
   * or to enable local execution.
   */
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int],
      allowLocal: Boolean): Iterator[Array[Byte]] = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions, allowLocal)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    flattenedPartition.iterator
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new FileInputStream(filename))
    val objs = new collection.mutable.ArrayBuffer[Array[Byte]]
    try {
      while (true) {
        val length = file.readInt()
        val obj = new Array[Byte](length)
        file.readFully(obj)
        objs.append(obj)
      }
    } catch {
      case eof: EOFException => {}
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {
    // The right way to implement this would be to use TypeTags to get the full
    // type of T.  Since I don't want to introduce breaking changes throughout the
    // entire Spark API, I have to use this hacky approach:
    if (iter.hasNext) {
      val first = iter.next()
      val newIter = Seq(first).iterator ++ iter
      first match {
        case arr: Array[Byte] =>
          newIter.asInstanceOf[Iterator[Array[Byte]]].foreach { bytes =>
            dataOut.writeInt(bytes.length)
            dataOut.write(bytes)
          }
        case string: String =>
          newIter.asInstanceOf[Iterator[String]].foreach { str =>
            writeUTF(str, dataOut)
          }
        case pair: Tuple2[_, _] =>
          pair._1 match {
            case bytePair: Array[Byte] =>
              newIter.asInstanceOf[Iterator[Tuple2[Array[Byte], Array[Byte]]]].foreach { pair =>
                dataOut.writeInt(pair._1.length)
                dataOut.write(pair._1)
                dataOut.writeInt(pair._2.length)
                dataOut.write(pair._2)
              }
            case stringPair: String =>
              newIter.asInstanceOf[Iterator[Tuple2[String, String]]].foreach { pair =>
                writeUTF(pair._1, dataOut)
                writeUTF(pair._2, dataOut)
              }
            case other =>
              throw new SparkException("Unexpected Tuple2 element type " + pair._1.getClass)
          }
        case other =>
          throw new SparkException("Unexpected element type " + first.getClass)
      }
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
      minSplits: Int) = {
    val keyClass = Option(keyClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    val valueClass = Option(valueClassMaybeNull).getOrElse("org.apache.hadoop.io.Text")
    implicit val kcm = ClassTag(Class.forName(keyClass)).asInstanceOf[ClassTag[K]]
    implicit val vcm = ClassTag(Class.forName(valueClass)).asInstanceOf[ClassTag[V]]
    val kc = kcm.runtimeClass.asInstanceOf[Class[K]]
    val vc = vcm.runtimeClass.asInstanceOf[Class[V]]

    val rdd = sc.sc.sequenceFile[K, V](path, kc, vc, minSplits)
    val keyConverter = Converter.getInstance(Option(keyConverterClass))
    val valueConverter = Converter.getInstance(Option(valueConverterClass))
    val converted = PythonHadoopUtil.convertRDD[K, V](rdd, keyConverter, valueConverter)
    JavaRDD.fromRDD(SerDeUtil.rddToPython(converted))
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
      confAsMap: java.util.HashMap[String, String]) = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    val baseConf = sc.hadoopConfiguration()
    val mergedConf = PythonHadoopUtil.mergeConfs(baseConf, conf)
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val keyConverter = Converter.getInstance(Option(keyConverterClass))
    val valueConverter = Converter.getInstance(Option(valueConverterClass))
    val converted = PythonHadoopUtil.convertRDD[K, V](rdd, keyConverter, valueConverter)
    JavaRDD.fromRDD(SerDeUtil.rddToPython(converted))
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
      confAsMap: java.util.HashMap[String, String]) = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    val rdd =
      newAPIHadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val keyConverter = Converter.getInstance(Option(keyConverterClass))
    val valueConverter = Converter.getInstance(Option(valueConverterClass))
    val converted = PythonHadoopUtil.convertRDD[K, V](rdd, keyConverter, valueConverter)
    JavaRDD.fromRDD(SerDeUtil.rddToPython(converted))
  }

  private def newAPIHadoopRDDFromClassNames[K, V, F <: NewInputFormat[K, V]](
      sc: JavaSparkContext,
      path: Option[String] = None,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      conf: Configuration) = {
    implicit val kcm = ClassTag(Class.forName(keyClass)).asInstanceOf[ClassTag[K]]
    implicit val vcm = ClassTag(Class.forName(valueClass)).asInstanceOf[ClassTag[V]]
    implicit val fcm = ClassTag(Class.forName(inputFormatClass)).asInstanceOf[ClassTag[F]]
    val kc = kcm.runtimeClass.asInstanceOf[Class[K]]
    val vc = vcm.runtimeClass.asInstanceOf[Class[V]]
    val fc = fcm.runtimeClass.asInstanceOf[Class[F]]
    val rdd = if (path.isDefined) {
      sc.sc.newAPIHadoopFile[K, V, F](path.get, fc, kc, vc, conf)
    } else {
      sc.sc.newAPIHadoopRDD[K, V, F](conf, fc, kc, vc)
    }
    rdd
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
      confAsMap: java.util.HashMap[String, String]) = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    val baseConf = sc.hadoopConfiguration()
    val mergedConf = PythonHadoopUtil.mergeConfs(baseConf, conf)
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val keyConverter = Converter.getInstance(Option(keyConverterClass))
    val valueConverter = Converter.getInstance(Option(valueConverterClass))
    val converted = PythonHadoopUtil.convertRDD[K, V](rdd, keyConverter, valueConverter)
    JavaRDD.fromRDD(SerDeUtil.rddToPython(converted))
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
      confAsMap: java.util.HashMap[String, String]) = {
    val conf = PythonHadoopUtil.mapToConf(confAsMap)
    val rdd =
      hadoopRDDFromClassNames[K, V, F](sc,
        None, inputFormatClass, keyClass, valueClass, conf)
    val keyConverter = Converter.getInstance(Option(keyConverterClass))
    val valueConverter = Converter.getInstance(Option(valueConverterClass))
    val converted = PythonHadoopUtil.convertRDD[K, V](rdd, keyConverter, valueConverter)
    JavaRDD.fromRDD(SerDeUtil.rddToPython(converted))
  }

  private def hadoopRDDFromClassNames[K, V, F <: InputFormat[K, V]](
      sc: JavaSparkContext,
      path: Option[String] = None,
      inputFormatClass: String,
      keyClass: String,
      valueClass: String,
      conf: Configuration) = {
    implicit val kcm = ClassTag(Class.forName(keyClass)).asInstanceOf[ClassTag[K]]
    implicit val vcm = ClassTag(Class.forName(valueClass)).asInstanceOf[ClassTag[V]]
    implicit val fcm = ClassTag(Class.forName(inputFormatClass)).asInstanceOf[ClassTag[F]]
    val kc = kcm.runtimeClass.asInstanceOf[Class[K]]
    val vc = vcm.runtimeClass.asInstanceOf[Class[V]]
    val fc = fcm.runtimeClass.asInstanceOf[Class[F]]
    val rdd = if (path.isDefined) {
      sc.sc.hadoopFile(path.get, fc, kc, vc)
    } else {
      sc.sc.hadoopRDD(new JobConf(conf), fc, kc, vc)
    }
    rdd
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(UTF8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }

  def writeToFile[T](items: java.util.Iterator[T], filename: String) {
    import scala.collection.JavaConverters._
    writeToFile(items.asScala, filename)
  }

  def writeToFile[T](items: Iterator[T], filename: String) {
    val file = new DataOutputStream(new FileOutputStream(filename))
    writeIteratorToStream(items, file)
    file.close()
  }

  /**
   * Convert an RDD of serialized Python dictionaries to Scala Maps
   * TODO: Support more Python types.
   */
  def pythonToJavaMap(pyRDD: JavaRDD[Array[Byte]]): JavaRDD[Map[String, _]] = {
    pyRDD.rdd.mapPartitions { iter =>
      val unpickle = new Unpickler
      // TODO: Figure out why flatMap is necessay for pyspark
      iter.flatMap { row =>
        unpickle.loads(row) match {
          case objs: java.util.ArrayList[JMap[String, _] @unchecked] => objs.map(_.toMap)
          // Incase the partition doesn't have a collection
          case obj: JMap[String @unchecked, _] => Seq(obj.toMap)
        }
      }
    }
  }

  /**
   * Convert and RDD of Java objects to and RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(jRDD: JavaRDD[Any]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter =>
      val pickle = new Pickler
      iter.map { row =>
        pickle.dumps(row)
      }
    }
  }
}

private
class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, PythonRDD.UTF8)
}

/**
 * Internal class that acts as an `AccumulatorParam` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
private class PythonAccumulatorParam(@transient serverHost: String, serverPort: Int)
  extends AccumulatorParam[JList[Array[Byte]]] {

  Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList

  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
      : JList[Array[Byte]] = {
    if (serverHost == null) {
      // This happens on the worker node, where we just want to remember all the updates
      val1.addAll(val2)
      val1
    } else {
      // This happens on the master, where we pass the updates to Python through a socket
      val socket = new Socket(serverHost, serverPort)
      val in = socket.getInputStream
      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      out.writeInt(val2.size)
      for (array <- val2) {
        out.writeInt(array.length)
        out.write(array)
      }
      out.flush()
      // Wait for a byte from the Python side as an acknowledgement
      val byteRead = in.read()
      if (byteRead == -1) {
        throw new SparkException("EOF reached before Python server acknowledged")
      }
      socket.close()
      null
    }
  }
}
