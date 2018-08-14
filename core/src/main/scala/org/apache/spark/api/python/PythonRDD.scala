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
import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.language.existentials
import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util._


private[spark] class PythonRDD(
    parent: RDD[_],
    func: PythonFunction,
    preservePartitoning: Boolean)
  extends RDD[Array[Byte]](parent) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuse_worker = conf.getBoolean("spark.python.worker.reuse", true)

  override def getPartitions: Array[Partition] = firstParent.partitions

  override val partitioner: Option[Partitioner] = {
    if (preservePartitoning) firstParent.partitioner else None
  }

  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val runner = PythonRunner(func, bufferSize, reuse_worker)
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

private[spark] object PythonRunner {
  def apply(func: PythonFunction, bufferSize: Int, reuse_worker: Boolean): PythonRunner = {
    new PythonRunner(
      Seq(ChainedPythonFunctions(Seq(func))), bufferSize, reuse_worker, false, Array(Array(0)))
  }
}

/**
 * A helper class to run Python mapPartition/UDFs in Spark.
 *
 * funcs is a list of independent Python functions, each one of them is a list of chained Python
 * functions (from bottom to top).
 */
private[spark] class PythonRunner(
    funcs: Seq[ChainedPythonFunctions],
    bufferSize: Int,
    reuse_worker: Boolean,
    isUDF: Boolean,
    argOffsets: Array[Array[Int]])
  extends Logging {

  require(funcs.length == argOffsets.length, "argOffsets should have the same length as funcs")

  // All the Python functions should have the same exec, version and envvars.
  private val envVars = funcs.head.funcs.head.envVars
  private val pythonExec = funcs.head.funcs.head.pythonExec
  private val pythonVer = funcs.head.funcs.head.pythonVer

  // TODO: support accumulator in multiple UDF
  private val accumulator = funcs.head.funcs.head.accumulator

  def compute(
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext): Iterator[Array[Byte]] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val worker: Socket = env.createPythonWorker(pythonExec, envVars.asScala.toMap)
    // Whether is the worker released into idle pool
    @volatile var released = false

    // Start a thread to feed the process input from our parent's iterator
    val writerThread = new WriterThread(env, worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuse_worker || !released) {
        try {
          worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }

    writerThread.start()
    new MonitorThread(env, worker, context).start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    val stdoutIterator = new Iterator[Array[Byte]] {
      override def next(): Array[Byte] = {
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
              val memoryBytesSpilled = stream.readLong()
              val diskBytesSpilled = stream.readLong()
              context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
              context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
              read()
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj, StandardCharsets.UTF_8),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator.add(update)
              }
              // Check whether the worker is ready to be re-used.
              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
                if (reuse_worker) {
                  env.releasePythonWorker(pythonExec, envVars.asScala.toMap, worker)
                  released = true
                }
              }
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException(context.getKillReason().getOrElse("unknown reason"))

          case e: Exception if env.isStopped =>
            logDebug("Exception thrown after context is stopped", e)
            null  // exit silently

          case e: Exception if writerThread.exception.isDefined =>
            logError("Python worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      override def hasNext: Boolean = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)
  }

  /**
   * The thread responsible for writing the data from the PythonRDD's parent iterator to the
   * Python process.
   */
  class WriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[_],
      partitionIndex: Int,
      context: TaskContext)
    extends Thread(s"stdout writer for $pythonExec") {

    @volatile private var _exception: Exception = null

    private val pythonIncludes = funcs.flatMap(_.funcs.flatMap(_.pythonIncludes.asScala)).toSet
    private val broadcastVars = funcs.flatMap(_.funcs.flatMap(_.broadcastVars.asScala))

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Python process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)
        val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Python version of driver
        PythonRDD.writeUTF(pythonVer, dataOut)
        // Write out the TaskContextInfo
        dataOut.writeInt(context.stageId())
        dataOut.writeInt(context.partitionId())
        dataOut.writeInt(context.attemptNumber())
        dataOut.writeLong(context.taskAttemptId())
        // sparkFilesDir
        PythonRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Python includes (*.zip and *.egg files)
        dataOut.writeInt(pythonIncludes.size)
        for (include <- pythonIncludes) {
          PythonRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        val oldBids = PythonRDD.getWorkerBroadcasts(worker)
        val newBids = broadcastVars.map(_.id).toSet
        // number of different broadcasts
        val toRemove = oldBids.diff(newBids)
        val addedBids = newBids.diff(oldBids)
        val cnt = toRemove.size + addedBids.size
        val needsDecryptionServer = env.serializerManager.encryptionEnabled && addedBids.nonEmpty
        dataOut.writeBoolean(needsDecryptionServer)
        dataOut.writeInt(cnt)
        def sendBidsToRemove(): Unit = {
          for (bid <- toRemove) {
            // remove the broadcast from worker
            dataOut.writeLong(-bid - 1) // bid >= 0
            oldBids.remove(bid)
          }
        }
        if (needsDecryptionServer) {
          // if there is encryption, we setup a server which reads the encrypted files, and sends
            // the decrypted data to python
          val idsAndFiles = broadcastVars.flatMap { broadcast =>
              if (oldBids.contains(broadcast.id)) {
                  None
                } else {
                  Some((broadcast.id, broadcast.value.path))
                }
            }
          val server = new EncryptedPythonBroadcastServer(env, idsAndFiles)
          dataOut.writeInt(server.port)
          logTrace(s"broadcast decryption server setup on ${server.port}")
          PythonRDD.writeUTF(server.secret, dataOut)
          sendBidsToRemove()
          idsAndFiles.foreach { case (id, _) =>
            // send new broadcast
            dataOut.writeLong(id)
            oldBids.add(id)
          }
          dataOut.flush()
          logTrace("waiting for python to read decrypted broadcast data from server")
          server.waitTillBroadcastDataSent()
          logTrace("done sending decrypted data to python")
        } else {
          sendBidsToRemove()
          for (broadcast <- broadcastVars) {
            if (!oldBids.contains(broadcast.id)) {
              // send new broadcast
              dataOut.writeLong(broadcast.id)
              PythonRDD.writeUTF(broadcast.value.path, dataOut)
              oldBids.add(broadcast.id)
            }
          }
        }
        dataOut.flush()
        // Serialized command:
        if (isUDF) {
          dataOut.writeInt(1)
          dataOut.writeInt(funcs.length)
          funcs.zip(argOffsets).foreach { case (chained, offsets) =>
            dataOut.writeInt(offsets.length)
            offsets.foreach { offset =>
              dataOut.writeInt(offset)
            }
            dataOut.writeInt(chained.funcs.length)
            chained.funcs.foreach { f =>
              dataOut.writeInt(f.command.length)
              dataOut.write(f.command)
            }
          }
        } else {
          dataOut.writeInt(0)
          val command = funcs.head.funcs.head.command
          dataOut.writeInt(command.length)
          dataOut.write(command)
        }
        // Data values
        PythonRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!worker.isClosed) {
            Utils.tryLog(worker.shutdownOutput())
          }
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

    /** How long to wait before killing the python worker if a task cannot be interrupted. */
    private val taskKillTimeout = env.conf.getTimeAsMs("spark.python.task.killTimeout", "2s")

    setDaemon(true)

    override def run() {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted && !context.isCompleted) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted) {
        Thread.sleep(taskKillTimeout)
        if (!context.isCompleted) {
          try {
            // Mimic the task name used in `Executor` to help the user find out the task to blame.
            val taskName = s"${context.partitionId}.${context.taskAttemptId} " +
              s"in stage ${context.stageId} (TID ${context.taskAttemptId})"
            logWarning(s"Incomplete task $taskName interrupted: Attempting to kill Python Worker")
            env.destroyPythonWorker(pythonExec, envVars.asScala.toMap, worker)
          } catch {
            case e: Exception =>
              logError("Exception when trying to kill worker", e)
          }
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

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
}

private[spark] object PythonRDD extends Logging {

  // remember the broadcasts sent to each worker
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()

  // Authentication helper used when serving iterator data.
  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new SocketAuthHelper(conf)
  }

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
   * @return 2-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, and the secret for authentication.
   */
  def runJob(
      sc: SparkContext,
      rdd: JavaRDD[Array[Byte]],
      partitions: JArrayList[Int]): Array[Any] = {
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
   * @return 2-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, and the secret for authentication.
   */
  def collectAndServe[T](rdd: RDD[T]): Array[Any] = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  def toLocalIteratorAndServe[T](rdd: RDD[T]): Array[Any] = {
    serveIterator(rdd.toLocalIterator, s"serve toLocalIterator")
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    readRDDFromInputStream(sc.sc, new FileInputStream(filename), parallelism)
  }

  def readRDDFromInputStream(
      sc: SparkContext,
      in: InputStream,
      parallelism: Int): JavaRDD[Array[Byte]] = {
    val din = new DataInputStream(in)
    try {
      val objs = new mutable.ArrayBuffer[Array[Byte]]
      try {
        while (true) {
          val length = din.readInt()
          val obj = new Array[Byte](length)
          din.readFully(obj)
          objs += obj
        }
      } catch {
        case eof: EOFException => // No-op
      }
      JavaRDD.fromRDD(sc.parallelize(objs, parallelism))
    } finally {
      din.close()
    }
  }

  def setupBroadcast(path: String): PythonBroadcast = {
    new PythonBroadcast(path)
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
   *
   * @return 2-tuple (as a Java array) with the port number of a local socket which serves the
   *         data collected from this job, and the secret for authentication.
   */
  def serveIterator(items: Iterator[_], threadName: String): Array[Any] = {
    val (port, secret) = PythonServer.setupOneConnectionServer(authHelper, threadName) { s =>
      val out = new DataOutputStream(new BufferedOutputStream(s.getOutputStream()))
      Utils.tryWithSafeFinally {
        writeIteratorToStream(items, out)
      } {
        out.close()
      }
    }
    Array(port, secret)
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
    private val serverPort: Int,
    private val secretToken: String)
  extends CollectionAccumulator[Array[Byte]] with Logging{

  Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  /**
   * We try to reuse a single Socket to transfer accumulator updates, as they are all added
   * by the DAGScheduler's single-threaded RpcEndpoint anyway.
   */
  @transient private var socket: Socket = _

  private def openSocket(): Socket = synchronized {
    if (socket == null || socket.isClosed) {
      socket = new Socket(serverHost, serverPort)
      logInfo(s"Connected to AccumulatorServer at host: $serverHost port: $serverPort")
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

// scalastyle:off no.finalize
private[spark] class PythonBroadcast(@transient var path: String) extends Serializable
    with Logging {

  private var encryptionServer: PythonServer[Unit] = null

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
  }

  def setupEncryptionServer(): Array[Any] = {
    encryptionServer = new PythonServer[Unit]("broadcast-encrypt-server") {
      override def handleConnection(sock: Socket): Unit = {
        val env = SparkEnv.get
        val in = sock.getInputStream()
        val dir = new File(Utils.getLocalDir(env.conf))
        val file = File.createTempFile("broadcast", "", dir)
        path = file.getAbsolutePath
        val out = env.serializerManager.wrapForEncryption(new FileOutputStream(path))
        DechunkedInputStream.dechunkAndCopyToOutput(in, out)
      }
    }
    Array(encryptionServer.port, encryptionServer.secret)
  }

  def waitTillDataReceived(): Unit = encryptionServer.getResult()
}
// scalastyle:on no.finalize

/**
 * The inverse of pyspark's ChunkedStream for sending broadcast data.
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
      into(0) & 0xFF
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
    return destPos - off
  }

  override def close(): Unit = wrapped.close()
}

/**
 * The inverse of pyspark's ChunkedStream for sending data of unknown size.
 *
 * We might be serializing a really large object from python -- we don't want
 * python to buffer the whole thing in memory, nor can it write to a file,
 * so we don't know the length in advance.  So python writes it in chunks, each chunk
 * preceeded by a length, till we get a "length" of -1 which serves as EOF.
 *
 * Tested from python tests.
 */
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
 * Creates a server in the jvm to communicate with python for handling one batch of data, with
 * authentication and error handling.
 */
private[spark] abstract class PythonServer[T](
    authHelper: SocketAuthHelper,
    threadName: String) {

  def this(env: SparkEnv, threadName: String) = this(new SocketAuthHelper(env.conf), threadName)
  def this(threadName: String) = this(SparkEnv.get, threadName)

  val (port, secret) = PythonServer.setupOneConnectionServer(authHelper, threadName) { sock =>
    promise.complete(Try(handleConnection(sock)))
  }

  /**
   * Handle a connection which has already been authenticated.  Any error from this function
   * will clean up this connection and the entire server, and get propogated to [[getResult]].
   */
  def handleConnection(sock: Socket): T

  val promise = Promise[T]()

  /**
   * Blocks indefinitely for [[handleConnection]] to finish, and returns that result.  If
   * handleConnection throws an exception, this will throw an exception which includes the original
   * exception as a cause.
   */
  def getResult(): T = {
    getResult(Duration.Inf)
  }

  def getResult(wait: Duration): T = {
    ThreadUtils.awaitResult(promise.future, wait)
  }

}

private[spark] object PythonServer {

  /**
   * Create a socket server and run user function on the socket in a background thread.
   *
   * The socket server can only accept one connection, or close if no connection
   * in 15 seconds.
   *
   * The thread will terminate after the supplied user function, or if there are any exceptions.
   *
   * If you need to get a result of the supplied function, create a subclass of [[PythonServer]]
   *
   * @return The port number of a local socket and the secret for authentication.
   */
  def setupOneConnectionServer(
      authHelper: SocketAuthHelper,
      threadName: String)
      (func: Socket => Unit): (Int, String) = {
    val serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))
    // Close the socket if no connection in 15 seconds
    serverSocket.setSoTimeout(15000)

    new Thread(threadName) {
      setDaemon(true)
      override def run(): Unit = {
        var sock: Socket = null
        try {
          sock = serverSocket.accept()
          authHelper.authClient(sock)
          func(sock)
        } finally {
          JavaUtils.closeQuietly(serverSocket)
          JavaUtils.closeQuietly(sock)
        }
      }
    }.start()
    (serverSocket.getLocalPort, authHelper.secret)
  }
}

/**
 * Sends decrypted broadcast data to python worker.  See [[PythonRunner]] for entire protocol.
 */
private[spark] class EncryptedPythonBroadcastServer(
    val env: SparkEnv,
    val idsAndFiles: Seq[(Long, String)])
    extends PythonServer[Unit]("broadcast-decrypt-server") with Logging {

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
    extends PythonServer[JavaRDD[Array[Byte]]]("pyspark-parallelize-server") {

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
