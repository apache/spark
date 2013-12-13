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
import java.util.{List => JList, ArrayList => JArrayList, Map => JMap, Collections}

import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
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

  val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt

  override def getPartitions = parent.partitions

  override val partitioner = if (preservePartitoning) parent.partitioner else None

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val worker = env.createPythonWorker(pythonExec, envVars.toMap)

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + pythonExec) {
      override def run() {
        try {
          SparkEnv.set(env)
          val stream = new BufferedOutputStream(worker.getOutputStream, bufferSize)
          val dataOut = new DataOutputStream(stream)
          // Partition index
          dataOut.writeInt(split.index)
          // sparkFilesDir
          dataOut.writeUTF(SparkFiles.getRootDirectory)
          // Broadcast variables
          dataOut.writeInt(broadcastVars.length)
          for (broadcast <- broadcastVars) {
            dataOut.writeLong(broadcast.id)
            dataOut.writeInt(broadcast.value.length)
            dataOut.write(broadcast.value)
          }
          // Python includes (*.zip and *.egg files)
          dataOut.writeInt(pythonIncludes.length)
          pythonIncludes.foreach(dataOut.writeUTF)
          dataOut.flush()
          // Serialized command:
          dataOut.writeInt(command.length)
          dataOut.write(command)
          // Data values
          for (elem <- parent.iterator(split, context)) {
            PythonRDD.writeToStream(elem, dataOut)
          }
          dataOut.flush()
          worker.shutdownOutput()
        } catch {
          case e: IOException =>
            // This can happen for legitimate reasons if the Python code stops returning data before we are done
            // passing elements through, e.g., for take(). Just log a message to say it happened.
            logInfo("stdin writer to Python finished early")
            logDebug("stdin writer to Python finished early", e)
        }
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(worker.getInputStream, bufferSize))
    return new Iterator[Array[Byte]] {
      def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          // FIXME: can deadlock if worker is waiting for us to
          // respond to current message (currently irrelevant because
          // output is shutdown before we read any input)
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case SpecialLengths.TIMING_DATA =>
              // Timing data from worker
              val bootTime = stream.readLong()
              val initTime = stream.readLong()
              val finishTime = stream.readLong()
              val boot = bootTime - startTime
              val init = initTime - bootTime
              val finish = finishTime - initTime
              val total = finishTime - startTime
              logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot, init, finish))
              read
            case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj))
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
              Array.empty[Byte]
          }
        } catch {
          case eof: EOFException => {
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
          }
          case e: Throwable => throw e
        }
      }

      var _nextObj = read()

      def hasNext = _nextObj.length != 0
    }
  }

  val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

/** Thrown for exceptions in user Python code. */
private class PythonException(msg: String) extends Exception(msg)

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
      case x          => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PYTHON_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
}

private[spark] object PythonRDD {

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
      case e: Throwable => throw e
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  def writeToStream(elem: Any, dataOut: DataOutputStream) {
    elem match {
      case bytes: Array[Byte] =>
        dataOut.writeInt(bytes.length)
        dataOut.write(bytes)
      case pair: (Array[Byte], Array[Byte]) =>
        dataOut.writeInt(pair._1.length)
        dataOut.write(pair._1)
        dataOut.writeInt(pair._2.length)
        dataOut.write(pair._2)
      case str: String =>
        dataOut.writeUTF(str)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }
  }

  def writeToFile[T](items: java.util.Iterator[T], filename: String) {
    import scala.collection.JavaConverters._
    writeToFile(items.asScala, filename)
  }

  def writeToFile[T](items: Iterator[T], filename: String) {
    val file = new DataOutputStream(new FileOutputStream(filename))
    for (item <- items) {
      writeToStream(item, file)
    }
    file.close()
  }

  def takePartition[T](rdd: RDD[T], partition: Int): Iterator[T] = {
    implicit val cm : ClassTag[T] = rdd.elementClassTag
    rdd.context.runJob(rdd, ((x: Iterator[T]) => x.toArray), Seq(partition), true).head.iterator
  }
}

private class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, "UTF-8")
}

/**
 * Internal class that acts as an `AccumulatorParam` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
private class PythonAccumulatorParam(@transient serverHost: String, serverPort: Int)
  extends AccumulatorParam[JList[Array[Byte]]] {

  Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt

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
