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

import org.apache.spark.api.java.{JavaSparkContext, JavaPairRDD, JavaRDD}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PipedRDD
import org.apache.spark.util.Utils


private[spark] class PythonRDD[T: ClassManifest](
    parent: RDD[T],
    command: Seq[String],
    envVars: JMap[String, String],
    pythonIncludes: JList[String],
    preservePartitoning: Boolean,
    pythonExec: String,
    broadcastVars: JList[Broadcast[Array[Byte]]],
    accumulator: Accumulator[JList[Array[Byte]]])
  extends RDD[Array[Byte]](parent) {

  val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(parent: RDD[T], command: String, envVars: JMap[String, String],
      pythonIncludes: JList[String],
      preservePartitoning: Boolean, pythonExec: String,
      broadcastVars: JList[Broadcast[Array[Byte]]],
      accumulator: Accumulator[JList[Array[Byte]]]) =
    this(parent, PipedRDD.tokenize(command), envVars, pythonIncludes, preservePartitoning, pythonExec,
      broadcastVars, accumulator)

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
          val printOut = new PrintWriter(stream)
          // Partition index
          dataOut.writeInt(split.index)
          // sparkFilesDir
          PythonRDD.writeAsPickle(SparkFiles.getRootDirectory, dataOut)
          // Broadcast variables
          dataOut.writeInt(broadcastVars.length)
          for (broadcast <- broadcastVars) {
            dataOut.writeLong(broadcast.id)
            dataOut.writeInt(broadcast.value.length)
            dataOut.write(broadcast.value)
          }
          // Python includes (*.zip and *.egg files)
          dataOut.writeInt(pythonIncludes.length)
          for (f <- pythonIncludes) {
            PythonRDD.writeAsPickle(f, dataOut)
          }
          dataOut.flush()
          // Serialized user code
          for (elem <- command) {
            printOut.println(elem)
          }
          printOut.flush()
          // Data values
          for (elem <- parent.iterator(split, context)) {
            PythonRDD.writeAsPickle(elem, dataOut)
          }
          dataOut.flush()
          printOut.flush()
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
            case -3 =>
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
            case -2 =>
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PythonException(new String(obj))
            case -1 =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates; let's do that, breaking when we
              // get a negative length record.
              var len2 = stream.readInt()
              while (len2 >= 0) {
                val update = new Array[Byte](len2)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
                len2 = stream.readInt()
              }
              new Array[Byte](0)
          }
        } catch {
          case eof: EOFException => {
            throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
          }
          case e => throw e
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
  RDD[(Array[Byte], Array[Byte])](prev) {
  override def getPartitions = prev.partitions
  override def compute(split: Partition, context: TaskContext) =
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (a, b)
      case x          => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Array[Byte], Array[Byte]] = JavaPairRDD.fromRDD(this)
}

private[spark] object PythonRDD {

  /** Strips the pickle PROTO and STOP opcodes from the start and end of a pickle */
  def stripPickle(arr: Array[Byte]) : Array[Byte] = {
    arr.slice(2, arr.length - 1)
  }

  /**
   * Write strings, pickled Python objects, or pairs of pickled objects to a data output stream.
   * The data format is a 32-bit integer representing the pickled object's length (in bytes),
   * followed by the pickled data.
   *
   * Pickle module:
   *
   *    http://docs.python.org/2/library/pickle.html
   *
   * The pickle protocol is documented in the source of the `pickle` and `pickletools` modules:
   *
   *    http://hg.python.org/cpython/file/2.6/Lib/pickle.py
   *    http://hg.python.org/cpython/file/2.6/Lib/pickletools.py
   *
   * @param elem the object to write
   * @param dOut a data output stream
   */
  def writeAsPickle(elem: Any, dOut: DataOutputStream) {
    if (elem.isInstanceOf[Array[Byte]]) {
      val arr = elem.asInstanceOf[Array[Byte]]
      dOut.writeInt(arr.length)
      dOut.write(arr)
    } else if (elem.isInstanceOf[scala.Tuple2[Array[Byte], Array[Byte]]]) {
      val t = elem.asInstanceOf[scala.Tuple2[Array[Byte], Array[Byte]]]
      val length = t._1.length + t._2.length - 3 - 3 + 4  // stripPickle() removes 3 bytes
      dOut.writeInt(length)
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(PythonRDD.stripPickle(t._1))
      dOut.write(PythonRDD.stripPickle(t._2))
      dOut.writeByte(Pickle.TUPLE2)
      dOut.writeByte(Pickle.STOP)
    } else if (elem.isInstanceOf[String]) {
      // For uniformity, strings are wrapped into Pickles.
      val s = elem.asInstanceOf[String].getBytes("UTF-8")
      val length = 2 + 1 + 4 + s.length + 1
      dOut.writeInt(length)
      dOut.writeByte(Pickle.PROTO)
      dOut.writeByte(Pickle.TWO)
      dOut.write(Pickle.BINUNICODE)
      dOut.writeInt(Integer.reverseBytes(s.length))
      dOut.write(s)
      dOut.writeByte(Pickle.STOP)
    } else {
      throw new SparkException("Unexpected RDD type")
    }
  }

  def readRDDFromPickleFile(sc: JavaSparkContext, filename: String, parallelism: Int) :
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
      case e => throw e
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  def writeIteratorToPickleFile[T](items: java.util.Iterator[T], filename: String) {
    import scala.collection.JavaConverters._
    writeIteratorToPickleFile(items.asScala, filename)
  }

  def writeIteratorToPickleFile[T](items: Iterator[T], filename: String) {
    val file = new DataOutputStream(new FileOutputStream(filename))
    for (item <- items) {
      writeAsPickle(item, file)
    }
    file.close()
  }

  def takePartition[T](rdd: RDD[T], partition: Int): Iterator[T] = {
    implicit val cm : ClassManifest[T] = rdd.elementClassManifest
    rdd.context.runJob(rdd, ((x: Iterator[T]) => x.toArray), Seq(partition), true).head.iterator
  }
}

private object Pickle {
  val PROTO: Byte = 0x80.toByte
  val TWO: Byte = 0x02.toByte
  val BINUNICODE: Byte = 'X'
  val STOP: Byte = '.'
  val TUPLE2: Byte = 0x86.toByte
  val EMPTY_LIST: Byte = ']'
  val MARK: Byte = '('
  val APPENDS: Byte = 'e'
}

private class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
  override def call(arr: Array[Byte]) : String = new String(arr, "UTF-8")
}

/**
 * Internal class that acts as an `AccumulatorParam` for Python accumulators. Inside, it
 * collects a list of pickled strings that we pass to Python through a socket.
 */
class PythonAccumulatorParam(@transient serverHost: String, serverPort: Int)
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
