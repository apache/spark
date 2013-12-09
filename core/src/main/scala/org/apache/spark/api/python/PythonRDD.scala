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
import org.apache.spark.util.Utils
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat
import org.apache.spark.api.java.function.PairFunction
import scala.util.{Success, Failure, Try}
import org.msgpack
import org.msgpack.ScalaMessagePack
import org.apache.hadoop.mapreduce.InputFormat

import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.conf.Configuration
import java.util

private[spark] class PythonRDD[T: ClassManifest](
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

case class TestClass(var id: String, var number: Int) {
  def this() = this("", 0)
}

object TestHadoop extends App {

  //PythonRDD.writeToStream((1, "bar"), new DataOutputStream(new FileOutputStream("/tmp/test.out")))


  //val n = new NullWritable

  import SparkContext._

  val path = "/tmp/spark/test/sfarray/"
  //val path = "/Users/Nick/workspace/java/faunus/output/job-0/"

  val sc = new SparkContext("local[2]", "test")

  //val rdd = sc.sequenceFile[NullWritable, FaunusVertex](path)
  //val data = Seq((1.0, "aa"), (2.0, "bb"), (2.0, "aa"), (3.0, "cc"), (2.0, "bb"), (1.0, "aa"))
  val data = Seq(
    (1, Array(1.0, 2.0, 3.0)),
    (2, Array(3.0, 4.0, 5.0)),
    (3, Array(4.0, 5.0, 6.0))
  )
  val d = new DoubleWritable(5.0)
  val a = new ArrayWritable(classOf[DoubleWritable], Array(d))

  val rdd = sc.parallelize(data, numSlices = 2)
    //.map({ case (k, v) => (new IntWritable(k), v.map(new DoubleWritable(_))) })
    .map{ case (k, v) => (new IntWritable(k), new ArrayWritable(classOf[DoubleWritable], v.map(new DoubleWritable(_)))) }
  rdd.saveAsNewAPIHadoopFile[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[IntWritable, ArrayWritable]](path)

  /*
  val data = Seq(
    ("1", TestClass("test1", 123)),
    ("2", TestClass("test2", 456)),
    ("1", TestClass("test3", 123)),
    ("3", TestClass("test56", 456)),
    ("2", TestClass("test2", 123))
  )
  val rdd = sc.parallelize(data, numSlices = 2).map{ case (k, v) => (new Text(k), v) }
  rdd.saveAsNewAPIHadoopFile(path,
                             classOf[Text], classOf[TestClass],
                             classOf[org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat[Text, TestClass]])

  //val rdd2 = Seq((1, ))

  val seq = sc.sequenceFile[Double, String](path)
  val seqR = seq.collect()

  val packed = PythonRDD.serMsgPack(rdd)
  val packedR = packed.collect()
  val packed2 = PythonRDD.serMsgPack(seq)
  val packedR2 = packed2.collect()

  println(seqR.mkString(","))
  println(packedR.mkString(","))
  println(packedR2.mkString(","))
  */

}

private[spark] object PythonRDD extends Logging {

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
      case e => throw e
    }
    JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
  }

  // PySpark / Hadoop InputFormat stuff

  def register[T](clazz: Class[T], msgpack: ScalaMessagePack) = {
    Try {
      if (!clazz.isPrimitive) msgpack.register(clazz)
    }.getOrElse(log.warn("Failed to register class (%s) with MsgPack. " +
      "Falling back to default MsgPack serialization, or 'toString' as last resort".format(clazz.toString)))
  }

  // serialize and RDD[(K, V)] -> RDD[Array[Byte]] using MsgPack
  def serMsgPack[K, V](rdd: RDD[(K, V)]) = {
    import org.msgpack.ScalaMessagePack._
    val msgpack = new ScalaMessagePack with Serializable
    val first = rdd.first()
    val kc = ClassManifest.fromClass(first._1.getClass).asInstanceOf[ClassManifest[K]].erasure.asInstanceOf[Class[K]]
    val vc = ClassManifest.fromClass(first._2.getClass).asInstanceOf[ClassManifest[V]].erasure.asInstanceOf[Class[V]]
    register(kc, msgpack)
    register(vc, msgpack)
    /*
    Try {
      if (!kc.isPrimitive) msgpack.register(kc)
      if (!vc.isPrimitive) msgpack.register(vc)
    } match {
      case Failure(err) => log.warn(("Failed to register key/value class (%s/%s) with MsgPack. " +
        "Falling back to default MsgPack serialization, or 'toString' as last resort. " +
        "Exception: %s").format(kc, vc, err.getMessage))
    }
    */
    rdd.map{ pair =>
      Try {
        msgpack.write(pair)
      } match {
        case Failure(err) =>
          Try {
            write((pair._1.toString, pair._2.toString))
          } match {
            case Success(result) => result
            case Failure(e) => throw e
          }
        case Success(result) => result

      }
      //write(_)
    }
  }

  // SequenceFile converted to Text and then to String
  def sequenceFile[K ,V](sc: JavaSparkContext,
                         path: String,
                         keyClass: String,
                         valueClass: String,
                         keyWrapper: String,
                         valueWrapper: String,
                         minSplits: Int) = {
    implicit val kcm = ClassManifest.fromClass(Class.forName(keyClass)).asInstanceOf[ClassManifest[K]]
    implicit val vcm = ClassManifest.fromClass(Class.forName(valueClass)).asInstanceOf[ClassManifest[V]]
    val kc = kcm.erasure.asInstanceOf[Class[K]]
    val vc = vcm.erasure.asInstanceOf[Class[V]]

    val rdd = sc.sc.sequenceFile[K, V](path, kc, vc, minSplits)
    val converted = convertRDD[K, V](rdd)
    JavaRDD.fromRDD(serMsgPack[K, V](converted))
    //JavaRDD.fromRDD(
    //  .map{ case (a, b) => (a.toString, b.toString) }.map(stuff => write(stuff)))
  }

  /*
  def sequenceFile[K, V](sc: JavaSparkContext,
                   path: String,
                   keyWrapper: String,
                   valueWrapper: String,
                   minSplits: Int): JavaRDD[Array[Byte]] = {
    val rdd = sc.sc.sequenceFile(path, classOf[Any], classOf[Any], minSplits)
    val converted = convertRDD[K, V](rdd)
    JavaRDD.fromRDD(serMsgPack[K, V](converted))
    //sequenceFile(sc, path, "java.lang.String", "java.lang.String", keyWrapper, valueWrapper, minSplits)
  }
  */

  def mapToConf(map: java.util.HashMap[String, String]) = {
    import collection.JavaConversions._
    val conf = new Configuration()
    map.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }

  /* Merges two configurations, keys from right overwrite any matching keys in left */
  def mergeConfs(left: Configuration, right: Configuration) = {
    import collection.JavaConversions._
    right.iterator().foreach(entry => left.set(entry.getKey, entry.getValue))
    left
  }

  // Arbitrary Hadoop InputFormat, key class and value class
  def newHadoopFile[K, V, F <: NewInputFormat[K, V]](sc: JavaSparkContext,
                                                     path: String,
                                                     inputFormatClazz: String,
                                                     keyClazz: String,
                                                     valueClazz: String,
                                                     keyWrapper: String,
                                                     valueWrapper: String,
                                                     confAsMap: java.util.HashMap[String, String]) = {
    val conf = mapToConf(confAsMap)
    val baseConf = sc.hadoopConfiguration()
    val mergedConf = mergeConfs(baseConf, conf)
    val rdd =
      newHadoopFileFromClassNames[K, V, F](sc,
        path, inputFormatClazz, keyClazz, valueClazz, keyWrapper, valueWrapper, mergedConf)
        //.map{ case (k, v) => (k.toString, v.toString) }
    val converted = convertRDD[K, V](rdd)
    JavaRDD.fromRDD(serMsgPack[K, V](converted))
    //JavaPairRDD.fromRDD(
    //  newHadoopFileFromClassNames(sc, path, inputFormatClazz, keyClazz, valueClazz, keyWrapper, valueWrapper)
    //    .map(new PairFunction[(K, V), String, String] { def call(t: (K, V)) = (t._1.toString, t._2.toString) } )
    //)
  }

  private def newHadoopFileFromClassNames[K, V, F <: NewInputFormat[K, V]](sc: JavaSparkContext,
                                                                           path: String,
                                                                           inputFormatClazz: String,
                                                                           keyClazz: String,
                                                                           valueClazz: String,
                                                                           keyWrapper: String,
                                                                           valueWrapper: String,
                                                                           conf: Configuration) = {
    implicit val kcm = ClassManifest.fromClass(Class.forName(keyClazz)).asInstanceOf[ClassManifest[K]]
    implicit val vcm = ClassManifest.fromClass(Class.forName(valueClazz)).asInstanceOf[ClassManifest[V]]
    implicit val fcm = ClassManifest.fromClass(Class.forName(inputFormatClazz)).asInstanceOf[ClassManifest[F]]
    val kc = kcm.erasure.asInstanceOf[Class[K]]
    val vc = vcm.erasure.asInstanceOf[Class[V]]
    val fc = fcm.erasure.asInstanceOf[Class[F]]
    sc.sc.newAPIHadoopFile(path, fc, kc, vc, conf)
  }

  /*
  private def sequenceFile[K, V](sc: JavaSparkContext,
                         path: String,
                         keyClazz: String,
                         valueClazz: String,
                         keyWrapper: String,
                         valueWrapper: String,
                         minSplits: Int) = {
    implicit val kcm = ClassManifest.fromClass(Class.forName("Any")).asInstanceOf[ClassManifest[K]]
    implicit val vcm = ClassManifest.fromClass(Class.forName("Any")).asInstanceOf[ClassManifest[V]]
    val kc = kcm.erasure.asInstanceOf[Class[K]]
    val vc = vcm.erasure.asInstanceOf[Class[V]]

    val rdd = sc.sc.sequenceFile[K, V](path, kc, vc, minSplits)
    val converted = convertRDD[K, V](rdd)
    JavaRDD.fromRDD(serMsgPack[K, V](converted))

    /*
    val rdd = if (kc.isInstanceOf[Writable] && vc.isInstanceOf[Writable]) {
      val writables = sc.sc.sequenceFile(path, kc.asInstanceOf[Class[Writable]], vc.asInstanceOf[Class[Writable]], minSplits)
      val w = writables.map{case (k,v) => (t.convert(k), t.convert(v))}
      //implicit val kcm = ClassManifest.fromClass(Class.forName(keyClazz)).asInstanceOf[ClassManifest[K <:< Writable]]
      //ClassManifest.fromClass(kc.asInstanceOf[Class[Writable]])
      //sequenceFileWritable(sc, path ,minSplits).asInstanceOf[RDD[(K, V)]]
      //sequenceFileWritable(sc, kc, vc, path, minSplits)
    }
    else {
      sc.sc.sequenceFile[K, V](path, minSplits)

    }

    */
  }
  */

  private def convertRDD[K, V](rdd: RDD[(K, V)]) = {
    rdd.map{
      case (k: Writable, v: Writable) => (convert(k).asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k: Writable, v) => (convert(k).asInstanceOf[K], v.asInstanceOf[V])
      case (k, v: Writable) => (k.asInstanceOf[K], convert(v).asInstanceOf[V])
      case (k, v) => (k.asInstanceOf[K], v.asInstanceOf[V])
    }
  }

  private def convert(writable: Writable): Any = {
    writable match {
      case iw: IntWritable => SparkContext.intWritableConverter().convert(iw)
      case dw: DoubleWritable => SparkContext.doubleWritableConverter().convert(dw)
      case lw: LongWritable => SparkContext.longWritableConverter().convert(lw)
      case fw: FloatWritable => SparkContext.floatWritableConverter().convert(fw)
      case t: Text => SparkContext.stringWritableConverter().convert(t)
      case bw: BooleanWritable => SparkContext.booleanWritableConverter().convert(bw)
      case byw: BytesWritable => SparkContext.bytesWritableConverter().convert(byw)
      case n: NullWritable => None
      case aw: ArrayWritable => aw.get().map(convert(_))
      case mw: MapWritable => mw.map{ case (k, v) => (convert(k), convert(v)) }.toMap
      case other => other
    }
  }

  /*
  def sequenceFileWritable[K, V](sc: JavaSparkContext,
                          path: String,
                          minSplits: Int)
                                //(implicit km: ClassManifest[K], vm: ClassManifest[V])
                                // kcf: () => WritableConverter[K], vcf: () => WritableConverter[V])
                                                = {

    import SparkContext._
    implicit val kcm = ClassManifest.fromClass(keyClazz) //.asInstanceOf[ClassManifest[K]]
    //implicit val vcm = ClassManifest.fromClass(valueClazz) //.asInstanceOf[ClassManifest[V]]
    sc.sc.sequenceFile(path) //, kc, vc, minSplits)
    // JavaRDD.fromRDD(serMsgPack[K, V](rdd))
  }
  */

  //

  def writeToStream(elem: Any, dataOut: DataOutputStream)(implicit m: ClassManifest[Any]) {
    elem match {
      case bytes: Array[Byte] =>
        dataOut.writeInt(bytes.length)
        dataOut.write(bytes)
      case (a: Array[Byte], b: Array[Byte]) =>
        dataOut.writeInt(a.length)
        dataOut.write(a)
        dataOut.writeInt(b.length)
        dataOut.write(b)
      case str: String =>
        dataOut.writeUTF(str)
      //case (a: String, b: String) =>
      //  dataOut.writeUTF(a)
      //  dataOut.writeUTF(b)
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
    implicit val cm : ClassManifest[T] = rdd.elementClassManifest
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
