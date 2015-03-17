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

package org.apache.spark.api.r

import java.io._
import java.net.ServerSocket
import java.util.{Map => JMap}

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark._

private abstract class BaseRRDD[T: ClassTag, U: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[U](parent) with Logging {
  override def getPartitions = parent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] = {

    // The parent may be also an RRDD, so we should launch it first.
    val parentIterator = firstParent[T].iterator(split, context)

    // we expect two connections
    val serverSocket = new ServerSocket(0, 2)
    val listenPort = serverSocket.getLocalPort()

    // The stdout/stderr is shared by multiple tasks, because we use one daemon
    // to launch child process as worker.
    val errThread = RRDD.createRWorker(rLibDir, listenPort)

    // We use two sockets to separate input and output, then it's easy to manage
    // the lifecycle of them to avoid deadlock.
    // TODO: optimize it to use one socket

    // the socket used to send out the input of task
    serverSocket.setSoTimeout(10000)
    val inSocket = serverSocket.accept()
    startStdinThread(inSocket.getOutputStream(), parentIterator, split.index)

    // the socket used to receive the output of task
    val outSocket = serverSocket.accept()
    val inputStream = new BufferedInputStream(outSocket.getInputStream)
    val dataStream = openDataStream(inputStream)
    serverSocket.close()

    try {

      return new Iterator[U] {
        def next(): U = {
          val obj = _nextObj
          if (hasNext) {
            _nextObj = read()
          }
          obj
        }

        var _nextObj = read()

        def hasNext(): Boolean = {
          val hasMore = (_nextObj != null)
          if (!hasMore) {
            dataStream.close()
          }
          hasMore
        }
      }
    } catch {
      case e: Exception =>
        throw new SparkException("R computation failed with\n " + errThread.getLines())
    }
  }

  /**
   * Start a thread to write RDD data to the R process.
   */
  private def startStdinThread[T](
    output: OutputStream,
    iter: Iterator[T],
    splitIndex: Int) = {

    val env = SparkEnv.get
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val stream = new BufferedOutputStream(output, bufferSize)

    new Thread("writer for R") {
      override def run() {
        try {
          SparkEnv.set(env)
          val dataOut = new DataOutputStream(stream)
          dataOut.writeInt(splitIndex)

          SerDe.writeString(dataOut, deserializer)
          SerDe.writeString(dataOut, serializer)

          dataOut.writeInt(packageNames.length)
          dataOut.write(packageNames)

          dataOut.writeInt(func.length)
          dataOut.write(func)

          dataOut.writeInt(broadcastVars.length)
          broadcastVars.foreach { broadcast =>
            // TODO(shivaram): Read a Long in R to avoid this cast
            dataOut.writeInt(broadcast.id.toInt)
            // TODO: Pass a byte array from R to avoid this cast ?
            val broadcastByteArr = broadcast.value.asInstanceOf[Array[Byte]]
            dataOut.writeInt(broadcastByteArr.length)
            dataOut.write(broadcastByteArr)
          }

          dataOut.writeInt(numPartitions)

          if (!iter.hasNext) {
            dataOut.writeInt(0)
          } else {
            dataOut.writeInt(1)
          }

          val printOut = new PrintStream(stream)
          for (elem <- iter) {
            if (deserializer == SerializationFormats.BYTE) {
              val elemArr = elem.asInstanceOf[Array[Byte]]
              dataOut.writeInt(elemArr.length)
              dataOut.write(elemArr)
            } else if (deserializer == SerializationFormats.ROW) {
              dataOut.write(elem.asInstanceOf[Array[Byte]])
            } else if (deserializer == SerializationFormats.STRING) {
              printOut.println(elem)
            }
          }
          stream.flush()
        } catch {
          // TODO: We should propogate this error to the task thread
          case e: Exception =>
            logError("R Writer thread got an exception", e)
        } finally {
          Try(output.close())
        }
      }
    }.start()
  }

  protected def openDataStream(input: InputStream): Closeable

  protected def read(): U
}

/**
 * Form an RDD[Int, Array[Byte])] from key-value pairs returned from R.
 * This is used by SparkR's shuffle operations.
 */
private class PairwiseRRDD[T: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    hashFunc: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, (Int, Array[Byte])](
    parent, numPartitions, hashFunc, deserializer,
    SerializationFormats.BYTE, packageNames, rLibDir,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: DataInputStream = _

  override protected def openDataStream(input: InputStream) = {
    dataStream = new DataInputStream(input)
    dataStream
  }

  override protected def read(): (Int, Array[Byte]) = {
    try {
      val length = dataStream.readInt()

      length match {
        case length if length == 2 =>
          val hashedKey = dataStream.readInt()
          val contentPairsLength = dataStream.readInt()
          val contentPairs = new Array[Byte](contentPairsLength)
          dataStream.readFully(contentPairs)
          (hashedKey, contentPairs)
        case _ => null   // End of input
      }
    } catch {
      case eof: EOFException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", eof)
      }
    }
  }

  lazy val asJavaPairRDD : JavaPairRDD[Int, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * An RDD that stores serialized R objects as Array[Byte].
 */
private class RRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, Array[Byte]](parent, -1, func, deserializer,
                                   serializer, packageNames, rLibDir,
                                   broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: DataInputStream = _

  override protected def openDataStream(input: InputStream) = {
    dataStream = new DataInputStream(input)
    dataStream
  }

  override protected def read(): Array[Byte] = {
    try {
      val length = dataStream.readInt()

      length match {
        case length if length > 0 =>
          val obj = new Array[Byte](length)
          dataStream.readFully(obj, 0, length)
          obj
        case _ => null
      }
    } catch {
      case eof: EOFException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", eof)
      }
    }
  }

  lazy val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

/**
 * An RDD that stores R objects as Array[String].
 */
private class StringRRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    rLibDir: String,
    broadcastVars: Array[Object])
  extends BaseRRDD[T, String](parent, -1, func, deserializer, SerializationFormats.STRING,
                              packageNames, rLibDir,
                              broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {

  private var dataStream: BufferedReader = _

  override protected def openDataStream(input: InputStream) = {
    dataStream = new BufferedReader(new InputStreamReader(input))
    dataStream
  }

  override protected def read(): String = {
    try {
      dataStream.readLine()
    } catch {
      case e: IOException => {
        throw new SparkException("R worker exited unexpectedly (crashed)", e)
      }
    }
  }

  lazy val asJavaRDD : JavaRDD[String] = JavaRDD.fromRDD(this)
}

private[spark] class BufferedStreamThread(
    in: InputStream,
    name: String,
    errBufferSize: Int) extends Thread(name) with Logging {
  val lines = new Array[String](errBufferSize)
  var lineIdx = 0
  override def run() {
    for (line <- Source.fromInputStream(in).getLines) {
      synchronized {
        lines(lineIdx) = line
        lineIdx = (lineIdx + 1) % errBufferSize
      }
      logInfo(line)
    }
  }

  def getLines(): String = synchronized {
    (0 until errBufferSize).filter { x =>
      lines((x + lineIdx) % errBufferSize) != null
    }.map { x =>
      lines((x + lineIdx) % errBufferSize)
    }.mkString("\n")
  }
}

object RRDD {
  // Because forking processes from Java is expensive, we prefer to launch
  // a single R daemon (daemon.R) and tell it to fork new workers for our tasks.
  // This daemon currently only works on UNIX-based systems now, so we should
  // also fall back to launching workers (worker.R) directly.
  val inWindows = System.getProperty("os.name").startsWith("Windows")
  private[this] var errThread: BufferedStreamThread = _
  private[this] var daemonChannel: DataOutputStream = _

  def createSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Array[String],
      sparkEnvirMap: JMap[Object, Object],
      sparkExecutorEnvMap: JMap[Object, Object]): JavaSparkContext = {

    val sparkConf = new SparkConf().setAppName(appName)
                                   .setSparkHome(sparkHome)
                                   .setJars(jars)

    // Override `master` if we have a user-specified value
    if (master != "") {
      sparkConf.setMaster(master)
    } else {
      // If conf has no master set it to "local" to maintain
      // backwards compatibility
      sparkConf.setIfMissing("spark.master", "local")
    }

    for ((name, value) <- sparkEnvirMap) {
      sparkConf.set(name.asInstanceOf[String], value.asInstanceOf[String])
    }
    for ((name, value) <- sparkExecutorEnvMap) {
      sparkConf.setExecutorEnv(name.asInstanceOf[String], value.asInstanceOf[String])
    }

    new JavaSparkContext(sparkConf)
  }

  /**
   * Start a thread to print the process's stderr to ours
   */
  private def startStdoutThread(proc: Process): BufferedStreamThread = {
    val BUFFER_SIZE = 100
    val thread = new BufferedStreamThread(proc.getInputStream, "stdout reader for R", BUFFER_SIZE)
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def createRProcess(rLibDir: String, port: Int, script: String) = {
    val rCommand = "Rscript"
    val rOptions = "--vanilla"
    val rExecScript = rLibDir + "/SparkR/worker/" + script
    val pb = new ProcessBuilder(List(rCommand, rOptions, rExecScript))
    // Unset the R_TESTS environment variable for workers.
    // This is set by R CMD check as startup.Rs
    // (http://svn.r-project.org/R/trunk/src/library/tools/R/testing.R)
    // and confuses worker script which tries to load a non-existent file
    pb.environment().put("R_TESTS", "")
    pb.environment().put("SPARKR_RLIBDIR", rLibDir)
    pb.environment().put("SPARKR_WORKER_PORT", port.toString)
    pb.redirectErrorStream(true)  // redirect stderr into stdout
    val proc = pb.start()
    val errThread = startStdoutThread(proc)
    errThread
  }

  /**
   * ProcessBuilder used to launch worker R processes.
   */
  def createRWorker(rLibDir: String, port: Int): BufferedStreamThread = {
    val useDaemon = SparkEnv.get.conf.getBoolean("spark.sparkr.use.daemon", true)
    if (!inWindows && useDaemon) {
      synchronized {
        if (daemonChannel == null) {
          // we expect one connections
          val serverSocket = new ServerSocket(0, 1)
          val daemonPort = serverSocket.getLocalPort
          errThread = createRProcess(rLibDir, daemonPort, "daemon.R")
          // the socket used to send out the input of task
          serverSocket.setSoTimeout(10000)
          val sock = serverSocket.accept()
          daemonChannel = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
          serverSocket.close()
        }
        try {
          daemonChannel.writeInt(port)
          daemonChannel.flush()
        } catch {
          case e: IOException =>
            // daemon process died
            daemonChannel.close()
            daemonChannel = null
            errThread = null
            // fail the current task, retry by scheduler
            throw e
        }
        errThread
      }
    } else {
      createRProcess(rLibDir, port, "worker.R")
    }
  }

  /**
   * Create an RRDD given a sequence of byte arrays. Used to create RRDD when `parallelize` is
   * called from R.
   */
  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr, arr.length))
  }

  def isRunningInYarnContainer(conf: SparkConf): Boolean = {
    // These environment variables are set by YARN.
    // For Hadoop 0.23.X, we check for YARN_LOCAL_DIRS (we use this below in getYarnLocalDirs())
    // For Hadoop 2.X, we check for CONTAINER_ID.
    System.getenv("CONTAINER_ID") != null || System.getenv("YARN_LOCAL_DIRS") != null
  }

  /**
   * Get the path of a temporary directory.  Spark's local directories can be configured through
   * multiple settings, which are used with the following precedence:
   *
   *   - If called from inside of a YARN container, this will return a directory chosen by YARN.
   *   - If the SPARK_LOCAL_DIRS environment variable is set, this will return a directory from it.
   *   - Otherwise, if the spark.local.dir is set, this will return a directory from it.
   *   - Otherwise, this will return java.io.tmpdir.
   *
   * Some of these configuration options might be lists of multiple paths, but this method will
   * always return a single directory.
   */
  def getLocalDir(conf: SparkConf): String = {
    getOrCreateLocalRootDirs(conf)(0)
  }

  /**
   * Gets or creates the directories listed in spark.local.dir or SPARK_LOCAL_DIRS,
   * and returns only the directories that exist / could be created.
   *
   * If no directories could be created, this will return an empty list.
   */
  def getOrCreateLocalRootDirs(conf: SparkConf): Array[String] = {
    val confValue = if (isRunningInYarnContainer(conf)) {
      // If we are in yarn mode, systems can have different disk layouts so we must set it
      // to what Yarn on this system said was available.
      getYarnLocalDirs(conf)
    } else {
      Option(System.getenv("SPARK_LOCAL_DIRS")).getOrElse(
        conf.get("spark.local.dir", System.getProperty("java.io.tmpdir")))
    }
    val rootDirs = confValue.split(',')

    rootDirs.flatMap { rootDir =>
      val localDir: File = new File(rootDir)
      val foundLocalDir = localDir.exists || localDir.mkdirs()
      if (!foundLocalDir) {
        None
      } else {
        Some(rootDir)
      }
    }
  }

  /** Get the Yarn approved local directories. */
  def getYarnLocalDirs(conf: SparkConf): String = {
    // Hadoop 0.23 and 2.x have different Environment variable names for the
    // local dirs, so lets check both. We assume one of the 2 is set.
    // LOCAL_DIRS => 2.X, YARN_LOCAL_DIRS => 0.23.X
    val localDirs = Option(System.getenv("YARN_LOCAL_DIRS"))
      .getOrElse(Option(System.getenv("LOCAL_DIRS"))
      .getOrElse(""))

    if (localDirs.isEmpty) {
      throw new Exception("Yarn Local dirs can't be empty")
    }
    localDirs
  }
}
