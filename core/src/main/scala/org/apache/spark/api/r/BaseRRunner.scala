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
import java.net.{InetAddress, ServerSocket}
import java.util.Arrays

import scala.io.Source
import scala.util.Try

import org.apache.spark._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.internal.config.R._
import org.apache.spark.util.Utils

/**
 * A helper class to run R UDFs in Spark.
 */
private[spark] abstract class BaseRRunner[IN, OUT](
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    numPartitions: Int,
    isDataFrame: Boolean,
    colNames: Array[String],
    mode: Int)
  extends Logging {
  protected var bootTime: Double = _
  protected var dataStream: DataInputStream = _

  def compute(
      inputIterator: Iterator[IN],
      partitionIndex: Int): Iterator[OUT] = {
    // Timing start
    bootTime = System.currentTimeMillis / 1000.0

    // we expect two connections
    val serverSocket = new ServerSocket(0, 2, InetAddress.getByName("localhost"))
    val listenPort = serverSocket.getLocalPort()

    // The stdout/stderr is shared by multiple tasks, because we use one daemon
    // to launch child process as worker.
    val errThread = BaseRRunner.createRWorker(listenPort)

    // We use two sockets to separate input and output, then it's easy to manage
    // the lifecycle of them to avoid deadlock.
    // TODO: optimize it to use one socket

    // the socket used to send out the input of task
    serverSocket.setSoTimeout(10000)
    dataStream = try {
      val inSocket = serverSocket.accept()
      BaseRRunner.authHelper.authClient(inSocket)
      newWriterThread(inSocket.getOutputStream(), inputIterator, partitionIndex).start()

      // the socket used to receive the output of task
      val outSocket = serverSocket.accept()
      BaseRRunner.authHelper.authClient(outSocket)
      val inputStream = new BufferedInputStream(outSocket.getInputStream)
      new DataInputStream(inputStream)
    } finally {
      serverSocket.close()
    }

    newReaderIterator(dataStream, errThread)
  }

  /**
   * Creates an iterator that reads data from R process.
   */
  protected def newReaderIterator(
      dataStream: DataInputStream, errThread: BufferedStreamThread): ReaderIterator

  /**
   * Start a thread to write RDD data to the R process.
   */
  protected def newWriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int): WriterThread

  abstract class ReaderIterator(
      stream: DataInputStream,
      errThread: BufferedStreamThread)
    extends Iterator[OUT] {

    private var nextObj: OUT = _
    // eos should be marked as true when the stream is ended.
    protected var eos = false

    override def hasNext: Boolean = nextObj != null || {
      if (!eos) {
        nextObj = read()
        hasNext
      } else {
        false
      }
    }

    override def next(): OUT = {
      if (hasNext) {
        val obj = nextObj
        nextObj = null.asInstanceOf[OUT]
        obj
      } else {
        Iterator.empty.next()
      }
    }

    /**
     * Reads next object from the stream.
     * When the stream reaches end of data, needs to process the following sections,
     * and then returns null.
     */
    protected def read(): OUT

    protected val handleException: PartialFunction[Throwable, OUT] = {
      case e: Exception =>
        var msg = "R unexpectedly exited."
        val lines = errThread.getLines()
        if (lines.trim().nonEmpty) {
          msg += s"\nR worker produced errors: $lines\n"
        }
        throw new SparkException(msg, e)
    }
  }

  /**
   * The thread responsible for writing the iterator to the R process.
   */
  abstract class WriterThread(
      output: OutputStream,
      iter: Iterator[IN],
      partitionIndex: Int)
    extends Thread("writer for R") {

    private val env = SparkEnv.get
    private val taskContext = TaskContext.get()
    private val bufferSize = System.getProperty(BUFFER_SIZE.key,
      BUFFER_SIZE.defaultValueString).toInt
    private val stream = new BufferedOutputStream(output, bufferSize)
    protected lazy val dataOut = new DataOutputStream(stream)
    protected lazy val printOut = new PrintStream(stream)

    override def run(): Unit = {
      try {
        SparkEnv.set(env)
        TaskContext.setTaskContext(taskContext)
        dataOut.writeInt(partitionIndex)

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
        dataOut.writeInt(mode)

        if (isDataFrame) {
          SerDe.writeObject(dataOut, colNames, jvmObjectTracker = null)
        }

        if (!iter.hasNext) {
          dataOut.writeInt(0)
        } else {
          dataOut.writeInt(1)
        }

        writeIteratorToStream(dataOut)

        stream.flush()
      } catch {
        // TODO: We should propagate this error to the task thread
        case e: Exception =>
          logError("R Writer thread got an exception", e)
      } finally {
        Try(output.close())
      }
    }

    /**
     * Writes input data to the stream connected to the R worker.
     */
    protected def writeIteratorToStream(dataOut: DataOutputStream): Unit
  }
}

private[spark] object SpecialLengths {
  val TIMING_DATA = -1
}

private[spark] object RRunnerModes {
  val RDD = 0
  val DATAFRAME_DAPPLY = 1
  val DATAFRAME_GAPPLY = 2
}

private[spark] class BufferedStreamThread(
    in: InputStream,
    name: String,
    errBufferSize: Int) extends Thread(name) with Logging {
  val lines = new Array[String](errBufferSize)
  var lineIdx = 0
  override def run(): Unit = {
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

private[r] object BaseRRunner {
  // Because forking processes from Java is expensive, we prefer to launch
  // a single R daemon (daemon.R) and tell it to fork new workers for our tasks.
  // This daemon currently only works on UNIX-based systems now, so we should
  // also fall back to launching workers (worker.R) directly.
  private[this] var errThread: BufferedStreamThread = _
  private[this] var daemonChannel: DataOutputStream = _

  private lazy val authHelper = {
    val conf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new RAuthHelper(conf)
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

  private[r] def getROptions(rCommand: String): String = Try {
    val result = scala.sys.process.Process(Seq(rCommand, "--version")).!!
    "([0-9]+)\\.([0-9]+)\\.([0-9]+)".r.findFirstMatchIn(result).map { m =>
      val major = m.group(1).toInt
      val minor = m.group(2).toInt
      val shouldUseNoRestore = major > 4 || major == 4 && minor >= 2
      if (shouldUseNoRestore) "--no-restore" else "--vanilla"
    }.getOrElse("--vanilla")
  }.getOrElse("--vanilla")

  private def createRProcess(port: Int, script: String): BufferedStreamThread = {
    // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
    // but kept here for backward compatibility.
    val sparkConf = SparkEnv.get.conf
    var rCommand = sparkConf.get(SPARKR_COMMAND)
    rCommand = sparkConf.get(R_COMMAND).orElse(Some(rCommand)).get

    val rConnectionTimeout = sparkConf.get(R_BACKEND_CONNECTION_TIMEOUT)
    val rOptions = getROptions(rCommand)
    val rLibDir = RUtils.sparkRPackagePath(isDriver = false)
    val rExecScript = rLibDir(0) + "/SparkR/worker/" + script
    val pb = new ProcessBuilder(Arrays.asList(rCommand, rOptions, rExecScript))
    // Unset the R_TESTS environment variable for workers.
    // This is set by R CMD check as startup.Rs
    // (http://svn.r-project.org/R/trunk/src/library/tools/R/testing.R)
    // and confuses worker script which tries to load a non-existent file
    pb.environment().put("R_TESTS", "")
    pb.environment().put("SPARKR_RLIBDIR", rLibDir.mkString(","))
    pb.environment().put("SPARKR_WORKER_PORT", port.toString)
    pb.environment().put("SPARKR_BACKEND_CONNECTION_TIMEOUT", rConnectionTimeout.toString)
    pb.environment().put("SPARKR_SPARKFILES_ROOT_DIR", SparkFiles.getRootDirectory())
    pb.environment().put("SPARKR_IS_RUNNING_ON_WORKER", "TRUE")
    pb.environment().put("SPARKR_WORKER_SECRET", authHelper.secret)
    pb.redirectErrorStream(true)  // redirect stderr into stdout
    val proc = pb.start()
    val errThread = startStdoutThread(proc)
    errThread
  }

  /**
   * ProcessBuilder used to launch worker R processes.
   */
  def createRWorker(port: Int): BufferedStreamThread = {
    val useDaemon = SparkEnv.get.conf.getBoolean("spark.sparkr.use.daemon", true)
    if (!Utils.isWindows && useDaemon) {
      synchronized {
        if (daemonChannel == null) {
          // we expect one connections
          val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
          val daemonPort = serverSocket.getLocalPort
          errThread = createRProcess(daemonPort, "daemon.R")
          // the socket used to send out the input of task
          serverSocket.setSoTimeout(10000)
          val sock = serverSocket.accept()
          try {
            authHelper.authClient(sock)
            daemonChannel = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
          } finally {
            serverSocket.close()
          }
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
      createRProcess(port, "worker.R")
    }
  }
}
