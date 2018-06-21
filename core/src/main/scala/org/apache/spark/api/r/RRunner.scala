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
import org.apache.spark.util.Utils

/**
 * A helper class to run R UDFs in Spark.
 */
private[spark] class RRunner[U](
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]],
    numPartitions: Int = -1,
    isDataFrame: Boolean = false,
    colNames: Array[String] = null,
    mode: Int = RRunnerModes.RDD)
  extends Logging {
  private var bootTime: Double = _
  private var dataStream: DataInputStream = _
  val readData = numPartitions match {
    case -1 =>
      serializer match {
        case SerializationFormats.STRING => readStringData _
        case _ => readByteArrayData _
      }
    case _ => readShuffledData _
  }

  def compute(
      inputIterator: Iterator[_],
      partitionIndex: Int): Iterator[U] = {
    // Timing start
    bootTime = System.currentTimeMillis / 1000.0

    // we expect two connections
    val serverSocket = new ServerSocket(0, 2, InetAddress.getByName("localhost"))
    val listenPort = serverSocket.getLocalPort()

    // The stdout/stderr is shared by multiple tasks, because we use one daemon
    // to launch child process as worker.
    val errThread = RRunner.createRWorker(listenPort)

    // We use two sockets to separate input and output, then it's easy to manage
    // the lifecycle of them to avoid deadlock.
    // TODO: optimize it to use one socket

    // the socket used to send out the input of task
    serverSocket.setSoTimeout(10000)
    dataStream = try {
      val inSocket = serverSocket.accept()
      RRunner.authHelper.authClient(inSocket)
      startStdinThread(inSocket.getOutputStream(), inputIterator, partitionIndex)

      // the socket used to receive the output of task
      val outSocket = serverSocket.accept()
      RRunner.authHelper.authClient(outSocket)
      val inputStream = new BufferedInputStream(outSocket.getInputStream)
      new DataInputStream(inputStream)
    } finally {
      serverSocket.close()
    }

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
  private def startStdinThread(
      output: OutputStream,
      iter: Iterator[_],
      partitionIndex: Int): Unit = {
    val env = SparkEnv.get
    val taskContext = TaskContext.get()
    val bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
    val stream = new BufferedOutputStream(output, bufferSize)

    new Thread("writer for R") {
      override def run(): Unit = {
        try {
          SparkEnv.set(env)
          TaskContext.setTaskContext(taskContext)
          val dataOut = new DataOutputStream(stream)
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

          val printOut = new PrintStream(stream)

          def writeElem(elem: Any): Unit = {
            if (deserializer == SerializationFormats.BYTE) {
              val elemArr = elem.asInstanceOf[Array[Byte]]
              dataOut.writeInt(elemArr.length)
              dataOut.write(elemArr)
            } else if (deserializer == SerializationFormats.ROW) {
              dataOut.write(elem.asInstanceOf[Array[Byte]])
            } else if (deserializer == SerializationFormats.STRING) {
              // write string(for StringRRDD)
              // scalastyle:off println
              printOut.println(elem)
              // scalastyle:on println
            }
          }

          for (elem <- iter) {
            elem match {
              case (key, innerIter: Iterator[_]) =>
                for (innerElem <- innerIter) {
                  writeElem(innerElem)
                }
                // Writes key which can be used as a boundary in group-aggregate
                dataOut.writeByte('r')
                writeElem(key)
              case (key, value) =>
                writeElem(key)
                writeElem(value)
              case _ =>
                writeElem(elem)
            }
          }

          stream.flush()
        } catch {
          // TODO: We should propagate this error to the task thread
          case e: Exception =>
            logError("R Writer thread got an exception", e)
        } finally {
          Try(output.close())
        }
      }
    }.start()
  }

  private def read(): U = {
    try {
      val length = dataStream.readInt()

      length match {
        case SpecialLengths.TIMING_DATA =>
          // Timing data from R worker
          val boot = dataStream.readDouble - bootTime
          val init = dataStream.readDouble
          val broadcast = dataStream.readDouble
          val input = dataStream.readDouble
          val compute = dataStream.readDouble
          val output = dataStream.readDouble
          logInfo(
            ("Times: boot = %.3f s, init = %.3f s, broadcast = %.3f s, " +
              "read-input = %.3f s, compute = %.3f s, write-output = %.3f s, " +
              "total = %.3f s").format(
                boot,
                init,
                broadcast,
                input,
                compute,
                output,
                boot + init + broadcast + input + compute + output))
          read()
        case length if length >= 0 =>
          readData(length).asInstanceOf[U]
      }
    } catch {
      case eof: EOFException =>
        throw new SparkException("R worker exited unexpectedly (cranshed)", eof)
    }
  }

  private def readShuffledData(length: Int): (Int, Array[Byte]) = {
    length match {
      case length if length == 2 =>
        val hashedKey = dataStream.readInt()
        val contentPairsLength = dataStream.readInt()
        val contentPairs = new Array[Byte](contentPairsLength)
        dataStream.readFully(contentPairs)
        (hashedKey, contentPairs)
      case _ => null
    }
  }

  private def readByteArrayData(length: Int): Array[Byte] = {
    length match {
      case length if length > 0 =>
        val obj = new Array[Byte](length)
        dataStream.readFully(obj)
        obj
      case _ => null
    }
  }

  private def readStringData(length: Int): String = {
    length match {
      case length if length > 0 =>
        SerDe.readStringBytes(dataStream, length)
      case _ => null
    }
  }
}

private object SpecialLengths {
  val TIMING_DATA = -1
}

private[spark] object RRunnerModes {
  val RDD = 0
  val DATAFRAME_DAPPLY = 1
  val DATAFRAME_GAPPLY = 2
}

private[r] class BufferedStreamThread(
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

private[r] object RRunner {
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

  private def createRProcess(port: Int, script: String): BufferedStreamThread = {
    // "spark.sparkr.r.command" is deprecated and replaced by "spark.r.command",
    // but kept here for backward compatibility.
    val sparkConf = SparkEnv.get.conf
    var rCommand = sparkConf.get("spark.sparkr.r.command", "Rscript")
    rCommand = sparkConf.get("spark.r.command", rCommand)

    val rConnectionTimeout = sparkConf.getInt(
      "spark.r.backendConnectionTimeout", SparkRDefaults.DEFAULT_CONNECTION_TIMEOUT)
    val rOptions = "--vanilla"
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
