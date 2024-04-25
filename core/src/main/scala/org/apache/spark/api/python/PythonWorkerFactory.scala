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

import java.io.{DataInputStream, DataOutputStream, EOFException, File, InputStream}
import java.net.{InetAddress, InetSocketAddress, SocketException}
import java.net.SocketTimeoutException
import java.nio.channels._
import java.util.Arrays
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import org.apache.spark._
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.Logging
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.{RedirectThread, Utils}

case class PythonWorker(channel: SocketChannel, selector: Selector, selectionKey: SelectionKey) {
  def stop(): Unit = {
    Option(selectionKey).foreach(_.cancel())
    selector.close()
    channel.close()
  }
}

private[spark] class PythonWorkerFactory(
    pythonExec: String,
    workerModule: String,
    daemonModule: String,
    envVars: Map[String, String],
    val useDaemonEnabled: Boolean)
  extends Logging { self =>

  def this(
      pythonExec: String,
      workerModule: String,
      envVars: Map[String, String],
      useDaemonEnabled: Boolean) =
    this(pythonExec, workerModule, PythonWorkerFactory.defaultDaemonModule,
      envVars, useDaemonEnabled)

  import PythonWorkerFactory._

  // Because forking processes from Java is expensive, we prefer to launch a single Python daemon,
  // pyspark/daemon.py (by default) and tell it to fork new workers for our tasks. This daemon
  // currently only works on UNIX-based systems now because it uses signals for child management,
  // so we can also fall back to launching workers, pyspark/worker.py (by default) directly.
  private val useDaemon = {
    // This flag is ignored on Windows as it's unable to fork.
    !System.getProperty("os.name").startsWith("Windows") && useDaemonEnabled
  }

  private val authHelper = new SocketAuthHelper(SparkEnv.get.conf)

  @GuardedBy("self")
  private var daemon: Process = null
  val daemonHost = InetAddress.getLoopbackAddress()
  @GuardedBy("self")
  private var daemonPort: Int = 0
  @GuardedBy("self")
  private val daemonWorkers = new mutable.WeakHashMap[PythonWorker, ProcessHandle]()
  @GuardedBy("self")
  private val idleWorkers = new mutable.Queue[PythonWorker]()
  @GuardedBy("self")
  private var lastActivityNs = 0L
  new MonitorThread().start()

  @GuardedBy("self")
  private val simpleWorkers = new mutable.WeakHashMap[PythonWorker, Process]()

  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  def create(): (PythonWorker, Option[Int]) = {
    if (useDaemon) {
      self.synchronized {
        // Pull from idle workers until we one that is alive, otherwise create a new one.
        while (idleWorkers.nonEmpty) {
          val worker = idleWorkers.dequeue()
          val workerHandle = daemonWorkers(worker)
          if (workerHandle.isAlive()) {
            try {
              worker.selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE)
              return (worker, Some(workerHandle.pid().toInt))
            } catch {
              case c: CancelledKeyException => /* pass */
            }
          }
          logWarning(s"Worker ${worker} process from idle queue is dead, discarding.")
          stopWorker(worker)
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker(blockingMode = false)
    }
  }

  /**
   * Connect to a worker launched through pyspark/daemon.py (by default), which forks python
   * processes itself to avoid the high cost of forking from Java. This currently only works
   * on UNIX-based systems.
   */
  private def createThroughDaemon(): (PythonWorker, Option[Int]) = {

    def createWorker(): (PythonWorker, Option[Int]) = {
      val socketChannel = SocketChannel.open(new InetSocketAddress(daemonHost, daemonPort))
      // These calls are blocking.
      val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }
      val processHandle = ProcessHandle.of(pid).orElseThrow(
        () => new IllegalStateException("Python daemon failed to launch worker.")
      )
      authHelper.authToServer(socketChannel.socket())
      socketChannel.configureBlocking(false)
      val selector = Selector.open()
      val selectionKey = socketChannel.register(selector,
        SelectionKey.OP_READ | SelectionKey.OP_WRITE)
      val worker = PythonWorker(socketChannel, selector, selectionKey)
      daemonWorkers.put(worker, processHandle)
      (worker, Some(pid))
    }

    self.synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createWorker()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createWorker()
      }
    }
  }

  /**
   * Launch a worker by executing worker.py (by default) directly and telling it to connect to us.
   */
  private[spark] def createSimpleWorker(blockingMode: Boolean): (PythonWorker, Option[Int]) = {
    var serverSocketChannel: ServerSocketChannel = null
    try {
      serverSocketChannel = ServerSocketChannel.open()
      serverSocketChannel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 1)

      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(pythonExec, "-m", workerModule))
      val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
      if (jobArtifactUUID != "default") {
        val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
        f.mkdir()
        pb.directory(f)
      }
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocketChannel.socket().getLocalPort
        .toString)
      workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      if (Utils.preferIPv6) {
        workerEnv.put("SPARK_PREFER_IPV6", "True")
      }
      val workerProcess = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(workerProcess.getInputStream, workerProcess.getErrorStream)

      // Wait for it to connect to our socket, and validate the auth secret.
      try {
        // Wait up to 10 seconds for client to connect.
        serverSocketChannel.configureBlocking(false)
        val serverSelector = Selector.open()
        serverSocketChannel.register(serverSelector, SelectionKey.OP_ACCEPT)
        val socketChannel =
          if (serverSelector.select(10 * 1000) > 0) { // Wait up to 10 seconds.
            serverSocketChannel.accept()
          } else {
            throw new SocketTimeoutException(
              "Timed out while waiting for the Python worker to connect back")
          }
        authHelper.authClient(socketChannel.socket())
        // TODO: When we drop JDK 8, we can just use workerProcess.pid()
        val pid = new DataInputStream(Channels.newInputStream(socketChannel)).readInt()
        if (pid < 0) {
          throw new IllegalStateException("Python failed to launch worker with code " + pid)
        }
        if (!blockingMode) {
          socketChannel.configureBlocking(false)
        }
        val selector = Selector.open()
        val selectionKey = if (blockingMode) {
          null
        } else {
          socketChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE)
        }
        val worker = PythonWorker(socketChannel, selector, selectionKey)
        self.synchronized {
          simpleWorkers.put(worker, workerProcess)
        }
        return (worker, Some(pid))
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker failed to connect back.", e)
      }
    } finally {
      if (serverSocketChannel != null) {
        serverSocketChannel.close()
      }
    }
    null
  }

  private def startDaemon(): Unit = {
    self.synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val command = Arrays.asList(pythonExec, "-m", daemonModule, workerModule)
        val pb = new ProcessBuilder(command)
        val jobArtifactUUID = envVars.getOrElse("SPARK_JOB_ARTIFACT_UUID", "default")
        if (jobArtifactUUID != "default") {
          val f = new File(SparkFiles.getRootDirectory(), jobArtifactUUID)
          f.mkdir()
          pb.directory(f)
        }
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        if (Utils.preferIPv6) {
          workerEnv.put("SPARK_PREFER_IPV6", "True")
        }
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          daemonPort = in.readInt()
        } catch {
          case _: EOFException if daemon.isAlive =>
            throw SparkCoreErrors.eofExceptionWhileReadPortNumberError(
              daemonModule)
          case _: EOFException =>
            throw SparkCoreErrors.
              eofExceptionWhileReadPortNumberError(daemonModule, Some(daemon.exitValue))
        }

        // test that the returned port number is within a valid range.
        // note: this does not cover the case where the port number
        // is arbitrary data but is also coincidentally within range
        if (daemonPort < 1 || daemonPort > 0xffff) {
          val exceptionMessage = f"""
            |Bad data in $daemonModule's standard output. Invalid port number:
            |  $daemonPort (0x$daemonPort%08x)
            |Python command to execute the daemon was:
            |  ${command.asScala.mkString(" ")}
            |Check that you don't have any unexpected modules or libraries in
            |your PYTHONPATH:
            |  $pythonPath
            |Also, check if you have a sitecustomize.py module in your python path,
            |or in your python installation, that is printing to standard output"""
          throw new SparkException(exceptionMessage.stripMargin)
        }

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)
      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
              |Error from python worker:
              |  $formattedStderr
              |PYTHONPATH was:
              |  $pythonPath
              |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream): Unit = {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + pythonExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + pythonExec).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
   * Monitor all the idle workers, kill them after timeout.
   */
  private class MonitorThread extends Thread(s"Idle Worker Monitor for $pythonExec") {

    setDaemon(true)

    override def run(): Unit = {
      while (true) {
        self.synchronized {
          if (IDLE_WORKER_TIMEOUT_NS < System.nanoTime() - lastActivityNs) {
            cleanupIdleWorkers()
            lastActivityNs = System.nanoTime()
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  private def cleanupIdleWorkers(): Unit = {
    while (idleWorkers.nonEmpty) {
      val worker = idleWorkers.dequeue()
      try {
        worker.stop()
      } catch {
        case e: Exception =>
          logWarning("Failed to stop worker socket", e)
      }
    }
  }

  private def stopDaemon(): Unit = {
    self.synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
      } else {
        simpleWorkers.values.foreach(_.destroy())
      }
    }
  }

  def stop(): Unit = {
    stopDaemon()
  }

  def stopWorker(worker: PythonWorker): Unit = {
    self.synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { processHandle =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(processHandle.pid().toInt)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.stop()
  }

  def releaseWorker(worker: PythonWorker): Unit = {
    if (useDaemon) {
      self.synchronized {
        lastActivityNs = System.nanoTime()
        idleWorkers.enqueue(worker)
      }
    } else {
      try {
        worker.stop()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker", e)
      }
    }
  }

  def isWorkerStopped(worker: PythonWorker): Boolean = {
    assert(!useDaemon, "isWorkerStopped() is not supported for daemon mode")
    simpleWorkers.get(worker).exists(!_.isAlive)
  }
}

private[spark] object PythonWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_NS = TimeUnit.MINUTES.toNanos(1)  // kill idle workers after 1 minute

  private[spark] val defaultDaemonModule = "pyspark.daemon"
}
