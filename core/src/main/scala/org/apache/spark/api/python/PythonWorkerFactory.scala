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

import java.io.{DataInputStream, DataOutputStream, EOFException, InputStream}
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.util.Arrays
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Python._
import org.apache.spark.security.SocketAuthHelper
import org.apache.spark.util.{RedirectThread, Utils}

private[spark] class PythonWorkerFactory(pythonExec: String, envVars: Map[String, String])
  extends Logging { self =>

  import PythonWorkerFactory._

  // Because forking processes from Java is expensive, we prefer to launch a single Python daemon,
  // pyspark/daemon.py (by default) and tell it to fork new workers for our tasks. This daemon
  // currently only works on UNIX-based systems now because it uses signals for child management,
  // so we can also fall back to launching workers, pyspark/worker.py (by default) directly.
  private val useDaemon = {
    val useDaemonEnabled = SparkEnv.get.conf.get(PYTHON_USE_DAEMON)

    // This flag is ignored on Windows as it's unable to fork.
    !System.getProperty("os.name").startsWith("Windows") && useDaemonEnabled
  }

  // WARN: Both configurations, 'spark.python.daemon.module' and 'spark.python.worker.module' are
  // for very advanced users and they are experimental. This should be considered
  // as expert-only option, and shouldn't be used before knowing what it means exactly.

  // This configuration indicates the module to run the daemon to execute its Python workers.
  private val daemonModule =
    SparkEnv.get.conf.get(PYTHON_DAEMON_MODULE).map { value =>
      logInfo(
        s"Python daemon module in PySpark is set to [$value] in '${PYTHON_DAEMON_MODULE.key}', " +
        "using this to start the daemon up. Note that this configuration only has an effect when " +
        s"'${PYTHON_USE_DAEMON.key}' is enabled and the platform is not Windows.")
      value
    }.getOrElse("pyspark.daemon")

  // This configuration indicates the module to run each Python worker.
  private val workerModule =
    SparkEnv.get.conf.get(PYTHON_WORKER_MODULE).map { value =>
      logInfo(
        s"Python worker module in PySpark is set to [$value] in '${PYTHON_WORKER_MODULE.key}', " +
        "using this to start the worker up. Note that this configuration only has an effect when " +
        s"'${PYTHON_USE_DAEMON.key}' is disabled or the platform is Windows.")
      value
    }.getOrElse("pyspark.worker")

  private val authHelper = new SocketAuthHelper(SparkEnv.get.conf)

  @GuardedBy("self")
  private var daemon: Process = null
  val daemonHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  @GuardedBy("self")
  private var daemonPort: Int = 0
  @GuardedBy("self")
  private val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  @GuardedBy("self")
  private val idleWorkers = new mutable.Queue[Socket]()
  @GuardedBy("self")
  private var lastActivityNs = 0L
  new MonitorThread().start()

  @GuardedBy("self")
  private val simpleWorkers = new mutable.WeakHashMap[Socket, Process]()

  private val pythonPath = PythonUtils.mergePythonPaths(
    PythonUtils.sparkPythonPath,
    envVars.getOrElse("PYTHONPATH", ""),
    sys.env.getOrElse("PYTHONPATH", ""))

  def create(): (Socket, Option[Int]) = {
    if (useDaemon) {
      self.synchronized {
        if (idleWorkers.nonEmpty) {
          val worker = idleWorkers.dequeue()
          return (worker, daemonWorkers.get(worker))
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker()
    }
  }

  /**
   * Connect to a worker launched through pyspark/daemon.py (by default), which forks python
   * processes itself to avoid the high cost of forking from Java. This currently only works
   * on UNIX-based systems.
   */
  private def createThroughDaemon(): (Socket, Option[Int]) = {

    def createSocket(): (Socket, Option[Int]) = {
      val socket = new Socket(daemonHost, daemonPort)
      val pid = new DataInputStream(socket.getInputStream).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Python daemon failed to launch worker with code " + pid)
      }

      authHelper.authToServer(socket)
      daemonWorkers.put(socket, pid)
      (socket, Some(pid))
    }

    self.synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Python daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }

  /**
   * Launch a worker by executing worker.py (by default) directly and telling it to connect to us.
   */
  private def createSimpleWorker(): (Socket, Option[Int]) = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // Create and start the worker
      val pb = new ProcessBuilder(Arrays.asList(pythonExec, "-m", workerModule))
      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
      workerEnv.put("PYTHONPATH", pythonPath)
      // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
      workerEnv.put("PYTHONUNBUFFERED", "YES")
      workerEnv.put("PYTHON_WORKER_FACTORY_PORT", serverSocket.getLocalPort.toString)
      workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Wait for it to connect to our socket, and validate the auth secret.
      serverSocket.setSoTimeout(10000)

      try {
        val socket = serverSocket.accept()
        authHelper.authClient(socket)
        // TODO: When we drop JDK 8, we can just use worker.pid()
        val pid = new DataInputStream(socket.getInputStream).readInt()
        if (pid < 0) {
          throw new IllegalStateException("Python failed to launch worker with code " + pid)
        }
        self.synchronized {
          simpleWorkers.put(socket, worker)
        }
        return (socket, Some(pid))
      } catch {
        case e: Exception =>
          throw new SparkException("Python worker failed to connect back.", e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
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
        val command = Arrays.asList(pythonExec, "-m", daemonModule)
        val pb = new ProcessBuilder(command)
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PYTHONPATH", pythonPath)
        workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PYTHONUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        try {
          daemonPort = in.readInt()
        } catch {
          case _: EOFException if daemon.isAlive =>
            throw new SparkException("EOFException occurred while reading the port number " +
              s"from $daemonModule's stdout")
          case _: EOFException =>
            throw new SparkException(
              s"EOFException occurred while reading the port number from $daemonModule's" +
              s" stdout and terminated with code: ${daemon.exitValue}.")
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
        // the worker will exit after closing the socket
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
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
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  def stop(): Unit = {
    stopDaemon()
  }

  def stopWorker(worker: Socket): Unit = {
    self.synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { pid =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(pid)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.close()
  }

  def releaseWorker(worker: Socket): Unit = {
    if (useDaemon) {
      self.synchronized {
        lastActivityNs = System.nanoTime()
        idleWorkers.enqueue(worker)
      }
    } else {
      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }
}

private object PythonWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_NS = TimeUnit.MINUTES.toNanos(1)  // kill idle workers after 1 minute
}
