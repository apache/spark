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

package org.apache.spark.sql.execution.python

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{Callable, ExecutionException, Executors, ThreadFactory, TimeoutException, TimeUnit}

import scala.jdk.CollectionConverters._

import jep.{JepConfig, JepException, MainInterpreter, PyConfig, SharedInterpreter}

import org.apache.spark.{TaskContext, TaskKilledException}
import org.apache.spark.api.python.{PythonException, PythonUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/** Owns one interpreter generation per executor plugin lifecycle. */
private[python] object InProcessPythonRuntime extends Logging {
  val SITE_PACKAGES_CONFIG = "spark.inprocess.python.sitePackages"
  private val TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"
  private var active: InterpreterSession = _
  private var configured = false

  private[python] class LifecycleException(message: String) extends IllegalStateException(message)

  private def configureInterpreter(sitePackages: Seq[String]): Unit = {
    if (!configured) {
      // Like Python workers, use a stable default hash seed on every executor. This must
      // happen before JEP creates its process-wide main interpreter, including on restarts.
      MainInterpreter.setInitParams(new PyConfig().setHashSeed(0).setUseHashSeed(true))
      // JEP imports its Python package during construction, before our bootstrap runs.
      SharedInterpreter.setConfig(new JepConfig().addIncludePaths(sitePackages: _*))
      configured = true
    }
  }

  private[python] def bootstrapScript(script: String): String = {
    "try:\n" + script.linesIterator.map("    " + _).mkString("\n") +
      "\nexcept BaseException as _bootstrap_error:\n" +
      "    raise RuntimeError('In-process Python bootstrap failed: ' + " +
      "repr(_bootstrap_error)) from None\n"
  }

  def initialize(sitePackages: Seq[String] = Seq.empty): Unit = synchronized {
    if (active != null && !active.isTerminated) {
      active.requireCompatible(sitePackages)
    } else {
      configureInterpreter(sitePackages)
      val candidate = new InterpreterSession(sitePackages)
      try {
        candidate.initialize()
        active = candidate
      } catch {
        case t: Throwable => Utils.tryWithSafeFinally { throw t } { candidate.shutdown() }
      }
    }
  }

  def currentSession: InterpreterSession = synchronized {
    checkState(active != null && active.isRunning)
    active
  }

  def shutdown(): Unit = {
    val session = synchronized { active }
    if (session != null) session.shutdown()
  }

  private def checkState(running: Boolean): Unit = {
    checkState(running, "In-process Python is not running; initialize the executor plugin first")
  }

  private def checkState(running: Boolean, message: String): Unit = {
    if (!running) throw new IllegalStateException(message)
  }

  /**
   * Tasks retain this generation, so stale tasks cannot enter a later SparkContext's interpreter.
   * Lifecycle operations only hold the monitor while enqueueing work, never while running Python.
   */
  private[python] class InterpreterSession(val sitePackages: Seq[String] = Seq.empty) {
    // CPython native calls need more stack than the usual JVM thread default. This is a
    // platform-dependent size request, not protection against arbitrary native crashes.
    private val executor = Executors.newSingleThreadExecutor(new ThreadFactory {
      override def newThread(runnable: Runnable): Thread = {
        val thread = new Thread(null, runnable, "inprocess-python", 8L * 1024 * 1024)
        thread.setDaemon(true)
        thread
      }
    })
    @volatile private var running = true
    // Accessed only on the owning thread.
    private var interp: SharedInterpreter = _

    def isRunning: Boolean = running
    def isTerminated: Boolean = executor.isTerminated

    def requireCompatible(paths: Seq[String]): Unit = {
      if (!isRunning) {
        throw new LifecycleException("In-process Python is still stopping. Wait for outstanding " +
          "native work to finish or replace the executor process before starting a new context.")
      }
      if (sitePackages != paths) {
        throw new LifecycleException("In-process Python is already running with different " +
          "sitePackages. Stop the existing context before changing interpreter configuration.")
      }
    }

    private[python] def onInterpreterThread[T](body: => T): T = {
      val context = Option(TaskContext.get())
      context.foreach(_.killTaskIfInterrupted())
      val gate = new Object
      var started = false
      var cancelled = false
      val future = synchronized {
        checkState(running)
        executor.submit(new Callable[T] {
          override def call(): T = {
            gate.synchronized {
              if (cancelled) throw new TaskKilledException("Cancelled before Python invocation")
              started = true
            }
            body
          }
        })
      }
      var interrupted = false
      try {
        while (true) {
          val taskCancelled = context.exists(_.isInterrupted())
          if (interrupted || taskCancelled) {
            val cancelledBeforeStart = gate.synchronized {
              if (started) false else {
                cancelled = true
                future.cancel(false)
                true
              }
            }
            if (cancelledBeforeStart) {
              context.foreach(_.killTaskIfInterrupted())
              throw new InterruptedException("Cancelled before Python invocation")
            }
          }
          try {
            val result = future.get(100, TimeUnit.MILLISECONDS)
            context.foreach(_.killTaskIfInterrupted())
            return result
          } catch {
            case _: TimeoutException =>
            case _: InterruptedException => interrupted = true
            case e: ExecutionException => throw e.getCause
          }
        }
        throw new IllegalStateException("Unreachable")
      } finally {
        // Once native work starts, wait for it even after cancellation: the caller still owns
        // CDI structs that Python may use. Pending work, however, is safe to cancel immediately.
        if (interrupted) Thread.currentThread().interrupt()
      }
    }

    def initialize(): Unit = onInterpreterThread {
      val candidate = new SharedInterpreter()
      try {
        candidate.set("_site_packages", sitePackages.asJava)
        val sparkPaths = PythonUtils.sparkPythonPath.split(File.pathSeparator).filter(_.nonEmpty)
        candidate.set("_spark_paths", sparkPaths.toSeq.asJava)
        candidate.exec(bootstrapScript(
          """import os, site, sys
            |_configured = [os.path.abspath(p) for p in _site_packages]
            |_before = set(sys.path)
            |for _path in _configured:
            |    site.addsitedir(_path)
            |_added = [p for p in sys.path if p not in _before and p not in _configured]
            |_preferred = list(dict.fromkeys(list(_spark_paths) + _configured + _added))
            |sys.path[:] = _preferred + [p for p in sys.path if p not in _preferred]
            |del _site_packages, _spark_paths, _configured, _before, _added, _preferred
            |""".stripMargin))
        candidate.exec(bootstrapScript("from pyspark.inprocess.runtime import " +
          "_inprocess_invoke, _inprocess_register, _inprocess_release, _udfs"))
        interp = candidate
      } catch {
        case t: Throwable => Utils.tryWithSafeFinally { throw t } { candidate.close() }
      }
    }

    /** Enqueue cleanup after outstanding calls without creating an executor or waiting. */
    def release(handles: Seq[String]): Unit = synchronized {
      if (running && handles.nonEmpty) {
        executor.submit(new Runnable {
          override def run(): Unit = {
            if (interp != null) interp.invoke("_inprocess_release", handles.asJava)
          }
        })
      }
      // During shutdown the queued close clears all remaining handles.
    }

    /** A timeout bounds plugin stop, not native execution or CDI buffer ownership. */
    def shutdown(waitMillis: Long = 5000L): Unit = {
      synchronized {
        if (running) {
          running = false
          executor.submit(new Runnable {
            override def run(): Unit = {
              if (interp != null) {
                try {
                  interp.exec("_udfs.clear()")
                } finally {
                  try { interp.close() } finally { interp = null }
                }
              }
            }
          })
          executor.shutdown()
        }
      }
      try {
        if (!executor.awaitTermination(waitMillis, TimeUnit.MILLISECONDS)) {
          logWarning("In-process Python is still stopping; native work and its buffers " +
            "remain alive until the invocation finishes or the process exits.")
        }
      } catch {
        case _: InterruptedException => Thread.currentThread().interrupt()
      }
    }

    def register(
        handle: String,
        serializedUdf: Array[Byte],
        timeZoneId: String,
        pythonVersion: String,
        largeVarTypes: Boolean,
        hideTraceback: Boolean,
        simplifiedTraceback: Boolean,
        tracebackWithLocals: Boolean): Unit = {
      // Bulk-copy on the task thread. JEP's PyJBuffer supports memoryview without per-byte JNI.
      val command = ByteBuffer.allocateDirect(serializedUdf.length)
      command.put(serializedUdf).flip()
      onInterpreterThread {
        withPythonException {
          interp.invoke("_inprocess_register", handle, command, timeZoneId,
            pythonVersion, java.lang.Boolean.valueOf(largeVarTypes),
            java.lang.Boolean.valueOf(hideTraceback),
            java.lang.Boolean.valueOf(simplifiedTraceback),
            java.lang.Boolean.valueOf(tracebackWithLocals))
        }
      }
    }

    def invoke(
        handle: String,
        inputArrayPtrs: Array[Long],
        inputSchemaPtrs: Array[Long],
        outputArrayAddr: Long,
        outputSchemaAddr: Long,
        expectedRows: Int,
        argumentNames: Array[String]): Long = onInterpreterThread {
      val start = System.nanoTime()
      val arrayPtrs = inputArrayPtrs.map(java.lang.Long.valueOf).toSeq.asJava
      val schemaPtrs = inputSchemaPtrs.map(java.lang.Long.valueOf).toSeq.asJava
      withPythonException {
        interp.invoke("_inprocess_invoke", handle, arrayPtrs, schemaPtrs,
          java.lang.Long.valueOf(outputArrayAddr), java.lang.Long.valueOf(outputSchemaAddr),
          java.lang.Integer.valueOf(expectedRows), argumentNames.toSeq.asJava)
      }
      System.nanoTime() - start
    }
  }

  private def withPythonException(body: => Unit): Unit = {
    try {
      body
    } catch {
      case e: JepException =>
        val msg = e.getMessage
        val sentinelIdx = if (msg != null) msg.indexOf(TRACEBACK_SENTINEL) else -1
        if (sentinelIdx >= 0) {
          throw new PythonException(
            errorClass = "PYTHON_EXCEPTION",
            messageParameters = Map(
              "msg" -> "An exception was thrown from the in-process Python UDF",
              "traceback" -> msg.substring(sentinelIdx + TRACEBACK_SENTINEL.length)),
            // The formatted Python message already applies the query's traceback policy.
            // Do not retain JEP's separate Python traceback through a nested cause.
            cause = null)
        } else {
          throw new RuntimeException(s"In-process Python infrastructure error: $msg", e)
        }
    }
  }
}
