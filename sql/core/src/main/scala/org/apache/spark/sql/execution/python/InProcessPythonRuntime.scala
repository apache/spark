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

import java.util.concurrent.{Callable, ExecutionException, ExecutorService, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import scala.jdk.CollectionConverters._

import jep.{JepException, SharedInterpreter}

import org.apache.spark.TaskContext
import org.apache.spark.api.python.PythonException
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Owns one interpreter on a dedicated thread per executor. JEP requires construction,
 * invocation and close to happen on the same thread, even when Spark tasks run serially.
 */
private[python] object InProcessPythonRuntime extends Logging {
  val SITE_PACKAGES_CONFIG = "spark.inprocess.python.sitePackages"

  // Access to the executor is serialized by onInterpreterThread and shutdown. The interpreter
  // itself is accessed only by the executor's thread.
  private val interpreterLock = new ReentrantLock()
  private var executor: ExecutorService = _
  private var interp: SharedInterpreter = _
  private val TRACEBACK_SENTINEL = "__INPROCESS_UDF_TRACEBACK__:"

  private def withInterpreterLock[T](cancellable: Boolean)(body: => T): T = {
    if (cancellable) {
      val context = Option(TaskContext.get())
      context.foreach(_.killTaskIfInterrupted())
      // Poll the task state as cancellation need not interrupt the Java thread.
      while (!interpreterLock.tryLock(100, TimeUnit.MILLISECONDS)) {
        context.foreach(_.killTaskIfInterrupted())
      }
    } else {
      interpreterLock.lock()
    }
    try {
      if (cancellable) Option(TaskContext.get()).foreach(_.killTaskIfInterrupted())
      body
    } finally {
      interpreterLock.unlock()
    }
  }

  /**
   * Wait for native code to finish even if the task is interrupted. Returning early would let
   * the task free CDI pointers that Python may still be accessing. Restore the interruption
   * afterwards so Spark can observe cancellation. Arbitrary Python code cannot be forcibly
   * interrupted safely in the executor process.
   */
  private[python] def onInterpreterThread[T](body: => T): T = {
    runOnInterpreterThread(cancellable = true)(body)
  }

  private def runOnInterpreterThread[T](cancellable: Boolean)(body: => T): T =
    withInterpreterLock(cancellable) {
      if (executor == null) {
        executor = ThreadUtils.newDaemonSingleThreadExecutor("inprocess-python")
      }
      val future = executor.submit(new Callable[T] {
        override def call(): T = body
      })
      var interrupted = false
      try {
        var result: Option[T] = None
        while (result.isEmpty) {
          try {
            result = Some(future.get())
          } catch {
            case _: InterruptedException => interrupted = true
            case e: ExecutionException => throw e.getCause
          }
        }
        if (cancellable) Option(TaskContext.get()).foreach(_.killTaskIfInterrupted())
        result.get
      } finally {
        if (interrupted) Thread.currentThread().interrupt()
      }
    }

  private def initializeInterpreter(sitePackages: Seq[String]): Unit = {
    if (interp == null) {
      val candidate = new SharedInterpreter()
      try {
        // Configure paths before importing the bridge and its dependencies.
        if (sitePackages.nonEmpty) {
          candidate.set("_site_packages", sitePackages.asJava)
          candidate.eval("import sys; sys.path.extend(list(_site_packages)); del _site_packages")
        }
        candidate.eval("from pyspark.inprocess.runtime import " +
          "_inprocess_invoke, _inprocess_register, _inprocess_release, _udfs")
        interp = candidate
      } catch {
        case t: Throwable =>
          Utils.tryWithSafeFinally { throw t } { candidate.close() }
      }
    }
  }

  def initialize(sitePackages: Seq[String] = Seq.empty): Unit =
    withInterpreterLock(cancellable = false) {
      try {
        runOnInterpreterThread(cancellable = false) { initializeInterpreter(sitePackages) }
      } catch {
        case t: Throwable =>
          executor.shutdown()
          executor = null
          throw t
      }
    }

  def shutdown(): Unit = withInterpreterLock(cancellable = false) {
    if (executor != null) {
      try {
        runOnInterpreterThread(cancellable = false) {
          if (interp != null) {
            try {
              interp.eval("_udfs.clear()")
            } finally {
              try { interp.close() } finally { interp = null }
            }
          }
        }
      } finally {
        executor.shutdown()
        executor = null
      }
    }
  }

  /** Register a separate function instance per task, copying its closure only once. */
  def register(
      handle: String,
      serializedUdf: Array[Byte],
      returnTypeJson: String,
      timeZoneId: String,
      pythonVersion: String): Unit = onInterpreterThread {
    initializeInterpreter(Seq.empty)
    withPythonException {
      interp.invoke("_inprocess_register",
        handle, serializedUdf, returnTypeJson, timeZoneId, pythonVersion)
    }
  }

  /** Cleanup must run even when the caller's task has been cancelled. */
  def release(handles: Seq[String]): Unit = runOnInterpreterThread(cancellable = false) {
    if (interp != null) {
      interp.invoke("_inprocess_release", handles.asJava)
    }
  }

  /** Pass CDI addresses to Python and wait until it has finished consuming them. */
  def invoke(
      handle: String,
      inputArrayPtrs: Array[Long],
      inputSchemaPtrs: Array[Long],
      outputArrayAddr: Long,
      outputSchemaAddr: Long,
      expectedRows: Int): Unit = onInterpreterThread {
    // Box long[] so JEP treats even single-column inputs as an iterable.
    val arrayPtrList = inputArrayPtrs.map(java.lang.Long.valueOf).toSeq.asJava
    val schemaPtrList = inputSchemaPtrs.map(java.lang.Long.valueOf).toSeq.asJava
    withPythonException {
      interp.invoke(
        "_inprocess_invoke",
        handle,
        arrayPtrList,
        schemaPtrList,
        java.lang.Long.valueOf(outputArrayAddr),
        java.lang.Long.valueOf(outputSchemaAddr),
        java.lang.Integer.valueOf(expectedRows))
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
            cause = e)
        } else {
          throw new RuntimeException(s"In-process Python infrastructure error: $msg", e)
        }
    }
  }
}
