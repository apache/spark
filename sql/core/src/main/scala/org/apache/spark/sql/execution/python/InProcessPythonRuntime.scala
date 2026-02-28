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

import java.util.{List => JList, Map => JMap}

import jep.{JepConfig, JepException, SharedInterpreter}

import org.apache.spark.internal.Logging

/**
 * Singleton runtime holding one jep [[SharedInterpreter]] per executor JVM process.
 *
 * Lifecycle:
 *  - Initialized once at executor startup by [[InProcessPythonPlugin]]
 *  - Used by [[InProcessArrowEvalExec]] to invoke Python UDFs on each batch
 *  - Shut down at executor shutdown
 *
 * Thread safety: designed for single-task-per-executor use (enforced by
 * [[InProcessPythonChecks]]). [[SharedInterpreter]] is not thread-safe; the
 * single-task constraint ensures only one thread calls [[invoke]] at a time.
 */
private[python] object InProcessPythonRuntime extends Logging {

  @volatile private var interp: SharedInterpreter = _
  @volatile private var initialized: Boolean = false

  /**
   * Initialize the embedded CPython interpreter. Called once per executor JVM process.
   * Bootstraps the bridge module so `_inprocess_invoke` is available.
   */
  def initialize(): Unit = synchronized {
    if (!initialized) {
      val config = new JepConfig()
      SharedInterpreter.setConfig(config)
      interp = new SharedInterpreter()
      // Import the bridge entry point into the interpreter's global namespace
      interp.eval("from pyspark.inprocess.runtime import _inprocess_invoke, _release_export")
      initialized = true
      logInfo("jep SharedInterpreter ready; bridge module loaded.")
    }
  }

  def shutdown(): Unit = synchronized {
    if (initialized && interp != null) {
      try { interp.close() } catch {
        case e: JepException => logWarning("Error closing jep interpreter", e)
      } finally {
        interp = null
        initialized = false
      }
    }
  }

  /**
   * Invoke a Python UDF in-process via jep.
   *
   * Called from [[InProcessArrowEvalExec]] once per Arrow batch. The interpreter is
   * shared for the executor process lifetime, so this call is serialized by the
   * single-task-per-executor constraint.
   *
   * @param serializedUdf  cloudpickle bytes of the Python function (cached inside Python)
   * @param inputAddrList  list of address maps, one per input column - see
   *                       [[InProcessArrowBridge.extractAddresses]] for key spec
   * @param numRows        number of rows in the batch
   * @return               map with native buffer addresses and export ID - see
   *                       [[InProcessArrowBridge.foreignToColumn]] for key spec
   */
  def invoke(
      serializedUdf: Array[Byte],
      inputAddrList: JList[JMap[String, AnyRef]],
      numRows: Int): JMap[String, AnyRef] = {
    // Lazily initialize on the first executor thread that calls invoke.
    // This ensures the SharedInterpreter is created on the task thread (required by jep).
    if (!initialized) initialize()
    try {
      interp
        .invoke("_inprocess_invoke", serializedUdf, inputAddrList, Integer.valueOf(numRows))
        .asInstanceOf[JMap[String, AnyRef]]
    } catch {
      case e: JepException =>
        throw new RuntimeException(
          s"In-process Python UDF execution failed: ${e.getMessage}", e)
    }
  }

  /**
   * Release the Python-side exported array identified by `exportId`.
   *
   * Must be called only after the JVM has closed all [[org.apache.arrow.vector.FieldVector]]s
   * that reference the array's native buffers (i.e. after
   * [[InProcessArrowEvalExec]] closes the result [[ArrowColumnVector]]s for
   * the completed batch).
   *
   * Failures are logged as warnings rather than propagated -- a missed release
   * causes a Python memory leak for this batch but does not corrupt results.
   */
  def releaseExport(exportId: Int): Unit = {
    if (!initialized) return
    try {
      interp.invoke("_release_export", Integer.valueOf(exportId))
    } catch {
      case e: JepException =>
        logWarning(s"Failed to release Python export $exportId: ${e.getMessage}", e)
    }
  }
}
