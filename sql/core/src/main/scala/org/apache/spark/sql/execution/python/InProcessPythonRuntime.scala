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
      interp.eval("from pyspark.inprocess.runtime import _inprocess_invoke")
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
   * The JVM pre-allocates [[ArrowArray]] and [[ArrowSchema]] C structs and passes
   * their native addresses here. Python fills them in-place via
   * ``arr._export_to_c(output_array_ptr, output_schema_ptr)``. The JVM then calls
   * [[InProcessArrowBridge.cdiToColumn]] to wrap the filled structs (zero-copy).
   *
   * @param serializedUdf    cloudpickle bytes of the Python function (cached inside Python)
   * @param inputAddrList    list of address maps, one per input column -- see
   *                         [[InProcessArrowBridge.extractAddresses]] for key spec
   * @param numRows          number of rows in the batch
   * @param outputArrayAddr  native address of a JVM-allocated ArrowArray C struct
   * @param outputSchemaAddr native address of a JVM-allocated ArrowSchema C struct
   */
  def invoke(
      serializedUdf: Array[Byte],
      inputAddrList: JList[JMap[String, AnyRef]],
      numRows: Int,
      outputArrayAddr: Long,
      outputSchemaAddr: Long): Unit = {
    // Lazily initialize on the first executor thread that calls invoke.
    // This ensures the SharedInterpreter is created on the task thread (required by jep).
    if (!initialized) initialize()
    try {
      interp.invoke(
        "_inprocess_invoke",
        serializedUdf,
        inputAddrList,
        Integer.valueOf(numRows),
        java.lang.Long.valueOf(outputArrayAddr),
        java.lang.Long.valueOf(outputSchemaAddr))
    } catch {
      case e: JepException =>
        throw new RuntimeException(
          s"In-process Python UDF execution failed: ${e.getMessage}", e)
    }
  }
}
