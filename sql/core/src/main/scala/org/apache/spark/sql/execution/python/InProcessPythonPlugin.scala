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

import java.util.{Map => JMap}

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

/**
 * Spark plugin that initializes jep's SharedInterpreter on each executor JVM process,
 * enabling in-process Python UDF execution with zero-copy Arrow data passing.
 *
 * Register via Spark config:
 *   spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin
 *
 * Requirements:
 *  - jep (Java Embedded Python) must be on the executor classpath (provided scope)
 *  - Python 3.8+ with PyArrow and cloudpickle installed in the executor environment
 *  - spark.executor.cores == spark.task.cpus (enforced at query planning time by
 *    [[InProcessPythonChecks]] to prevent GIL contention on the shared interpreter)
 *
 * @see [[InProcessPythonRuntime]] for the interpreter singleton
 * @see [[InProcessPythonChecks]] for the concurrency config validation rule
 */
class InProcessPythonPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = null

  override def executorPlugin(): ExecutorPlugin = new InProcessPythonExecutorPlugin()
}

private[python] class InProcessPythonExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    logInfo("Initializing in-process Python runtime (jep SharedInterpreter).")
    try {
      InProcessPythonRuntime.initialize()
      logInfo("In-process Python runtime initialized successfully.")
    } catch {
      case e: Exception =>
        logError(
          "Failed to initialize in-process Python runtime. " +
          "Verify that: (1) libjep.so/libjep.dylib is on LD_LIBRARY_PATH/DYLD_LIBRARY_PATH, " +
          "(2) jep.jar is on the executor classpath, " +
          "(3) Python 3.8+, PyArrow, and cloudpickle are installed.", e)
        throw e
    }
  }

  override def shutdown(): Unit = {
    logInfo("Shutting down in-process Python runtime.")
    InProcessPythonRuntime.shutdown()
  }
}
