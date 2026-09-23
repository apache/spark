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

import scala.util.control.NonFatal

import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.Logging

/**
 * Spark plugin that initializes jep's SharedInterpreter on a dedicated executor thread,
 * enabling in-process Python UDF execution with zero-copy Arrow data passing.
 *
 * Register via Spark config:
 *   spark.plugins=org.apache.spark.sql.execution.python.InProcessPythonPlugin
 *
 * Requirements:
 *  - jep (Java Embedded Python) must be on the executor classpath (provided scope)
 *  - Python 3.11+ with PyArrow 18+ and PySpark installed in the executor environment
 *
 * Calls from concurrent tasks are serialized on the interpreter thread. One task per executor
 * is recommended for throughput but is not required for correctness.
 *
 * @see [[InProcessPythonRuntime]] for the interpreter singleton
 */
class InProcessPythonPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = null

  override def executorPlugin(): ExecutorPlugin = new InProcessPythonExecutorPlugin()
}

private[python] class InProcessPythonExecutorPlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    logInfo("Initializing in-process Python runtime (jep SharedInterpreter).")
    try {
      val sitePackages = ctx.conf()
        .getOption(InProcessPythonRuntime.SITE_PACKAGES_CONFIG)
        .map(_.split(",").map(_.trim).filter(_.nonEmpty).toSeq)
        .getOrElse(Seq.empty)
      InProcessPythonRuntime.initialize(sitePackages)
      logInfo("In-process Python runtime initialized successfully.")
    } catch {
      case e if NonFatal(e) || e.isInstanceOf[LinkageError] =>
        logError(
          "Failed to initialize in-process Python runtime. " +
          "Verify that: (1) libjep.so/libjep.dylib is on LD_LIBRARY_PATH/DYLD_LIBRARY_PATH, " +
          "(2) jep.jar is on the executor classpath, " +
          "(3) Python 3.11+, PyArrow 18+, and PySpark are installed.", e)
        throw e
    }
  }

  override def shutdown(): Unit = {
    logInfo("Shutting down in-process Python runtime.")
    InProcessPythonRuntime.shutdown()
  }
}
