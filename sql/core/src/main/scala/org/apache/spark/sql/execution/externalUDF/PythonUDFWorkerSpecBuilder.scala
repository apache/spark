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
package org.apache.spark.sql.execution.externalUDF

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkConf
import org.apache.spark.api.python.{PythonFunction, PythonUtils}
import org.apache.spark.internal.config.OptionalConfigEntry
import org.apache.spark.internal.config.Python.PYTHON_WORKER_MODULE
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.udf.worker._

/**
 * Builds a [[UDFWorkerSpecification]] for Python UDFs from a [[PythonFunction]] and
 * [[SparkConf]]. This helper adapts Python launch metadata without adding Python-specific
 * behavior to the language-neutral worker protocol.
 *
 * Reuses the same information the existing
 * [[org.apache.spark.api.python.PythonWorkerFactory]] uses:
 *  - `pythonExec` from the function
 *  - Environment variables from the function (which already
 *    contain the caller-assembled `PYTHONPATH`), merged with
 *    Spark's built-in Python path and the system `PYTHONPATH`
 *  - Worker module from `spark.python.worker.module`
 *
 * Note: `pythonIncludes` are not added to the process environment.
 * Unified execution serializes them in the per-UDF payload delivered
 * to the already-running worker during Init.
 */
private[externalUDF] object PythonUDFWorkerSpecBuilder {

  private[externalUDF] val ARTIFACTS_RESOURCE_DIRECTORY: String = "artifacts"

  /**
   * Creates a [[UDFWorkerSpecification]] from a [[PythonFunction]].
   *
   * @param func the Python function containing pythonExec, env vars,
   *             and includes
   * @param conf the SparkConf for reading the worker module config
   * @return a fully populated [[UDFWorkerSpecification]]
   */
  def build(
      func: PythonFunction,
      conf: SparkConf): UDFWorkerSpecification = {

    val workerModule = conf.get(PYTHON_WORKER_MODULE)
      .getOrElse("pyspark.worker")

    // Assemble PYTHONPATH the same way PythonWorkerFactory does
    val pythonPath = PythonUtils.mergePythonPaths(
      PythonUtils.sparkPythonPath,
      func.envVars.asScala
        .getOrElse("PYTHONPATH", ""),
      sys.env.getOrElse("PYTHONPATH", ""))

    // Merge func.envVars with the assembled PYTHONPATH
    val envVars = new java.util.HashMap[String, String]()
    envVars.putAll(func.envVars)
    envVars.put("PYTHONPATH", pythonPath)
    // Match PythonWorkerFactory behavior
    envVars.put("PYTHONUNBUFFERED", "YES")
    // Required by pyspark.worker_util to allow import
    envVars.put("SPARK_PYTHON_RUNTIME", "PYTHON_WORKER")
    // Enable the execution mode supporting the new UDF execution
    // framework.
    // TODO(SPARK-59368): Enable this in the Python worker.
    envVars.put("PYTHON_WORKER_UNIFIED_EXECUTION_ENABLED", "YES")

    // Build the ProcessCallable:
    //   command = [pythonExec, "-m", workerModule]
    val callable = ProcessCallable.newBuilder()
    callable.addCommand(func.pythonExec)
    callable.addCommand("-m")
    callable.addCommand(workerModule)
    // TODO(SPARK-59368): Add Python-specific environment variables or expose them as
    // Init fields.
    envVars.forEach((k, v) => callable.putEnvironmentVariables(k, v))

    // Capabilities: ARROW data format, bidirectional streaming
    val caps = WorkerCapabilities.newBuilder()
      .addSupportedDataFormats(UDFWorkerDataFormat.ARROW)
      .addSupportedCommunicationPatterns(
        UDFProtoCommunicationPattern.BIDIRECTIONAL_STREAMING)

    // Connection: Unix domain socket
    val conn = WorkerConnectionSpec.newBuilder()
      .setUnixDomainSocket(UnixDomainSocket.newBuilder())

    val props = UDFWorkerProperties.newBuilder()
      .setConnection(conn)

    val direct = DirectWorker.newBuilder()
      .setRunner(callable)
      .setProperties(props)

    val session = WorkerSessionSpecification.newBuilder()
      .addRequiredResourceDirectories(ARTIFACTS_RESOURCE_DIRECTORY)
    val pythonSqlConfEntries = ArrowPythonRunner.getPythonRunnerConfEntries
      .filterNot(_.key == SQLConf.SESSION_LOCAL_TIMEZONE.key)
      .groupBy(_.key)
      .values
      .map(_.head)
      .toSeq
      .sortBy(_.key)
    pythonSqlConfEntries.foreach { entry =>
      session.putDynamicConfig(
        entry.key,
        dynamicConfigRequirement(!entry.isInstanceOf[OptionalConfigEntry[_]]))
    }

    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(caps)
      .setSession(session)
      .setDirect(direct)
      .build()
  }

  private def dynamicConfigRequirement(isRequired: Boolean): DynamicConfigRequirement = {
    DynamicConfigRequirement.newBuilder().setIsRequired(isRequired).build()
  }
}
