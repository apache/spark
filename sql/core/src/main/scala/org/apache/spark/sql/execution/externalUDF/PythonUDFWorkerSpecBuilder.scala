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
import org.apache.spark.internal.config.Python.PYTHON_WORKER_MODULE
import org.apache.spark.sql.execution.python.ArrowPythonRunner
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.udf.worker._

/**
 * Builds worker-launch and session specifications for Python UDFs from a [[PythonFunction]] and
 * [[SparkConf]]. This helper adapts Python metadata without adding Python-specific behavior to
 * the language-neutral worker protocol.
 *
 * Reuses the same information the existing
 * [[org.apache.spark.api.python.PythonWorkerFactory]] uses:
 *  - `pythonExec` from the function
 *  - Environment variables from the function (which already
 *    contain the caller-assembled `PYTHONPATH`), merged with
 *    Spark's built-in Python path and the system `PYTHONPATH`
 *  - Worker module from `spark.python.worker.module`
 *
 * Note: `pythonIncludes` are not added to the process environment. Worker-side unified execution
 * support will define how to provide them when it consumes the UDF payload.
 */
private[externalUDF] object PythonUDFWorkerSpecBuilder {

  final case class Result(
      workerSpec: UDFWorkerSpecification,
      sessionSpec: WorkerSessionSpecification)

  private[externalUDF] val ARTIFACTS_RESOURCE_DIRECTORY: String = "artifacts"

  /**
   * Creates a [[UDFWorkerSpecification]] from a [[PythonFunction]].
   *
   * @param func the Python function containing pythonExec, env vars,
   *             and includes
   * @param conf the SparkConf for reading the worker module config
   * @return the worker-launch and per-session specifications
   */
  def build(
      func: PythonFunction,
      conf: SparkConf): Result = {

    val workerModule = conf.get(PYTHON_WORKER_MODULE)
      .getOrElse("pyspark.worker")
    val functionEnv = validatedEnvironmentVariables(func.envVars)

    // Assemble PYTHONPATH the same way PythonWorkerFactory does
    val pythonPath = PythonUtils.mergePythonPaths(
      PythonUtils.sparkPythonPath,
      functionEnv.getOrElse("PYTHONPATH", ""),
      sys.env.getOrElse("PYTHONPATH", ""))

    // Merge func.envVars with the assembled PYTHONPATH
    val envVars = new java.util.HashMap[String, String]()
    envVars.putAll(functionEnv.asJava)
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
    // TODO(SPARK-59368): Define the remaining Python initialization metadata with the
    // worker consumer.
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
    val pythonPropertyRequirements = ArrowPythonRunner.pythonRunnerConfRequirements
      .filterNot(_.key == SQLConf.SESSION_LOCAL_TIMEZONE.key)
    val duplicateKeys = pythonPropertyRequirements.groupBy(_.key)
      .collect { case (key, entries) if entries.size > 1 => key }
      .toSeq
      .sorted
    require(
      duplicateKeys.isEmpty,
      s"Duplicate Python runner property requirements: ${duplicateKeys.mkString(", ")}")
    pythonPropertyRequirements.sortBy(_.key).foreach { requirement =>
      session.putPropertyRequirements(
        requirement.key,
        propertyRequirement(requirement.isRequired))
    }

    val workerSpec = UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(caps)
      .setDirect(direct)
      .build()
    Result(workerSpec, session.build())
  }

  private def validatedEnvironmentVariables(
      environmentVariables: java.util.Map[String, String]): Map[String, String] = {
    val variables = Option(environmentVariables).map(_.asScala.toMap).getOrElse(Map.empty)
    require(!variables.contains(null), "Python worker environment contains a null name")
    require(!variables.contains(""), "Python worker environment contains an empty name")
    val nullValues = variables.collect { case (name, null) => name }.toSeq.sorted
    require(
      nullValues.isEmpty,
      s"Python worker environment contains null values for: ${nullValues.mkString(", ")}")
    variables
  }

  private def propertyRequirement(isRequired: Boolean): PropertyRequirement = {
    PropertyRequirement.newBuilder().setIsRequired(isRequired).build()
  }
}
