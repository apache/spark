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
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.python.{PythonFunction, PythonUtils}
import org.apache.spark.internal.config.Python.PYTHON_WORKER_MODULE
import org.apache.spark.udf.worker._

/**
 * :: Experimental ::
 * Builds a [[UDFWorkerSpecification]] for Python UDFs from a
 * [[PythonFunction]] and [[SparkConf]].
 *
 * Reuses the same information the existing
 * [[org.apache.spark.api.python.PythonWorkerFactory]] uses:
 *  - `pythonExec` from the function
 *  - Environment variables from the function (which already
 *    contain the caller-assembled `PYTHONPATH`), merged with
 *    Spark's built-in Python path and the system `PYTHONPATH`
 *  - Worker module from `spark.python.worker.module`
 *
 * Note: `pythonIncludes` are not added to the process
 * environment. They are sent over the data channel to the
 * already-running worker by the runner (see
 * [[org.apache.spark.api.python.PythonRunner]]).
 */
@Experimental
object PythonUDFWorkerSpecification {

  /**
   * Creates a [[UDFWorkerSpecification]] from a [[PythonFunction]].
   *
   * @param func the Python function containing pythonExec, env vars,
   *             and includes
   * @param conf the SparkConf for reading the worker module config
   * @return a fully populated [[UDFWorkerSpecification]]
   */
  def fromPythonFunction(
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
    // Enable the execution mode supporting the new UDF execution
    // framework.
    // TODO [SPARK-55278]: Enable this on the python code
    envVars.put("PYTHON_WORKER_UNIFIED_EXECUTION_ENABLED", "YES")

    // Build the ProcessCallable:
    //   command = [pythonExec, "-m", workerModule]
    val callable = ProcessCallable.newBuilder()
    callable.addCommand(func.pythonExec)
    callable.addCommand("-m")
    callable.addCommand(workerModule)
    // TODO [SPARK-55278]: Add additional, python specific env vars
    // or transform them into init-message fields
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

    UDFWorkerSpecification.newBuilder()
      .setEnvironment(WorkerEnvironment.newBuilder())
      .setCapabilities(caps)
      .setDirect(direct)
      .build()
  }
}
