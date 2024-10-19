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

package org.apache.spark.deploy.rest

import java.io.File

import jakarta.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.internal.config
import org.apache.spark.launcher.{JavaModuleOptions, SparkLauncher}
import org.apache.spark.resource.ResourceUtils
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * A server that responds to requests submitted by the [[RestSubmissionClient]].
 * This is intended to be embedded in the standalone Master and used in cluster mode only.
 *
 * This server responds with different HTTP codes depending on the situation:
 *   200 OK - Request was processed successfully
 *   400 BAD REQUEST - Request was malformed, not successfully validated, or of unexpected type
 *   468 UNKNOWN PROTOCOL VERSION - Request specified a protocol this server does not understand
 *   500 INTERNAL SERVER ERROR - Server throws an exception internally while processing the request
 *
 * The server always includes a JSON representation of the relevant [[SubmitRestProtocolResponse]]
 * in the HTTP body. If an error occurs, however, the server will include an [[ErrorResponse]]
 * instead of the one expected by the client. If the construction of this error response itself
 * fails, the response will consist of an empty body with a response code that indicates internal
 * server error.
 *
 * @param host the address this server should bind to
 * @param requestedPort the port this server will attempt to bind to
 * @param masterConf the conf used by the Master
 * @param masterEndpoint reference to the Master endpoint to which requests can be sent
 * @param masterUrl the URL of the Master new drivers will attempt to connect to
 */
private[deploy] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf,
    masterEndpoint: RpcEndpointRef,
    masterUrl: String)
  extends RestSubmissionServer(host, requestedPort, masterConf) {

  protected override val submitRequestServlet =
    new StandaloneSubmitRequestServlet(masterEndpoint, masterUrl, masterConf)
  protected override val killRequestServlet =
    new StandaloneKillRequestServlet(masterEndpoint, masterConf)
  protected override val killAllRequestServlet =
    new StandaloneKillAllRequestServlet(masterEndpoint, masterConf)
  protected override val statusRequestServlet =
    new StandaloneStatusRequestServlet(masterEndpoint, masterConf)
  protected override val clearRequestServlet =
    new StandaloneClearRequestServlet(masterEndpoint, masterConf)
  protected override val readyzRequestServlet =
    new StandaloneReadyzRequestServlet(masterEndpoint, masterConf)
}

/**
 * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneKillRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillRequestServlet {

  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = masterEndpoint.askSync[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId))
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
 * A servlet for handling killAll requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneKillAllRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends KillAllRequestServlet {

  protected def handleKillAll() : KillAllSubmissionResponse = {
    val response = masterEndpoint.askSync[DeployMessages.KillAllDriversResponse](
      DeployMessages.RequestKillAllDrivers)
    val k = new KillAllSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.success = response.success
    k
  }
}

/**
 * A servlet for handling status requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneStatusRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends StatusRequestServlet {

  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = masterEndpoint.askSync[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId))
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
 * A servlet for handling clear requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneClearRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends ClearRequestServlet {

  protected def handleClear(): ClearResponse = {
    val response = masterEndpoint.askSync[Boolean](
      DeployMessages.RequestClearCompletedDriversAndApps)
    val c = new ClearResponse
    c.serverSparkVersion = sparkVersion
    c.message = ""
    c.success = response
    c
  }
}

/**
 * A servlet for handling readyz requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneReadyzRequestServlet(masterEndpoint: RpcEndpointRef, conf: SparkConf)
  extends ReadyzRequestServlet {

  protected def handleReadyz(): ReadyzResponse = {
    val success = masterEndpoint.askSync[Boolean](DeployMessages.RequestReadyz)
    val r = new ReadyzResponse
    r.serverSparkVersion = sparkVersion
    r.message = ""
    r.success = success
    r
  }
}

/**
 * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StandaloneSubmitRequestServlet(
    masterEndpoint: RpcEndpointRef,
    masterUrl: String,
    conf: SparkConf)
  extends SubmitRequestServlet {

  private def replacePlaceHolder(variable: String) = variable match {
    case s"{{$name}}" if System.getenv(name) != null => System.getenv(name)
    case _ => variable
  }

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This involves constructing a command that takes into account memory, java options,
   * classpath and other settings to launch the driver. This does not currently consider
   * fields used by python applications since python is not supported in standalone
   * cluster mode yet.
   */
  private[rest] def buildDriverDescription(
      request: CreateSubmissionRequest,
      masterUrl: String,
      masterRestPort: Int): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
      .map(x => (x._1, replacePlaceHolder(x._2)))
    val driverMemory = sparkProperties.get(config.DRIVER_MEMORY.key)
    val driverCores = sparkProperties.get(config.DRIVER_CORES.key)
    val driverDefaultJavaOptions = sparkProperties.get(SparkLauncher.DRIVER_DEFAULT_JAVA_OPTIONS)
    val driverExtraJavaOptions = sparkProperties.get(config.DRIVER_JAVA_OPTIONS.key)
    val driverExtraClassPath = sparkProperties.get(config.DRIVER_CLASS_PATH.key)
    val driverExtraLibraryPath = sparkProperties.get(config.DRIVER_LIBRARY_PATH.key)
    val superviseDriver = sparkProperties.get(config.DRIVER_SUPERVISE.key)
    // The semantics of "spark.master" and the masterUrl are different. While the
    // property "spark.master" could contain all registered masters, masterUrl
    // contains only the active master. To make sure a Spark driver can recover
    // in a multi-master setup, we use the "spark.master" property while submitting
    // the driver.
    val masters = sparkProperties.get("spark.master")
    val (_, masterPort) = Utils.extractHostPortFromSparkUrl(masterUrl)
    val updatedMasters = masters.map(
      _.replace(s":$masterRestPort", s":$masterPort")).getOrElse(masterUrl)
    val appArgs = Option(request.appArgs).getOrElse(Array[String]())
    // Filter SPARK_LOCAL_(IP|HOSTNAME) environment variables from being set on the remote system.
    // In addition, the placeholders are replaced into the values of environment variables.
    val environmentVariables =
      Option(request.environmentVariables).getOrElse(Map.empty[String, String])
        .filterNot(x => x._1.matches("SPARK_LOCAL_(IP|HOSTNAME)"))
        .map(x => (x._1, replacePlaceHolder(x._2)))

    // Construct driver description
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", updatedMasters)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val defaultJavaOpts = driverDefaultJavaOptions.map(Utils.splitCommandString)
      .getOrElse(Seq.empty)
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaModuleOptions = JavaModuleOptions.defaultModuleOptionArray().toImmutableArraySeq
    val javaOpts = javaModuleOptions ++ sparkJavaOpts ++ defaultJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    val driverResourceReqs = ResourceUtils.parseResourceRequirements(conf,
      config.SPARK_DRIVER_PREFIX)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command,
      driverResourceReqs)
  }

  /**
   * Handle the submit request and construct an appropriate response to return to the client.
   *
   * This assumes that the request message is already successfully validated.
   * If the request message is not of the expected type, return error to the client.
   */
  protected override def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(
          submitRequest, masterUrl, conf.get(config.MASTER_REST_SERVER_PORT))
        val response = masterEndpoint.askSync[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription))
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success
        submitResponse.submissionId = response.driverId.orNull
        val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
        if (unknownFields.nonEmpty) {
          // If there are fields that the server does not know about, warn the client
          submitResponse.unknownFields = unknownFields
        }
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }
}
