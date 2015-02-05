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

import java.io.{DataOutputStream, File}
import java.net.InetSocketAddress
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import com.google.common.base.Charsets
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._
import org.apache.spark.deploy.master.Master

/**
 * A server that responds to requests submitted by the [[StandaloneRestClient]].
 * This is intended to be embedded in the standalone Master and used in cluster mode only.
 *
 * When an error occurs, this server sends an error response with an appropriate message
 * back to the client. If the construction of this error message itself is faulty, the
 * server indicates internal error through the response code.
 */
private[spark] class StandaloneRestServer(master: Master, host: String, requestedPort: Int)
  extends Logging {

  import StandaloneRestServer._

  private var _server: Option[Server] = None
  private val basePrefix = s"/$PROTOCOL_VERSION/submissions"

  // A mapping from servlets to the URL prefixes they are responsible for
  private val servletToPrefix = Map[StandaloneRestServlet, String](
    new SubmitRequestServlet(master) -> s"$basePrefix/create/*",
    new KillRequestServlet(master) -> s"$basePrefix/kill/*",
    new StatusRequestServlet(master) -> s"$basePrefix/status/*",
    new ErrorServlet -> "/"
  )

  /** Start the server and return the bound port. */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, master.conf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
  }

  /**
   * Map the servlets to their corresponding contexts and attach them to a server.
   * Return a 2-tuple of the started server and the bound port.
   */
  private def doStart(startPort: Int): (Server, Int) = {
    val server = new Server(new InetSocketAddress(host, requestedPort))
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    server.setThreadPool(threadPool)
    val mainHandler = new ServletContextHandler
    mainHandler.setContextPath("/")
    servletToPrefix.foreach { case (servlet, prefix) =>
      mainHandler.addServlet(new ServletHolder(servlet), prefix)
    }
    server.setHandler(mainHandler)
    server.start()
    val boundPort = server.getConnectors()(0).getLocalPort
    (server, boundPort)
  }

  def stop(): Unit = {
    _server.foreach(_.stop())
  }
}

private object StandaloneRestServer {
  val PROTOCOL_VERSION = StandaloneRestClient.PROTOCOL_VERSION
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
 * An abstract servlet for handling requests passed to the [[StandaloneRestServer]].
 */
private[spark] abstract class StandaloneRestServlet extends HttpServlet with Logging {

  /** Service a request. If an exception is thrown in the process, indicate server error. */
  protected override def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    try {
      super.service(request, response)
    } catch {
      case e: Exception =>
        logError("Exception while handling request", e)
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    }
  }

  /**
   * Serialize the given response message to JSON and send it through the response servlet.
   * This validates the response before sending it to ensure it is properly constructed.
   */
  protected def sendResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): Unit = {
    val message = validateResponse(responseMessage, responseServlet)
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.setStatus(HttpServletResponse.SC_OK)
    val content = message.toJson.getBytes(Charsets.UTF_8)
    val out = new DataOutputStream(responseServlet.getOutputStream)
    out.write(content)
    out.close()
  }

  /**
   * Return any fields in the client request message that the server does not know about.
   *
   * The mechanism for this is to reconstruct the JSON on the server side and compare the
   * diff between this JSON and the one generated on the client side. Any fields that are
   * only in the client JSON are treated as unexpected.
   */
  protected def findUnknownFields(
      requestJson: String,
      requestMessage: SubmitRestProtocolMessage): Array[String] = {
    val clientSideJson = parse(requestJson)
    val serverSideJson = parse(requestMessage.toJson)
    val Diff(_, _, unknown) = clientSideJson.diff(serverSideJson)
    unknown.asInstanceOf[JObject].obj.map { case (k, _) => k }.toArray
  }

  /** Return a human readable String representation of the exception. */
  protected def formatException(e: Exception): String = {
    val stackTraceString = e.getStackTrace.map { "\t" + _ }.mkString("\n")
    s"$e\n$stackTraceString"
  }

  /** Construct an error message to signal the fact that an exception has been thrown. */
  protected def handleError(message: String): ErrorResponse = {
    val e = new ErrorResponse
    e.serverSparkVersion = sparkVersion
    e.message = message
    e
  }

  /**
   * Validate the response message to ensure that it is correctly constructed.
   * If it is, simply return the response as is. Otherwise, return an error response
   * to propagate the exception back to the client.
   */
  private def validateResponse(
      responseMessage: SubmitRestProtocolResponse,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    try {
      responseMessage.validate()
      responseMessage
    } catch {
      case e: Exception =>
        responseServlet.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
        handleError("Internal server error: " + formatException(e))
    }
  }
}

/**
 * A servlet for handling kill requests passed to the [[StandaloneRestServer]].
 */
private[spark] class KillRequestServlet(master: Master) extends StandaloneRestServlet {
  private val askTimeout = AkkaUtils.askTimeout(master.conf)

  /**
   * If a submission ID is specified in the URL, have the Master kill the corresponding
   * driver and return an appropriate response to the client. Otherwise, return error.
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = request.getPathInfo.stripPrefix("/")
    val responseMessage =
      if (submissionId.nonEmpty) {
        handleKill(submissionId)
      } else {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError("Submission ID is missing in kill request")
      }
    sendResponse(responseMessage, response)
  }

  private def handleKill(submissionId: String): KillSubmissionResponse = {
    val response = AkkaUtils.askWithReply[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId), master.self, askTimeout)
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success.toString
    k
  }
}

/**
 * A servlet for handling status requests passed to the [[StandaloneRestServer]].
 */
private[spark] class StatusRequestServlet(master: Master) extends StandaloneRestServlet {
  private val askTimeout = AkkaUtils.askTimeout(master.conf)

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = request.getPathInfo.stripPrefix("/")
    val responseMessage =
      if (submissionId.nonEmpty) {
        handleStatus(submissionId)
      } else {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError("Submission ID is missing in status request")
      }
    sendResponse(responseMessage, response)
  }

  private def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val response = AkkaUtils.askWithReply[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId), master.self, askTimeout)
    val message = response.exception.map { s"Exception from the cluster:\n" + formatException(_) }
    val d = new SubmissionStatusResponse
    d.serverSparkVersion = sparkVersion
    d.submissionId = submissionId
    d.success = response.found.toString
    d.driverState = response.state.map(_.toString).orNull
    d.workerId = response.workerId.orNull
    d.workerHostPort = response.workerHostPort.orNull
    d.message = message.orNull
    d
  }
}

/**
 * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
 */
private[spark] class SubmitRequestServlet(master: Master) extends StandaloneRestServlet {
  private val askTimeout = AkkaUtils.askTimeout(master.conf)

  /**
   * Submit an application to the Master with parameters specified in the request message.
   *
   * The request is assumed to be a [[SubmitRestProtocolRequest]] in the form of JSON.
   * If this is successful, return an appropriate response to the client indicating so.
   * Otherwise, return error instead.
   */
  protected override def doPost(
      requestServlet: HttpServletRequest,
      responseServlet: HttpServletResponse): Unit = {
    val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
    val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
    val responseMessage = handleSubmit(requestMessage, responseServlet)
    val unknownFields = findUnknownFields(requestMessageJson, requestMessage)
    if (unknownFields.nonEmpty) {
      // If there are fields that the server does not know about, warn the client
      responseMessage.unknownFields = unknownFields
    }
    responseServlet.setContentType("application/json")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.setStatus(HttpServletResponse.SC_OK)
    val content = responseMessage.toJson.getBytes(Charsets.UTF_8)
    val out = new DataOutputStream(responseServlet.getOutputStream)
    out.write(content)
    out.close()
  }

  /**
   * Handle a submit request by first validating the request message, then submitting the
   * application using the parameters specified in the message. If the message is not of
   * the expected type, return error to the client.
   */
  private def handleSubmit(
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    // The response should have already been validated on the client.
    // In case this is not true, validate it ourselves to avoid potential NPEs.
    try {
      requestMessage.validate()
    } catch {
      case e: SubmitRestProtocolException =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(formatException(e))
    }
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val driverDescription = buildDriverDescription(submitRequest)
        val response = AkkaUtils.askWithReply[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription), master.self, askTimeout)
        val submitResponse = new CreateSubmissionResponse
        submitResponse.serverSparkVersion = sparkVersion
        submitResponse.message = response.message
        submitResponse.success = response.success.toString
        submitResponse.submissionId = response.driverId.orNull
        submitResponse
      case unexpected =>
        responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        handleError(s"Received message of unexpected type ${unexpected.messageType}.")
    }
  }

  /**
   * Build a driver description from the fields specified in the submit request.
   *
   * This involves constructing a command that takes into account memory, java options,
   * classpath and other settings to launch the driver. This does not currently consider
   * fields used by python applications since python is not supported in standalone
   * cluster mode yet.
   */
  private def buildDriverDescription(request: CreateSubmissionRequest): DriverDescription = {
    // Required fields, including the main class because python is not yet supported
    val appName = request.appName
    val appResource = request.appResource
    val mainClass = request.mainClass
    if (mainClass == null) {
      throw new SubmitRestMissingFieldException("Main class must be set in submit request.")
    }

    // Optional fields
    val jars = Option(request.jars)
    val files = Option(request.files)
    val driverMemory = Option(request.driverMemory)
    val driverCores = Option(request.driverCores)
    val driverExtraJavaOptions = Option(request.driverExtraJavaOptions)
    val driverExtraClassPath = Option(request.driverExtraClassPath)
    val driverExtraLibraryPath = Option(request.driverExtraLibraryPath)
    val superviseDriver = Option(request.superviseDriver)
    val executorMemory = Option(request.executorMemory)
    val totalExecutorCores = Option(request.totalExecutorCores)
    val appArgs = request.appArgs
    val sparkProperties = request.sparkProperties
    val environmentVariables = request.environmentVariables

    // Translate all fields to the relevant Spark properties
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", master.masterUrl)
      .set("spark.app.name", appName)
    jars.foreach { j => conf.set("spark.jars", j) }
    files.foreach { f => conf.set("spark.files", f) }
    driverExtraJavaOptions.foreach { j => conf.set("spark.driver.extraJavaOptions", j) }
    driverExtraClassPath.foreach { cp => conf.set("spark.driver.extraClassPath", cp) }
    driverExtraLibraryPath.foreach { lp => conf.set("spark.driver.extraLibraryPath", lp) }
    executorMemory.foreach { m => conf.set("spark.executor.memory", m) }
    totalExecutorCores.foreach { c => conf.set("spark.cores.max", c) }

    // Construct driver description and submit it
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", mainClass) ++ appArgs, // args to the DriverWrapper
      environmentVariables, extraClassPath, extraLibraryPath, javaOpts)
    val actualDriverMemory = driverMemory.map(Utils.memoryStringToMb).getOrElse(DEFAULT_MEMORY)
    val actualDriverCores = driverCores.map(_.toInt).getOrElse(DEFAULT_CORES)
    val actualSuperviseDriver = superviseDriver.map(_.toBoolean).getOrElse(DEFAULT_SUPERVISE)
    new DriverDescription(
      appResource, actualDriverMemory, actualDriverCores, actualSuperviseDriver, command)
  }
}

/**
 * A default servlet that handles error cases that are not captured by other servlets.
 */
private[spark] class ErrorServlet extends StandaloneRestServlet {
  private val expectedVersion = StandaloneRestServer.PROTOCOL_VERSION

  /** Service a faulty request by returning an appropriate error message to the client. */
  protected override def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").toSeq
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          // http://host:port/
          "Missing protocol version."
        case `expectedVersion` :: Nil =>
          // http://host:port/correct-version
          "Missing the /submissions prefix."
        case `expectedVersion` :: "submissions" :: Nil =>
          // http://host:port/correct-version/submissions
          "Missing an action: please specify one of /create, /kill, or /status."
        case unknownVersion :: _ =>
          // http://host:port/unknown-version/*
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          // never reached
          s"Malformed path $path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$expectedVersion/submissions/..."
    val error = handleError(msg)
    // If there is a version mismatch, include the highest protocol version that
    // this server supports in case the client wants to retry with our version
    if (versionMismatch) {
      error.protocolVersion = expectedVersion
      response.setStatus(StandaloneRestServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
  }
}
