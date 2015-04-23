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
import java.net.InetSocketAddress
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}

import scala.io.Source

import akka.actor.ActorRef
import com.fasterxml.jackson.core.JsonProcessingException
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletHolder, ServletContextHandler}
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{Logging, SparkConf, SPARK_VERSION => sparkVersion}
import org.apache.spark.util.{AkkaUtils, RpcUtils, Utils}
import org.apache.spark.deploy.{Command, DeployMessages, DriverDescription}
import org.apache.spark.deploy.ClientArguments._

/**
 * A server that responds to requests submitted by the [[StandaloneRestClient]].
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
 * @param masterActor reference to the Master actor to which requests can be sent
 * @param masterUrl the URL of the Master new drivers will attempt to connect to
 * @param masterConf the conf used by the Master
 */
private[deploy] class StandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterActor: ActorRef,
    masterUrl: String,
    masterConf: SparkConf)
  extends Logging {

  import StandaloneRestServer._

  private var _server: Option[Server] = None

  // A mapping from URL prefixes to servlets that serve them. Exposed for testing.
  protected val baseContext = s"/$PROTOCOL_VERSION/submissions"
  protected val contextToServlet = Map[String, StandaloneRestServlet](
    s"$baseContext/create/*" -> new SubmitRequestServlet(masterActor, masterUrl, masterConf),
    s"$baseContext/kill/*" -> new KillRequestServlet(masterActor, masterConf),
    s"$baseContext/status/*" -> new StatusRequestServlet(masterActor, masterConf),
    "/*" -> new ErrorServlet // default handler
  )

  /** Start the server and return the bound port. */
  def start(): Int = {
    val (server, boundPort) = Utils.startServiceOnPort[Server](requestedPort, doStart, masterConf)
    _server = Some(server)
    logInfo(s"Started REST server for submitting applications on port $boundPort")
    boundPort
  }

  /**
   * Map the servlets to their corresponding contexts and attach them to a server.
   * Return a 2-tuple of the started server and the bound port.
   */
  private def doStart(startPort: Int): (Server, Int) = {
    val server = new Server(new InetSocketAddress(host, startPort))
    val threadPool = new QueuedThreadPool
    threadPool.setDaemon(true)
    server.setThreadPool(threadPool)
    val mainHandler = new ServletContextHandler
    mainHandler.setContextPath("/")
    contextToServlet.foreach { case (prefix, servlet) =>
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

private[rest] object StandaloneRestServer {
  val PROTOCOL_VERSION = StandaloneRestClient.PROTOCOL_VERSION
  val SC_UNKNOWN_PROTOCOL_VERSION = 468
}

/**
 * An abstract servlet for handling requests passed to the [[StandaloneRestServer]].
 */
private[rest] abstract class StandaloneRestServlet extends HttpServlet with Logging {

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
    responseServlet.getWriter.write(message.toJson)
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
    unknown match {
      case j: JObject => j.obj.map { case (k, _) => k }.toArray
      case _ => Array.empty[String] // No difference
    }
  }

  /** Return a human readable String representation of the exception. */
  protected def formatException(e: Throwable): String = {
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
   * Parse a submission ID from the relative path, assuming it is the first part of the path.
   * For instance, we expect the path to take the form /[submission ID]/maybe/something/else.
   * The returned submission ID cannot be empty. If the path is unexpected, return None.
   */
  protected def parseSubmissionId(path: String): Option[String] = {
    if (path == null || path.isEmpty) {
      None
    } else {
      path.stripPrefix("/").split("/").headOption.filter(_.nonEmpty)
    }
  }

  /**
   * Validate the response to ensure that it is correctly constructed.
   *
   * If it is, simply return the message as is. Otherwise, return an error response instead
   * to propagate the exception back to the client and set the appropriate error code.
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
private[rest] class KillRequestServlet(masterActor: ActorRef, conf: SparkConf)
  extends StandaloneRestServlet {

  /**
   * If a submission ID is specified in the URL, have the Master kill the corresponding
   * driver and return an appropriate response to the client. Otherwise, return error.
   */
  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleKill).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in kill request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleKill(submissionId: String): KillSubmissionResponse = {
    val askTimeout = RpcUtils.askTimeout(conf)
    val response = AkkaUtils.askWithReply[DeployMessages.KillDriverResponse](
      DeployMessages.RequestKillDriver(submissionId), masterActor, askTimeout)
    val k = new KillSubmissionResponse
    k.serverSparkVersion = sparkVersion
    k.message = response.message
    k.submissionId = submissionId
    k.success = response.success
    k
  }
}

/**
 * A servlet for handling status requests passed to the [[StandaloneRestServer]].
 */
private[rest] class StatusRequestServlet(masterActor: ActorRef, conf: SparkConf)
  extends StandaloneRestServlet {

  /**
   * If a submission ID is specified in the URL, request the status of the corresponding
   * driver from the Master and include it in the response. Otherwise, return error.
   */
  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val submissionId = parseSubmissionId(request.getPathInfo)
    val responseMessage = submissionId.map(handleStatus).getOrElse {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
      handleError("Submission ID is missing in status request.")
    }
    sendResponse(responseMessage, response)
  }

  protected def handleStatus(submissionId: String): SubmissionStatusResponse = {
    val askTimeout = RpcUtils.askTimeout(conf)
    val response = AkkaUtils.askWithReply[DeployMessages.DriverStatusResponse](
      DeployMessages.RequestDriverStatus(submissionId), masterActor, askTimeout)
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
 * A servlet for handling submit requests passed to the [[StandaloneRestServer]].
 */
private[rest] class SubmitRequestServlet(
    masterActor: ActorRef,
    masterUrl: String,
    conf: SparkConf)
  extends StandaloneRestServlet {

  /**
   * Submit an application to the Master with parameters specified in the request.
   *
   * The request is assumed to be a [[SubmitRestProtocolRequest]] in the form of JSON.
   * If the request is successfully processed, return an appropriate response to the
   * client indicating so. Otherwise, return error instead.
   */
  protected override def doPost(
      requestServlet: HttpServletRequest,
      responseServlet: HttpServletResponse): Unit = {
    val responseMessage =
      try {
        val requestMessageJson = Source.fromInputStream(requestServlet.getInputStream).mkString
        val requestMessage = SubmitRestProtocolMessage.fromJson(requestMessageJson)
        // The response should have already been validated on the client.
        // In case this is not true, validate it ourselves to avoid potential NPEs.
        requestMessage.validate()
        handleSubmit(requestMessageJson, requestMessage, responseServlet)
      } catch {
        // The client failed to provide a valid JSON, so this is not our fault
        case e @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
          responseServlet.setStatus(HttpServletResponse.SC_BAD_REQUEST)
          handleError("Malformed request: " + formatException(e))
      }
    sendResponse(responseMessage, responseServlet)
  }

  /**
   * Handle the submit request and construct an appropriate response to return to the client.
   *
   * This assumes that the request message is already successfully validated.
   * If the request message is not of the expected type, return error to the client.
   */
  private def handleSubmit(
      requestMessageJson: String,
      requestMessage: SubmitRestProtocolMessage,
      responseServlet: HttpServletResponse): SubmitRestProtocolResponse = {
    requestMessage match {
      case submitRequest: CreateSubmissionRequest =>
        val askTimeout = RpcUtils.askTimeout(conf)
        val driverDescription = buildDriverDescription(submitRequest)
        val response = AkkaUtils.askWithReply[DeployMessages.SubmitDriverResponse](
          DeployMessages.RequestSubmitDriver(driverDescription), masterActor, askTimeout)
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
    val appResource = Option(request.appResource).getOrElse {
      throw new SubmitRestMissingFieldException("Application jar is missing.")
    }
    val mainClass = Option(request.mainClass).getOrElse {
      throw new SubmitRestMissingFieldException("Main class is missing.")
    }

    // Optional fields
    val sparkProperties = request.sparkProperties
    val driverMemory = sparkProperties.get("spark.driver.memory")
    val driverCores = sparkProperties.get("spark.driver.cores")
    val driverExtraJavaOptions = sparkProperties.get("spark.driver.extraJavaOptions")
    val driverExtraClassPath = sparkProperties.get("spark.driver.extraClassPath")
    val driverExtraLibraryPath = sparkProperties.get("spark.driver.extraLibraryPath")
    val superviseDriver = sparkProperties.get("spark.driver.supervise")
    val appArgs = request.appArgs
    val environmentVariables = request.environmentVariables

    // Construct driver description
    val conf = new SparkConf(false)
      .setAll(sparkProperties)
      .set("spark.master", masterUrl)
    val extraClassPath = driverExtraClassPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraLibraryPath = driverExtraLibraryPath.toSeq.flatMap(_.split(File.pathSeparator))
    val extraJavaOpts = driverExtraJavaOptions.map(Utils.splitCommandString).getOrElse(Seq.empty)
    val sparkJavaOpts = Utils.sparkJavaOpts(conf)
    val javaOpts = sparkJavaOpts ++ extraJavaOpts
    val command = new Command(
      "org.apache.spark.deploy.worker.DriverWrapper",
      Seq("{{WORKER_URL}}", "{{USER_JAR}}", mainClass) ++ appArgs, // args to the DriverWrapper
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
private class ErrorServlet extends StandaloneRestServlet {
  private val serverVersion = StandaloneRestServer.PROTOCOL_VERSION

  /** Service a faulty request by returning an appropriate error message to the client. */
  protected override def service(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    val path = request.getPathInfo
    val parts = path.stripPrefix("/").split("/").filter(_.nonEmpty).toList
    var versionMismatch = false
    var msg =
      parts match {
        case Nil =>
          // http://host:port/
          "Missing protocol version."
        case `serverVersion` :: Nil =>
          // http://host:port/correct-version
          "Missing the /submissions prefix."
        case `serverVersion` :: "submissions" :: tail =>
          // http://host:port/correct-version/submissions/*
          "Missing an action: please specify one of /create, /kill, or /status."
        case unknownVersion :: tail =>
          // http://host:port/unknown-version/*
          versionMismatch = true
          s"Unknown protocol version '$unknownVersion'."
        case _ =>
          // never reached
          s"Malformed path $path."
      }
    msg += s" Please submit requests through http://[host]:[port]/$serverVersion/submissions/..."
    val error = handleError(msg)
    // If there is a version mismatch, include the highest protocol version that
    // this server supports in case the client wants to retry with our version
    if (versionMismatch) {
      error.highestProtocolVersion = serverVersion
      response.setStatus(StandaloneRestServer.SC_UNKNOWN_PROTOCOL_VERSION)
    } else {
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
    }
    sendResponse(error, response)
  }
}
