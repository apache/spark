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

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{ConnectException, HttpURLConnection, SocketException, URL}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeoutException

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.control.NonFatal

import com.fasterxml.jackson.core.JsonProcessingException
import jakarta.servlet.http.HttpServletResponse

import org.apache.spark.{SPARK_VERSION => sparkVersion, SparkConf, SparkException}
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CLASS_NAME, ERROR, SUBMISSION_ID}
import org.apache.spark.util.Utils

/**
 * A client that submits applications to a [[RestSubmissionServer]].
 *
 * In protocol version v1, the REST URL takes the form http://[host:port]/v1/submissions/[action],
 * where [action] can be one of create, kill, or status. Each type of request is represented in
 * an HTTP message sent to the following prefixes:
 *   (1) submit - POST to /submissions/create
 *   (2) kill - POST /submissions/kill/[submissionId]
 *   (3) status - GET /submissions/status/[submissionId]
 *
 * In the case of (1), parameters are posted in the HTTP body in the form of JSON fields.
 * Otherwise, the URL fully specifies the intended action of the client.
 *
 * Since the protocol is expected to be stable across Spark versions, existing fields cannot be
 * added or removed, though new optional fields can be added. In the rare event that forward or
 * backward compatibility is broken, Spark must introduce a new protocol version (e.g. v2).
 *
 * The client and the server must communicate using the same version of the protocol. If there
 * is a mismatch, the server will respond with the highest protocol version it supports. A future
 * implementation of this client can use that information to retry using the version specified
 * by the server.
 */
private[spark] class RestSubmissionClient(master: String) extends Logging {
  import RestSubmissionClient._

  private val masters: Array[String] = if (master.startsWith("spark://")) {
    Utils.parseStandaloneMasterUrls(master)
  } else {
    Array(master)
  }

  // Set of masters that lost contact with us, used to keep track of
  // whether there are masters still alive for us to communicate with
  private val lostMasters = new mutable.HashSet[String]

  /**
   * Submit an application specified by the parameters in the provided request.
   *
   * If the submission was successful, poll the status of the submission and report
   * it to the user. Otherwise, report the error message provided by the server.
   */
  def createSubmission(request: CreateSubmissionRequest): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to launch an application in $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getSubmitUrl(m)
      try {
        response = postJson(url, request.toJson)
        response match {
          case s: CreateSubmissionResponse =>
            if (s.success) {
              reportSubmissionStatus(s)
              handleRestResponse(s)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Request that the server kill the specified submission. */
  def killSubmission(submissionId: String): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to kill submission $submissionId in $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getKillUrl(m, submissionId)
      try {
        response = post(url)
        response match {
          case k: KillSubmissionResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Request that the server kill all submissions. */
  def killAllSubmissions(): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to kill all submissions in $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getKillAllUrl(m)
      try {
        response = post(url)
        response match {
          case k: KillAllSubmissionResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Request that the server clears all submissions and applications. */
  def clear(): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to clear $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getClearUrl(m)
      try {
        response = post(url)
        response match {
          case k: ClearResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Check the readiness of Master. */
  def readyz(): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request to check the status of $master.")
    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = new ErrorResponse
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getReadyzUrl(m)
      try {
        response = get(url)
        response match {
          case k: ReadyzResponse =>
            if (!Utils.responseFromBackup(k.message)) {
              handleRestResponse(k)
              handled = true
            }
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Request the status of a submission from the server. */
  def requestSubmissionStatus(
      submissionId: String,
      quiet: Boolean = false): SubmitRestProtocolResponse = {
    logInfo(s"Submitting a request for the status of submission $submissionId in $master.")

    var handled: Boolean = false
    var response: SubmitRestProtocolResponse = null
    for (m <- masters if !handled) {
      validateMaster(m)
      val url = getStatusUrl(m, submissionId)
      try {
        response = get(url)
        response match {
          case s: SubmissionStatusResponse if s.success =>
            if (!quiet) {
              handleRestResponse(s)
            }
            handled = true
          case unexpected =>
            handleUnexpectedRestResponse(unexpected)
        }
      } catch {
        case e: SubmitRestConnectionException =>
          if (handleConnectionException(m)) {
            throw new SubmitRestConnectionException("Unable to connect to server", e)
          }
      }
    }
    response
  }

  /** Construct a message that captures the specified parameters for submitting an application. */
  def constructSubmitRequest(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      sparkProperties: Map[String, String],
      environmentVariables: Map[String, String]): CreateSubmissionRequest = {
    val message = new CreateSubmissionRequest
    message.clientSparkVersion = sparkVersion
    message.appResource = appResource
    message.mainClass = mainClass
    message.appArgs = appArgs
    message.sparkProperties = sparkProperties
    message.environmentVariables = environmentVariables
    message.validate()
    message
  }

  /** Send a GET request to the specified URL. */
  private def get(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending GET request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    readResponse(conn)
  }

  /** Send a POST request to the specified URL. */
  private def post(url: URL): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    readResponse(conn)
  }

  /** Send a POST request with the given JSON as the body to the specified URL. */
  private def postJson(url: URL, json: String): SubmitRestProtocolResponse = {
    logDebug(s"Sending POST request to server at $url:\n$json")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(json.getBytes(StandardCharsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    readResponse(conn)
  }

  /**
   * Read the response from the server and return it as a validated [[SubmitRestProtocolResponse]].
   * If the response represents an error, report the embedded message to the user.
   * Exposed for testing.
   */
  private[rest] def readResponse(connection: HttpURLConnection): SubmitRestProtocolResponse = {
    // scalastyle:off executioncontextglobal
    import scala.concurrent.ExecutionContext.Implicits.global
    // scalastyle:on executioncontextglobal
    val responseFuture = Future {
      val responseCode = connection.getResponseCode

      if (responseCode != HttpServletResponse.SC_OK) {
        val errString = Some(Source.fromInputStream(connection.getErrorStream())
          .getLines().mkString("\n"))
        if (responseCode == HttpServletResponse.SC_INTERNAL_SERVER_ERROR &&
          !connection.getContentType().contains("application/json")) {
          throw new SubmitRestProtocolException(s"Server responded with exception:\n${errString}")
        }
        logError(log"Server responded with error:\n${MDC(ERROR, errString)}")
        val error = new ErrorResponse
        if (responseCode == RestSubmissionServer.SC_UNKNOWN_PROTOCOL_VERSION) {
          error.highestProtocolVersion = RestSubmissionServer.PROTOCOL_VERSION
        }
        error.message = errString.get
        error
      } else {
        val dataStream = connection.getInputStream

        // If the server threw an exception while writing a response, it will not have a body
        if (dataStream == null) {
          throw new SubmitRestProtocolException("Server returned empty body")
        }
        val responseJson = Source.fromInputStream(dataStream).mkString
        logDebug(s"Response from the server:\n$responseJson")
        val response = SubmitRestProtocolMessage.fromJson(responseJson)
        response.validate()
        response match {
          // If the response is an error, log the message
          case error: ErrorResponse =>
            logError(log"Server responded with error:\n${MDC(ERROR, error.message)}")
            error
          // Otherwise, simply return the response
          case response: SubmitRestProtocolResponse => response
          case unexpected =>
            throw new SubmitRestProtocolException(
              s"Message received from server was not a response:\n${unexpected.toJson}")
        }
      }
    }

    // scalastyle:off awaitresult
    try { Await.result(responseFuture, 10.seconds) } catch {
      // scalastyle:on awaitresult
      case unreachable @ (_: FileNotFoundException | _: SocketException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
      case malformed @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        throw new SubmitRestProtocolException("Malformed response received from server", malformed)
      case timeout: TimeoutException =>
        throw new SubmitRestConnectionException("No response from server", timeout)
      case NonFatal(t) =>
        throw new SparkException("Exception while waiting for response", t)
    }
  }

  /** Return the REST URL for creating a new submission. */
  private def getSubmitUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/create")
  }

  /** Return the REST URL for killing an existing submission. */
  private def getKillUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/kill/$submissionId")
  }

  /** Return the REST URL for killing all submissions. */
  private def getKillAllUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/killall")
  }

  /** Return the REST URL for clear all existing submissions and applications. */
  private def getClearUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/clear")
  }

  /** Return the REST URL for requesting the readyz API. */
  private def getReadyzUrl(master: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/readyz")
  }

  /** Return the REST URL for requesting the status of an existing submission. */
  private def getStatusUrl(master: String, submissionId: String): URL = {
    val baseUrl = getBaseUrl(master)
    new URL(s"$baseUrl/status/$submissionId")
  }

  /** Return the base URL for communicating with the server, including the protocol version. */
  private def getBaseUrl(master: String): String = {
    var masterUrl = master
    supportedMasterPrefixes.foreach { prefix =>
      if (master.startsWith(prefix)) {
        masterUrl = master.stripPrefix(prefix)
      }
    }
    masterUrl = masterUrl.stripSuffix("/")
    s"http://$masterUrl/$PROTOCOL_VERSION/submissions"
  }

  /** Throw an exception if this is not standalone mode. */
  private def validateMaster(master: String): Unit = {
    val valid = supportedMasterPrefixes.exists { prefix => master.startsWith(prefix) }
    if (!valid) {
      throw new IllegalArgumentException(
        "This REST client only supports master URLs that start with " +
          "one of the following: " + supportedMasterPrefixes.mkString(","))
    }
  }

  /** Report the status of a newly created submission. */
  private def reportSubmissionStatus(
      submitResponse: CreateSubmissionResponse): Unit = {
    if (submitResponse.success) {
      val submissionId = submitResponse.submissionId
      if (submissionId != null) {
        logInfo(s"Submission successfully created as $submissionId. Polling submission state...")
        pollSubmissionStatus(submissionId)
      } else {
        // should never happen
        logError("Application successfully submitted, but submission ID was not provided!")
      }
    } else {
      val failMessage = Option(submitResponse.message).map { ": " + _ }.getOrElse("")
      logError(log"Application submission failed${MDC(ERROR, failMessage)}")
    }
  }

  /**
   * Poll the status of the specified submission and log it.
   * This retries up to a fixed number of times before giving up.
   */
  private def pollSubmissionStatus(submissionId: String): Unit = {
    (1 to REPORT_DRIVER_STATUS_MAX_TRIES).foreach { _ =>
      val response = requestSubmissionStatus(submissionId, quiet = true)
      val statusResponse = response match {
        case s: SubmissionStatusResponse => s
        case _ => return // unexpected type, let upstream caller handle it
      }
      if (statusResponse.success) {
        val driverState = Option(statusResponse.driverState)
        val workerId = Option(statusResponse.workerId)
        val workerHostPort = Option(statusResponse.workerHostPort)
        val exception = Option(statusResponse.message)
        // Log driver state, if present
        driverState match {
          case Some(state) => logInfo(s"State of driver $submissionId is now $state.")
          case _ =>
            logError(log"State of driver ${MDC(SUBMISSION_ID, submissionId)} was not found!")
        }
        // Log worker node, if present
        (workerId, workerHostPort) match {
          case (Some(id), Some(hp)) => logInfo(s"Driver is running on worker $id at $hp.")
          case _ =>
        }
        // Log exception stack trace, if present
        exception.foreach { e => logError(log"${MDC(ERROR, e)}") }
        return
      }
      Thread.sleep(REPORT_DRIVER_STATUS_INTERVAL)
    }
    logError(log"Error: Master did not recognize driver ${MDC(SUBMISSION_ID, submissionId)}.")
  }

  /** Log the response sent by the server in the REST application submission protocol. */
  private def handleRestResponse(response: SubmitRestProtocolResponse): Unit = {
    logInfo(s"Server responded with ${response.messageType}:\n${response.toJson}")
  }

  /** Log an appropriate error if the response sent by the server is not of the expected type. */
  private def handleUnexpectedRestResponse(unexpected: SubmitRestProtocolResponse): Unit = {
    // scalastyle:off line.size.limit
    logError(log"Error: Server responded with message of unexpected type ${MDC(CLASS_NAME, unexpected.messageType)}.")
    // scalastyle:on
  }

  /**
   * When a connection exception is caught, return true if all masters are lost.
   * Note that the heuristic used here does not take into account that masters
   * can recover during the lifetime of this client. This assumption should be
   * harmless because this client currently does not support retrying submission
   * on failure yet (SPARK-6443).
   */
  private def handleConnectionException(masterUrl: String): Boolean = {
    if (!lostMasters.contains(masterUrl)) {
      logWarning(s"Unable to connect to server ${masterUrl}.")
      lostMasters += masterUrl
    }
    lostMasters.size >= masters.length
  }
}

private[spark] object RestSubmissionClient {

  val supportedMasterPrefixes = Seq("spark://")

  // SPARK_HOME and SPARK_CONF_DIR are filtered out because they are usually wrong
  // on the remote machine (SPARK-12345) (SPARK-25934)
  private val EXCLUDED_SPARK_ENV_VARS = Set("SPARK_ENV_LOADED", "SPARK_HOME", "SPARK_CONF_DIR")
  private val REPORT_DRIVER_STATUS_INTERVAL = 1000
  private val REPORT_DRIVER_STATUS_MAX_TRIES = 10
  val PROTOCOL_VERSION = "v1"

  /**
   * Filter non-spark environment variables from any environment.
   */
  private[rest] def filterSystemEnvironment(env: Map[String, String]): Map[String, String] = {
    env.filter { case (k, _) =>
      k.startsWith("SPARK_") && !EXCLUDED_SPARK_ENV_VARS.contains(k)
    }
  }

  private[spark] def supportsRestClient(master: String): Boolean = {
    supportedMasterPrefixes.exists(master.startsWith)
  }
}

private[spark] class RestSubmissionClientApp extends SparkApplication {

  /** Submits a request to run the application and return the response. Visible for testing. */
  def run(
      appResource: String,
      mainClass: String,
      appArgs: Array[String],
      conf: SparkConf,
      env: Map[String, String] = Map()): SubmitRestProtocolResponse = {
    val master = conf.getOption("spark.master").getOrElse {
      throw new IllegalArgumentException("'spark.master' must be set.")
    }
    val sparkProperties = conf.getAll.toMap
    val client = new RestSubmissionClient(master)
    val submitRequest = client.constructSubmitRequest(
      appResource, mainClass, appArgs, sparkProperties, env)
    client.createSubmission(submitRequest)
  }

  override def start(args: Array[String], conf: SparkConf): Unit = {
    if (args.length < 2) {
      sys.error("Usage: RestSubmissionClient [app resource] [main class] [app args*]")
      sys.exit(1)
    }
    val appResource = args(0)
    val mainClass = args(1)
    val appArgs = args.slice(2, args.length)
    val env = RestSubmissionClient.filterSystemEnvironment(sys.env)
    run(appResource, mainClass, appArgs, conf, env)
  }
}
