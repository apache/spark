package org.apache.spark.deploy.rest.yarn

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{SocketException, HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.google.common.base.Charsets
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.{AppState, ApplicationSubmissionContextInfo}
import org.apache.spark.Logging
import org.apache.spark.deploy.rest.{SubmitRestConnectionException, SubmitRestProtocolException}
import org.apache.spark.util.Utils

import scala.io.Source
import scala.reflect.{classTag, ClassTag}
import scala.util.control.NonFatal

private[spark] class YarnRestSubmissionClient(rmWebAppAddress: String) extends Logging {
  import YarnRestSubmissionClient._

  Utils.checkHostPort(rmWebAppAddress, "Resource Manager address is illegal")

  def createSubmission(request: ApplicationSubmissionContextInfo): Unit = {
  }

  def requestStateSubmission(applicationId: String, quiet: Boolean = false): AppState = {
    logInfo(s"Submit a request for status of application $applicationId in resource manager")

    val url = getStateUrl(rmWebAppAddress, applicationId)
    var appState: AppState = null
    try {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("GET")
      appState = getResponse[AppState](conn, classOf[AppState])
      if (!quiet) {
        logInfo(s"The current requested status of application $applicationId is" +
          s" ${appState.getState}")
      }
    } catch {
      case NonFatal(e) => logWarning(s"Unable to get state from resource manager", e)
    }

    appState
  }

  def killSubmission(applicationId: String): Unit = {
    logInfo(s"Submit a request for kill application $applicationId in resource manager")

    val url = getStateUrl(rmWebAppAddress, applicationId)
    try {
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("PUT")
      conn.setRequestProperty("Content-Type", "application/json")
      conn.setRequestProperty("charset", "utf-8")
      conn.setDoOutput(true)
      val killState = new AppState("KILLED")
      val json = mapper.writeValueAsString(killState)
      println(json)

      val out = new DataOutputStream(conn.getOutputStream)
      out.write(json.getBytes("utf-8"))
      out.close()

      val a = getResponse[AppState](conn, classOf[AppState])
      println(a)

    }
  }


  private def decodeResponse[T: ClassTag](json: String): T = {
    try {
      val clazz = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    }


    try {
      val responseCode = connection.getResponseCode
      if (responseCode != HttpServletResponse.SC_OK) {

      }
      val dataStream = if (responseCode == HttpServletResponse.SC_OK) {
        connection.getInputStream
      } else {
        connection.getErrorStream
      }

      if (dataStream == null) {
        throw new SubmitRestProtocolException("Server returned empty body")
      }

      val responseMsg = Source.fromInputStream(dataStream).mkString
      println(s"$responseMsg")
      if (responseCode != HttpServletResponse.SC_OK) {
        logError(s"Server responded with error:\n$responseMsg")
      }

      mapper.readValue(responseMsg, clazz)
    } catch {
      case unreachable @ (_: FileNotFoundException | _: SocketException) =>
        throw new SubmitRestConnectionException("Unable to connect to server", unreachable)
      case malformed @ (_: JsonProcessingException | _: SubmitRestProtocolException) =>
        throw new SubmitRestProtocolException("Malformed response received from server", malformed)
    }
  }


  /**
   * Return the REST URL for get new application id and resource, used for submitting new
   * application.
   */
  private def getNewApplicationUrl(rmAddr: String): URL =
    new URL(s"${getBaseUrl(rmAddr)}/apps/new-application")

  /** Return the REST URL for submitting application */
  private def getSubmitUrl(rmAddr: String): URL = new URL(s"${getBaseUrl(rmAddr)}/apps")

  /**
   * Return the REST URL for requesting the state of application, or killing the application by
   * changing the current state.
   */
  private def getStateUrl(rmAddr: String, applicationId: String): URL =
    new URL(s"${getBaseUrl(rmAddr)}/apps/$applicationId/state")

  /** Return the base URL string for communicating with RM REST server */
  private def getBaseUrl(rmAddr: String): String = s"http://$rmAddr/ws/$PROTOCOL_VERSION/cluster"
}

private[spark] object YarnRestSubmissionClient {
  val PROTOCOL_VERSION = "v1"
  val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)


  def main(args: Array[String]): Unit = {

    val restClient = new YarnRestSubmissionClient("localhost:8088")

    val applicationId = "application_1439974491336_0003"

    restClient.killSubmission(applicationId)
  }
}



