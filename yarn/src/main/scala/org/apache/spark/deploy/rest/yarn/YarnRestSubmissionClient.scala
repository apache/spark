package org.apache.spark.deploy.rest.yarn

import java.io.{DataOutputStream, FileNotFoundException}
import java.net.{ConnectException, SocketException, HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

import org.apache.hadoop.util.StringUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.deploy.SparkHadoopUtil

import scala.collection.JavaConversions._
import scala.io.Source
import scala.reflect.{classTag, ClassTag}

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.base.Charsets
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.util.{Records, ConverterUtils}

import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.deploy.rest.{SubmitRestConnectionException, SubmitRestProtocolException}
import org.apache.spark.deploy.yarn.{Client, ClientArguments}
import org.apache.spark.util.Utils

private[spark] class YarnRestSubmissionClient(rmWebAppAddress: String) extends Logging {
  import YarnRestSubmissionClient._

  Utils.checkHostPort(rmWebAppAddress, "Resource Manager address is illegal")

  def createSubmission(appSubmissionInfo: ApplicationSubmissionContextInfo): Unit = {
    logWarning(
      s"""
         |Submitting applications using Yarn REST API is only supported for Hadoop 2.6+.
         |Currently it only supports non-secure mode.
       """.stripMargin)

    logInfo("submit a request for submitting a new application in resource manager")

    val url = getSubmitUrl(rmWebAppAddress, currentUser)
    try {
      val conn = updateJson(url, "POST", appSubmissionInfo.toJson)
      val respCode = conn.getResponseCode
      if (respCode == HttpServletResponse.SC_ACCEPTED) {
        val location = conn.getHeaderField("Location")
        logInfo(s"Return location is $location, you could check the runnning status of " +
          "application in the web page of resource manager.")
      } else {
        val respJson = Source.fromInputStream(conn.getErrorStream).mkString
        logError(s"Resource manager responded error message:\n$respJson")
      }
    } catch {
      case e: SubmitRestConnectionException =>
        logError(s"Unable to connect to resource manager", e)
      case t: SubmitRestProtocolException =>
        logError(s"Unable to parse the received JSON message", t)
    }
  }

  def constructAppSubmissionInfo(
      args: ClientArguments,
      sparkConf: SparkConf
    ): ApplicationSubmissionContextInfo = {

    val newApplication = newApplicationSubmission()

    // Create yarn client internally to handle environment related things.
    val client = new Client(args, sparkConf)

    // Verify whether the cluster has enough resources for AM
    client.verifyClusterResources(newApplication.maxResourceCapability.memory)

    val appId = ConverterUtils.toApplicationId(newApplication.applicationId)
    val resource = Records.newRecord(classOf[Resource])
    resource.setMemory(args.amMemory + args.amMemoryOverhead)
    resource.setVirtualCores(args.amCores)

    // Set up the credentials and putting this into ContainerContext
    client.setupCredentials()

    // Create the ContainerLaunchContext through Client, used to create ContainerLaunchContextInfo
    val containerContext = client.createContainerLaunchContext(appId)

    // Get application tags
    val tags = sparkConf.getOption(Client.CONF_SPARK_YARN_APPLICATION_TAGS)
      .map(StringUtils.getTrimmedStringCollection(_))
      .filter(!_.isEmpty)
      .map(_.toSet)

    new ApplicationSubmissionContextInfo().buildFrom(
      newApplication.applicationId,
      args.appName,
      args.amQueue,
      containerContext,
      resource,
      tags.getOrElse(Set.empty),
      sparkConf.getInt("spark.yarn.maxAppAttempts", 2))
  }

  def requestStateSubmission(applicationId: String, quiet: Boolean = false
      ): YarnSubmitRestProtocolResponse = {
    logInfo(s"Submit a request for status of application $applicationId in resource manager")

    val url = getStateUrl(rmWebAppAddress, applicationId, currentUser)
    var response: YarnSubmitRestProtocolResponse = null
    try {
      val conn = get(url)
      response = readResponse[AppState](conn)
      response match {
        case a: AppState =>
          if (!quiet) {
            logInfo(s"The current requested status of application $applicationId is ${a.state}")
          }
        case unexpected =>
          logError(s"Resource manager responded error message:\n${unexpected.toJson}")
      }
    } catch {
      case e: SubmitRestConnectionException =>
        logError(s"Unable to connect to resource manager", e)
      case t: SubmitRestProtocolException =>
        logError(s"Unable to parse the received JSON message", t)
    }
    response
  }

  def killSubmission(applicationId: String): YarnSubmitRestProtocolResponse = {
    logInfo(s"Submit a request for killing application $applicationId in resource manager")

    val url = getStateUrl(rmWebAppAddress, applicationId, currentUser)
    var response: YarnSubmitRestProtocolResponse = null
    try {
      val appState = new AppState
      appState.state = "KILLED"
      val conn = updateJson(url, "PUT", appState.toJson)
      response = readResponse[AppState](conn)
      response match {
        case a: AppState =>
          logInfo(s"Successfully put the KILL command to resource manager, state: ${a.state}")
        case unexpected =>
          logError(s"Resource manager responded error message:\n${unexpected.toJson}")
      }
    } catch {
      case e: SubmitRestConnectionException =>
        logError(s"Unable to connect to resource manager", e)
      case t: SubmitRestProtocolException =>
        logError(s"Unable to parse the received JSON message", t)
    }
    response
  }

  private def newApplicationSubmission(): NewApplication = {
    logInfo(s"Submit a request for creating new application in resource manager")

    val url = getNewApplicationUrl(rmWebAppAddress, currentUser)
    var newApplication: NewApplication = null
    try {
      val conn = update(url, "POST")
      readResponse[NewApplication](conn) match {
        case n: NewApplication =>
          logInfo(s"successfully request a new application: ${n.toJson}")
          newApplication = n
        case unexpected =>
          logError(s"Resource manager responded error message:\n${unexpected.toJson}")
      }
    } catch {
      case e: SubmitRestConnectionException =>
        logError("Unable to connect to resource manager", e)
      case t: SubmitRestProtocolException =>
        logError(s"Unable to parse the received JSON message", t)
    }

    newApplication
  }

  private def get(url: URL): HttpURLConnection = {
    logDebug(s"Sending GET request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("GET")
    conn.connect()
    conn
  }

  private def update(url: URL, method: String): HttpURLConnection = {
    logDebug(s"Sending update request to server at $url.")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    conn
  }

  private def updateJson(url: URL, method: String, json: String): HttpURLConnection = {
    logDebug(s"Sending update request to server at $url:\n$json")
    val conn = url.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("charset", "utf-8")
    conn.setDoOutput(true)
    try {
      val out = new DataOutputStream(conn.getOutputStream)
      Utils.tryWithSafeFinally {
        out.write(json.getBytes(Charsets.UTF_8))
      } {
        out.close()
      }
    } catch {
      case e: ConnectException =>
        throw new SubmitRestConnectionException("Connect Exception when connect to server", e)
    }
    conn
  }

  private def readResponse[T <: YarnSubmitRestProtocolResponse : ClassTag](
      connection: HttpURLConnection): YarnSubmitRestProtocolResponse = {
    try {
      val respCode = connection.getResponseCode
      val dataStream =
        if (respCode == HttpServletResponse.SC_OK || respCode == HttpServletResponse.SC_ACCEPTED) {
          connection.getInputStream
        } else {
          connection.getErrorStream
        }
      // If the server threw an exception while writing a response, it will not have a body
      if (dataStream == null) {
        throw new SubmitRestProtocolException("Server returned empty body")
      }
      val responseJson = Source.fromInputStream(dataStream).mkString
      logDebug(s"Response from the server:\n$responseJson")
      val response = if (respCode == HttpServletResponse.SC_OK) {
        fromJson[T](responseJson)
      } else {
        fromJson[RemoteExceptionData](responseJson)
      }
      response.validate()
      response match {
        // If the response is an error, log the message
        case error: RemoteExceptionData =>
          logError(s"Server responded with error:\n${error.message}")
          error
        // Otherwise, simply return the response
        case response: YarnSubmitRestProtocolResponse => response
        case unexpected =>
          throw new SubmitRestProtocolException(
            s"Message received from server was not a response:\n${unexpected.toJson}")
      }
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
  private def getNewApplicationUrl(rmAddr: String, user: String = "dr.who"): URL =
    new URL(s"${getBaseUrl(rmAddr)}/apps/new-application?user.name=$user")

  /** Return the REST URL for submitting application */
  private def getSubmitUrl(rmAddr: String, user: String = "dr.who"): URL =
    new URL(s"${getBaseUrl (rmAddr)}/apps?user.name=$user")

  /**
   * Return the REST URL for requesting the state of application, or killing the application by
   * changing the current state.
   */
  private def getStateUrl(rmAddr: String, applicationId: String, user: String = "dr.who"): URL =
    new URL(s"${getBaseUrl(rmAddr)}/apps/$applicationId/state?user.name=$user")

  /** Return the base URL string for communicating with RM REST server */
  private def getBaseUrl(rmAddr: String): String = s"http://$rmAddr/ws/$PROTOCOL_VERSION/cluster"
}

private[spark] object YarnRestSubmissionClient extends Logging {
  val PROTOCOL_VERSION = "v1"

  val currentUser = UserGroupInformation.getCurrentUser.getShortUserName

  val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  def fromJson[T <: YarnSubmitRestProtocolResponse : ClassTag](json: String
      ): YarnSubmitRestProtocolResponse = {
    val clazz = classTag[T].runtimeClass
      .asSubclass[YarnSubmitRestProtocolResponse](classOf[YarnSubmitRestProtocolResponse])
    mapper.readValue(json, clazz)
  }

  def main(args: Array[String]): Unit = {
    // Set an env variable indicating we are running in YARN mode.
    // Note that any env variable with the SPARK_ prefix gets propagated to all (remote) processes
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf

    val clientArgs = new ClientArguments(args, sparkConf)
    // to maintain backwards-compatibility
    if (!Utils.isDynamicAllocationEnabled(sparkConf)) {
      sparkConf.setIfMissing("spark.executor.instances", clientArgs.numExecutors.toString)
    }

    val yarnConf = SparkHadoopUtil.get.newConfiguration(sparkConf).asInstanceOf[YarnConfiguration]

    val rmWebAddress = yarnConf.get("yarn.resourcemanager.webapp.address")
    require(rmWebAddress != null, "resource manager web app address is null")

    val restClient = new YarnRestSubmissionClient(rmWebAddress)

    val appSubmissionInfo = restClient.constructAppSubmissionInfo(clientArgs, sparkConf)

    logInfo(s"Application submission context info:\n${appSubmissionInfo.toJson}")

    restClient.createSubmission(appSubmissionInfo)
  }
}
