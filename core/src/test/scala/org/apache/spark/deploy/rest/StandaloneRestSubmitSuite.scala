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

import java.io.DataOutputStream
import java.net.{HttpURLConnection, URL}
import javax.servlet.http.HttpServletResponse

import scala.collection.mutable

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.google.common.base.Charsets
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark._
import org.apache.spark.util.{AkkaUtils, Utils}
import org.apache.spark.deploy.DeployMessages._
import org.apache.spark.deploy.{SparkSubmit, SparkSubmitArguments}
import org.apache.spark.deploy.master.DriverState._

/**
 * Tests for the REST application submission protocol used in standalone cluster mode.
 */
class StandaloneRestSubmitSuite extends FunSuite with BeforeAndAfterEach {
  private val client = new StandaloneRestClient
  private var actorSystem: Option[ActorSystem] = None
  private var server: Option[StandaloneRestServer] = None

  override def afterEach() {
    actorSystem.foreach(_.shutdown())
    server.foreach(_.stop())
  }

  test("construct submit request") {
    val appArgs = Array("one", "two", "three")
    val sparkProperties = Map("spark.app.name" -> "pi")
    val environmentVariables = Map("SPARK_ONE" -> "UN", "SPARK_TWO" -> "DEUX")
    val request = client.constructSubmitRequest(
      "my-app-resource", "my-main-class", appArgs, sparkProperties, environmentVariables)
    assert(request.action === Utils.getFormattedClassName(request))
    assert(request.clientSparkVersion === SPARK_VERSION)
    assert(request.appResource === "my-app-resource")
    assert(request.mainClass === "my-main-class")
    assert(request.appArgs === appArgs)
    assert(request.sparkProperties === sparkProperties)
    assert(request.environmentVariables === environmentVariables)
  }

  test("create submission") {
    val submittedDriverId = "my-driver-id"
    val submitMessage = "your driver is submitted"
    val masterUrl = startDummyServer(submitId = submittedDriverId, submitMessage = submitMessage)
    val appArgs = Array("one", "two", "four")
    val request = constructSubmitRequest(masterUrl, appArgs)
    assert(request.appArgs === appArgs)
    assert(request.sparkProperties("spark.master") === masterUrl)
    val response = client.createSubmission(masterUrl, request)
    val submitResponse = getSubmitResponse(response)
    assert(submitResponse.action === Utils.getFormattedClassName(submitResponse))
    assert(submitResponse.serverSparkVersion === SPARK_VERSION)
    assert(submitResponse.message === submitMessage)
    assert(submitResponse.submissionId === submittedDriverId)
    assert(submitResponse.success)
  }

  test("create submission from main method") {
    val submittedDriverId = "your-driver-id"
    val submitMessage = "my driver is submitted"
    val masterUrl = startDummyServer(submitId = submittedDriverId, submitMessage = submitMessage)
    val conf = new SparkConf(loadDefaults = false)
    conf.set("spark.master", masterUrl)
    conf.set("spark.app.name", "dreamer")
    val appArgs = Array("one", "two", "six")
    // main method calls this
    val response = StandaloneRestClient.run("app-resource", "main-class", appArgs, conf)
    val submitResponse = getSubmitResponse(response)
    assert(submitResponse.action === Utils.getFormattedClassName(submitResponse))
    assert(submitResponse.serverSparkVersion === SPARK_VERSION)
    assert(submitResponse.message === submitMessage)
    assert(submitResponse.submissionId === submittedDriverId)
    assert(submitResponse.success)
  }

  test("kill submission") {
    val submissionId = "my-lyft-driver"
    val killMessage = "your driver is killed"
    val masterUrl = startDummyServer(killMessage = killMessage)
    val response = client.killSubmission(masterUrl, submissionId)
    val killResponse = getKillResponse(response)
    assert(killResponse.action === Utils.getFormattedClassName(killResponse))
    assert(killResponse.serverSparkVersion === SPARK_VERSION)
    assert(killResponse.message === killMessage)
    assert(killResponse.submissionId === submissionId)
    assert(killResponse.success)
  }

  test("request submission status") {
    val submissionId = "my-uber-driver"
    val submissionState = KILLED
    val submissionException = new Exception("there was an irresponsible mix of alcohol and cars")
    val masterUrl = startDummyServer(state = submissionState, exception = Some(submissionException))
    val response = client.requestSubmissionStatus(masterUrl, submissionId)
    val statusResponse = getStatusResponse(response)
    assert(statusResponse.action === Utils.getFormattedClassName(statusResponse))
    assert(statusResponse.serverSparkVersion === SPARK_VERSION)
    assert(statusResponse.message.contains(submissionException.getMessage))
    assert(statusResponse.submissionId === submissionId)
    assert(statusResponse.driverState === submissionState.toString)
    assert(statusResponse.success)
  }

  test("create then kill") {
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val response1 = client.createSubmission(masterUrl, request)
    val submitResponse = getSubmitResponse(response1)
    assert(submitResponse.success)
    assert(submitResponse.submissionId != null)
    // kill submission that was just created
    val submissionId = submitResponse.submissionId
    val response2 = client.killSubmission(masterUrl, submissionId)
    val killResponse = getKillResponse(response2)
    assert(killResponse.success)
    assert(killResponse.submissionId === submissionId)
  }

  test("create then request status") {
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val response1 = client.createSubmission(masterUrl, request)
    val submitResponse = getSubmitResponse(response1)
    assert(submitResponse.success)
    assert(submitResponse.submissionId != null)
    // request status of submission that was just created
    val submissionId = submitResponse.submissionId
    val response2 = client.requestSubmissionStatus(masterUrl, submissionId)
    val statusResponse = getStatusResponse(response2)
    assert(statusResponse.success)
    assert(statusResponse.submissionId === submissionId)
    assert(statusResponse.driverState === RUNNING.toString)
  }

  test("create then kill then request status") {
    val masterUrl = startSmartServer()
    val request = constructSubmitRequest(masterUrl)
    val response1 = client.createSubmission(masterUrl, request)
    val response2 = client.createSubmission(masterUrl, request)
    val submitResponse1 = getSubmitResponse(response1)
    val submitResponse2 = getSubmitResponse(response2)
    assert(submitResponse1.success)
    assert(submitResponse2.success)
    assert(submitResponse1.submissionId != null)
    assert(submitResponse2.submissionId != null)
    val submissionId1 = submitResponse1.submissionId
    val submissionId2 = submitResponse2.submissionId
    // kill only submission 1, but not submission 2
    val response3 = client.killSubmission(masterUrl, submissionId1)
    val killResponse = getKillResponse(response3)
    assert(killResponse.success)
    assert(killResponse.submissionId === submissionId1)
    // request status for both submissions: 1 should be KILLED but 2 should be RUNNING still
    val response4 = client.requestSubmissionStatus(masterUrl, submissionId1)
    val response5 = client.requestSubmissionStatus(masterUrl, submissionId2)
    val statusResponse1 = getStatusResponse(response4)
    val statusResponse2 = getStatusResponse(response5)
    assert(statusResponse1.submissionId === submissionId1)
    assert(statusResponse2.submissionId === submissionId2)
    assert(statusResponse1.driverState === KILLED.toString)
    assert(statusResponse2.driverState === RUNNING.toString)
  }

  test("kill or request status before create") {
    val masterUrl = startSmartServer()
    val doesNotExist = "does-not-exist"
    // kill a non-existent submission
    val response1 = client.killSubmission(masterUrl, doesNotExist)
    val killResponse = getKillResponse(response1)
    assert(!killResponse.success)
    assert(killResponse.submissionId === doesNotExist)
    // request status for a non-existent submission
    val response2 = client.requestSubmissionStatus(masterUrl, doesNotExist)
    val statusResponse = getStatusResponse(response2)
    assert(!statusResponse.success)
    assert(statusResponse.submissionId === doesNotExist)
  }

  /* ---------------------------------------- *
   |     Aberrant client / server behavior    |
   * ---------------------------------------- */

  test("good request paths") {
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = StandaloneRestServer.PROTOCOL_VERSION
    val json = constructSubmitRequest(masterUrl).toJson
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill"
    val statusRequestPath = s"$httpUrl/$v/submissions/status"
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST", json)
    val (response2, code2) = sendHttpRequestWithResponse(s"$killRequestPath/anything", "POST")
    val (response3, code3) = sendHttpRequestWithResponse(s"$killRequestPath/any/thing", "POST")
    val (response4, code4) = sendHttpRequestWithResponse(s"$statusRequestPath/anything", "GET")
    val (response5, code5) = sendHttpRequestWithResponse(s"$statusRequestPath/any/thing", "GET")
    // these should all succeed and the responses should be of the correct types
    getSubmitResponse(response1)
    val killResponse1 = getKillResponse(response2)
    val killResponse2 = getKillResponse(response3)
    val statusResponse1 = getStatusResponse(response4)
    val statusResponse2 = getStatusResponse(response5)
    assert(killResponse1.submissionId === "anything")
    assert(killResponse2.submissionId === "any")
    assert(statusResponse1.submissionId === "anything")
    assert(statusResponse2.submissionId === "any")
    assert(code1 === HttpServletResponse.SC_OK)
    assert(code2 === HttpServletResponse.SC_OK)
    assert(code3 === HttpServletResponse.SC_OK)
    assert(code4 === HttpServletResponse.SC_OK)
    assert(code5 === HttpServletResponse.SC_OK)
  }

  test("good request paths, bad requests") {
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = StandaloneRestServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill"
    val statusRequestPath = s"$httpUrl/$v/submissions/status"
    val goodJson = constructSubmitRequest(masterUrl).toJson
    val badJson1 = goodJson.replaceAll("action", "fraction") // invalid JSON
    val badJson2 = goodJson.substring(goodJson.size / 2) // malformed JSON
    val notJson = "\"hello, world\""
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST") // missing JSON
    val (response2, code2) = sendHttpRequestWithResponse(submitRequestPath, "POST", badJson1)
    val (response3, code3) = sendHttpRequestWithResponse(submitRequestPath, "POST", badJson2)
    val (response4, code4) = sendHttpRequestWithResponse(killRequestPath, "POST") // missing ID
    val (response5, code5) = sendHttpRequestWithResponse(s"$killRequestPath/", "POST")
    val (response6, code6) = sendHttpRequestWithResponse(statusRequestPath, "GET") // missing ID
    val (response7, code7) = sendHttpRequestWithResponse(s"$statusRequestPath/", "GET")
    val (response8, code8) = sendHttpRequestWithResponse(submitRequestPath, "POST", notJson)
    // these should all fail as error responses
    getErrorResponse(response1)
    getErrorResponse(response2)
    getErrorResponse(response3)
    getErrorResponse(response4)
    getErrorResponse(response5)
    getErrorResponse(response6)
    getErrorResponse(response7)
    getErrorResponse(response8)
    assert(code1 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code2 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code3 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code4 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code5 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code6 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code7 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code8 === HttpServletResponse.SC_BAD_REQUEST)
  }

  test("bad request paths") {
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = StandaloneRestServer.PROTOCOL_VERSION
    val (response1, code1) = sendHttpRequestWithResponse(httpUrl, "GET")
    val (response2, code2) = sendHttpRequestWithResponse(s"$httpUrl/", "GET")
    val (response3, code3) = sendHttpRequestWithResponse(s"$httpUrl/$v", "GET")
    val (response4, code4) = sendHttpRequestWithResponse(s"$httpUrl/$v/", "GET")
    val (response5, code5) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions", "GET")
    val (response6, code6) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions/", "GET")
    val (response7, code7) = sendHttpRequestWithResponse(s"$httpUrl/$v/submissions/bad", "GET")
    val (response8, code8) = sendHttpRequestWithResponse(s"$httpUrl/bad-version", "GET")
    assert(code1 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code2 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code3 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code4 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code5 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code6 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code7 === HttpServletResponse.SC_BAD_REQUEST)
    assert(code8 === StandaloneRestServer.SC_UNKNOWN_PROTOCOL_VERSION)
    // all responses should be error responses
    val errorResponse1 = getErrorResponse(response1)
    val errorResponse2 = getErrorResponse(response2)
    val errorResponse3 = getErrorResponse(response3)
    val errorResponse4 = getErrorResponse(response4)
    val errorResponse5 = getErrorResponse(response5)
    val errorResponse6 = getErrorResponse(response6)
    val errorResponse7 = getErrorResponse(response7)
    val errorResponse8 = getErrorResponse(response8)
    // only the incompatible version response should have server protocol version set
    assert(errorResponse1.highestProtocolVersion === null)
    assert(errorResponse2.highestProtocolVersion === null)
    assert(errorResponse3.highestProtocolVersion === null)
    assert(errorResponse4.highestProtocolVersion === null)
    assert(errorResponse5.highestProtocolVersion === null)
    assert(errorResponse6.highestProtocolVersion === null)
    assert(errorResponse7.highestProtocolVersion === null)
    assert(errorResponse8.highestProtocolVersion === StandaloneRestServer.PROTOCOL_VERSION)
  }

  test("server returns unknown fields") {
    val masterUrl = startSmartServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = StandaloneRestServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val oldJson = constructSubmitRequest(masterUrl).toJson
    val oldFields = parse(oldJson).asInstanceOf[JObject].obj
    val newFields = oldFields ++ Seq(
      JField("tomato", JString("not-a-fruit")),
      JField("potato", JString("not-po-tah-to"))
    )
    val newJson = pretty(render(JObject(newFields)))
    // send two requests, one with the unknown fields and the other without
    val (response1, code1) = sendHttpRequestWithResponse(submitRequestPath, "POST", oldJson)
    val (response2, code2) = sendHttpRequestWithResponse(submitRequestPath, "POST", newJson)
    val submitResponse1 = getSubmitResponse(response1)
    val submitResponse2 = getSubmitResponse(response2)
    assert(code1 === HttpServletResponse.SC_OK)
    assert(code2 === HttpServletResponse.SC_OK)
    // only the response to the modified request should have unknown fields set
    assert(submitResponse1.unknownFields === null)
    assert(submitResponse2.unknownFields === Array("tomato", "potato"))
  }

  test("client handles faulty server") {
    val masterUrl = startFaultyServer()
    val httpUrl = masterUrl.replace("spark://", "http://")
    val v = StandaloneRestServer.PROTOCOL_VERSION
    val submitRequestPath = s"$httpUrl/$v/submissions/create"
    val killRequestPath = s"$httpUrl/$v/submissions/kill/anything"
    val statusRequestPath = s"$httpUrl/$v/submissions/status/anything"
    val json = constructSubmitRequest(masterUrl).toJson
    // server returns malformed response unwittingly
    // client should throw an appropriate exception to indicate server failure
    val conn1 = sendHttpRequest(submitRequestPath, "POST", json)
    intercept[SubmitRestProtocolException] { client.readResponse(conn1) }
    // server attempts to send invalid response, but fails internally on validation
    // client should receive an error response as server is able to recover
    val conn2 = sendHttpRequest(killRequestPath, "POST")
    val response2 = client.readResponse(conn2)
    getErrorResponse(response2)
    assert(conn2.getResponseCode === HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
    // server explodes internally beyond recovery
    // client should throw an appropriate exception to indicate server failure
    val conn3 = sendHttpRequest(statusRequestPath, "GET")
    intercept[SubmitRestProtocolException] { client.readResponse(conn3) } // empty response
    assert(conn3.getResponseCode === HttpServletResponse.SC_INTERNAL_SERVER_ERROR)
  }

  /* --------------------- *
   |     Helper methods    |
   * --------------------- */

  /** Start a dummy server that responds to requests using the specified parameters. */
  private def startDummyServer(
      submitId: String = "fake-driver-id",
      submitMessage: String = "driver is submitted",
      killMessage: String = "driver is killed",
      state: DriverState = FINISHED,
      exception: Option[Exception] = None): String = {
    startServer(new DummyMaster(submitId, submitMessage, killMessage, state, exception))
  }

  /** Start a smarter dummy server that keeps track of submitted driver states. */
  private def startSmartServer(): String = {
    startServer(new SmarterMaster)
  }

  /** Start a dummy server that is faulty in many ways... */
  private def startFaultyServer(): String = {
    startServer(new DummyMaster, faulty = true)
  }

  /**
   * Start a [[StandaloneRestServer]] that communicates with the given actor.
   * If `faulty` is true, start an [[FaultyStandaloneRestServer]] instead.
   * Return the master URL that corresponds to the address of this server.
   */
  private def startServer(makeFakeMaster: => Actor, faulty: Boolean = false): String = {
    val name = "test-standalone-rest-protocol"
    val conf = new SparkConf
    val localhost = Utils.localHostName()
    val securityManager = new SecurityManager(conf)
    val (_actorSystem, _) = AkkaUtils.createActorSystem(name, localhost, 0, conf, securityManager)
    val fakeMasterRef = _actorSystem.actorOf(Props(makeFakeMaster))
    val _server =
      if (faulty) {
        new FaultyStandaloneRestServer(localhost, 0, fakeMasterRef, "spark://fake:7077", conf)
      } else {
        new StandaloneRestServer(localhost, 0, fakeMasterRef, "spark://fake:7077", conf)
      }
    val port = _server.start()
    // set these to clean them up after every test
    actorSystem = Some(_actorSystem)
    server = Some(_server)
    s"spark://$localhost:$port"
  }

  /** Create a submit request with real parameters using Spark submit. */
  private def constructSubmitRequest(
      masterUrl: String,
      appArgs: Array[String] = Array.empty): CreateSubmissionRequest = {
    val mainClass = "main-class-not-used"
    val mainJar = "dummy-jar-not-used.jar"
    val commandLineArgs = Array(
      "--deploy-mode", "cluster",
      "--master", masterUrl,
      "--name", mainClass,
      "--class", mainClass,
      mainJar) ++ appArgs
    val args = new SparkSubmitArguments(commandLineArgs)
    val (_, _, sparkProperties, _) = SparkSubmit.prepareSubmitEnvironment(args)
    client.constructSubmitRequest(
      mainJar, mainClass, appArgs, sparkProperties.toMap, Map.empty)
  }

  /** Return the response as a submit response, or fail with error otherwise. */
  private def getSubmitResponse(response: SubmitRestProtocolResponse): CreateSubmissionResponse = {
    response match {
      case s: CreateSubmissionResponse => s
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected submit response. Actual: ${r.toJson}")
    }
  }

  /** Return the response as a kill response, or fail with error otherwise. */
  private def getKillResponse(response: SubmitRestProtocolResponse): KillSubmissionResponse = {
    response match {
      case k: KillSubmissionResponse => k
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected kill response. Actual: ${r.toJson}")
    }
  }

  /** Return the response as a status response, or fail with error otherwise. */
  private def getStatusResponse(response: SubmitRestProtocolResponse): SubmissionStatusResponse = {
    response match {
      case s: SubmissionStatusResponse => s
      case e: ErrorResponse => fail(s"Server returned error: ${e.message}")
      case r => fail(s"Expected status response. Actual: ${r.toJson}")
    }
  }

  /** Return the response as an error response, or fail if the response was not an error. */
  private def getErrorResponse(response: SubmitRestProtocolResponse): ErrorResponse = {
    response match {
      case e: ErrorResponse => e
      case r => fail(s"Expected error response. Actual: ${r.toJson}")
    }
  }

  /**
   * Send an HTTP request to the given URL using the method and the body specified.
   * Return the connection object.
   */
  private def sendHttpRequest(
      url: String,
      method: String,
      body: String = ""): HttpURLConnection = {
    val conn = new URL(url).openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod(method)
    if (body.nonEmpty) {
      conn.setDoOutput(true)
      val out = new DataOutputStream(conn.getOutputStream)
      out.write(body.getBytes(Charsets.UTF_8))
      out.close()
    }
    conn
  }

  /**
   * Send an HTTP request to the given URL using the method and the body specified.
   * Return a 2-tuple of the response message from the server and the response code.
   */
  private def sendHttpRequestWithResponse(
      url: String,
      method: String,
      body: String = ""): (SubmitRestProtocolResponse, Int) = {
    val conn = sendHttpRequest(url, method, body)
    (client.readResponse(conn), conn.getResponseCode)
  }
}

/**
 * A mock standalone Master that responds with dummy messages.
 * In all responses, the success parameter is always true.
 */
private class DummyMaster(
    submitId: String = "fake-driver-id",
    submitMessage: String = "submitted",
    killMessage: String = "killed",
    state: DriverState = FINISHED,
    exception: Option[Exception] = None)
  extends Actor {

  override def receive = {
    case RequestSubmitDriver(driverDesc) =>
      sender ! SubmitDriverResponse(success = true, Some(submitId), submitMessage)
    case RequestKillDriver(driverId) =>
      sender ! KillDriverResponse(driverId, success = true, killMessage)
    case RequestDriverStatus(driverId) =>
      sender ! DriverStatusResponse(found = true, Some(state), None, None, exception)
  }
}

/**
 * A mock standalone Master that keeps track of drivers that have been submitted.
 *
 * If a driver is submitted, its state is immediately set to RUNNING.
 * If an existing driver is killed, its state is immediately set to KILLED.
 * If an existing driver's status is requested, its state is returned in the response.
 * Submits are always successful while kills and status requests are successful only
 * if the driver was submitted in the past.
 */
private class SmarterMaster extends Actor {
  private var counter: Int = 0
  private val submittedDrivers = new mutable.HashMap[String, DriverState]

  override def receive = {
    case RequestSubmitDriver(driverDesc) =>
      val driverId = s"driver-$counter"
      submittedDrivers(driverId) = RUNNING
      counter += 1
      sender ! SubmitDriverResponse(success = true, Some(driverId), "submitted")

    case RequestKillDriver(driverId) =>
      val success = submittedDrivers.contains(driverId)
      if (success) {
        submittedDrivers(driverId) = KILLED
      }
      sender ! KillDriverResponse(driverId, success, "killed")

    case RequestDriverStatus(driverId) =>
      val found = submittedDrivers.contains(driverId)
      val state = submittedDrivers.get(driverId)
      sender ! DriverStatusResponse(found, state, None, None, None)
  }
}

/**
 * A [[StandaloneRestServer]] that is faulty in many ways.
 *
 * When handling a submit request, the server returns a malformed JSON.
 * When handling a kill request, the server returns an invalid JSON.
 * When handling a status request, the server throws an internal exception.
 * The purpose of this class is to test that client handles these cases gracefully.
 */
private class FaultyStandaloneRestServer(
    host: String,
    requestedPort: Int,
    masterActor: ActorRef,
    masterUrl: String,
    masterConf: SparkConf)
  extends StandaloneRestServer(host, requestedPort, masterActor, masterUrl, masterConf) {

  protected override val contextToServlet = Map[String, StandaloneRestServlet](
    s"$baseContext/create/*" -> new MalformedSubmitServlet,
    s"$baseContext/kill/*" -> new InvalidKillServlet,
    s"$baseContext/status/*" -> new ExplodingStatusServlet,
    "/*" -> new ErrorServlet
  )

  /** A faulty servlet that produces malformed responses. */
  class MalformedSubmitServlet extends SubmitRequestServlet(masterActor, masterUrl, masterConf) {
    protected override def sendResponse(
        responseMessage: SubmitRestProtocolResponse,
        responseServlet: HttpServletResponse): Unit = {
      val badJson = responseMessage.toJson.drop(10).dropRight(20)
      responseServlet.getWriter.write(badJson)
    }
  }

  /** A faulty servlet that produces invalid responses. */
  class InvalidKillServlet extends KillRequestServlet(masterActor, masterConf) {
    protected override def handleKill(submissionId: String): KillSubmissionResponse = {
      val k = super.handleKill(submissionId)
      k.submissionId = null
      k
    }
  }

  /** A faulty status servlet that explodes. */
  class ExplodingStatusServlet extends StatusRequestServlet(masterActor, masterConf) {
    private def explode: Int = 1 / 0
    protected override def handleStatus(submissionId: String): SubmissionStatusResponse = {
      val s = super.handleStatus(submissionId)
      s.workerId = explode.toString
      s
    }
  }
}
