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

package org.apache.spark.deploy.rest.yarn

import java.io.File
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.io.Source

import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.util.Records

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkFunSuite, SparkConf}
import org.apache.spark.deploy.rest._
import org.apache.spark.deploy.yarn.ClientArguments
import org.apache.spark.util.Utils

class YarnRestSubmissionClientSuite extends SparkFunSuite with BeforeAndAfterAll {
  import YarnRestSubmissionClientSuite._

  private val sparkConf = new SparkConf()

  private val host = "localhost"
  private var port = 0
  private var restServer: TestRestSubmissionServer = _
  private var tempDir: File = _
  private var fakeSparkJar: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (restServer == null) {
      restServer = new TestRestSubmissionServer(host, port, sparkConf)
      port = restServer.start()
    }

    tempDir = Utils.createTempDir()
    fakeSparkJar = File.createTempFile("sparkJar", null, tempDir)
    System.setProperty("SPARK_YARN_MODE", "true")
  }

  override def afterAll(): Unit = {
    if (restServer !=  null) {
      restServer.stop()
      restServer = null
      port = 0
    }

    System.clearProperty("SPARK_YARN_MODE")

    super.afterAll()
  }

  test("get new application id") {
    val restClient = new YarnRestSubmissionClient(s"$host:$port")
    val newApplication = restClient.newApplicationSubmission()

    // Assert return message is not null
    assert(newApplication != null)

    // Assert return message has all the information we requested
    assert(newApplication.applicationId === testApplicationId)
    assert(newApplication.maxResourceCapability.memory === testResource.getMemory)
    assert(newApplication.maxResourceCapability.vCores === testResource.getVirtualCores)
  }

  test("kill application") {
    val restClient = new YarnRestSubmissionClient(s"$host:$port")
    val resp = restClient.killSubmission(testApplicationId)

    // Assert return message is not null
    assert(resp != null)

    // Assert return message is the type we hoped
    assert(resp.isInstanceOf[AppState])
    assert(resp.asInstanceOf[AppState].state == "KILLED")
  }

  test("query the status of application") {
    val restClient = new YarnRestSubmissionClient(s"$host:$port")
    val resp = restClient.requestStateSubmission(testApplicationId)

    // Assert return message is not null
    assert(resp != null)

    // Assert return message is the type we hoped
    assert(resp.isInstanceOf[AppState])
    assert(resp.asInstanceOf[AppState].state == "TEST")
  }

  test("construct application submission context") {
    sparkConf.set("spark.yarn.jar", "local:" + fakeSparkJar.getAbsolutePath())
    val fakeClientArgs = new ClientArguments(new Array[String](0), sparkConf)
    fakeClientArgs.executorMemory = 512
    fakeClientArgs.userClass = fakeSparkJar.getAbsolutePath
    fakeClientArgs.userJar = fakeSparkJar.getAbsolutePath

    val restClient = new YarnRestSubmissionClient(s"$host:$port")
    val appSubmissionContext = restClient.constructAppSubmissionInfo(fakeClientArgs, sparkConf)

    // Assert whether basic info is set
    assert(appSubmissionContext.applicationId == testApplicationId)
    assert(appSubmissionContext.applicationName == "Spark")
    assert(appSubmissionContext.applicationType == "SPARK")

    // Assert resource is correct
    assert(appSubmissionContext.resource.memory ==
      fakeClientArgs.amMemory + fakeClientArgs.amMemoryOverhead)

    // Assert jar is set
    assert(
      appSubmissionContext.containerInfo.localResources.entry.map(_.key).contains("__app__.jar"))
  }

  test("submit application") {
    sparkConf.setIfMissing("spark.yarn.jar", "local:" + fakeSparkJar.getAbsolutePath())
    val fakeClientArgs = new ClientArguments(new Array[String](0), sparkConf)
    fakeClientArgs.executorMemory = 512
    fakeClientArgs.userClass = fakeSparkJar.getAbsolutePath
    fakeClientArgs.userJar = fakeSparkJar.getAbsolutePath

    val restClient = new YarnRestSubmissionClient(s"$host:$port")
    val appSubmissionContext = restClient.constructAppSubmissionInfo(fakeClientArgs, sparkConf)
    restClient.createSubmission(appSubmissionContext)
  }
}

object YarnRestSubmissionClientSuite {
  val testApplicationId = "application_1440495448334_0001"

  val testResource = Records.newRecord(classOf[Resource])
  testResource.setMemory(1024)
  testResource.setVirtualCores(1)

  val testAppState = new AppState
  testAppState.state = "TEST"
}


class TestRestSubmissionServer(
    host: String,
    requestedPort: Int,
    masterConf: SparkConf) extends RestSubmissionServer(host, requestedPort, masterConf) {
  protected override val submitRequestServlet: SubmitRequestServlet = null
  protected override val killRequestServlet: KillRequestServlet = null
  protected override val statusRequestServlet: StatusRequestServlet = null

  protected override val baseContext = s"/ws/${YarnRestSubmissionClient.PROTOCOL_VERSION}/cluster"

  protected override lazy val contextToServlet = Map[String, RestServlet](
    s"$baseContext/*" -> new TestRestServlet
  )
}

class TestRestServlet extends RestServlet {
  import YarnRestSubmissionClientSuite._

  protected def sendResponse(
      responseMessage: YarnSubmitRestProtocolResponse,
      responseServlet: HttpServletResponse
    ): Unit = {
    responseMessage.validate()
    responseServlet.setContentType("application/type")
    responseServlet.setCharacterEncoding("utf-8")
    responseServlet.getWriter.write(responseMessage.toJson)
  }

  protected override def doPost(
      request: HttpServletRequest,
      response: HttpServletResponse
    ): Unit = {
    assert(request.getQueryString == s"user.name=${YarnRestSubmissionClient.currentUser}")
    request.getPathInfo match {
      case "/apps/new-application" =>
        val newApplication = new NewApplication
        newApplication.applicationId = testApplicationId
        newApplication.maxResourceCapability = new ResourceInfo().buildFrom(testResource)
        sendResponse(newApplication, response)

      case "/apps" =>
        val requestMessage = Source.fromInputStream(request.getInputStream).mkString
        val submissionContext =
          SubmitRestProtocolMessage.fromJson[ApplicationSubmissionContextInfo](
            requestMessage, classOf[ApplicationSubmissionContextInfo])

        // Check the arguments
        val appSubmissionContext = submissionContext.asInstanceOf[ApplicationSubmissionContextInfo]
        assert(appSubmissionContext.applicationId == testApplicationId)
        assert(appSubmissionContext.applicationName == "Spark")
        assert(appSubmissionContext.applicationType == "SPARK")

        // Assert jar is set
        assert(appSubmissionContext.containerInfo.localResources.entry
          .map(_.key)
          .contains("__app__.jar"))

        response.setStatus(HttpServletResponse.SC_ACCEPTED)

      case unexpected =>
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        val exception = new RemoteExceptionData
        exception.remoteException = new exception.RemoteException
        exception.remoteException.message = "unknown request"
        sendResponse(exception, response)
    }
  }

  protected override def doPut(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    assert(request.getQueryString == s"user.name=${YarnRestSubmissionClient.currentUser}")
    request.getPathInfo match {
      case "/apps/application_1440495448334_0001/state" =>
        val requestMessage = Source.fromInputStream(request.getInputStream).mkString
        val state = YarnRestSubmissionClient.fromJson[AppState](requestMessage)
        assert(state.isInstanceOf[AppState])
        val appState = state.asInstanceOf[AppState]
        assert(appState.state == "KILLED")

        val respState = new AppState
        respState.state = "KILLED"
        sendResponse(respState, response)

      case unexpected =>
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        val exception = new RemoteExceptionData
        exception.remoteException = new exception.RemoteException
        exception.remoteException.message = "unknown request"
        sendResponse(exception, response)
    }
  }

  protected override def doGet(
      request: HttpServletRequest,
      response: HttpServletResponse): Unit = {
    assert(request.getQueryString == s"user.name=${YarnRestSubmissionClient.currentUser}")
    request.getPathInfo match {
      case "/apps/application_1440495448334_0001/state" =>
        sendResponse(testAppState, response)

      case unexpected =>
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST)
        val exception = new RemoteExceptionData
        exception.remoteException = new exception.RemoteException
        exception.remoteException.message = "unknown request"
        sendResponse(exception, response)
    }
  }
}
