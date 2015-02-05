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

import org.json4s.jackson.JsonMethods._
import org.scalatest.FunSuite

/**
 * Tests for the REST application submission protocol.
 */
class SubmitRestProtocolSuite extends FunSuite {

  test("validate") {
    val request = new DummyRequest
    intercept[SubmitRestProtocolException] { request.validate() } // missing everything
    request.clientSparkVersion = "1.2.3"
    intercept[SubmitRestProtocolException] { request.validate() } // missing name and age
    request.name = "something"
    intercept[SubmitRestProtocolException] { request.validate() } // missing only age
    request.age = "2"
    intercept[SubmitRestProtocolException] { request.validate() } // age too low
    request.age = "10"
    request.validate() // everything is set properly
    request.clientSparkVersion = null
    intercept[SubmitRestProtocolException] { request.validate() } // missing only Spark version
    request.clientSparkVersion = "1.2.3"
    request.name = null
    intercept[SubmitRestProtocolException] { request.validate() } // missing only name
    request.message = "not-setting-name"
    intercept[SubmitRestProtocolException] { request.validate() } // still missing name
  }

  test("validate with illegal argument") {
    val request = new DummyRequest
    request.clientSparkVersion = "1.2.3"
    request.name = "abc"
    request.age = "not-a-number"
    intercept[SubmitRestProtocolException] { request.validate() }
    request.age = "true"
    intercept[SubmitRestProtocolException] { request.validate() }
    request.age = "150"
    request.validate()
    request.active = "not-a-boolean"
    intercept[SubmitRestProtocolException] { request.validate() }
    request.active = "150"
    intercept[SubmitRestProtocolException] { request.validate() }
    request.active = "true"
    request.validate()
  }

  test("request to and from JSON") {
    val request = new DummyRequest
    intercept[SubmitRestProtocolException] { request.toJson } // implicit validation
    request.clientSparkVersion = "1.2.3"
    request.active = "true"
    request.age = "25"
    request.name = "jung"
    val json = request.toJson
    assertJsonEquals(json, dummyRequestJson)
    val newRequest = SubmitRestProtocolMessage.fromJson(json, classOf[DummyRequest])
    assert(newRequest.clientSparkVersion === "1.2.3")
    assert(newRequest.clientSparkVersion === "1.2.3")
    assert(newRequest.active === "true")
    assert(newRequest.age === "25")
    assert(newRequest.name === "jung")
    assert(newRequest.message === null)
  }

  test("response to and from JSON") {
    val response = new DummyResponse
    response.serverSparkVersion = "3.3.4"
    response.success = "true"
    val json = response.toJson
    assertJsonEquals(json, dummyResponseJson)
    val newResponse = SubmitRestProtocolMessage.fromJson(json, classOf[DummyResponse])
    assert(newResponse.serverSparkVersion === "3.3.4")
    assert(newResponse.serverSparkVersion === "3.3.4")
    assert(newResponse.success === "true")
    assert(newResponse.message === null)
  }

  test("CreateSubmissionRequest") {
    val message = new CreateSubmissionRequest
    intercept[SubmitRestProtocolException] { message.validate() }
    message.clientSparkVersion = "1.2.3"
    message.appName = "SparkPie"
    message.appResource = "honey-walnut-cherry.jar"
    message.validate()
    // optional fields
    message.mainClass = "org.apache.spark.examples.SparkPie"
    message.jars = "mayonnaise.jar,ketchup.jar"
    message.files = "fireball.png"
    message.pyFiles = "do-not-eat-my.py"
    message.driverMemory = "512m"
    message.driverCores = "180"
    message.driverExtraJavaOptions = " -Dslices=5 -Dcolor=mostly_red"
    message.driverExtraClassPath = "food-coloring.jar"
    message.driverExtraLibraryPath = "pickle.jar"
    message.superviseDriver = "false"
    message.executorMemory = "256m"
    message.totalExecutorCores = "10000"
    message.validate()
    // bad fields
    message.driverCores = "one hundred feet"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.driverCores = "180"
    message.superviseDriver = "nope, never"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.superviseDriver = "false"
    message.totalExecutorCores = "two men"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.totalExecutorCores = "10000"
    // special fields
    message.addAppArg("two slices")
    message.addAppArg("a hint of cinnamon")
    message.setSparkProperty("spark.live.long", "true")
    message.setSparkProperty("spark.shuffle.enabled", "false")
    message.setEnvironmentVariable("PATH", "/dev/null")
    message.setEnvironmentVariable("PYTHONPATH", "/dev/null")
    assert(message.appArgs === Seq("two slices", "a hint of cinnamon"))
    assert(message.sparkProperties.size === 2)
    assert(message.sparkProperties("spark.live.long") === "true")
    assert(message.sparkProperties("spark.shuffle.enabled") === "false")
    assert(message.environmentVariables.size === 2)
    assert(message.environmentVariables("PATH") === "/dev/null")
    assert(message.environmentVariables("PYTHONPATH") === "/dev/null")
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverRequestJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[CreateSubmissionRequest])
    assert(newMessage.clientSparkVersion === "1.2.3")
    assert(newMessage.appName === "SparkPie")
    assert(newMessage.appResource === "honey-walnut-cherry.jar")
    assert(newMessage.mainClass === "org.apache.spark.examples.SparkPie")
    assert(newMessage.jars === "mayonnaise.jar,ketchup.jar")
    assert(newMessage.files === "fireball.png")
    assert(newMessage.pyFiles === "do-not-eat-my.py")
    assert(newMessage.driverMemory === "512m")
    assert(newMessage.driverCores === "180")
    assert(newMessage.driverExtraJavaOptions === " -Dslices=5 -Dcolor=mostly_red")
    assert(newMessage.driverExtraClassPath === "food-coloring.jar")
    assert(newMessage.driverExtraLibraryPath === "pickle.jar")
    assert(newMessage.superviseDriver === "false")
    assert(newMessage.executorMemory === "256m")
    assert(newMessage.totalExecutorCores === "10000")
    assert(newMessage.appArgs === message.appArgs)
    assert(newMessage.sparkProperties === message.sparkProperties)
    assert(newMessage.environmentVariables === message.environmentVariables)
  }

  test("CreateSubmissionResponse") {
    val message = new CreateSubmissionResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = "true"
    message.validate()
    // bad fields
    message.success = "maybe not"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.success = "true"
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[CreateSubmissionResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.success === "true")
  }

  test("KillSubmissionResponse") {
    val message = new KillSubmissionResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = "true"
    message.validate()
    // bad fields
    message.success = "maybe not"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.success = "true"
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, killDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[KillSubmissionResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.success === "true")
  }

  test("SubmissionStatusResponse") {
    val message = new SubmissionStatusResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = "true"
    message.validate()
    // optional fields
    message.driverState = "RUNNING"
    message.workerId = "worker_123"
    message.workerHostPort = "1.2.3.4:7780"
    // bad fields
    message.success = "maybe"
    intercept[SubmitRestProtocolException] { message.validate() }
    message.success = "true"
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, driverStatusResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[SubmissionStatusResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.driverState === "RUNNING")
    assert(newMessage.success === "true")
    assert(newMessage.workerId === "worker_123")
    assert(newMessage.workerHostPort === "1.2.3.4:7780")
  }

  test("ErrorResponse") {
    val message = new ErrorResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.message = "Field not found in submit request: X"
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, errorJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[ErrorResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.message === "Field not found in submit request: X")
  }

  private val dummyRequestJson =
    """
      |{
      |  "action" : "DummyRequest",
      |  "active" : "true",
      |  "age" : "25",
      |  "clientSparkVersion" : "1.2.3",
      |  "name" : "jung"
      |}
    """.stripMargin

  private val dummyResponseJson =
    """
      |{
      |  "action" : "DummyResponse",
      |  "serverSparkVersion" : "3.3.4",
      |  "success": "true"
      |}
    """.stripMargin

  private val submitDriverRequestJson =
    """
      |{
      |  "action" : "CreateSubmissionRequest",
      |  "appArgs" : ["two slices","a hint of cinnamon"],
      |  "appName" : "SparkPie",
      |  "appResource" : "honey-walnut-cherry.jar",
      |  "clientSparkVersion" : "1.2.3",
      |  "driverCores" : "180",
      |  "driverExtraClassPath" : "food-coloring.jar",
      |  "driverExtraJavaOptions" : " -Dslices=5 -Dcolor=mostly_red",
      |  "driverExtraLibraryPath" : "pickle.jar",
      |  "driverMemory" : "512m",
      |  "environmentVariables" : {"PATH":"/dev/null","PYTHONPATH":"/dev/null"},
      |  "executorMemory" : "256m",
      |  "files" : "fireball.png",
      |  "jars" : "mayonnaise.jar,ketchup.jar",
      |  "mainClass" : "org.apache.spark.examples.SparkPie",
      |  "pyFiles" : "do-not-eat-my.py",
      |  "sparkProperties" : {"spark.live.long":"true","spark.shuffle.enabled":"false"},
      |  "superviseDriver" : "false",
      |  "totalExecutorCores" : "10000"
      |}
    """.stripMargin

  private val submitDriverResponseJson =
    """
      |{
      |  "action" : "CreateSubmissionResponse",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : "true"
      |}
    """.stripMargin

  private val killDriverResponseJson =
    """
      |{
      |  "action" : "KillSubmissionResponse",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : "true"
      |}
    """.stripMargin

  private val driverStatusResponseJson =
    """
      |{
      |  "action" : "SubmissionStatusResponse",
      |  "driverState" : "RUNNING",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : "true",
      |  "workerHostPort" : "1.2.3.4:7780",
      |  "workerId" : "worker_123"
      |}
    """.stripMargin

  private val errorJson =
    """
      |{
      |  "action" : "ErrorResponse",
      |  "message" : "Field not found in submit request: X",
      |  "serverSparkVersion" : "1.2.3"
      |}
    """.stripMargin

  /** Assert that the contents in the two JSON strings are equal after ignoring whitespace. */
  private def assertJsonEquals(jsonString1: String, jsonString2: String): Unit = {
    val trimmedJson1 = jsonString1.trim
    val trimmedJson2 = jsonString2.trim
    val json1 = compact(render(parse(trimmedJson1)))
    val json2 = compact(render(parse(trimmedJson2)))
    // Put this on a separate line to avoid printing comparison twice when test fails
    val equals = json1 == json2
    assert(equals, "\"[%s]\" did not equal \"[%s]\"".format(trimmedJson1, trimmedJson2))
  }
}

private class DummyResponse extends SubmitRestProtocolResponse
private class DummyRequest extends SubmitRestProtocolRequest {
  var active: String = null
  var age: String = null
  var name: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(name, "name")
    assertFieldIsSet(age, "age")
    assertFieldIsBoolean(active, "active")
    assertFieldIsNumeric(age, "age")
    assert(age.toInt > 5, "Not old enough!")
  }
}
