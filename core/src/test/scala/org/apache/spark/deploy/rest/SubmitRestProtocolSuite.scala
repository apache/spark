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

import java.lang.Boolean

import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Tests for the REST application submission protocol.
 */
class SubmitRestProtocolSuite extends SparkFunSuite {

  test("validate") {
    val request = new DummyRequest
    intercept[SubmitRestProtocolException] { request.validate() } // missing everything
    request.clientSparkVersion = "1.2.3"
    intercept[SubmitRestProtocolException] { request.validate() } // missing name and age
    request.name = "something"
    intercept[SubmitRestProtocolException] { request.validate() } // missing only age
    request.age = 2
    intercept[SubmitRestProtocolException] { request.validate() } // age too low
    request.age = 10
    request.validate() // everything is set properly
    request.clientSparkVersion = null
    intercept[SubmitRestProtocolException] { request.validate() } // missing only Spark version
    request.clientSparkVersion = "1.2.3"
    request.name = null
    intercept[SubmitRestProtocolException] { request.validate() } // missing only name
    request.message = "not-setting-name"
    intercept[SubmitRestProtocolException] { request.validate() } // still missing name
  }

  test("request to and from JSON") {
    val request = new DummyRequest
    intercept[SubmitRestProtocolException] { request.toJson } // implicit validation
    request.clientSparkVersion = "1.2.3"
    request.active = true
    request.age = 25
    request.name = "jung"
    val json = request.toJson
    assertJsonEquals(json, dummyRequestJson)
    val newRequest = SubmitRestProtocolMessage.fromJson(json, classOf[DummyRequest])
    assert(newRequest.clientSparkVersion === "1.2.3")
    assert(newRequest.clientSparkVersion === "1.2.3")
    assert(newRequest.active)
    assert(newRequest.age === 25)
    assert(newRequest.name === "jung")
    assert(newRequest.message === null)
  }

  test("response to and from JSON") {
    val response = new DummyResponse
    response.serverSparkVersion = "3.3.4"
    response.success = true
    val json = response.toJson
    assertJsonEquals(json, dummyResponseJson)
    val newResponse = SubmitRestProtocolMessage.fromJson(json, classOf[DummyResponse])
    assert(newResponse.serverSparkVersion === "3.3.4")
    assert(newResponse.serverSparkVersion === "3.3.4")
    assert(newResponse.success)
    assert(newResponse.message === null)
  }

  test("CreateSubmissionRequest") {
    val message = new CreateSubmissionRequest
    intercept[SubmitRestProtocolException] { message.validate() }
    message.clientSparkVersion = "1.2.3"
    message.appResource = "honey-walnut-cherry.jar"
    message.mainClass = "org.apache.spark.examples.SparkPie"
    val conf = new SparkConf(false)
    conf.set("spark.app.name", "SparkPie")
    message.sparkProperties = conf.getAll.toMap
    message.validate()
    // optional fields
    conf.set(JARS, Seq("mayonnaise.jar", "ketchup.jar"))
    conf.set(FILES.key, "fireball.png")
    conf.set(ARCHIVES.key, "fireballs.zip")
    conf.set("spark.driver.memory", s"${Utils.DEFAULT_DRIVER_MEM_MB}m")
    conf.set(DRIVER_CORES, 180)
    conf.set("spark.driver.extraJavaOptions", " -Dslices=5 -Dcolor=mostly_red")
    conf.set("spark.driver.extraClassPath", "food-coloring.jar")
    conf.set("spark.driver.extraLibraryPath", "pickle.jar")
    conf.set(DRIVER_SUPERVISE, false)
    conf.set("spark.executor.memory", "256m")
    conf.set(CORES_MAX, 10000)
    message.sparkProperties = conf.getAll.toMap
    message.appArgs = Array("two slices", "a hint of cinnamon")
    message.environmentVariables = Map("PATH" -> "/dev/null")
    message.validate()
    // bad fields
    var badConf = conf.clone().set(DRIVER_CORES.key, "one hundred feet")
    message.sparkProperties = badConf.getAll.toMap
    intercept[SubmitRestProtocolException] { message.validate() }
    badConf = conf.clone().set(DRIVER_SUPERVISE.key, "nope, never")
    message.sparkProperties = badConf.getAll.toMap
    intercept[SubmitRestProtocolException] { message.validate() }
    badConf = conf.clone().set(CORES_MAX.key, "two men")
    message.sparkProperties = badConf.getAll.toMap
    intercept[SubmitRestProtocolException] { message.validate() }
    message.sparkProperties = conf.getAll.toMap
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverRequestJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[CreateSubmissionRequest])
    assert(newMessage.clientSparkVersion === "1.2.3")
    assert(newMessage.appResource === "honey-walnut-cherry.jar")
    assert(newMessage.mainClass === "org.apache.spark.examples.SparkPie")
    assert(newMessage.sparkProperties("spark.app.name") === "SparkPie")
    assert(newMessage.sparkProperties(JARS.key) === "mayonnaise.jar,ketchup.jar")
    assert(newMessage.sparkProperties(FILES.key) === "fireball.png")
    assert(newMessage.sparkProperties("spark.driver.memory") === s"${Utils.DEFAULT_DRIVER_MEM_MB}m")
    assert(newMessage.sparkProperties(DRIVER_CORES.key) === "180")
    assert(newMessage.sparkProperties("spark.driver.extraJavaOptions") ===
      " -Dslices=5 -Dcolor=mostly_red")
    assert(newMessage.sparkProperties("spark.driver.extraClassPath") === "food-coloring.jar")
    assert(newMessage.sparkProperties("spark.driver.extraLibraryPath") === "pickle.jar")
    assert(newMessage.sparkProperties(DRIVER_SUPERVISE.key) === "false")
    assert(newMessage.sparkProperties("spark.executor.memory") === "256m")
    assert(newMessage.sparkProperties(CORES_MAX.key) === "10000")
    assert(newMessage.appArgs === message.appArgs)
    assert(newMessage.sparkProperties === message.sparkProperties)
    assert(newMessage.environmentVariables === message.environmentVariables)
  }

  test("CreateSubmissionResponse") {
    val message = new CreateSubmissionResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = true
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[CreateSubmissionResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.success)
  }

  test("KillSubmissionResponse") {
    val message = new KillSubmissionResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = true
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, killDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[KillSubmissionResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.success)
  }

  test("SubmissionStatusResponse") {
    val message = new SubmissionStatusResponse
    intercept[SubmitRestProtocolException] { message.validate() }
    message.serverSparkVersion = "1.2.3"
    message.submissionId = "driver_123"
    message.success = true
    message.validate()
    // optional fields
    message.driverState = "RUNNING"
    message.workerId = "worker_123"
    message.workerHostPort = "1.2.3.4:7780"
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, driverStatusResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[SubmissionStatusResponse])
    assert(newMessage.serverSparkVersion === "1.2.3")
    assert(newMessage.submissionId === "driver_123")
    assert(newMessage.driverState === "RUNNING")
    assert(newMessage.success)
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
      |  "active" : true,
      |  "age" : 25,
      |  "clientSparkVersion" : "1.2.3",
      |  "name" : "jung"
      |}
    """.stripMargin

  private val dummyResponseJson =
    """
      |{
      |  "action" : "DummyResponse",
      |  "serverSparkVersion" : "3.3.4",
      |  "success": true
      |}
    """.stripMargin

  private lazy val submitDriverRequestJson =
    s"""
      |{
      |  "action" : "CreateSubmissionRequest",
      |  "appArgs" : [ "two slices", "a hint of cinnamon" ],
      |  "appResource" : "honey-walnut-cherry.jar",
      |  "clientSparkVersion" : "1.2.3",
      |  "environmentVariables" : {
      |    "PATH" : "/dev/null"
      |  },
      |  "mainClass" : "org.apache.spark.examples.SparkPie",
      |  "sparkProperties" : {
      |    "spark.archives" : "fireballs.zip",
      |    "spark.driver.extraLibraryPath" : "pickle.jar",
      |    "spark.jars" : "mayonnaise.jar,ketchup.jar",
      |    "spark.driver.supervise" : "false",
      |    "spark.driver.memory" : "${Utils.DEFAULT_DRIVER_MEM_MB}m",
      |    "spark.files" : "fireball.png",
      |    "spark.driver.cores" : "180",
      |    "spark.driver.extraJavaOptions" : " -Dslices=5 -Dcolor=mostly_red",
      |    "spark.app.name" : "SparkPie",
      |    "spark.cores.max" : "10000",
      |    "spark.executor.memory" : "256m",
      |    "spark.driver.extraClassPath" : "food-coloring.jar"
      |  }
      |}
    """.stripMargin

  private val submitDriverResponseJson =
    """
      |{
      |  "action" : "CreateSubmissionResponse",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : true
      |}
    """.stripMargin

  private val killDriverResponseJson =
    """
      |{
      |  "action" : "KillSubmissionResponse",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : true
      |}
    """.stripMargin

  private val driverStatusResponseJson =
    """
      |{
      |  "action" : "SubmissionStatusResponse",
      |  "driverState" : "RUNNING",
      |  "serverSparkVersion" : "1.2.3",
      |  "submissionId" : "driver_123",
      |  "success" : true,
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
  var active: Boolean = null
  var age: Integer = null
  var name: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(name, "name")
    assertFieldIsSet(age, "age")
    assert(age > 5, "Not old enough!")
  }
}
