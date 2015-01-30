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

  test("get and set fields") {
    val request = new DummyRequest
    assert(request.getSparkVersion === null)
    assert(request.getMessage === null)
    assert(request.getActive === null)
    assert(request.getAge === null)
    assert(request.getName === null)
    request.setSparkVersion("1.2.3")
    request.setActive("true")
    request.setAge("10")
    request.setName("dolphin")
    assert(request.getSparkVersion === "1.2.3")
    assert(request.getMessage === null)
    assert(request.getActive === "true")
    assert(request.getAge === "10")
    assert(request.getName === "dolphin")
    // overwrite
    request.setName("shark")
    request.setActive("false")
    assert(request.getName === "shark")
    assert(request.getActive === "false")
  }

  test("get and set fields with null values") {
    val request = new DummyRequest
    request.setSparkVersion(null)
    request.setActive(null)
    request.setAge(null)
    request.setName(null)
    request.setMessage(null)
    assert(request.getSparkVersion === null)
    assert(request.getMessage === null)
    assert(request.getActive === null)
    assert(request.getAge === null)
    assert(request.getName === null)
  }

  test("set fields with illegal argument") {
    val request = new DummyRequest
    intercept[IllegalArgumentException] { request.setActive("not-a-boolean") }
    intercept[IllegalArgumentException] { request.setActive("150") }
    intercept[IllegalArgumentException] { request.setAge("not-a-number") }
    intercept[IllegalArgumentException] { request.setAge("true") }
  }

  test("validate") {
    val request = new DummyRequest
    intercept[SubmitRestValidationException] { request.validate() } // missing everything
    request.setSparkVersion("1.4.8")
    intercept[SubmitRestValidationException] { request.validate() } // missing name and age
    request.setName("something")
    intercept[SubmitRestValidationException] { request.validate() } // missing only age
    request.setAge("2")
    intercept[SubmitRestValidationException] { request.validate() } // age too low
    request.setAge("10")
    request.validate() // everything is set
    request.setSparkVersion(null)
    intercept[SubmitRestValidationException] { request.validate() } // missing only Spark version
    request.setSparkVersion("1.2.3")
    request.setName(null)
    intercept[SubmitRestValidationException] { request.validate() } // missing only name
    request.setMessage("not-setting-name")
    intercept[SubmitRestValidationException] { request.validate() } // still missing name
  }

  test("request to and from JSON") {
    val request = new DummyRequest()
      .setSparkVersion("1.2.3")
      .setActive("true")
      .setAge("25")
      .setName("jung")
    val json = request.toJson
    assertJsonEquals(json, dummyRequestJson)
    val newRequest = SubmitRestProtocolMessage.fromJson(json, classOf[DummyRequest])
    assert(newRequest.getSparkVersion === "1.2.3")
    assert(newRequest.getClientSparkVersion === "1.2.3")
    assert(newRequest.getActive === "true")
    assert(newRequest.getAge === "25")
    assert(newRequest.getName === "jung")
    assert(newRequest.getMessage === null)
  }

  test("response to and from JSON") {
    val response = new DummyResponse().setSparkVersion("3.3.4")
    val json = response.toJson
    assertJsonEquals(json, dummyResponseJson)
    val newResponse = SubmitRestProtocolMessage.fromJson(json, classOf[DummyResponse])
    assert(newResponse.getSparkVersion === "3.3.4")
    assert(newResponse.getServerSparkVersion === "3.3.4")
    assert(newResponse.getMessage === null)
  }

  test("SubmitDriverRequest") {
    val message = new SubmitDriverRequest
    intercept[SubmitRestValidationException] { message.validate() }
    intercept[IllegalArgumentException] { message.setDriverCores("one hundred feet") }
    intercept[IllegalArgumentException] { message.setSuperviseDriver("nope, never") }
    intercept[IllegalArgumentException] { message.setTotalExecutorCores("two men") }
    message.setSparkVersion("1.2.3")
    message.setAppName("SparkPie")
    message.setAppResource("honey-walnut-cherry.jar")
    message.validate()
    // optional fields
    message.setMainClass("org.apache.spark.examples.SparkPie")
    message.setJars("mayonnaise.jar,ketchup.jar")
    message.setFiles("fireball.png")
    message.setPyFiles("do-not-eat-my.py")
    message.setDriverMemory("512m")
    message.setDriverCores("180")
    message.setDriverExtraJavaOptions(" -Dslices=5 -Dcolor=mostly_red")
    message.setDriverExtraClassPath("food-coloring.jar")
    message.setDriverExtraLibraryPath("pickle.jar")
    message.setSuperviseDriver("false")
    message.setExecutorMemory("256m")
    message.setTotalExecutorCores("10000")
    // special fields
    message.addAppArg("two slices")
    message.addAppArg("a hint of cinnamon")
    message.setSparkProperty("spark.live.long", "true")
    message.setSparkProperty("spark.shuffle.enabled", "false")
    message.setEnvironmentVariable("PATH", "/dev/null")
    message.setEnvironmentVariable("PYTHONPATH", "/dev/null")
    assert(message.getAppArgs === Seq("two slices", "a hint of cinnamon"))
    assert(message.getSparkProperties.size === 2)
    assert(message.getSparkProperties("spark.live.long") === "true")
    assert(message.getSparkProperties("spark.shuffle.enabled") === "false")
    assert(message.getEnvironmentVariables.size === 2)
    assert(message.getEnvironmentVariables("PATH") === "/dev/null")
    assert(message.getEnvironmentVariables("PYTHONPATH") === "/dev/null")
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverRequestJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[SubmitDriverRequest])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getClientSparkVersion === "1.2.3")
    assert(newMessage.getAppName === "SparkPie")
    assert(newMessage.getAppResource === "honey-walnut-cherry.jar")
    assert(newMessage.getMainClass === "org.apache.spark.examples.SparkPie")
    assert(newMessage.getJars === "mayonnaise.jar,ketchup.jar")
    assert(newMessage.getFiles === "fireball.png")
    assert(newMessage.getPyFiles === "do-not-eat-my.py")
    assert(newMessage.getDriverMemory === "512m")
    assert(newMessage.getDriverCores === "180")
    assert(newMessage.getDriverExtraJavaOptions === " -Dslices=5 -Dcolor=mostly_red")
    assert(newMessage.getDriverExtraClassPath === "food-coloring.jar")
    assert(newMessage.getDriverExtraLibraryPath === "pickle.jar")
    assert(newMessage.getSuperviseDriver === "false")
    assert(newMessage.getExecutorMemory === "256m")
    assert(newMessage.getTotalExecutorCores === "10000")
    assert(newMessage.getAppArgs === message.getAppArgs)
    assert(newMessage.getSparkProperties === message.getSparkProperties)
    assert(newMessage.getEnvironmentVariables === message.getEnvironmentVariables)
  }

  test("SubmitDriverResponse") {
    val message = new SubmitDriverResponse
    intercept[SubmitRestValidationException] { message.validate() }
    intercept[IllegalArgumentException] { message.setSuccess("maybe not") }
    message.setSparkVersion("1.2.3")
    message.setDriverId("driver_123")
    message.setSuccess("true")
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, submitDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[SubmitDriverResponse])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getServerSparkVersion === "1.2.3")
    assert(newMessage.getDriverId === "driver_123")
    assert(newMessage.getSuccess === "true")
  }

  test("KillDriverRequest") {
    val message = new KillDriverRequest
    intercept[SubmitRestValidationException] { message.validate() }
    message.setSparkVersion("1.2.3")
    message.setDriverId("driver_123")
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, killDriverRequestJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[KillDriverRequest])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getClientSparkVersion === "1.2.3")
    assert(newMessage.getDriverId === "driver_123")
  }

  test("KillDriverResponse") {
    val message = new KillDriverResponse
    intercept[SubmitRestValidationException] { message.validate() }
    intercept[IllegalArgumentException] { message.setSuccess("maybe not") }
    message.setSparkVersion("1.2.3")
    message.setDriverId("driver_123")
    message.setSuccess("true")
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, killDriverResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[KillDriverResponse])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getServerSparkVersion === "1.2.3")
    assert(newMessage.getDriverId === "driver_123")
    assert(newMessage.getSuccess === "true")
  }

  test("DriverStatusRequest") {
    val message = new DriverStatusRequest
    intercept[SubmitRestValidationException] { message.validate() }
    message.setSparkVersion("1.2.3")
    message.setDriverId("driver_123")
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, driverStatusRequestJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[DriverStatusRequest])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getClientSparkVersion === "1.2.3")
    assert(newMessage.getDriverId === "driver_123")
  }

  test("DriverStatusResponse") {
    val message = new DriverStatusResponse
    intercept[SubmitRestValidationException] { message.validate() }
    intercept[IllegalArgumentException] { message.setSuccess("maybe") }
    message.setSparkVersion("1.2.3")
    message.setDriverId("driver_123")
    message.setSuccess("true")
    message.validate()
    // optional fields
    message.setDriverState("RUNNING")
    message.setWorkerId("worker_123")
    message.setWorkerHostPort("1.2.3.4:7780")
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, driverStatusResponseJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[DriverStatusResponse])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getServerSparkVersion === "1.2.3")
    assert(newMessage.getDriverId === "driver_123")
    assert(newMessage.getSuccess === "true")
  }

  test("ErrorResponse") {
    val message = new ErrorResponse
    intercept[SubmitRestValidationException] { message.validate() }
    message.setSparkVersion("1.2.3")
    message.setMessage("Field not found in submit request: X")
    message.validate()
    // test JSON
    val json = message.toJson
    assertJsonEquals(json, errorJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(json, classOf[ErrorResponse])
    assert(newMessage.getSparkVersion === "1.2.3")
    assert(newMessage.getServerSparkVersion === "1.2.3")
    assert(newMessage.getMessage === "Field not found in submit request: X")
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
      |  "serverSparkVersion" : "3.3.4"
      |}
    """.stripMargin

  private val submitDriverRequestJson =
    """
      |{
      |  "action" : "SubmitDriverRequest",
      |  "appArgs" : "[\"two slices\",\"a hint of cinnamon\"]",
      |  "appName" : "SparkPie",
      |  "appResource" : "honey-walnut-cherry.jar",
      |  "clientSparkVersion" : "1.2.3",
      |  "driverCores" : "180",
      |  "driverExtraClassPath" : "food-coloring.jar",
      |  "driverExtraJavaOptions" : " -Dslices=5 -Dcolor=mostly_red",
      |  "driverExtraLibraryPath" : "pickle.jar",
      |  "driverMemory" : "512m",
      |  "environmentVariables" : "{\"PATH\":\"/dev/null\",\"PYTHONPATH\":\"/dev/null\"}",
      |  "executorMemory" : "256m",
      |  "files" : "fireball.png",
      |  "jars" : "mayonnaise.jar,ketchup.jar",
      |  "mainClass" : "org.apache.spark.examples.SparkPie",
      |  "pyFiles" : "do-not-eat-my.py",
      |  "sparkProperties" : "{\"spark.live.long\":\"true\",\"spark.shuffle.enabled\":\"false\"}",
      |  "superviseDriver" : "false",
      |  "totalExecutorCores" : "10000"
      |}
    """.stripMargin

  private val submitDriverResponseJson =
    """
      |{
      |  "action" : "SubmitDriverResponse",
      |  "driverId" : "driver_123",
      |  "serverSparkVersion" : "1.2.3",
      |  "success" : "true"
      |}
    """.stripMargin

  private val killDriverRequestJson =
    """
      |{
      |  "action" : "KillDriverRequest",
      |  "clientSparkVersion" : "1.2.3",
      |  "driverId" : "driver_123"
      |}
    """.stripMargin

  private val killDriverResponseJson =
    """
      |{
      |  "action" : "KillDriverResponse",
      |  "driverId" : "driver_123",
      |  "serverSparkVersion" : "1.2.3",
      |  "success" : "true"
      |}
    """.stripMargin

  private val driverStatusRequestJson =
    """
      |{
      |  "action" : "DriverStatusRequest",
      |  "clientSparkVersion" : "1.2.3",
      |  "driverId" : "driver_123"
      |}
    """.stripMargin

  private val driverStatusResponseJson =
    """
      |{
      |  "action" : "DriverStatusResponse",
      |  "driverId" : "driver_123",
      |  "driverState" : "RUNNING",
      |  "serverSparkVersion" : "1.2.3",
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
  private val active = new SubmitRestProtocolField[Boolean]("active")
  private val age = new SubmitRestProtocolField[Int]("age")
  private val name = new SubmitRestProtocolField[String]("name")

  def getActive: String = active.toString
  def getAge: String = age.toString
  def getName: String = name.toString

  def setActive(s: String): this.type = setBooleanField(active, s)
  def setAge(s: String): this.type = setNumericField(age, s)
  def setName(s: String): this.type = setField(name, s)

  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(name)
    assertFieldIsSet(age)
    assert(age.getValue.get > 5, "Not old enough!")
  }
}
