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

class DummyRequest extends SubmitRestProtocolRequest {
  private val active = new SubmitRestProtocolField[Boolean]
  private val age = new SubmitRestProtocolField[Int]
  private val name = new SubmitRestProtocolField[String]

  def getActive: String = active.toString
  def getAge: String = age.toString
  def getName: String = name.toString

  def setActive(s: String): this.type = setBooleanField(active, s)
  def setAge(s: String): this.type = setNumericField(age, s)
  def setName(s: String): this.type = setField(name, s)

  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(name, "name")
    assertFieldIsSet(age, "age")
    assert(age.getValue > 5, "Not old enough!")
  }
}

class DummyResponse extends SubmitRestProtocolResponse

/**
 * Tests for the stable application submission REST protocol.
 */
class SubmitRestProtocolSuite extends FunSuite {

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
    intercept[AssertionError] { request.validate() } // missing everything
    request.setSparkVersion("1.4.8")
    intercept[AssertionError] { request.validate() } // missing name and age
    request.setName("something")
    intercept[AssertionError] { request.validate() } // missing only age
    request.setAge("2")
    intercept[AssertionError] { request.validate() } // age too low
    request.setAge("10")
    request.validate() // everything is set
    request.setSparkVersion(null)
    intercept[AssertionError] { request.validate() } // missing only Spark version
    request.setSparkVersion("1.2.3")
    request.setName(null)
    intercept[AssertionError] { request.validate() } // missing only name
    request.setMessage("not-setting-name")
    intercept[AssertionError] { request.validate() } // still missing name
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
    intercept[AssertionError] { message.validate() }
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
      |  "action" : "dummy_request",
      |  "active" : "true",
      |  "age" : "25",
      |  "client_spark_version" : "1.2.3",
      |  "name" : "jung"
      |}
    """.stripMargin

  private val dummyResponseJson =
    """
      |{
      |  "action" : "dummy_response",
      |  "server_spark_version" : "3.3.4"
      |}
    """.stripMargin

  private val submitDriverRequestJson =
    """
      |{
      |  "action" : "submit_driver_request",
      |  "app_args" : "[\"two slices\",\"a hint of cinnamon\"]",
      |  "app_name" : "SparkPie",
      |  "app_resource" : "honey-walnut-cherry.jar",
      |  "client_spark_version" : "1.2.3",
      |  "driver_cores" : "180",
      |  "driver_extra_class_path" : "food-coloring.jar",
      |  "driver_extra_java_options" : " -Dslices=5 -Dcolor=mostly_red",
      |  "driver_extra_library_path" : "pickle.jar",
      |  "driver_memory" : "512m",
      |  "environment_variables" : "{\"PATH\":\"/dev/null\",\"PYTHONPATH\":\"/dev/null\"}",
      |  "executor_memory" : "256m",
      |  "files" : "fireball.png",
      |  "jars" : "mayonnaise.jar,ketchup.jar",
      |  "main_class" : "org.apache.spark.examples.SparkPie",
      |  "py_files" : "do-not-eat-my.py",
      |  "spark_properties" : "{\"spark.live.long\":\"true\",\"spark.shuffle.enabled\":\"false\"}",
      |  "supervise_driver" : "false",
      |  "total_executor_cores" : "10000"
      |}
    """.stripMargin

  private val submitDriverResponseJson =
    """
      |{
      |  "action" : "submit_driver_response",
      |  "driver_id" : "driver_123",
      |  "server_spark_version" : "1.2.3",
      |  "success" : "true"
      |}
    """.stripMargin

  private val killDriverRequestJson =
    """
      |{
      |  "action" : "kill_driver_request",
      |  "client_spark_version" : "1.2.3",
      |  "driver_id" : "driver_123"
      |}
    """.stripMargin

  private val killDriverResponseJson =
    """
      |{
      |  "action" : "kill_driver_response",
      |  "driver_id" : "driver_123",
      |  "server_spark_version" : "1.2.3",
      |  "success" : "true"
      |}
    """.stripMargin

  private val driverStatusRequestJson =
    """
      |{
      |  "action" : "driver_status_request",
      |  "client_spark_version" : "1.2.3",
      |  "driver_id" : "driver_123"
      |}
    """.stripMargin

  private val driverStatusResponseJson =
    """
      |{
      |  "action" : "driver_status_response",
      |  "driver_id" : "driver_123",
      |  "driver_state" : "RUNNING",
      |  "server_spark_version" : "1.2.3",
      |  "success" : "true",
      |  "worker_host_port" : "1.2.3.4:7780",
      |  "worker_id" : "worker_123"
      |}
    """.stripMargin

  private val errorJson =
    """
      |{
      |  "action" : "error_response",
      |  "message" : "Field not found in submit request: X",
      |  "server_spark_version" : "1.2.3"
      |}
    """.stripMargin
}
