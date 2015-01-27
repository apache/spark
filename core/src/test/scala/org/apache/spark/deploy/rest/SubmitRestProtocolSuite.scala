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

import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._
import org.scalatest.FunSuite

/**
 * Dummy fields and messages for testing.
 */
private abstract class DummyField extends SubmitRestProtocolField
private object DummyField extends SubmitRestProtocolFieldCompanion[DummyField] {
  case object ACTION extends DummyField with ActionField
  case object DUMMY_FIELD extends DummyField
  case object BOOLEAN_FIELD extends DummyField with BooleanField
  case object MEMORY_FIELD extends DummyField with MemoryField
  case object NUMERIC_FIELD extends DummyField with NumericField
  case object REQUIRED_FIELD extends DummyField
  override val requiredFields = Seq(ACTION, REQUIRED_FIELD)
  override val optionalFields = Seq(DUMMY_FIELD, BOOLEAN_FIELD, MEMORY_FIELD, NUMERIC_FIELD)
}
private object DUMMY_ACTION extends SubmitRestProtocolAction {
  override def toString: String = "DUMMY_ACTION"
}
private class DummyMessage extends SubmitRestProtocolMessage(
    DUMMY_ACTION,
    DummyField.ACTION,
    DummyField.requiredFields)
private object DummyMessage extends SubmitRestProtocolMessageCompanion[DummyMessage] {
  protected override def newMessage() = new DummyMessage
  protected override def fieldFromString(f: String) = DummyField.fromString(f)
}


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
    import DummyField._
    val message = new DummyMessage
    // action field is already set on instantiation
    assert(message.getFields.size === 1)
    assert(message.getField(ACTION) === DUMMY_ACTION.toString)
    // required field not set yet
    intercept[IllegalArgumentException] { message.validate() }
    intercept[IllegalArgumentException] { message.getFieldNotNull(DUMMY_FIELD) }
    intercept[IllegalArgumentException] { message.getFieldNotNull(REQUIRED_FIELD) }
    message.setField(DUMMY_FIELD, "dummy value")
    message.setField(BOOLEAN_FIELD, "true")
    message.setField(MEMORY_FIELD, "401k")
    message.setField(NUMERIC_FIELD, "401")
    message.setFieldIfNotNull(REQUIRED_FIELD, null) // no-op because value is null
    assert(message.getFields.size === 5)
    // required field still not set
    intercept[IllegalArgumentException] { message.validate() }
    intercept[IllegalArgumentException] { message.getFieldNotNull(REQUIRED_FIELD) }
    message.setFieldIfNotNull(REQUIRED_FIELD, "dummy value")
    // all required fields are now set
    assert(message.getFields.size === 6)
    assert(message.getField(DUMMY_FIELD) === "dummy value")
    assert(message.getField(BOOLEAN_FIELD) === "true")
    assert(message.getField(MEMORY_FIELD) === "401k")
    assert(message.getField(NUMERIC_FIELD) === "401")
    assert(message.getField(REQUIRED_FIELD) === "dummy value")
    message.validate()
    // bad field values
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    intercept[IllegalArgumentException] { message.setField(BOOLEAN_FIELD, "not T nor F") }
    intercept[IllegalArgumentException] { message.setField(MEMORY_FIELD, "not memory") }
    intercept[IllegalArgumentException] { message.setField(NUMERIC_FIELD, "not a number") }
  }

  test("to and from JSON") {
    import DummyField._
    val message = new DummyMessage()
      .setField(DUMMY_FIELD, "dummy value")
      .setField(BOOLEAN_FIELD, "true")
      .setField(MEMORY_FIELD, "401k")
      .setField(NUMERIC_FIELD, "401")
      .setField(REQUIRED_FIELD, "dummy value")
      .validate()
    val expectedJson =
      """
        |{
        |  "ACTION" : "DUMMY_ACTION",
        |  "DUMMY_FIELD" : "dummy value",
        |  "BOOLEAN_FIELD" : "true",
        |  "MEMORY_FIELD" : "401k",
        |  "NUMERIC_FIELD" : "401",
        |  "REQUIRED_FIELD" : "dummy value"
        |}
      """.stripMargin
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    // Do not use SubmitRestProtocolMessage.fromJson here
    // because DUMMY_ACTION is not a known action
    val jsonObject = parse(expectedJson).asInstanceOf[JObject]
    val newMessage = DummyMessage.fromJsonObject(jsonObject)
    assert(newMessage.getFieldNotNull(ACTION) === "DUMMY_ACTION")
    assert(newMessage.getFieldNotNull(DUMMY_FIELD) === "dummy value")
    assert(newMessage.getFieldNotNull(BOOLEAN_FIELD) === "true")
    assert(newMessage.getFieldNotNull(MEMORY_FIELD) === "401k")
    assert(newMessage.getFieldNotNull(NUMERIC_FIELD) === "401")
    assert(newMessage.getFieldNotNull(REQUIRED_FIELD) === "dummy value")
    assert(newMessage.getFields.size === 6)
  }

  test("SubmitDriverRequestMessage") {
    import SubmitDriverRequestField._
    val message = new SubmitDriverRequestMessage
    intercept[IllegalArgumentException] { message.validate() }
    message.setField(CLIENT_SPARK_VERSION, "1.2.3")
    message.setField(MESSAGE, "Submitting them drivers.")
    message.setField(APP_NAME, "SparkPie")
    message.setField(APP_RESOURCE, "honey-walnut-cherry.jar")
    // all required fields are now set
    message.validate()
    message.setField(MAIN_CLASS, "org.apache.spark.examples.SparkPie")
    message.setField(JARS, "mayonnaise.jar,ketchup.jar")
    message.setField(FILES, "fireball.png")
    message.setField(PY_FILES, "do-not-eat-my.py")
    message.setField(DRIVER_MEMORY, "512m")
    message.setField(DRIVER_CORES, "180")
    message.setField(DRIVER_EXTRA_JAVA_OPTIONS, " -Dslices=5 -Dcolor=mostly_red")
    message.setField(DRIVER_EXTRA_CLASS_PATH, "food-coloring.jar")
    message.setField(DRIVER_EXTRA_LIBRARY_PATH, "pickle.jar")
    message.setField(SUPERVISE_DRIVER, "false")
    message.setField(EXECUTOR_MEMORY, "256m")
    message.setField(TOTAL_EXECUTOR_CORES, "10000")
    // bad field values
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    intercept[IllegalArgumentException] { message.setField(DRIVER_MEMORY, "more than expected") }
    intercept[IllegalArgumentException] { message.setField(DRIVER_CORES, "one hundred feet") }
    intercept[IllegalArgumentException] { message.setField(SUPERVISE_DRIVER, "nope, never") }
    intercept[IllegalArgumentException] { message.setField(EXECUTOR_MEMORY, "less than expected") }
    intercept[IllegalArgumentException] { message.setField(TOTAL_EXECUTOR_CORES, "two men") }
    intercept[IllegalArgumentException] { message.setField(APP_ARGS, "anything") }
    intercept[IllegalArgumentException] { message.setField(SPARK_PROPERTIES, "anything") }
    intercept[IllegalArgumentException] { message.setField(ENVIRONMENT_VARIABLES, "anything") }
    // special fields
    message.appendAppArg("two slices")
    message.appendAppArg("a hint of cinnamon")
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
    val expectedJson = submitDriverRequestJson
    assertJsonEquals(message.toJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
      .asInstanceOf[SubmitDriverRequestMessage]
    assert(newMessage.getFields === message.getFields)
    assert(newMessage.getAppArgs === message.getAppArgs)
    assert(newMessage.getSparkProperties === message.getSparkProperties)
    assert(newMessage.getEnvironmentVariables === message.getEnvironmentVariables)
  }

  test("SubmitDriverResponseMessage") {
    import SubmitDriverResponseField._
    val message = new SubmitDriverResponseMessage
    intercept[IllegalArgumentException] { message.validate() }
    message.setField(SERVER_SPARK_VERSION, "1.2.3")
    message.setField(MESSAGE, "Dem driver is now submitted.")
    message.setField(DRIVER_ID, "driver_123")
    message.setField(SUCCESS, "true")
    // all required fields are now set
    message.validate()
    // bad field values
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    intercept[IllegalArgumentException] { message.setField(SUCCESS, "maybe not") }
    // test JSON
    val expectedJson = submitDriverResponseJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[SubmitDriverResponseMessage])
    assert(newMessage.getFields === message.getFields)
  }

  test("KillDriverRequestMessage") {
    import KillDriverRequestField._
    val message = new KillDriverRequestMessage
    intercept[IllegalArgumentException] { message.validate() }
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    message.setField(CLIENT_SPARK_VERSION, "1.2.3")
    message.setField(DRIVER_ID, "driver_123")
    // all required fields are now set
    message.validate()
    // test JSON
    val expectedJson = killDriverRequestJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[KillDriverRequestMessage])
    assert(newMessage.getFields === message.getFields)
  }

  test("KillDriverResponseMessage") {
    import KillDriverResponseField._
    val message = new KillDriverResponseMessage
    intercept[IllegalArgumentException] { message.validate() }
    message.setField(SERVER_SPARK_VERSION, "1.2.3")
    message.setField(DRIVER_ID, "driver_123")
    message.setField(SUCCESS, "true")
    // all required fields are now set
    message.validate()
    message.setField(MESSAGE, "Killing dem reckless drivers.")
    // bad field values
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    intercept[IllegalArgumentException] { message.setField(SUCCESS, "maybe?") }
    // test JSON
    val expectedJson = killDriverResponseJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[KillDriverResponseMessage])
    assert(newMessage.getFields === message.getFields)
  }

  test("DriverStatusRequestMessage") {
    import DriverStatusRequestField._
    val message = new DriverStatusRequestMessage
    intercept[IllegalArgumentException] { message.validate() }
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    message.setField(CLIENT_SPARK_VERSION, "1.2.3")
    message.setField(DRIVER_ID, "driver_123")
    // all required fields are now set
    message.validate()
    // test JSON
    val expectedJson = driverStatusRequestJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[DriverStatusRequestMessage])
    assert(newMessage.getFields === message.getFields)
  }

  test("DriverStatusResponseMessage") {
    import DriverStatusResponseField._
    val message = new DriverStatusResponseMessage
    intercept[IllegalArgumentException] { message.validate() }
    message.setField(SERVER_SPARK_VERSION, "1.2.3")
    message.setField(DRIVER_ID, "driver_123")
    message.setField(SUCCESS, "true")
    // all required fields are now set
    message.validate()
    message.setField(MESSAGE, "Your driver is having some trouble...")
    message.setField(DRIVER_STATE, "RUNNING")
    message.setField(WORKER_ID, "worker_123")
    message.setField(WORKER_HOST_PORT, "1.2.3.4:7780")
    // bad field values
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    intercept[IllegalArgumentException] { message.setField(SUCCESS, "maybe") }
    // test JSON
    val expectedJson = driverStatusResponseJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[DriverStatusResponseMessage])
    assert(newMessage.getFields === message.getFields)
  }

  test("ErrorMessage") {
    import ErrorField._
    val message = new ErrorMessage
    intercept[IllegalArgumentException] { message.validate() }
    intercept[IllegalArgumentException] { message.setField(ACTION, "anything") }
    message.setField(SERVER_SPARK_VERSION, "1.2.3")
    message.setField(MESSAGE, "Your wife threw an exception!")
    // all required fields are now set
    message.validate()
    // test JSON
    val expectedJson = errorJson
    val actualJson = message.toJson
    assertJsonEquals(actualJson, expectedJson)
    val newMessage = SubmitRestProtocolMessage.fromJson(expectedJson)
    assert(newMessage.isInstanceOf[ErrorMessage])
    assert(newMessage.getFields === message.getFields)
  }

  private val submitDriverRequestJson =
    """
      |{
      |  "ACTION" : "SUBMIT_DRIVER_REQUEST",
      |  "CLIENT_SPARK_VERSION" : "1.2.3",
      |  "MESSAGE" : "Submitting them drivers.",
      |  "APP_NAME" : "SparkPie",
      |  "APP_RESOURCE" : "honey-walnut-cherry.jar",
      |  "MAIN_CLASS" : "org.apache.spark.examples.SparkPie",
      |  "JARS" : "mayonnaise.jar,ketchup.jar",
      |  "FILES" : "fireball.png",
      |  "PY_FILES" : "do-not-eat-my.py",
      |  "DRIVER_MEMORY" : "512m",
      |  "DRIVER_CORES" : "180",
      |  "DRIVER_EXTRA_JAVA_OPTIONS" : " -Dslices=5 -Dcolor=mostly_red",
      |  "DRIVER_EXTRA_CLASS_PATH" : "food-coloring.jar",
      |  "DRIVER_EXTRA_LIBRARY_PATH" : "pickle.jar",
      |  "SUPERVISE_DRIVER" : "false",
      |  "EXECUTOR_MEMORY" : "256m",
      |  "TOTAL_EXECUTOR_CORES" : "10000",
      |  "APP_ARGS" : [ "two slices", "a hint of cinnamon" ],
      |  "SPARK_PROPERTIES" : {
      |    "spark.live.long" : "true",
      |    "spark.shuffle.enabled" : "false"
      |  },
      |  "ENVIRONMENT_VARIABLES" : {
      |    "PATH" : "/dev/null",
      |    "PYTHONPATH" : "/dev/null"
      |  }
      |}
    """.stripMargin

  private val submitDriverResponseJson =
    """
      |{
      |  "ACTION" : "SUBMIT_DRIVER_RESPONSE",
      |  "SERVER_SPARK_VERSION" : "1.2.3",
      |  "MESSAGE" : "Dem driver is now submitted.",
      |  "DRIVER_ID" : "driver_123",
      |  "SUCCESS" : "true"
      |}
    """.stripMargin

  private val killDriverRequestJson =
    """
      |{
      |  "ACTION" : "KILL_DRIVER_REQUEST",
      |  "CLIENT_SPARK_VERSION" : "1.2.3",
      |  "DRIVER_ID" : "driver_123"
      |}
    """.stripMargin

  private val killDriverResponseJson =
    """
      |{
      |  "ACTION" : "KILL_DRIVER_RESPONSE",
      |  "SERVER_SPARK_VERSION" : "1.2.3",
      |  "DRIVER_ID" : "driver_123",
      |  "SUCCESS" : "true",
      |  "MESSAGE" : "Killing dem reckless drivers."
      |}
    """.stripMargin

  private val driverStatusRequestJson =
    """
      |{
      |  "ACTION" : "DRIVER_STATUS_REQUEST",
      |  "CLIENT_SPARK_VERSION" : "1.2.3",
      |  "DRIVER_ID" : "driver_123"
      |}
    """.stripMargin

  private val driverStatusResponseJson =
    """
      |{
      |  "ACTION" : "DRIVER_STATUS_RESPONSE",
      |  "SERVER_SPARK_VERSION" : "1.2.3",
      |  "DRIVER_ID" : "driver_123",
      |  "SUCCESS" : "true",
      |  "MESSAGE" : "Your driver is having some trouble...",
      |  "DRIVER_STATE" : "RUNNING",
      |  "WORKER_ID" : "worker_123",
      |  "WORKER_HOST_PORT" : "1.2.3.4:7780"
      |}
    """.stripMargin

  private val errorJson =
    """
      |{
      |  "ACTION" : "ERROR",
      |  "SERVER_SPARK_VERSION" : "1.2.3",
      |  "MESSAGE" : "Your wife threw an exception!"
      |}
    """.stripMargin
}
