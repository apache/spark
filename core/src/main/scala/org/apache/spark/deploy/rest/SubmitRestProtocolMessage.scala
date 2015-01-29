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

import com.fasterxml.jackson.annotation._
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.util.Utils
import org.apache.spark.deploy.rest.SubmitRestProtocolAction._

@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
abstract class SubmitRestProtocolMessage {
  import SubmitRestProtocolMessage._

  private val messageType = Utils.getFormattedClassName(this)
  protected val action: SubmitRestProtocolAction
  protected val sparkVersion = new SubmitRestProtocolField[String]
  protected val message = new SubmitRestProtocolField[String]

  // Required for JSON de/serialization and not explicitly used
  private def getAction: String = action.toString
  private def setAction(s: String): this.type = this

  // Spark version implementation depends on whether this is a request or a response
  @JsonIgnore
  def getSparkVersion: String
  @JsonIgnore
  def setSparkVersion(s: String): this.type

  def getMessage: String = message.toString
  def setMessage(s: String): this.type = setField(message, s)

  def toJson: String = {
    validate()
    val mapper = new ObjectMapper
    val json = mapper.writeValueAsString(this)
    postProcessJson(json)
  }

  def validate(): Unit = {
    assert(action != null, s"The action field is missing in $messageType!")
  }

  protected def assertFieldIsSet(field: SubmitRestProtocolField[_], name: String): Unit = {
    assert(field.isSet, s"The $name field is missing in $messageType!")
  }

  protected def setField(field: SubmitRestProtocolField[String], value: String): this.type = {
    if (value == null) { field.clearValue() } else { field.setValue(value) }
    this
  }

  protected def setBooleanField(
      field: SubmitRestProtocolField[Boolean],
      value: String): this.type = {
    if (value == null) { field.clearValue() } else { field.setValue(value.toBoolean) }
    this
  }

  protected def setNumericField(
      field: SubmitRestProtocolField[Int],
      value: String): this.type = {
    if (value == null) { field.clearValue() } else { field.setValue(value.toInt) }
    this
  }

  protected def setMemoryField(
      field: SubmitRestProtocolField[String],
      value: String): this.type = {
    Utils.memoryStringToMb(value)
    setField(field, value)
    this
  }

  private def postProcessJson(json: String): String = {
    val fields = parse(json).asInstanceOf[JObject].obj
    val newFields = fields.map { case (k, v) => (camelCaseToUnderscores(k), v) }
    pretty(render(JObject(newFields)))
  }
}

abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  def getClientSparkVersion: String = sparkVersion.toString
  def setClientSparkVersion(s: String): this.type = setField(sparkVersion, s)
  override def getSparkVersion: String = getClientSparkVersion
  override def setSparkVersion(s: String) = setClientSparkVersion(s)
  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(sparkVersion, "client_spark_version")
  }
}

abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  def getServerSparkVersion: String = sparkVersion.toString
  def setServerSparkVersion(s: String): this.type = setField(sparkVersion, s)
  override def getSparkVersion: String = getServerSparkVersion
  override def setSparkVersion(s: String) = setServerSparkVersion(s)
  override def validate(): Unit = {
    super.validate()
    assertFieldIsSet(sparkVersion, "server_spark_version")
  }
}

object SubmitRestProtocolMessage {
  private val mapper = new ObjectMapper

  def fromJson(json: String): SubmitRestProtocolMessage = {
    val fields = parse(json).asInstanceOf[JObject].obj
    val action = fields
      .find { case (f, _) => f == "action" }
      .map { case (_, v) => v.asInstanceOf[JString].s }
      .getOrElse {
        throw new IllegalArgumentException(s"Could not find action field in message:\n$json")
      }
    val clazz = SubmitRestProtocolAction.fromString(action) match {
      case SUBMIT_DRIVER_REQUEST => classOf[SubmitDriverRequest]
      case SUBMIT_DRIVER_RESPONSE => classOf[SubmitDriverResponse]
      case KILL_DRIVER_REQUEST => classOf[KillDriverRequest]
      case KILL_DRIVER_RESPONSE => classOf[KillDriverResponse]
      case DRIVER_STATUS_REQUEST => classOf[DriverStatusRequest]
      case DRIVER_STATUS_RESPONSE => classOf[DriverStatusResponse]
      case ERROR => classOf[ErrorResponse]
    }
    fromJson(json, clazz)
  }

  def fromJson[T <: SubmitRestProtocolMessage](json: String, clazz: Class[T]): T = {
    val fields = parse(json).asInstanceOf[JObject].obj
    val processedFields = fields.map { case (k, v) => (underscoresToCamelCase(k), v) }
    val processedJson = compact(render(JObject(processedFields)))
    mapper.readValue(processedJson, clazz)
  }

  private def camelCaseToUnderscores(s: String): String = {
    val newString = new StringBuilder
    s.foreach { c =>
      if (c.isUpper) {
        newString.append("_" + c.toLower)
      } else {
        newString.append(c)
      }
    }
    newString.toString()
  }

  private def underscoresToCamelCase(s: String): String = {
    val newString = new StringBuilder
    var capitalizeNext = false
    s.foreach { c =>
      if (c == '_') {
        capitalizeNext = true
      } else {
        val nextChar = if (capitalizeNext) c.toUpper else c
        newString.append(nextChar)
        capitalizeNext = false
      }
    }
    newString.toString()
  }
}

object SubmitRestProtocolRequest {
  def fromJson(s: String): SubmitRestProtocolRequest = {
    SubmitRestProtocolMessage.fromJson(s) match {
      case req: SubmitRestProtocolRequest => req
      case res: SubmitRestProtocolResponse =>
        throw new IllegalArgumentException(s"Message was not a request:\n$s")
    }
  }
}

object SubmitRestProtocolResponse {
  def fromJson(s: String): SubmitRestProtocolResponse = {
    SubmitRestProtocolMessage.fromJson(s) match {
      case req: SubmitRestProtocolRequest =>
        throw new IllegalArgumentException(s"Message was not a response:\n$s")
      case res: SubmitRestProtocolResponse => res
    }
  }
}
