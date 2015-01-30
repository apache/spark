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

/**
 * An abstract message exchanged in the REST application submission protocol.
 *
 * This message is intended to be serialized to and deserialized from JSON in the exchange.
 * Each message can either be a request or a response and consists of three common fields:
 *   (1) the action, which fully specifies the type of the message
 *   (2) the Spark version of the client / server
 *   (3) an optional message
 */
@JsonInclude(Include.NON_NULL)
@JsonAutoDetect(getterVisibility = Visibility.ANY, setterVisibility = Visibility.ANY)
@JsonPropertyOrder(alphabetic = true)
abstract class SubmitRestProtocolMessage {
  private val messageType = Utils.getFormattedClassName(this)
  protected val action: String = messageType
  protected val sparkVersion: SubmitRestProtocolField[String]
  protected val message = new SubmitRestProtocolField[String]("message")

  // Required for JSON de/serialization and not explicitly used
  private def getAction: String = action
  private def setAction(s: String): this.type = this

  // Intended for the user and not for JSON de/serialization, which expects more specific keys
  @JsonIgnore
  def getSparkVersion: String
  @JsonIgnore
  def setSparkVersion(s: String): this.type

  def getMessage: String = message.toString
  def setMessage(s: String): this.type = setField(message, s)

  /**
   * Serialize the message to JSON.
   * This also ensures that the message is valid and its fields are in the expected format.
   */
  def toJson: String = {
    validate()
    val mapper = new ObjectMapper
    pretty(parse(mapper.writeValueAsString(this)))
  }

  /**
   * Assert the validity of the message.
   * If the validation fails, throw a [[SubmitRestProtocolException]].
   */
  final def validate(): Unit = {
    try {
      doValidate()
    } catch {
      case e: Exception =>
        throw new SubmitRestProtocolException(s"Validation of message $messageType failed!", e)
    }
  }

  /** Assert the validity of the message */
  protected def doValidate(): Unit = {
    if (action == null) {
      throw new SubmitRestMissingFieldException(s"The action field is missing in $messageType")
    }
    assertFieldIsSet(sparkVersion)
  }

  /** Assert that the specified field is set in this message. */
  protected def assertFieldIsSet(field: SubmitRestProtocolField[_]): Unit = {
    if (!field.isSet) {
      throw new SubmitRestMissingFieldException(
        s"Field '${field.name}' is missing in message $messageType.")
    }
  }

  /**
   * Assert a condition when validating this message.
   * If the assertion fails, throw a [[SubmitRestProtocolException]].
   */
  protected def assert(condition: Boolean, failMessage: String): Unit = {
    if (!condition) { throw new SubmitRestProtocolException(failMessage) }
  }

  /** Set the field to the given value, or clear the field if the value is null. */
  protected def setField(f: SubmitRestProtocolField[String], v: String): this.type = {
    if (v == null) { f.clearValue() } else { f.setValue(v) }
    this
  }

  /**
   * Set the field to the given boolean value, or clear the field if the value is null.
   * If the provided value does not represent a boolean, throw an exception.
   */
  protected def setBooleanField(f: SubmitRestProtocolField[Boolean], v: String): this.type = {
    if (v == null) { f.clearValue() } else { f.setValue(v.toBoolean) }
    this
  }

  /**
   * Set the field to the given numeric value, or clear the field if the value is null.
   * If the provided value does not represent a numeric, throw an exception.
   */
  protected def setNumericField(f: SubmitRestProtocolField[Int], v: String): this.type = {
    if (v == null) { f.clearValue() } else { f.setValue(v.toInt) }
    this
  }

  /**
   * Set the field to the given memory value, or clear the field if the value is null.
   * If the provided value does not represent a memory value, throw an exception.
   * Valid examples of memory values include "512m", "24g", and "128000".
   */
  protected def setMemoryField(f: SubmitRestProtocolField[String], v: String): this.type = {
    Utils.memoryStringToMb(v)
    setField(f, v)
  }
}

/**
 * An abstract request sent from the client in the REST application submission protocol.
 */
abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  protected override val sparkVersion = new SubmitRestProtocolField[String]("client_spark_version")
  def getClientSparkVersion: String = sparkVersion.toString
  def setClientSparkVersion(s: String): this.type = setField(sparkVersion, s)
  override def getSparkVersion: String = getClientSparkVersion
  override def setSparkVersion(s: String) = setClientSparkVersion(s)
}

/**
 * An abstract response sent from the server in the REST application submission protocol.
 */
abstract class SubmitRestProtocolResponse extends SubmitRestProtocolMessage {
  protected override val sparkVersion = new SubmitRestProtocolField[String]("server_spark_version")
  def getServerSparkVersion: String = sparkVersion.toString
  def setServerSparkVersion(s: String): this.type = setField(sparkVersion, s)
  override def getSparkVersion: String = getServerSparkVersion
  override def setSparkVersion(s: String) = setServerSparkVersion(s)
}

object SubmitRestProtocolMessage {
  private val mapper = new ObjectMapper
  private val packagePrefix = this.getClass.getPackage.getName

  /**
   * Parse the value of the action field from the given JSON.
   * If the action field is not found, throw a [[SubmitRestMissingFieldException]].
   */
  def parseAction(json: String): String = {
    parseField(json, "action").getOrElse {
      throw new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
    }
  }

  /** Parse the value of the specified field from the given JSON. */
  def parseField(json: String, field: String): Option[String] = {
    parse(json).asInstanceOf[JObject].obj
      .find { case (f, _) => f == field }
      .map { case (_, v) => v.asInstanceOf[JString].s }
  }

  /**
   * Construct a [[SubmitRestProtocolMessage]] from its JSON representation.
   *
   * This method first parses the action from the JSON and uses it to infer the message type.
   * Note that the action must represent one of the [[SubmitRestProtocolMessage]]s defined in
   * this package. Otherwise, a [[ClassNotFoundException]] will be thrown.
   */
  def fromJson(json: String): SubmitRestProtocolMessage = {
    val className = parseAction(json)
    val clazz = Class.forName(packagePrefix + "." + className)
      .asSubclass[SubmitRestProtocolMessage](classOf[SubmitRestProtocolMessage])
    fromJson(json, clazz)
  }

  /**
   * Construct a [[SubmitRestProtocolMessage]] from its JSON representation.
   *
   * This method determines the type of the message from the class provided instead of
   * inferring it from the action field. This is useful for deserializing JSON that
   * represents custom user-defined messages.
   */
  def fromJson[T <: SubmitRestProtocolMessage](json: String, clazz: Class[T]): T = {
    mapper.readValue(json, clazz)
  }
}
