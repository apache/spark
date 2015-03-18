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
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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
private[rest] abstract class SubmitRestProtocolMessage {
  @JsonIgnore
  val messageType = Utils.getFormattedClassName(this)

  val action: String = messageType
  var message: String = null

  // For JSON deserialization
  private def setAction(a: String): Unit = { }

  /**
   * Serialize the message to JSON.
   * This also ensures that the message is valid and its fields are in the expected format.
   */
  def toJson: String = {
    validate()
    SubmitRestProtocolMessage.mapper.writeValueAsString(this)
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
  }

  /** Assert that the specified field is set in this message. */
  protected def assertFieldIsSet[T](value: T, name: String): Unit = {
    if (value == null) {
      throw new SubmitRestMissingFieldException(s"'$name' is missing in message $messageType.")
    }
  }

  /**
   * Assert a condition when validating this message.
   * If the assertion fails, throw a [[SubmitRestProtocolException]].
   */
  protected def assert(condition: Boolean, failMessage: String): Unit = {
    if (!condition) { throw new SubmitRestProtocolException(failMessage) }
  }
}

/**
 * Helper methods to process serialized [[SubmitRestProtocolMessage]]s.
 */
private[spark] object SubmitRestProtocolMessage {
  private val packagePrefix = this.getClass.getPackage.getName
  private val mapper = new ObjectMapper()
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    .enable(SerializationFeature.INDENT_OUTPUT)
    .registerModule(DefaultScalaModule)

  /**
   * Parse the value of the action field from the given JSON.
   * If the action field is not found, throw a [[SubmitRestMissingFieldException]].
   */
  def parseAction(json: String): String = {
    val value: Option[String] = parse(json) match {
      case JObject(fields) =>
        fields.collectFirst { case ("action", v) => v }.collect { case JString(s) => s }
      case _ => None
    }
    value.getOrElse {
      throw new SubmitRestMissingFieldException(s"Action field not found in JSON:\n$json")
    }
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
