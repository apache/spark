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

import scala.collection.Map
import scala.collection.JavaConversions._

import org.json4s.jackson.JsonMethods._
import org.json4s.JsonAST._

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * A general message exchanged in the stable application submission REST protocol.
 *
 * The message is represented by a set of fields in the form of key value pairs.
 * Each message must contain an ACTION field, which fully specifies the type of the message.
 * For compatibility with older versions of Spark, existing fields must not be removed or
 * modified, though new fields can be added as necessary.
 */
private[spark] abstract class SubmitRestProtocolMessage(
    action: SubmitRestProtocolAction,
    actionField: ActionField,
    requiredFields: Seq[SubmitRestProtocolField]) {

  // Maintain the insert order for converting to JSON later
  private val fields = new java.util.LinkedHashMap[SubmitRestProtocolField, String]
  val className = Utils.getFormattedClassName(this)

  // Set the action field
  fields.put(actionField, action.toString)

  /** Return all fields currently set in this message. */
  def getFields: Map[SubmitRestProtocolField, String] = fields.toMap

  /** Return the value of the given field. If the field is not present, return null. */
  def getField(key: SubmitRestProtocolField): String = getFieldOption(key).orNull

  /** Return the value of the given field. If the field is not present, throw an exception. */
  def getFieldNotNull(key: SubmitRestProtocolField): String = {
    getFieldOption(key).getOrElse {
      throw new IllegalArgumentException(s"Field $key is not set in message $className")
    }
  }

  /** Return the value of the given field as an option. */
  def getFieldOption(key: SubmitRestProtocolField): Option[String] = Option(fields.get(key))

  /** Assign the given value to the field, overriding any existing value. */
  def setField(key: SubmitRestProtocolField, value: String): this.type = {
    key.validateValue(value)
    fields.put(key, value)
    this
  }

  /** Assign the given value to the field only if the value is not null. */
  def setFieldIfNotNull(key: SubmitRestProtocolField, value: String): this.type = {
    if (value != null) {
      setField(key, value)
    }
    this
  }

  /**
   * Validate that all required fields are set and the value of the ACTION field is as expected.
   * If any of these conditions are not met, throw an exception.
   */
  def validate(): this.type = {
    if (!fields.contains(actionField)) {
      throw new IllegalArgumentException(s"The action field is missing from message $className.")
    }
    if (fields(actionField) != action.toString) {
      throw new IllegalArgumentException(
        s"Expected action $action in message $className, but actual was ${fields(actionField)}.")
    }
    val missingFields = requiredFields.filterNot(fields.contains)
    if (missingFields.nonEmpty) {
      val missingFieldsString = missingFields.mkString(", ")
      throw new IllegalArgumentException(
        s"The following fields are missing from message $className: $missingFieldsString.")
    }
    this
  }

  /** Return the JSON representation of this message. */
  def toJson: String = pretty(render(toJsonObject))

  /**
   * Return a JObject that represents the JSON form of this message.
   * This ignores fields with null values.
   */
  protected def toJsonObject: JObject = {
    val jsonFields = fields.toSeq
      .filter { case (_, v) => v != null }
      .map { case (k, v) => JField(k.toString, JString(v)) }
      .toList
    JObject(jsonFields)
  }
}

private[spark] object SubmitRestProtocolMessage {
  import SubmitRestProtocolField._
  import SubmitRestProtocolAction._

  /**
   * Construct a SubmitRestProtocolMessage from its JSON representation.
   * This uses the ACTION field to determine the type of the message to reconstruct.
   * If such a field does not exist, throw an exception.
   */
  def fromJson(json: String): SubmitRestProtocolMessage = {
    val jsonObject = parse(json).asInstanceOf[JObject]
    val action = getAction(jsonObject).getOrElse {
      throw new IllegalArgumentException(s"ACTION not found in message:\n$json")
    }
    SubmitRestProtocolAction.fromString(action) match {
      case SUBMIT_DRIVER_REQUEST => SubmitDriverRequestMessage.fromJsonObject(jsonObject)
      case SUBMIT_DRIVER_RESPONSE => SubmitDriverResponseMessage.fromJsonObject(jsonObject)
      case KILL_DRIVER_REQUEST => KillDriverRequestMessage.fromJsonObject(jsonObject)
      case KILL_DRIVER_RESPONSE => KillDriverResponseMessage.fromJsonObject(jsonObject)
      case DRIVER_STATUS_REQUEST => DriverStatusRequestMessage.fromJsonObject(jsonObject)
      case DRIVER_STATUS_RESPONSE => DriverStatusResponseMessage.fromJsonObject(jsonObject)
      case ERROR => ErrorMessage.fromJsonObject(jsonObject)
    }
  }

  /**
   * Extract the value of the ACTION field in the JSON object.
   */
  private def getAction(jsonObject: JObject): Option[String] = {
    jsonObject.obj
      .collect { case JField(k, JString(v)) if isActionField(k) => v }
      .headOption
  }
}

/**
 * Common methods used by companion objects of SubmitRestProtocolMessage's subclasses.
 */
private[spark] trait SubmitRestProtocolMessageCompanion[MessageType <: SubmitRestProtocolMessage]
  extends Logging {

  import SubmitRestProtocolField._

  /** Construct a new message of the relevant type. */
  protected def newMessage(): MessageType

  /** Return a field of the relevant type from the field's string representation. */
  protected def fieldFromString(field: String): SubmitRestProtocolField

  /**
   * Populate the given field and value in the provided message.
   * The default behavior only handles fields that have flat values and ignores other fields.
   * If the subclass uses fields with nested values, it should override this method appropriately.
   */
  protected def handleField(
      message: MessageType,
      field: SubmitRestProtocolField,
      value: JValue): Unit = {
    value match {
      case JString(s) => message.setField(field, s)
      case _ => logWarning(
        s"Unexpected value for field $field in message ${message.className}:\n$value")
    }
  }

  /** Construct a SubmitRestProtocolMessage from the given JSON object. */
  def fromJsonObject(jsonObject: JObject): MessageType = {
    val message = newMessage()
    val fields = jsonObject.obj
      .map { case JField(k, v) => (k, v) }
      // The ACTION field is already handled on instantiation
      .filter { case (k, _) => !isActionField(k) }
      .flatMap { case (k, v) =>
        try {
          Some((fieldFromString(k), v))
        } catch {
          case e: IllegalArgumentException =>
            logWarning(s"Unexpected field $k in message ${Utils.getFormattedClassName(this)}")
            None
        }
      }
    fields.foreach { case (k, v) => handleField(message, k, v) }
    message
  }
}
