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
import scala.util.Try

import org.apache.spark.util.Utils

/**
 * A field used in a SubmitRestProtocolMessage.
 * There are a few special fields:
 *   - ACTION entirely specifies the type of the message and is required in all messages
 *   - MESSAGE contains arbitrary messages and is common, but not required, in all messages
 *   - CLIENT_SPARK_VERSION is required in all messages sent from the client
 *   - SERVER_SPARK_VERSION is required in all messages sent from the server
 */
private[spark] abstract class SubmitRestProtocolField {
  protected val fieldName = Utils.getFormattedClassName(this)
  def validateValue(value: String): Unit = { }
  def validateFailed(v: String, msg: String): Unit = {
    throw new IllegalArgumentException(s"Detected setting of $fieldName to $v: $msg")
  }
}
private[spark] object SubmitRestProtocolField {
  def isActionField(field: String): Boolean = field == "ACTION"
}

/** A field that should accept only boolean values. */
private[spark] trait BooleanField extends SubmitRestProtocolField {
  override def validateValue(v: String): Unit = {
    Try(v.toBoolean).getOrElse { validateFailed(v, s"Error parsing $v as a boolean!") }
  }
}

/** A field that should accept only numeric values. */
private[spark] trait NumericField extends SubmitRestProtocolField {
  override def validateValue(v: String): Unit = {
    Try(v.toInt).getOrElse { validateFailed(v, s"Error parsing $v as an integer!") }
  }
}

/** A field that should accept only memory values. */
private[spark] trait MemoryField extends SubmitRestProtocolField {
  override def validateValue(v: String): Unit = {
    Try(Utils.memoryStringToMb(v)).getOrElse {
      validateFailed(v, s"Error parsing $v as a memory string!")
    }
  }
}

/**
 * The main action field in every message.
 * This should be set only on message instantiation.
 */
private[spark] trait ActionField extends SubmitRestProtocolField {
  override def validateValue(v: String): Unit = {
    validateFailed(v, "The ACTION field must not be set directly after instantiation.")
  }
}

/**
 * All possible values of the ACTION field in a SubmitRestProtocolMessage.
 */
private[spark] abstract class SubmitRestProtocolAction
private[spark] object SubmitRestProtocolAction {
  case object SUBMIT_DRIVER_REQUEST extends SubmitRestProtocolAction
  case object SUBMIT_DRIVER_RESPONSE extends SubmitRestProtocolAction
  case object KILL_DRIVER_REQUEST extends SubmitRestProtocolAction
  case object KILL_DRIVER_RESPONSE extends SubmitRestProtocolAction
  case object DRIVER_STATUS_REQUEST extends SubmitRestProtocolAction
  case object DRIVER_STATUS_RESPONSE extends SubmitRestProtocolAction
  case object ERROR extends SubmitRestProtocolAction
  private val allActions =
    Seq(SUBMIT_DRIVER_REQUEST, SUBMIT_DRIVER_RESPONSE, KILL_DRIVER_REQUEST,
      KILL_DRIVER_RESPONSE, DRIVER_STATUS_REQUEST, DRIVER_STATUS_RESPONSE, ERROR)
  private val allActionsMap = allActions.map { a => (a.toString, a) }.toMap

  def fromString(action: String): SubmitRestProtocolAction = {
    allActionsMap.get(action).getOrElse {
      throw new IllegalArgumentException(s"Unknown action $action")
    }
  }
}

/**
 * Common methods used by companion objects of SubmitRestProtocolField's subclasses.
 * This keeps track of all fields that belong to this object in order to reconstruct
 * the fields from their names.
 */
private[spark] trait SubmitRestProtocolFieldCompanion[FieldType <: SubmitRestProtocolField] {
  val requiredFields: Seq[FieldType]
  val optionalFields: Seq[FieldType]

  // Listing of all fields indexed by the field's string representation
  private lazy val allFieldsMap: Map[String, FieldType] = {
    (requiredFields ++ optionalFields).map { f => (f.toString, f) }.toMap
  }

  /** Return the appropriate SubmitRestProtocolField from its string representation. */
  def fromString(field: String): FieldType = {
    allFieldsMap.get(field).getOrElse {
      throw new IllegalArgumentException(s"Unknown field $field")
    }
  }
}
