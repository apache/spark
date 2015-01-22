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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.json4s.JsonAST._

import org.apache.spark.util.JsonProtocol

/**
 * A field used in a SubmitDriverRequestMessage.
 */
private[spark] abstract class SubmitDriverRequestField extends SubmitRestProtocolField
private[spark] object SubmitDriverRequestField
  extends SubmitRestProtocolFieldCompanion[SubmitDriverRequestField] {
  case object ACTION extends SubmitDriverRequestField
  case object CLIENT_SPARK_VERSION extends SubmitDriverRequestField
  case object MESSAGE extends SubmitDriverRequestField
  case object MASTER extends SubmitDriverRequestField
  case object APP_NAME extends SubmitDriverRequestField
  case object APP_RESOURCE extends SubmitDriverRequestField
  case object MAIN_CLASS extends SubmitDriverRequestField
  case object JARS extends SubmitDriverRequestField
  case object FILES extends SubmitDriverRequestField
  case object PY_FILES extends SubmitDriverRequestField
  case object DRIVER_MEMORY extends SubmitDriverRequestField
  case object DRIVER_CORES extends SubmitDriverRequestField
  case object DRIVER_EXTRA_JAVA_OPTIONS extends SubmitDriverRequestField
  case object DRIVER_EXTRA_CLASS_PATH extends SubmitDriverRequestField
  case object DRIVER_EXTRA_LIBRARY_PATH extends SubmitDriverRequestField
  case object SUPERVISE_DRIVER extends SubmitDriverRequestField // standalone cluster mode only
  case object EXECUTOR_MEMORY extends SubmitDriverRequestField
  case object TOTAL_EXECUTOR_CORES extends SubmitDriverRequestField
  case object APP_ARGS extends SubmitDriverRequestField
  case object SPARK_PROPERTIES extends SubmitDriverRequestField
  case object ENVIRONMENT_VARIABLES extends SubmitDriverRequestField
  override val requiredFields = Seq(ACTION, CLIENT_SPARK_VERSION, MASTER, APP_NAME, APP_RESOURCE)
  override val optionalFields = Seq(MESSAGE, MAIN_CLASS, JARS, FILES, PY_FILES, DRIVER_MEMORY,
    DRIVER_CORES, DRIVER_EXTRA_JAVA_OPTIONS, DRIVER_EXTRA_CLASS_PATH, DRIVER_EXTRA_LIBRARY_PATH,
    SUPERVISE_DRIVER, EXECUTOR_MEMORY, TOTAL_EXECUTOR_CORES, APP_ARGS, SPARK_PROPERTIES,
    ENVIRONMENT_VARIABLES)
}

/**
 * A request sent to the cluster manager to submit a driver
 * in the stable application submission REST protocol.
 */
private[spark] class SubmitDriverRequestMessage extends SubmitRestProtocolMessage(
    SubmitRestProtocolAction.SUBMIT_DRIVER_REQUEST,
    SubmitDriverRequestField.ACTION,
    SubmitDriverRequestField.requiredFields) {

  import SubmitDriverRequestField._

  private val appArgs = new ArrayBuffer[String]
  private val sparkProperties = new mutable.HashMap[String, String]
  private val environmentVariables = new mutable.HashMap[String, String]

  // Setters for special fields
  def appendAppArg(arg: String): Unit = { appArgs += arg }
  def setSparkProperty(k: String, v: String): Unit = { sparkProperties(k) = v }
  def setEnvironmentVariable(k: String, v: String): Unit = { environmentVariables(k) = v }

  // Getters for special fields
  def getAppArgs: Seq[String] = appArgs.clone()
  def getSparkProperties: Map[String, String] = sparkProperties.toMap
  def getEnvironmentVariables: Map[String, String] = environmentVariables.toMap

  // Include app args, spark properties, and environment variables in the JSON object
  // The order imposed here is as follows: * < APP_ARGS < SPARK_PROPERTIES < ENVIRONMENT_VARIABLES
  override def toJsonObject: JObject = {
    val otherFields = super.toJsonObject.obj
    val appArgsJson = JArray(appArgs.map(JString).toList)
    val sparkPropertiesJson = JsonProtocol.mapToJson(sparkProperties)
    val environmentVariablesJson = JsonProtocol.mapToJson(environmentVariables)
    val allFields = otherFields ++ List(
      (APP_ARGS.toString, appArgsJson),
      (SPARK_PROPERTIES.toString, sparkPropertiesJson),
      (ENVIRONMENT_VARIABLES.toString, environmentVariablesJson)
    )
    JObject(allFields)
  }
}

private[spark] object SubmitDriverRequestMessage
  extends SubmitRestProtocolMessageCompanion[SubmitDriverRequestMessage] {

  import SubmitDriverRequestField._

  protected override def newMessage() = new SubmitDriverRequestMessage
  protected override def fieldFromString(f: String) = SubmitDriverRequestField.fromString(f)

  /**
   * Process the given field and value appropriately based on the type of the field.
   * This handles certain nested values in addition to flat values.
   */
  override def handleField(
      message: SubmitDriverRequestMessage,
      field: SubmitRestProtocolField,
      value: JValue): Unit = {
    (field, value) match {
      case (APP_ARGS, JArray(args)) =>
        args.map(_.asInstanceOf[JString].s).foreach { arg =>
          message.appendAppArg(arg)
        }
      case (SPARK_PROPERTIES, props: JObject) =>
        JsonProtocol.mapFromJson(props).foreach { case (k, v) =>
          message.setSparkProperty(k, v)
        }
      case (ENVIRONMENT_VARIABLES, envVars: JObject) =>
        JsonProtocol.mapFromJson(envVars).foreach { case (envKey, envValue) =>
          message.setEnvironmentVariable(envKey, envValue)
        }
      // All other fields are assumed to have flat values
      case _ => super.handleField(message, field, value)
    }
  }
}
