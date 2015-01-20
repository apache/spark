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

import scala.util.matching.Regex

import org.apache.spark.util.Utils

/**
 * A field used in a SubmitDriverRequestMessage.
 */
private[spark] abstract class SubmitDriverRequestField extends SubmitRestProtocolField
private[spark] object SubmitDriverRequestField extends SubmitRestProtocolFieldCompanion {
  case object ACTION extends SubmitDriverRequestField
  case object SPARK_VERSION extends SubmitDriverRequestField
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
  case class APP_ARG(index: Int) extends SubmitDriverRequestField {
    override def toString: String = Utils.getFormattedClassName(this) + "_" + index
  }
  case class SPARK_PROPERTY(prop: String) extends SubmitDriverRequestField {
    override def toString: String = Utils.getFormattedClassName(this) + "_" + prop
  }
  case class ENVIRONMENT_VARIABLE(envVar: String) extends SubmitDriverRequestField {
    override def toString: String = Utils.getFormattedClassName(this) + "_" + envVar
  }
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MASTER, APP_NAME, APP_RESOURCE)
  override val optionalFields = Seq(MESSAGE, MAIN_CLASS, JARS, FILES, PY_FILES, DRIVER_MEMORY,
    DRIVER_CORES, DRIVER_EXTRA_JAVA_OPTIONS, DRIVER_EXTRA_CLASS_PATH, DRIVER_EXTRA_LIBRARY_PATH,
    SUPERVISE_DRIVER, EXECUTOR_MEMORY, TOTAL_EXECUTOR_CORES)

  // Because certain fields taken in arguments, we cannot simply rely on the
  // list of all fields to reconstruct a field from its String representation.
  // Instead, we must treat these fields as special cases and match on their prefixes.
  override def withName(field: String): SubmitRestProtocolField = {
    def buildRegex(obj: AnyRef): Regex = s"${Utils.getFormattedClassName(obj)}_(.*)".r
    val appArg = buildRegex(APP_ARG)
    val sparkProperty = buildRegex(SPARK_PROPERTY)
    val environmentVariable = buildRegex(ENVIRONMENT_VARIABLE)
    field match {
      case appArg(f) => APP_ARG(f.toInt)
      case sparkProperty(f) => SPARK_PROPERTY(f)
      case environmentVariable(f) => ENVIRONMENT_VARIABLE(f)
      case _ => super.withName(field)
    }
  }
}

/**
 * A request sent to the cluster manager to submit a driver.
 */
private[spark] class SubmitDriverRequestMessage extends SubmitRestProtocolMessage(
  SubmitRestProtocolAction.SUBMIT_DRIVER_REQUEST,
  SubmitDriverRequestField.ACTION,
  SubmitDriverRequestField.requiredFields) {

  // Ensure continuous range of app arg indices starting from 0
  override def validate(): this.type = {
    import SubmitDriverRequestField._
    val indices = fields.collect { case (a: APP_ARG, _) => a }.toSeq.sortBy(_.index).map(_.index)
    val expectedIndices = (0 until indices.size).toSeq
    if (indices != expectedIndices) {
      throw new IllegalArgumentException(s"Malformed app arg indices: ${indices.mkString(",")}")
    }
    super.validate()
  }
}

private[spark] object SubmitDriverRequestMessage extends SubmitRestProtocolMessageCompanion {
  protected override def newMessage(): SubmitRestProtocolMessage =
    new SubmitDriverRequestMessage
  protected override def fieldWithName(field: String): SubmitRestProtocolField =
    SubmitDriverRequestField.withName(field)
}
