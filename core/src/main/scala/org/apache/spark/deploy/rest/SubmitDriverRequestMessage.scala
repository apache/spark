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

import org.apache.spark.util.Utils

/**
 * A field used in a SubmitDriverRequestMessage.
 */
private[spark] abstract class SubmitDriverRequestField extends StandaloneRestProtocolField
private[spark] object SubmitDriverRequestField extends StandaloneRestProtocolFieldCompanion {
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
  case object SUPERVISE_DRIVER extends SubmitDriverRequestField
  case object EXECUTOR_MEMORY extends SubmitDriverRequestField
  case object TOTAL_EXECUTOR_CORES extends SubmitDriverRequestField
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
}

/**
 * A request sent to the standalone Master to submit a driver.
 */
private[spark] class SubmitDriverRequestMessage extends StandaloneRestProtocolMessage(
  StandaloneRestProtocolAction.SUBMIT_DRIVER_REQUEST,
  SubmitDriverRequestField.ACTION,
  SubmitDriverRequestField.requiredFields)

private[spark] object SubmitDriverRequestMessage extends StandaloneRestProtocolMessageCompanion {
  protected override def newMessage(): StandaloneRestProtocolMessage =
    new SubmitDriverRequestMessage
  protected override def fieldWithName(field: String): StandaloneRestProtocolField =
    SubmitDriverRequestField.withName(field)
}
