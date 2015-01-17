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

/**
 * A field used in a SubmitDriverResponseMessage.
 */
private[spark] abstract class SubmitDriverResponseField extends StandaloneRestProtocolField
private[spark] object SubmitDriverResponseField extends StandaloneRestProtocolFieldCompanion {
  case object ACTION extends SubmitDriverResponseField
  case object SPARK_VERSION extends SubmitDriverResponseField
  case object MESSAGE extends SubmitDriverResponseField
  case object MASTER extends SubmitDriverResponseField
  case object SUCCESS extends SubmitDriverResponseField
  case object DRIVER_ID extends SubmitDriverResponseField
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MESSAGE, MASTER, SUCCESS)
  override val optionalFields = Seq(DRIVER_ID)
}

/**
 * A message sent from the standalone Master in response to a SubmitDriverRequestMessage.
 */
private[spark] class SubmitDriverResponseMessage extends StandaloneRestProtocolMessage(
  StandaloneRestProtocolAction.SUBMIT_DRIVER_RESPONSE,
  SubmitDriverResponseField.ACTION,
  SubmitDriverResponseField.requiredFields)

private[spark] object SubmitDriverResponseMessage extends StandaloneRestProtocolMessageCompanion {
  protected override def newMessage(): StandaloneRestProtocolMessage =
    new SubmitDriverResponseMessage
  protected override def fieldWithName(field: String): StandaloneRestProtocolField =
    SubmitDriverResponseField.withName(field)
}
