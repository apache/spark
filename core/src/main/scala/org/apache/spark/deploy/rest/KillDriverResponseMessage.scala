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
 * A field used in a KillDriverResponseMessage.
 */
private[spark] abstract class KillDriverResponseField extends SubmitRestProtocolField
private[spark] object KillDriverResponseField
  extends SubmitRestProtocolFieldCompanion[KillDriverResponseField] {
  case object ACTION extends KillDriverResponseField with ActionField
  case object SERVER_SPARK_VERSION extends KillDriverResponseField
  case object MESSAGE extends KillDriverResponseField
  case object DRIVER_ID extends KillDriverResponseField
  case object SUCCESS extends KillDriverResponseField with BooleanField
  override val requiredFields = Seq(ACTION, SERVER_SPARK_VERSION, DRIVER_ID, SUCCESS)
  override val optionalFields = Seq(MESSAGE)
}

/**
 * A message sent from the cluster manager in response to a KillDriverRequestMessage
 * in the stable application submission REST protocol.
 */
private[spark] class KillDriverResponseMessage extends SubmitRestProtocolMessage(
    SubmitRestProtocolAction.KILL_DRIVER_RESPONSE,
    KillDriverResponseField.ACTION,
    KillDriverResponseField.requiredFields)

private[spark] object KillDriverResponseMessage
  extends SubmitRestProtocolMessageCompanion[KillDriverResponseMessage] {
  protected override def newMessage() = new KillDriverResponseMessage
  protected override def fieldFromString(f: String) = KillDriverResponseField.fromString(f)
}
