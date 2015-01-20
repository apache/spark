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
 * A field used in a KillDriverRequestMessage.
 */
private[spark] abstract class KillDriverRequestField extends SubmitRestProtocolField
private[spark] object KillDriverRequestField
  extends SubmitRestProtocolFieldCompanion[KillDriverRequestField] {
  case object ACTION extends KillDriverRequestField
  case object SPARK_VERSION extends KillDriverRequestField
  case object MESSAGE extends KillDriverRequestField
  case object MASTER extends KillDriverRequestField
  case object DRIVER_ID extends KillDriverRequestField
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MASTER, DRIVER_ID)
  override val optionalFields = Seq(MESSAGE)
}

/**
 * A request sent to the cluster manager to kill a driver.
 */
private[spark] class KillDriverRequestMessage extends SubmitRestProtocolMessage(
  SubmitRestProtocolAction.KILL_DRIVER_REQUEST,
  KillDriverRequestField.ACTION,
  KillDriverRequestField.requiredFields)

private[spark] object KillDriverRequestMessage
  extends SubmitRestProtocolMessageCompanion[KillDriverRequestMessage] {
  protected override def newMessage() = new KillDriverRequestMessage
  protected override def fieldWithName(field: String) = KillDriverRequestField.withName(field)
}
