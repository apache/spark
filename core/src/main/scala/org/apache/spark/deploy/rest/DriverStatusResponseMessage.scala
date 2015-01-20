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
 * A field used in a DriverStatusResponseMessage.
 */
private[spark] abstract class DriverStatusResponseField extends SubmitRestProtocolField
private[spark] object DriverStatusResponseField extends SubmitRestProtocolFieldCompanion {
  case object ACTION extends DriverStatusResponseField
  case object SPARK_VERSION extends DriverStatusResponseField
  case object MESSAGE extends DriverStatusResponseField
  case object MASTER extends DriverStatusResponseField
  case object DRIVER_ID extends DriverStatusResponseField
  case object DRIVER_STATE extends SubmitDriverResponseField
  case object WORKER_ID extends SubmitDriverResponseField
  case object WORKER_HOST_PORT extends SubmitDriverResponseField
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MESSAGE,
    MASTER, DRIVER_ID, DRIVER_STATE, WORKER_ID, WORKER_HOST_PORT)
  override val optionalFields = Seq.empty
}

/**
 * A message sent from the cluster manager in response to a DriverStatusResponseMessage.
 */
private[spark] class DriverStatusResponseMessage extends SubmitRestProtocolMessage(
  SubmitRestProtocolAction.DRIVER_STATUS_RESPONSE,
  DriverStatusResponseField.ACTION,
  DriverStatusResponseField.requiredFields)

private[spark] object DriverStatusResponseMessage extends SubmitRestProtocolMessageCompanion {
  protected override def newMessage(): SubmitRestProtocolMessage =
    new DriverStatusResponseMessage
  protected override def fieldWithName(field: String): SubmitRestProtocolField =
    DriverStatusResponseField.withName(field)
}
