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
 * A field used in a DriverStatusRequestMessage.
 */
private[spark] abstract class DriverStatusRequestField extends SubmitRestProtocolField
private[spark] object DriverStatusRequestField extends SubmitRestProtocolFieldCompanion {
  case object ACTION extends DriverStatusRequestField
  case object SPARK_VERSION extends DriverStatusRequestField
  case object MESSAGE extends DriverStatusRequestField
  case object MASTER extends DriverStatusRequestField
  case object DRIVER_ID extends DriverStatusRequestField
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MASTER, DRIVER_ID)
  override val optionalFields = Seq(MESSAGE)
}

/**
 * A request sent to the cluster manager to query the status of a driver.
 */
private[spark] class DriverStatusRequestMessage extends SubmitRestProtocolMessage(
  SubmitRestProtocolAction.DRIVER_STATUS_REQUEST,
  DriverStatusRequestField.ACTION,
  DriverStatusRequestField.requiredFields)

private[spark] object DriverStatusRequestMessage extends SubmitRestProtocolMessageCompanion {
  protected override def newMessage(): SubmitRestProtocolMessage =
    new DriverStatusRequestMessage
  protected override def fieldWithName(field: String): SubmitRestProtocolField =
    DriverStatusRequestField.withName(field)
}
