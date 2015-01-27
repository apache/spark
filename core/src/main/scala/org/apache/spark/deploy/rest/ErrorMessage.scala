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
 * A field used in an ErrorMessage.
 */
private[spark] abstract class ErrorField extends SubmitRestProtocolField
private[spark] object ErrorField extends SubmitRestProtocolFieldCompanion[ErrorField] {
  case object ACTION extends ErrorField with ActionField
  case object SERVER_SPARK_VERSION extends ErrorField
  case object MESSAGE extends ErrorField
  override val requiredFields = Seq(ACTION, SERVER_SPARK_VERSION, MESSAGE)
  override val optionalFields = Seq.empty
}

/**
 * An error message sent from the cluster manager
 * in the stable application submission REST protocol.
 */
private[spark] class ErrorMessage extends SubmitRestProtocolMessage(
    SubmitRestProtocolAction.ERROR,
    ErrorField.ACTION,
    ErrorField.requiredFields)

private[spark] object ErrorMessage extends SubmitRestProtocolMessageCompanion[ErrorMessage] {
  protected override def newMessage() = new ErrorMessage
  protected override def fieldFromString(f: String) = ErrorField.fromString(f)
}
