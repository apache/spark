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
 * A field used in a ErrorMessage.
 */
private[spark] abstract class ErrorField extends StandaloneRestProtocolField
private[spark] object ErrorField extends StandaloneRestProtocolFieldCompanion {
  case object ACTION extends ErrorField
  case object SPARK_VERSION extends ErrorField
  case object MESSAGE extends ErrorField
  override val requiredFields = Seq(ACTION, SPARK_VERSION, MESSAGE)
  override val optionalFields = Seq.empty
}

/**
 * An error message exchanged in the standalone REST protocol.
 */
private[spark] class ErrorMessage extends StandaloneRestProtocolMessage(
  StandaloneRestProtocolAction.ERROR,
  ErrorField.ACTION,
  ErrorField.requiredFields)

private[spark] object ErrorMessage extends StandaloneRestProtocolMessageCompanion {
  protected override def newMessage(): StandaloneRestProtocolMessage = new ErrorMessage
  protected override def fieldWithName(field: String): StandaloneRestProtocolField =
    ErrorField.withName(field)
}
