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

package org.apache.spark.sql.streaming

import org.json4s._
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.annotation.Evolving

/**
 * Reports information about the instantaneous status of a streaming query.
 *
 * @param message A human readable description of what the stream is currently doing.
 * @param isDataAvailable True when there is new data to be processed. Doesn't apply
 *                        to ContinuousExecution where it is always false.
 * @param isTriggerActive True when the trigger is actively firing, false when waiting for the
 *                        next trigger time. Doesn't apply to ContinuousExecution where it is
 *                        always false.
 *
 * @since 2.1.0
 */
@Evolving
class StreamingQueryStatus protected[sql](
    val message: String,
    val isDataAvailable: Boolean,
    val isTriggerActive: Boolean) extends Serializable {

  /** The compact JSON representation of this status. */
  def json: String = compact(render(jsonValue))

  /** The pretty (i.e. indented) JSON representation of this status. */
  def prettyJson: String = pretty(render(jsonValue))

  override def toString: String = prettyJson

  private[sql] def copy(
      message: String = this.message,
      isDataAvailable: Boolean = this.isDataAvailable,
      isTriggerActive: Boolean = this.isTriggerActive): StreamingQueryStatus = {
    new StreamingQueryStatus(
      message = message,
      isDataAvailable = isDataAvailable,
      isTriggerActive = isTriggerActive)
  }

  private[sql] def jsonValue: JValue = {
    ("message" -> JString(message.toString)) ~
    ("isDataAvailable" -> JBool(isDataAvailable)) ~
    ("isTriggerActive" -> JBool(isTriggerActive))
  }
}
