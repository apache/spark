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
package org.apache.spark.sql.execution.streaming

import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.sql.execution.streaming.StateVariableType.StateVariableType

// Enum of possible State Variable types
object StateVariableType extends Enumeration {
  type StateVariableType = Value
  val ValueState, ListState, MapState = Value
}

case class TransformWithStateVariableInfo(
    stateName: String,
    stateVariableType: StateVariableType,
    ttlEnabled: Boolean) {
  def jsonValue: JValue = {
    ("stateName" -> JString(stateName)) ~
      ("stateVariableType" -> JString(stateVariableType.toString)) ~
      ("ttlEnabled" -> JBool(ttlEnabled))
  }

  def json: String = {
    compact(render(jsonValue))
  }
}

object TransformWithStateVariableInfo {

  def fromJson(json: String): TransformWithStateVariableInfo = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val parsed = JsonMethods.parse(json).extract[Map[String, Any]]
    fromMap(parsed)
  }

  def fromMap(map: Map[String, Any]): TransformWithStateVariableInfo = {
    val stateName = map("stateName").asInstanceOf[String]
    val stateVariableType = StateVariableType.withName(
      map("stateVariableType").asInstanceOf[String])
    val ttlEnabled = map("ttlEnabled").asInstanceOf[Boolean]
    TransformWithStateVariableInfo(stateName, stateVariableType, ttlEnabled)
  }
}
object TransformWithStateVariableUtils {
  def getValueState(stateName: String, ttlEnabled: Boolean): TransformWithStateVariableInfo = {
    TransformWithStateVariableInfo(stateName, StateVariableType.ValueState, ttlEnabled)
  }

  def getListState(stateName: String, ttlEnabled: Boolean): TransformWithStateVariableInfo = {
    TransformWithStateVariableInfo(stateName, StateVariableType.ListState, ttlEnabled)
  }

  def getMapState(stateName: String, ttlEnabled: Boolean): TransformWithStateVariableInfo = {
      TransformWithStateVariableInfo(stateName, StateVariableType.MapState, ttlEnabled)
  }
}
