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

import org.json4s.{DefaultFormats, Formats, JValue}
import org.json4s.JsonAST.{JBool, JString}
import org.json4s.JsonDSL._

// Enum to store the types of state variables we support
sealed trait StateVariableType

case object ValueState extends StateVariableType
case object ListState extends StateVariableType
case object MapState extends StateVariableType

// This object is used to convert the state type from string to the corresponding enum
object StateVariableType {
  def withName(name: String): StateVariableType = name match {
    case "ValueState" => ValueState
    case "ListState" => ListState
    case "MapState" => MapState
    case _ => throw new IllegalArgumentException(s"Unknown state type: $name")
  }
}

// This class is used to store the information about a state variable.
// It is stored in operatorProperties for the TransformWithStateExec operator
// to be able to validate that the State Variables are the same across restarts.
class StateVariableInfo(
   val stateName: String,
   val stateType: StateVariableType,
   val isTtlEnabled: Boolean
 ) {
  def jsonValue: JValue = {
    ("stateName" -> JString(stateName)) ~
      ("stateType" -> JString(stateType.toString)) ~
      ("isTtlEnabled" -> JBool(isTtlEnabled))
  }
}

// This object is used to convert the state variable information
// from JSON to a list of StateVariableInfo
object StateVariableInfo {
  implicit val formats: Formats = DefaultFormats
  def fromJson(json: Any): List[StateVariableInfo] = {
    assert(json.isInstanceOf[List[_]], s"Expected List but got ${json.getClass}")
    val stateVariables = json.asInstanceOf[List[Map[String, Any]]]
    // Extract each JValue to StateVariableInfo
    stateVariables.map { stateVariable =>
      new StateVariableInfo(
        stateVariable("stateName").asInstanceOf[String],
        StateVariableType.withName(stateVariable("stateType").asInstanceOf[String]),
        stateVariable("isTtlEnabled").asInstanceOf[Boolean]
      )
    }
  }
}
