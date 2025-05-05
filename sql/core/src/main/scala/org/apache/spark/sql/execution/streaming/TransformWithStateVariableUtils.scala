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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.StateVariableType.StateVariableType
import org.apache.spark.sql.execution.streaming.state.{OperatorInfoV1, OperatorStateMetadata, OperatorStateMetadataV2, StateStoreErrors, StateStoreId, StateStoreMetadataV2}
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}

/**
 * This file contains utility classes and functions for managing state variables in
 * the operatorProperties field of the OperatorStateMetadata for TransformWithState.
 * We use these utils to read and write state variable information for validation purposes.
 */
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

  def getTimerState(stateName: String): TransformWithStateVariableInfo = {
    TransformWithStateVariableInfo(stateName, StateVariableType.TimerState, ttlEnabled = false)
  }

  def validateTimeMode(timeMode: TimeMode, timestampOpt: Option[Long]): Unit = {
    if (timeMode != TimeMode.None() && timestampOpt.isEmpty) {
      throw StateStoreErrors.missingTimeValues(timeMode.toString)
    }
  }

  def getRowCounterCFName(stateName: String): String = "$rowCounter_" + stateName
}

// Enum of possible State Variable types
object StateVariableType extends Enumeration {
  type StateVariableType = Value
  val ValueState, ListState, MapState, TimerState = Value
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

case class TransformWithStateOperatorProperties(
    timeMode: String,
    outputMode: String,
    stateVariables: List[TransformWithStateVariableInfo]) {

  def json: String = {
    val stateVariablesJson = stateVariables.map(_.jsonValue)
    val json =
      ("timeMode" -> timeMode) ~
        ("outputMode" -> outputMode) ~
        ("stateVariables" -> stateVariablesJson)
    compact(render(json))
  }
}

object TransformWithStateOperatorProperties extends Logging {
  def fromJson(json: String): TransformWithStateOperatorProperties = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val jsonMap = JsonMethods.parse(json).extract[Map[String, Any]]
    TransformWithStateOperatorProperties(
      jsonMap("timeMode").asInstanceOf[String],
      jsonMap("outputMode").asInstanceOf[String],
      jsonMap("stateVariables").asInstanceOf[List[Map[String, Any]]].map { stateVarMap =>
        TransformWithStateVariableInfo.fromMap(stateVarMap)
      }
    )
  }

  // This function is to confirm that the operator properties and state variables have
  // only changed in an acceptable way after query restart. If the properties have changed
  // in an unacceptable way, this function will throw an exception.
  def validateOperatorProperties(
      oldOperatorProperties: TransformWithStateOperatorProperties,
      newOperatorProperties: TransformWithStateOperatorProperties): Unit = {
    if (oldOperatorProperties.timeMode != newOperatorProperties.timeMode) {
      throw StateStoreErrors.invalidConfigChangedAfterRestart(
        "timeMode", oldOperatorProperties.timeMode, newOperatorProperties.timeMode)
    }

    if (oldOperatorProperties.outputMode != newOperatorProperties.outputMode) {
      throw StateStoreErrors.invalidConfigChangedAfterRestart(
        "outputMode", oldOperatorProperties.outputMode, newOperatorProperties.outputMode)
    }

    val oldStateVariableInfos = oldOperatorProperties.stateVariables
    val newStateVariableInfos = newOperatorProperties.stateVariables.map { stateVarInfo =>
      stateVarInfo.stateName -> stateVarInfo
    }.toMap
    oldStateVariableInfos.foreach { oldInfo =>
      val newInfo = newStateVariableInfos.get(oldInfo.stateName)
      newInfo match {
        case Some(stateVarInfo) =>
          if (oldInfo.stateVariableType != stateVarInfo.stateVariableType) {
            throw StateStoreErrors.invalidVariableTypeChange(
              stateVarInfo.stateName,
              oldInfo.stateVariableType.toString,
              stateVarInfo.stateVariableType.toString
            )
          }
        case None =>
      }
    }
  }
}

/**
 * This trait contains utils functions related to TransformWithState metadata.
 * This is used both in Scala and Python side of TransformWithState metadata support when calling
 * `init()` with DriverStatefulProcessorHandleImpl, and get the state schema and state metadata
 * on driver during physical planning phase.
 */
trait TransformWithStateMetadataUtils extends SchemaValidationUtils with Logging {

  // TransformWithState operators are allowed to evolve their schemas
  override val schemaEvolutionEnabledForOperator: Boolean = true

  def getStateVariableInfos(): Map[String, TransformWithStateVariableInfo]

  def getOperatorStateMetadata(
      stateSchemaPaths: List[List[String]],
      info: StatefulOperatorStateInfo,
      shortName: String,
      timeMode: TimeMode,
      outputMode: OutputMode): OperatorStateMetadata = {
    val operatorInfo = OperatorInfoV1(info.operatorId, shortName)
    // stateSchemaFilePath should be populated at this point
    val stateStoreInfo =
      Array(StateStoreMetadataV2(
        StateStoreId.DEFAULT_STORE_NAME, 0, info.numPartitions, stateSchemaPaths.head))

    val operatorProperties = TransformWithStateOperatorProperties(
      timeMode.toString,
      outputMode.toString,
      getStateVariableInfos().values.toList
    )
    OperatorStateMetadataV2(operatorInfo, stateStoreInfo, operatorProperties.json)
  }

  def validateNewMetadataForTWS(
      oldOperatorMetadata: OperatorStateMetadata,
      newOperatorMetadata: OperatorStateMetadata): Unit = {
    (oldOperatorMetadata, newOperatorMetadata) match {
      case (
        oldMetadataV2: OperatorStateMetadataV2,
        newMetadataV2: OperatorStateMetadataV2) =>
        val oldOperatorProps = TransformWithStateOperatorProperties.fromJson(
          oldMetadataV2.operatorPropertiesJson)
        val newOperatorProps = TransformWithStateOperatorProperties.fromJson(
          newMetadataV2.operatorPropertiesJson)
        TransformWithStateOperatorProperties.validateOperatorProperties(
          oldOperatorProps, newOperatorProps)
      case (_, _) =>
    }
  }
}
