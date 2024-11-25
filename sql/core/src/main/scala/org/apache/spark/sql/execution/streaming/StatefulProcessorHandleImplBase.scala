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

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.NoTime
import org.apache.spark.sql.execution.streaming.StatefulProcessorHandleState.{INITIALIZED, PRE_INIT, StatefulProcessorHandleState, TIMER_PROCESSED}
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.streaming.{StatefulProcessorHandle, TimeMode}

abstract class StatefulProcessorHandleImplBase(
    timeMode: TimeMode, keyExprEnc: ExpressionEncoder[Any]) extends StatefulProcessorHandle {

  protected var currState: StatefulProcessorHandleState = PRE_INIT

  def setHandleState(newState: StatefulProcessorHandleState): Unit = {
    currState = newState
  }

  def getHandleState: StatefulProcessorHandleState = currState

  def verifyTimerOperations(operationType: String): Unit = {
    if (timeMode == NoTime) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidTimeMode(operationType,
        timeMode.toString)
    }

    if (currState < INITIALIZED || currState >= TIMER_PROCESSED) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidHandleState(operationType,
        currState.toString)
    }
  }

  def verifyStateVarOperations(
      operationType: String,
      requiredState: StatefulProcessorHandleState): Unit = {
    if (currState != requiredState) {
      throw StateStoreErrors.cannotPerformOperationWithInvalidHandleState(operationType,
        currState.toString)
    }
  }
}
