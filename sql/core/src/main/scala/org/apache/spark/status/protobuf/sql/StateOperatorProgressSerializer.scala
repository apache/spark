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

package org.apache.spark.status.protobuf.sql

import java.util.{HashMap => JHashMap, List => JList}

import org.apache.spark.sql.streaming.StateOperatorProgress
import org.apache.spark.status.protobuf.StoreTypes
import org.apache.spark.status.protobuf.Utils.{getStringField, setStringField}

private[protobuf] object StateOperatorProgressSerializer {

  def serialize(stateOperator: StateOperatorProgress): StoreTypes.StateOperatorProgress = {
    import org.apache.spark.status.protobuf.Utils.setJMapField
    val builder = StoreTypes.StateOperatorProgress.newBuilder()
    setStringField(stateOperator.operatorName, builder.setOperatorName)
    builder.setNumRowsTotal(stateOperator.numRowsTotal)
    builder.setNumRowsUpdated(stateOperator.numRowsUpdated)
    builder.setAllUpdatesTimeMs(stateOperator.allUpdatesTimeMs)
    builder.setNumRowsRemoved(stateOperator.numRowsRemoved)
    builder.setAllRemovalsTimeMs(stateOperator.allRemovalsTimeMs)
    builder.setCommitTimeMs(stateOperator.commitTimeMs)
    builder.setMemoryUsedBytes(stateOperator.memoryUsedBytes)
    builder.setNumRowsDroppedByWatermark(stateOperator.numRowsDroppedByWatermark)
    builder.setNumShufflePartitions(stateOperator.numShufflePartitions)
    builder.setNumStateStoreInstances(stateOperator.numStateStoreInstances)
    setJMapField(stateOperator.customMetrics, builder.putAllCustomMetrics)
    builder.build()
  }

  def deserializeToArray(
      stateOperatorList: JList[StoreTypes.StateOperatorProgress]): Array[StateOperatorProgress] = {
    val size = stateOperatorList.size()
    val result = new Array[StateOperatorProgress](size)
    var i = 0
    while (i < size) {
      result(i) = deserialize(stateOperatorList.get(i))
      i += 1
    }
    result
  }

  private def deserialize(
      stateOperator: StoreTypes.StateOperatorProgress): StateOperatorProgress = {
    new StateOperatorProgress(
      operatorName =
        getStringField(stateOperator.hasOperatorName, () => stateOperator.getOperatorName),
      numRowsTotal = stateOperator.getNumRowsTotal,
      numRowsUpdated = stateOperator.getNumRowsUpdated,
      allUpdatesTimeMs = stateOperator.getAllUpdatesTimeMs,
      numRowsRemoved = stateOperator.getNumRowsRemoved,
      allRemovalsTimeMs = stateOperator.getAllRemovalsTimeMs,
      commitTimeMs = stateOperator.getCommitTimeMs,
      memoryUsedBytes = stateOperator.getMemoryUsedBytes,
      numRowsDroppedByWatermark = stateOperator.getNumRowsDroppedByWatermark,
      numShufflePartitions = stateOperator.getNumShufflePartitions,
      numStateStoreInstances = stateOperator.getNumStateStoreInstances,
      customMetrics = new JHashMap(stateOperator.getCustomMetricsMap)
    )
  }
}
