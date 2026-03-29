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

package org.apache.spark.sql.execution.streaming.operators.stateful

import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorsUtils
import org.apache.spark.sql.execution.streaming.operators.stateful.flatmapgroupswithstate.FlatMapGroupsWithStatePartitionKeyExtractor
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.{TransformWithStatePartitionKeyExtractorFactory, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state.{OfflineStateRepartitionErrors, StatePartitionKeyExtractor, StateStore, StateStoreId}
import org.apache.spark.sql.types.StructType

/**
 * Factory for creating state partition key extractor for various streaming stateful operators.
 * This is used for offline state repartitioning, when we need to repartition
 * the state for a given operator. If an operator isn't included in this factory,
 * then offline repartitioning will not be supported for it.
 *
 * To support offline repartitioning for a new stateful operator, you need to:
 * 1. Create a state partition key extractor for the operator state.
 * 2. Register the state partition key extractor in this factory.
 */
object StatePartitionKeyExtractorFactory {
  import StatefulOperatorsUtils._

  /**
   * Creates a state partition key extractor for the given operator.
   * An operator may have different extractor for different stores/column families.
   *
   * @param operatorName The name of the operator.
   * @param stateKeySchema The schema of the state key.
   * @param storeName The name of the store.
   * @param colFamilyName The name of the column family.
   * @param stateFormatVersion Optional, the version of the state format. Used by operators
   *                           that have different extractors for different state formats.
   * @param stateVariableInfo Optional, the state variable info for TransformWithState.
   * @return The state partition key extractor.
   */
  def create(
      operatorName: String,
      stateKeySchema: StructType,
      storeName: String = StateStoreId.DEFAULT_STORE_NAME,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME,
      stateFormatVersion: Option[Int] = None,
      stateVariableInfo: Option[TransformWithStateVariableInfo] = None
  ): StatePartitionKeyExtractor = {
    operatorName match {
      case STATE_STORE_SAVE_EXEC_OP_NAME =>
        new StreamingAggregationStatePartitionKeyExtractor(stateKeySchema)
      case DEDUPLICATE_EXEC_OP_NAME =>
        new StreamingDeduplicateStatePartitionKeyExtractor(stateKeySchema)
      case DEDUPLICATE_WITHIN_WATERMARK_EXEC_OP_NAME =>
        new StreamingDedupWithinWatermarkStatePartitionKeyExtractor(stateKeySchema)
      case SESSION_WINDOW_STATE_STORE_SAVE_EXEC_OP_NAME =>
        new StreamingSessionWindowStatePartitionKeyExtractor(stateKeySchema)
      case SYMMETRIC_HASH_JOIN_EXEC_OP_NAME =>
        SymmetricHashJoinStateManager.createPartitionKeyExtractor(
          storeName, colFamilyName, stateKeySchema, stateFormatVersion.get)
      case fmg if FLAT_MAP_GROUPS_OP_NAMES.contains(fmg) =>
        new FlatMapGroupsWithStatePartitionKeyExtractor(stateKeySchema)
      case tws if TRANSFORM_WITH_STATE_OP_NAMES.contains(tws) =>
        require(stateVariableInfo.isDefined,
          "stateVariableInfo is required for TransformWithState")
        TransformWithStatePartitionKeyExtractorFactory.create(
          storeName, colFamilyName, stateKeySchema, stateVariableInfo.get)
      case _ => throw OfflineStateRepartitionErrors
        .unsupportedStatefulOperatorError(checkpointLocation = "", operatorName)
    }
  }
}
