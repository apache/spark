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
package org.apache.spark.sql.execution.datasources.v2.state

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.v2.state.StateSchemaExtractor.{StateKind, StateSchemaInfo}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * This class enables extracting state schema and its format version via analyzing
 * the streaming query. The query should have its state operators but it should exclude sink(s).
 *
 * Note that it only returns which can be extracted by this class, so number of state
 * in given query may not be same as returned number of schema information.
 */
class StateSchemaExtractor(spark: SparkSession) extends Logging {

  def extract(query: DataFrame): Seq[StateSchemaInfo] = {
    require(query.isStreaming, "Given query is not a streaming query!")

    val queryExecution = new IncrementalExecution(spark, query.logicalPlan,
      OutputMode.Update(), "<unknown>", UUID.randomUUID(), UUID.randomUUID(),
      0, OffsetSeqMetadata())

    // TODO: handle Streaming Join (if possible), etc.
    queryExecution.executedPlan.collect {
      case store: StateStoreSaveExec =>
        val stateFormatVersion = store.stateFormatVersion
        val keySchema = store.keyExpressions.toStructType
        val valueSchema = store.stateManager.getStateValueSchema
        store.stateInfo match {
          case Some(stInfo) =>
            val operatorId = stInfo.operatorId
            StateSchemaInfo(operatorId, StateKind.StreamingAggregation,
              stateFormatVersion, keySchema, valueSchema)

          case None => throw new IllegalStateException("State information not set!")
        }

      case store: FlatMapGroupsWithStateExec =>
        val stateFormatVersion = store.stateFormatVersion
        val keySchema = store.groupingAttributes.toStructType
        val valueSchema = store.stateManager.stateSchema
        store.stateInfo match {
          case Some(stInfo) =>
            val operatorId = stInfo.operatorId
            StateSchemaInfo(operatorId, StateKind.FlatMapGroupsWithState,
              stateFormatVersion, keySchema, valueSchema)

          case None => throw new IllegalStateException("State information not set!")
        }
    }
  }

}

object StateSchemaExtractor {
  object StateKind extends Enumeration {
    val StreamingAggregation, StreamingJoin, FlatMapGroupsWithState = Value
  }

  case class StateSchemaInfo(
      opId: Long,
      stateKind: StateKind.Value,
      formatVersion: Int,
      keySchema: StructType,
      valueSchema: StructType)
}
