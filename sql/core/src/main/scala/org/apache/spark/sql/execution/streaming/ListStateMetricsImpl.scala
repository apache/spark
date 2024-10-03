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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateStore}
import org.apache.spark.sql.types._

/**
 * Trait that provides helper methods to maintain metrics for a list state.
 * For list state, we keep track of the count of entries in the list in a separate column family
 * to get an accurate view of the number of entries that are updated/removed from the list and
 * reported as part of the query progress metrics.
 */
trait ListStateMetricsImpl {
  def stateStore: StateStore

  def baseStateName: String

  def exprEncSchema: StructType

  // We keep track of the count of entries in the list in a separate column family
  // to avoid scanning the entire list to get the count.
  private val counterCFValueSchema: StructType =
    StructType(Seq(StructField("count", LongType, nullable = false)))

  private val counterCFProjection = UnsafeProjection.create(counterCFValueSchema)

  private def getRowCounterCFName(stateName: String) = "$rowCounter_" + stateName

  stateStore.createColFamilyIfAbsent(getRowCounterCFName(baseStateName), exprEncSchema,
    counterCFValueSchema, NoPrefixKeyStateEncoderSpec(exprEncSchema), isInternal = true)

  /**
   * Function to get the number of entries in the list state for a given grouping key
   * @param encodedKey - encoded grouping key
   * @return - number of entries in the list state
   */
  def getEntryCount(encodedKey: UnsafeRow): Long = {
    val countRow = stateStore.get(encodedKey, getRowCounterCFName(baseStateName))
    if (countRow != null) {
      countRow.getLong(0)
    } else {
      0L
    }
  }

  /**
   * Function to update the number of entries in the list state for a given grouping key
   * @param encodedKey - encoded grouping key
   * @param updatedCount - updated count of entries in the list state
   */
  def updateEntryCount(
      encodedKey: UnsafeRow,
      updatedCount: Long): Unit = {
    val updatedCountRow = new GenericInternalRow(1)
    updatedCountRow.setLong(0, updatedCount)
    stateStore.put(encodedKey,
      counterCFProjection(updatedCountRow.asInstanceOf[InternalRow]),
      getRowCounterCFName(baseStateName))
  }

  /**
   * Function to remove the number of entries in the list state for a given grouping key
   * @param encodedKey - encoded grouping key
   */
  def removeEntryCount(encodedKey: UnsafeRow): Unit = {
    stateStore.remove(encodedKey, getRowCounterCFName(baseStateName))
  }
}
