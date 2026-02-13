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

package org.apache.spark.sql.execution.streaming.state

import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types.StructType

/**
 * Trait for extracting partition keys from state key rows.
 * The partition key is the key used by shuffle.
 * This is used for offline state repartitioning.
 */
trait StatePartitionKeyExtractor {
  /**
   * Returns the schema of the partition key.
   */
  def partitionKeySchema: StructType

  /**
   * Extracts the partition key row from the given state key row.
   *
   * @note Depending on the implementation, it might not be safe to buffer the
   *       returned UnsafeRow across multiple calls of this method, due to UnsafeRow re-use.
   *       If you are holding on to the row between multiple calls, you should copy the row.
   */
  def partitionKey(stateKeyRow: UnsafeRow): UnsafeRow
}

/**
 * No-op state partition key extractor that returns the state key row as the partition key row.
 * This is used by operators that use the partition key as the state key.
 *
 * @param stateKeySchema The schema of the state key row
 */
class NoopStatePartitionKeyExtractor(stateKeySchema: StructType)
  extends StatePartitionKeyExtractor {
  override lazy val partitionKeySchema: StructType = stateKeySchema

  override def partitionKey(stateKeyRow: UnsafeRow): UnsafeRow = stateKeyRow
}

/**
 * State partition key extractor that returns the field at the specified index
 * of the state key row as the partition key row.
 *
 * @param stateKeySchema The schema of the state key row
 * @param partitionKeyIndex The index of the field to extract as the partition key
 */
class IndexBasedStatePartitionKeyExtractor(stateKeySchema: StructType, partitionKeyIndex: Int)
  extends StatePartitionKeyExtractor {
  override lazy val partitionKeySchema: StructType =
    stateKeySchema.fields(partitionKeyIndex).dataType.asInstanceOf[StructType]

  override def partitionKey(stateKeyRow: UnsafeRow): UnsafeRow = {
    stateKeyRow.getStruct(partitionKeyIndex, partitionKeySchema.length)
  }
}

/**
 * State partition key extractor that drops the last N fields of the state key row
 * and returns the remaining fields as the partition key row.
 *
 * @param stateKeySchema The schema of the state key row
 * @param numLastColsToDrop The number of last columns to drop in the state key
 */
class DropLastNFieldsStatePartitionKeyExtractor(stateKeySchema: StructType, numLastColsToDrop: Int)
  extends StatePartitionKeyExtractor {
  override lazy val partitionKeySchema: StructType = {
    require(numLastColsToDrop < stateKeySchema.length,
      s"numLastColsToDrop: $numLastColsToDrop must be less than the number of fields in the " +
        s"state key schema: ${stateKeySchema.length}, to avoid empty partition key schema")
    StructType(stateKeySchema.dropRight(numLastColsToDrop))
  }

  private lazy val partitionKeyExpr: Array[BoundReference] =
    partitionKeySchema.fields.zipWithIndex.map { case (field, index) =>
      BoundReference(index, field.dataType, field.nullable)
    }

  private lazy val partitionKeyProjection: UnsafeProjection = UnsafeProjection.create(
    ArraySeq.unsafeWrapArray(partitionKeyExpr),
    toAttributes(stateKeySchema))

  override def partitionKey(stateKeyRow: UnsafeRow): UnsafeRow = {
    partitionKeyProjection(stateKeyRow)
  }
}
