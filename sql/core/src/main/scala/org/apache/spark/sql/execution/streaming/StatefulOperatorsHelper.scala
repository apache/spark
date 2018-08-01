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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.execution.streaming.state.{StateStore, UnsafeRowPair}
import org.apache.spark.sql.types.StructType

object StatefulOperatorsHelper {

  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  sealed trait StreamingAggregationStateManager extends Serializable {
    def getKey(row: InternalRow): UnsafeRow
    def getStateValueSchema: StructType
    def restoreOriginRow(rowPair: UnsafeRowPair): UnsafeRow
    def get(store: StateStore, key: UnsafeRow): UnsafeRow
    def put(store: StateStore, row: UnsafeRow): Unit
  }

  object StreamingAggregationStateManager extends Logging {
    def createStateManager(
        keyExpressions: Seq[Attribute],
        inputRowAttributes: Seq[Attribute],
        stateFormatVersion: Int): StreamingAggregationStateManager = {
      stateFormatVersion match {
        case 1 => new StreamingAggregationStateManagerImplV1(keyExpressions, inputRowAttributes)
        case 2 => new StreamingAggregationStateManagerImplV2(keyExpressions, inputRowAttributes)
        case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
      }
    }
  }

  abstract class StreamingAggregationStateManagerBaseImpl(
      protected val keyExpressions: Seq[Attribute],
      protected val inputRowAttributes: Seq[Attribute]) extends StreamingAggregationStateManager {

    @transient protected lazy val keyProjector =
      GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

    def getKey(row: InternalRow): UnsafeRow = keyProjector(row)
  }

  class StreamingAggregationStateManagerImplV1(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute])
    extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

    override def getStateValueSchema: StructType = inputRowAttributes.toStructType

    override def restoreOriginRow(rowPair: UnsafeRowPair): UnsafeRow = {
      rowPair.value
    }

    override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
      store.get(key)
    }

    override def put(store: StateStore, row: UnsafeRow): Unit = {
      store.put(getKey(row), row)
    }
  }

  class StreamingAggregationStateManagerImplV2(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute])
    extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

    private val valueExpressions: Seq[Attribute] = inputRowAttributes.diff(keyExpressions)
    private val keyValueJoinedExpressions: Seq[Attribute] = keyExpressions ++ valueExpressions
    private val needToProjectToRestoreValue: Boolean =
      keyValueJoinedExpressions != inputRowAttributes

    @transient private lazy val valueProjector =
      GenerateUnsafeProjection.generate(valueExpressions, inputRowAttributes)

    @transient private lazy val joiner =
      GenerateUnsafeRowJoiner.create(StructType.fromAttributes(keyExpressions),
      StructType.fromAttributes(valueExpressions))
    @transient private lazy val restoreValueProjector = GenerateUnsafeProjection.generate(
      keyValueJoinedExpressions, inputRowAttributes)

    override def getStateValueSchema: StructType = valueExpressions.toStructType

    override def restoreOriginRow(rowPair: UnsafeRowPair): UnsafeRow = {
      val joinedRow = joiner.join(rowPair.key, rowPair.value)
      if (needToProjectToRestoreValue) {
        restoreValueProjector(joinedRow)
      } else {
        joinedRow
      }
    }

    override def get(store: StateStore, key: UnsafeRow): UnsafeRow = {
      val savedState = store.get(key)
      if (savedState == null) {
        return savedState
      }

      val joinedRow = joiner.join(key, savedState)
      if (needToProjectToRestoreValue) {
        restoreValueProjector(joinedRow)
      } else {
        joinedRow
      }
    }

    override def put(store: StateStore, row: UnsafeRow): Unit = {
      val key = keyProjector(row)
      val value = valueProjector(row)
      store.put(key, value)
    }
  }

}
