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

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.Serializer
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types.{BinaryType, StructType}

object StateKeyValueRowSchema {
  val KEY_ROW_SCHEMA: StructType = new StructType().add("key", BinaryType)
  val VALUE_ROW_SCHEMA: StructType = new StructType().add("value", BinaryType)
}

/**
 * Helper class providing APIs to encode the grouping key, and user provided values
 * to Spark [[UnsafeRow]].
 *
 * CAUTION: StateTypesEncoder class instance is *not* thread-safe.
 * This class reuses the keyProjection and valueProjection for encoding grouping
 * key and state value respectively. As UnsafeProjection is not thread safe, this
 * class is also not thread safe.
 *
 * @param keySerializer - serializer to serialize the grouping key of type `GK`
 *     to an [[InternalRow]]
 * @param valEncoder - SQL encoder for value of type `S`
 * @param stateName - name of logical state partition
 * @tparam GK - grouping key type
 * @tparam V - value type
 */
class StateTypesEncoder[GK, V](
    keySerializer: Serializer[GK],
    valEncoder: Encoder[V],
    stateName: String) {
  import org.apache.spark.sql.execution.streaming.StateKeyValueRowSchema._

  /** Variables reused for conversions between byte array and UnsafeRow */
  private val keyProjection = UnsafeProjection.create(KEY_ROW_SCHEMA)
  private val valueProjection = UnsafeProjection.create(VALUE_ROW_SCHEMA)

  /** Variables reused for value conversions between spark sql and object */
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()
  private val reusedValRow = new UnsafeRow(valEncoder.schema.fields.length)

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    val groupingKey = keyOption.get.asInstanceOf[GK]
    val keyByteArr = keySerializer.apply(groupingKey).asInstanceOf[UnsafeRow].getBytes()
    val keyRow = keyProjection(InternalRow(keyByteArr))
    keyRow
  }

  def encodeValue(value: V): UnsafeRow = {
    val objRow: InternalRow = objToRowSerializer.apply(value)
    val bytes = objRow.asInstanceOf[UnsafeRow].getBytes()
    val valRow = valueProjection(InternalRow(bytes))
    valRow
  }

  def decodeValue(row: UnsafeRow): V = {
    val bytes = row.getBinary(0)
    reusedValRow.pointTo(bytes, bytes.length)
    val value = rowToObjDeserializer.apply(reusedValRow)
    value
  }
}

object StateTypesEncoder {
  def apply[GK, V](
      keySerializer: Serializer[GK],
      valEncoder: Encoder[V],
      stateName: String): StateTypesEncoder[GK, V] = {
    new StateTypesEncoder[GK, V](keySerializer, valEncoder, stateName)
  }
}

class CompositeKeyStateEncoder[GK, K, V](
    keySerializer: Serializer[GK],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    schemaForCompositeKeyRow: StructType,
    stateName: String)
  extends StateTypesEncoder[GK, V](keySerializer, valEncoder, stateName) {

  private val compositeKeyProjection = UnsafeProjection.create(schemaForCompositeKeyRow)
  private val reusedKeyRow = new UnsafeRow(userKeyEnc.schema.fields.length)
  private val userKeyExpressionEnc = encoderFor(userKeyEnc)

  private val userKeyRowToObjDeserializer =
    userKeyExpressionEnc.resolveAndBind().createDeserializer()
  private val userKeySerializer = encoderFor(userKeyEnc).createSerializer()

  /**
   * Grouping key and user key are encoded as a row of `schemaForCompositeKeyRow` schema.
   * Grouping key will be encoded in `RocksDBStateEncoder` as the prefix column.
   */
  def encodeCompositeKey(userKey: K): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get.asInstanceOf[GK]
    // generate grouping key byte array
    val groupingKeyByteArr = keySerializer.apply(groupingKey).asInstanceOf[UnsafeRow].getBytes()
    // generate user key byte array
    val userKeyBytesArr = userKeySerializer.apply(userKey).asInstanceOf[UnsafeRow].getBytes()

    val compositeKeyRow = compositeKeyProjection(InternalRow(groupingKeyByteArr, userKeyBytesArr))
    compositeKeyRow
  }

  /**
   * The input row is of composite Key schema.
   * Only user key is returned though grouping key also exist in the row.
   */
  def decodeCompositeKey(row: UnsafeRow): K = {
    val bytes = row.getBinary(1)
    reusedKeyRow.pointTo(bytes, bytes.length)
    val userKey = userKeyRowToObjDeserializer.apply(reusedKeyRow)
    userKey
  }
}
