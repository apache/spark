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

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types.{BinaryType, StructType}

object StateEncoder {
  private val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)
  private val keyEncoder = UnsafeProjection.create(schemaForKeyRow)

  private val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
  private val valueEncoder = UnsafeProjection.create(schemaForValueRow)

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeKey(stateName: String, keyExprEnc: ExpressionEncoder[Any]): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (!keyOption.isDefined) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    val keySerializer = keyExprEnc.createSerializer()
    val keyByteArr = keySerializer.apply(keyOption.get).asInstanceOf[UnsafeRow].getBytes()
    val keyRow = keyEncoder(InternalRow(keyByteArr))
    keyRow
  }

  def encodeUserKey[K](userKey: K): UnsafeRow = {
    val schemaForKeyRow: StructType = new StructType().add("userKey", BinaryType)
    val keyByteArr = SerializationUtils.serialize(userKey.asInstanceOf[Serializable])
    val keyEncoder = UnsafeProjection.create(schemaForKeyRow)
    val keyRow = keyEncoder(InternalRow(keyByteArr))
    keyRow
  }

  def encodeValue[S] (value: S): UnsafeRow = {
    val schemaForValueRow: StructType = new StructType().add("value", BinaryType)
    val valueByteArr = SerializationUtils.serialize(value.asInstanceOf[Serializable])
    val valueEncoder = UnsafeProjection.create(schemaForValueRow)
    val valueRow = valueEncoder(InternalRow(valueByteArr))
    valueRow
  }

  def decode[S](row: UnsafeRow): S = {
    SerializationUtils
      .deserialize(row.getBinary(0))
      .asInstanceOf[S]
  }
}
