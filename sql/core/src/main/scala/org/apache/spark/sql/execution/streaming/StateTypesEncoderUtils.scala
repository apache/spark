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
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema._
import org.apache.spark.sql.execution.streaming.state.StateStoreErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object TransformWithStateKeyValueRowSchema {
  /**
   * The following are the key/value row schema used in StateStore layer.
   * Key/value rows will be serialized into Binary format in `StateTypesEncoder`.
   * The "real" key/value row schema will be written into state schema metadata.
   */
  def valueRowSchemaWithTTL(valueRowSchema: StructType): StructType =
    valueRowSchema.add("ttlExpirationMs", LongType)

  /** Helper functions for passing the key/value schema to write to state schema metadata. */
  // Return value schema with additional TTL column if TTL is enabled.
  def getValueSchemaWithTTL(schema: StructType, hasTTL: Boolean): StructType = {
    val valSchema = if (hasTTL) {
      new StructType(schema.fields).add("ttlExpirationMs", LongType)
    } else schema
    new StructType()
      .add("value", valSchema)
  }

  // Given grouping key and user key schema, return the schema of the composite key.
  def getCompositeKeySchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType = {
    new StructType()
      .add("key", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))
  }

  // keyEncoder.schema will attach a default "value" as field name if
  // the object is a primitive type encoder
  private def isPrimitiveType(schema: StructType): Boolean = {
    schema.length == 1 && schema.fields.head.name == "value"
  }

  private def getPrimitiveType(schema: StructType): DataType = {
    assert(isPrimitiveType(schema))
    schema.fields.head.dataType
  }

  def singleKeyTTLRowSchema(keySchema: StructType): StructType =
    new StructType()
      .add("expirationMs", LongType)
      .add("groupingKey", keySchema)

  def compositeKeyTTLRowSchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType =
    new StructType()
      .add("expirationMs", LongType)
      .add("groupingKey", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))
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
 * @param keyEncoder - SQL encoder for the grouping key, key type is implicit
 * @param valEncoder - SQL encoder for value of type `S`
 * @param stateName - name of logical state partition
 * @tparam V - value type
 */
class StateTypesEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean) {

  /** Variables reused for value conversions between spark sql and object */
  private val keySerializer = keyEncoder.createSerializer()
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()
  private val valueTTLProjection =
    UnsafeProjection.create(valEncoder.schema.add("ttlExpirationMs", LongType))

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    keySerializer.apply(keyOption.get).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow with no ttl.
   */
  def encodeValue(value: V): UnsafeRow = {
    objToRowSerializer.apply(value).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow
   * with provided ttl expiration.
   */
  def encodeValue(value: V, expirationMs: Long): UnsafeRow = {
    val objRow: InternalRow = objToRowSerializer.apply(value)
    val newValArr: Array[Any] =
      objRow.toSeq(valEncoder.schema).toArray :+ expirationMs

    valueTTLProjection.apply(new GenericInternalRow(newValArr))
  }

  def decodeValue(row: UnsafeRow): V = {
    rowToObjDeserializer.apply(row)
  }

  /**
   * Decode the ttl information out of Value row. If the ttl has
   * not been set (-1L specifies no user defined value), the API will
   * return None.
   */
  def decodeTtlExpirationMs(row: UnsafeRow): Option[Long] = {
    // ensure ttl has been set
    assert(hasTtl)
    val expirationMs = row.getLong(valEncoder.schema.length)
    if (expirationMs == -1) {
      None
    } else {
      Some(expirationMs)
    }
  }

  def isExpired(row: UnsafeRow, batchTimestampMs: Long): Boolean = {
    val expirationMs = decodeTtlExpirationMs(row)
    println("I am inside isExpired, decoded expirationMs: " + expirationMs)
    expirationMs.exists(StateTTL.isExpired(_, batchTimestampMs))
  }
}

object StateTypesEncoder {
  def apply[V](
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[V],
      stateName: String,
      hasTtl: Boolean = false): StateTypesEncoder[V] = {
    new StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl)
  }
}

class CompositeKeyStateEncoder[K, V](
    keyEncoder: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean = false)
  extends StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl) {
  import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema._

  // keyEncoder.schema will attach a default "value" as field name if
  // the object is a primitive type encoder
  private def isPrimitiveType(schema: StructType): Boolean = {
    schema.length == 1 && schema.fields.head.name == "value"
  }

  private def getPrimitiveType(schema: StructType): DataType = {
    assert(isPrimitiveType(schema))
    schema.fields.head.dataType
  }

  private val schemaForCompositeKeyRow =
    getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
  private val compositeKeyProjection = UnsafeProjection.create(schemaForCompositeKeyRow)
  private val reusedKeyRow = new UnsafeRow(userKeyEnc.schema.fields.length)
  private val userKeyExpressionEnc = encoderFor(userKeyEnc)

  private val userKeyRowToObjDeserializer =
    userKeyExpressionEnc.resolveAndBind().createDeserializer()
  private val userKeySerializer = encoderFor(userKeyEnc).createSerializer()

  override def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val realGroupingKey =
      if (groupingKey.isInstanceOf[String]) UTF8String.fromString(groupingKey.asInstanceOf[String])
      else groupingKey
    val keyRow = new GenericInternalRow(Array[Any](realGroupingKey))

    val keyProj = UnsafeProjection.create(new StructType().add("key", keyEncoder.schema))

    keyProj.apply(InternalRow(keyRow))
  }

  def encodeUserKey(userKey: K): UnsafeRow = {
    val realKey =
      if (userKey.isInstanceOf[String]) UTF8String.fromString(userKey.asInstanceOf[String])
      else userKey
    val keyRow = new GenericInternalRow(Array[Any](realKey))

    val keyProj = UnsafeProjection.create(new StructType().add("userKey", keyEncoder.schema))

    keyProj.apply(InternalRow(keyRow))
  }


  /**
   * Grouping key and user key are encoded as a row of `schemaForCompositeKeyRow` schema.
   * Grouping key will be encoded in `RocksDBStateEncoder` as the prefix column.
   */
  def encodeCompositeKey(userKey: K): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val realGroupingKey =
      if (groupingKey.isInstanceOf[String]) UTF8String.fromString(groupingKey.asInstanceOf[String])
      else groupingKey
    val realUserKey =
      if (userKey.isInstanceOf[String]) UTF8String.fromString(userKey.asInstanceOf[String])
      else userKey
    println("I am inside encode Composite key, composite key schema: " +
      schemaForCompositeKeyRow)

    // Create the nested InternalRows for the inner structs (value)
    val keyRow = new GenericInternalRow(Array[Any](realGroupingKey))
    val userKeyRow = new GenericInternalRow(Array[Any](realUserKey))

    // Create the final InternalRow combining the nested keyRow and userKeyRow
    val compositeRow = new GenericInternalRow(Array[Any](keyRow, userKeyRow))

    val compositeKey = compositeKeyProjection(compositeRow)
    println("I am encoding unsafe row, after encode: " + decodeCompositeKey(compositeKey))
    println("inside encode composite key, num of composite field: " + compositeKey.numFields())

    val decode = compositeKey.getString(0)
    println("inside encode composite key, first field: " + decode)

    compositeKey
  }

  def decodeUserKey(row: UnsafeRow): K = {
    /*
    println("I am inside decodeUserKeyFromTTLRow, row.getStruct: " +
    row.getStruct(0, userKeyEnc.schema.length).getString(0).length)
    println("I am inside decodeUserKeyFromTTLRow, row.numfileds: " +
      row.numFields())
    println("I am inside decodeUserKeyFromTTLRow, row.numFields: " +
      row.getStruct(0, userKeyEnc.schema.length).numFields())
    println("I am inside decodeUserKeyFromTTLRow, after decoding: " +
      userKeyRowToObjDeserializer.apply(row.getStruct(0, userKeyEnc.schema.length))
        .asInstanceOf[String].length)
    println("I am inside decodeUserKeyFromTTLRow, schema: " +
      userKeyExpressionEnc.schema)
    println("I am inside decodeUserKeyFromTTLRow, composite schema: " +
      schemaForCompositeKeyRow) */
    userKeyRowToObjDeserializer.apply(row)
  }

  /**
   * The input row is of composite Key schema.
   * Only user key is returned though grouping key also exist in the row.
   */
  def decodeCompositeKey(row: UnsafeRow): K = {
    val userKey = userKeyRowToObjDeserializer.apply(row.getStruct(1, 1))
    println("I am inside decodeCompositeKey, after decode: " + userKey)
    println("I am inside decodeCompositeKey, after length: " +
      userKey.asInstanceOf[String].length)
    userKey
  }
}

class SingleKeyTTLEncoder(
  keyExprEnc: ExpressionEncoder[Any]) {

  private val TTLKeySchema = singleKeyTTLRowSchema(keyExprEnc.schema)
  def encodeTTLRow(expirationMs: Long, groupingKey: UnsafeRow): UnsafeRow = {
    // unsafeRow -> unsafeRow
    UnsafeProjection.create(TTLKeySchema).apply(
      InternalRow(expirationMs, groupingKey.asInstanceOf[InternalRow]))
  }
}

class CompositeKeyTTLEncoder[K](
  keyExprEnc: ExpressionEncoder[Any],
  userKeyEnc: Encoder[K]) {

  private val TTLKeySchema = compositeKeyTTLRowSchema(
    keyExprEnc.schema, userKeyEnc.schema)
  def encodeTTLRow(expirationMs: Long,
                   groupingKey: UnsafeRow,
                   userKey: UnsafeRow): UnsafeRow = {
    // unsafeRow -> unsafeRow
    println("I am inside encodeTTLRow, TTLKeySchema: " + TTLKeySchema)
    UnsafeProjection.create(TTLKeySchema).apply(
      new GenericInternalRow(
        Array[Any](expirationMs,
        groupingKey.getStruct(0, 1).asInstanceOf[InternalRow],
        userKey.getStruct(0, 1).asInstanceOf[InternalRow])))
  }
}
