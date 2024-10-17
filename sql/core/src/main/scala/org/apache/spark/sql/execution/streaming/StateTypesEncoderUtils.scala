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

import java.io.ByteArrayOutputStream

import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.core.avro.SchemaConverters
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{AvroSerde, StateStoreErrors}
import org.apache.spark.sql.types._

/**
 * Helper object for getting schema of key/value row that are used in state schema
 * files and to be passed into `RocksDBStateKey(/Value)Encoder`.
 */
object TransformWithStateKeyValueRowSchemaUtils {
  def getCompositeKeySchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType = {
    new StructType()
      .add("key", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))
  }

  def getSingleKeyTTLRowSchema(keySchema: StructType): StructType =
    new StructType()
      .add("expirationMs", LongType)
      .add("groupingKey", keySchema)

  def getCompositeKeyTTLRowSchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType =
    new StructType()
      .add("expirationMs", LongType)
      .add("groupingKey", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))

  def getValueSchemaWithTTL(schema: StructType, hasTTL: Boolean): StructType = {
    if (hasTTL) {
      new StructType().add("value", schema)
        .add("ttlExpirationMs", LongType)
    } else schema
  }
}

trait StateTypesEncoder[V, S] {
  def encodeGroupingKey(): S

  def encodeValue(value: V): S

  def decodeValue(row: S): V

  def encodeValue(value: V, expirationMs: Long): S


  def decodeTtlExpirationMs(row: S): Option[Long]

  def isExpired(row: S, batchTimestampMs: Long): Boolean
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
class UnsafeRowTypesEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean) extends StateTypesEncoder[V, UnsafeRow] {

  /** Variables reused for value conversions between spark sql and object */
  private val keySerializer = keyEncoder.createSerializer()
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()
  // This variable is only used when has Ttl is true
  private val valueTTLProjection =
    UnsafeProjection.create(getValueSchemaWithTTL(valEncoder.schema, true))

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
    valueTTLProjection.apply(InternalRow(objRow, expirationMs))
  }

  def decodeValue(row: UnsafeRow): V = {
    if (hasTtl) {
      rowToObjDeserializer.apply(row.getStruct(0, valEncoder.schema.length))
    } else rowToObjDeserializer.apply(row)
  }

  /**
   * Decode the ttl information out of Value row. If the ttl has
   * not been set (-1L specifies no user defined value), the API will
   * return None.
   */
  def decodeTtlExpirationMs(row: UnsafeRow): Option[Long] = {
    // ensure ttl has been set
    assert(hasTtl)
    val expirationMs = row.getLong(1)
    if (expirationMs == -1) {
      None
    } else {
      Some(expirationMs)
    }
  }

  def isExpired(row: UnsafeRow, batchTimestampMs: Long): Boolean = {
    val expirationMs = decodeTtlExpirationMs(row)
    expirationMs.exists(StateTTL.isExpired(_, batchTimestampMs))
  }
}


object UnsafeRowTypesEncoder {
  def apply[V](
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[V],
      stateName: String,
      hasTtl: Boolean = false): UnsafeRowTypesEncoder[V] = {
    new UnsafeRowTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl)
  }
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
class AvroTypesEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean,
    avroSerde: Option[AvroSerde]) extends StateTypesEncoder[V, Array[Byte]] {

  val out = new ByteArrayOutputStream

  /** Variables reused for value conversions between spark sql and object */
  private val keySerializer = keyEncoder.createSerializer()
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()

  private val keySchema = keyEncoder.schema
  private val keyAvroType = SchemaConverters.toAvroType(keySchema)

  // case class -> dataType
  private val valSchema: StructType = valEncoder.schema
  // dataType -> avroType
  private val valueAvroType = SchemaConverters.toAvroType(valSchema)

  override def encodeGroupingKey(): Array[Byte] = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    val keyRow = keySerializer.apply(keyOption.get).copy() // V -> InternalRow
    val avroData = avroSerde.get.keySerializer.serialize(keyRow) // InternalRow -> GenericDataRecord

    out.reset()
    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val writer = new GenericDatumWriter[Any](keyAvroType)

    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  override def encodeValue(value: V): Array[Byte] = {
    val objRow: InternalRow = objToRowSerializer.apply(value).copy() // V -> InternalRow
    val avroData =
      avroSerde.get.valueSerializer.serialize(objRow) // InternalRow -> GenericDataRecord
    out.reset()

    val encoder = EncoderFactory.get().directBinaryEncoder(out, null)
    val writer = new GenericDatumWriter[Any](
      valueAvroType) // Defining Avro writer for this struct type

    writer.write(avroData, encoder) // GenericDataRecord -> bytes
    encoder.flush()
    out.toByteArray
  }

  override def decodeValue(row: Array[Byte]): V = {
    val reader = new GenericDatumReader[Any](valueAvroType)
    val decoder = DecoderFactory.get().binaryDecoder(row, 0, row.length, null)
    val genericData = reader.read(null, decoder) // bytes -> GenericDataRecord
    val internalRow = avroSerde.get.valueDeserializer.deserialize(
      genericData).orNull.asInstanceOf[InternalRow] // GenericDataRecord -> InternalRow
    if (hasTtl) {
      rowToObjDeserializer.apply(internalRow.getStruct(0, valEncoder.schema.length))
    } else rowToObjDeserializer.apply(internalRow)
  }

  override def encodeValue(value: V, expirationMs: Long): Array[Byte] = {
    throw new UnsupportedOperationException
  }

  override def decodeTtlExpirationMs(row: Array[Byte]): Option[Long] = {
    throw new UnsupportedOperationException
  }

  override def isExpired(row: Array[Byte], batchTimestampMs: Long): Boolean = {
    throw new UnsupportedOperationException
  }
}

class CompositeKeyUnsafeRowEncoder[K, V](
    keyEncoder: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean = false)
  extends UnsafeRowTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl) {
  import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._

  /** Encoders */
  private val userKeyExpressionEnc = encoderFor(userKeyEnc)

  /** Schema */
  private val schemaForGroupingKeyRow = new StructType().add("key", keyEncoder.schema)
  private val schemaForUserKeyRow = new StructType().add("userKey", userKeyEnc.schema)
  private val schemaForCompositeKeyRow =
    getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)

  /** Projection */
  private val userKeyProjection = UnsafeProjection.create(schemaForUserKeyRow)
  private val groupingKeyProjection = UnsafeProjection.create(schemaForGroupingKeyRow)
  private val compositeKeyProjection = UnsafeProjection.create(schemaForCompositeKeyRow)

  /** Serializer */
  private val groupingKeySerializer = keyEncoder.createSerializer()
  private val userKeySerializer = userKeyExpressionEnc.createSerializer()

  /** Deserializer */
  private val userKeyRowToObjDeserializer =
    userKeyExpressionEnc.resolveAndBind().createDeserializer()

  override def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get
    val groupingKeyRow = groupingKeySerializer.apply(groupingKey)

    // Create the final unsafeRow mapping column name "key" to the keyRow
    groupingKeyProjection(InternalRow(groupingKeyRow))
  }

  def encodeUserKey(userKey: K): UnsafeRow = {
    val userKeyRow = userKeySerializer.apply(userKey)

    // Create the final unsafeRow mapping column name "userKey" to the userKeyRow
    userKeyProjection(InternalRow(userKeyRow))
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

    val keyRow = groupingKeySerializer.apply(groupingKey)
    val userKeyRow = userKeySerializer.apply(userKey)

    // Create the final unsafeRow combining the keyRow and userKeyRow
    compositeKeyProjection(InternalRow(keyRow, userKeyRow))
  }

  def encodeCompositeKey(groupingKey: UnsafeRow, userKey: UnsafeRow): UnsafeRow = {
    compositeKeyProjection(InternalRow(groupingKey, userKey))
  }

  def decodeUserKey(row: UnsafeRow): K = {
    userKeyRowToObjDeserializer.apply(row)
  }

  /**
   * The input row is of composite Key schema.
   * Only user key is returned though grouping key also exist in the row.
   */
  def decodeCompositeKey(row: UnsafeRow): K = {
    userKeyRowToObjDeserializer.apply(row.getStruct(1, userKeyEnc.schema.length))
  }
}

/** Class for TTL with single key serialization */
class SingleKeyTTLEncoder(
    keyExprEnc: ExpressionEncoder[Any]) {

  private val ttlKeyProjection = UnsafeProjection.create(
    getSingleKeyTTLRowSchema(keyExprEnc.schema))

  def encodeTTLRow(expirationMs: Long, groupingKey: UnsafeRow): UnsafeRow = {
    ttlKeyProjection.apply(
      InternalRow(expirationMs, groupingKey.asInstanceOf[InternalRow]))
  }
}

/** Class for TTL with composite key serialization */
class CompositeKeyTTLEncoder[K](
    keyExprEnc: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K]) {

  private val ttlKeyProjection = UnsafeProjection.create(
    getCompositeKeyTTLRowSchema(keyExprEnc.schema, userKeyEnc.schema))

  def encodeTTLRow(
      expirationMs: Long,
      groupingKey: UnsafeRow,
      userKey: UnsafeRow): UnsafeRow = {
    ttlKeyProjection.apply(
      InternalRow(
        expirationMs,
        groupingKey.getStruct(0, keyExprEnc.schema.length)
          .asInstanceOf[InternalRow],
        userKey.getStruct(0, userKeyEnc.schema.length)
          .asInstanceOf[InternalRow]))
  }
}

/** Class for timer state serialization */
class TimerKeyEncoder(keyExprEnc: ExpressionEncoder[Any]) {
  private val schemaForPrefixKey: StructType =
    new StructType()
      .add("key", new StructType(keyExprEnc.schema.fields))

  val keySchemaForSecIndex: StructType =
    new StructType()
      .add("expiryTimestampMs", LongType, nullable = false)
      .add("key", new StructType(keyExprEnc.schema.fields))

  val schemaForKeyRow: StructType = new StructType()
    .add("key", new StructType(keyExprEnc.schema.fields))
    .add("expiryTimestampMs", LongType, nullable = false)

  val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  private val keySerializer = keyExprEnc.createSerializer()
  private val keyDeserializer = keyExprEnc.resolveAndBind().createDeserializer()
  private val prefixKeyProjection = UnsafeProjection.create(schemaForPrefixKey)
  private val keyRowProjection = UnsafeProjection.create(schemaForKeyRow)
  private val secIndexKeyProjection = UnsafeProjection.create(keySchemaForSecIndex)

  def encodedKey(groupingKey: Any, expiryTimestampMs: Long): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    keyRowProjection.apply(InternalRow(keyRow, expiryTimestampMs))
  }

  def encodeSecIndexKey(groupingKey: Any, expiryTimestampMs: Long): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    secIndexKeyProjection.apply(InternalRow(expiryTimestampMs, keyRow))
  }

  def encodePrefixKey(groupingKey: Any): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    prefixKeyProjection.apply(InternalRow(keyRow))
  }

  def decodePrefixKey(retUnsafeRow: UnsafeRow): Any = {
    keyDeserializer.apply(retUnsafeRow)
  }
}
