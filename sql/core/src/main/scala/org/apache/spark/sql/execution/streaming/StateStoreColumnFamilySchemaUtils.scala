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

import scala.collection.mutable

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.TransformWithStateVariableUtils.getRowCounterCFName
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStoreColFamilySchema}
import org.apache.spark.sql.types._

object StateStoreColumnFamilySchemaUtils {

  /**
   * Avro uses zig-zag encoding for some fixed-length types, like Longs and Ints. For range scans
   * we want to use big-endian encoding, so we need to convert the source schema to replace these
   * types with BinaryType.
   *
   * @param schema The schema to convert
   * @param ordinals If non-empty, only convert fields at these ordinals.
   *                 If empty, convert all fields.
   */
  def convertForRangeScan(schema: StructType, ordinals: Seq[Int] = Seq.empty): StructType = {
    val ordinalSet = ordinals.toSet

    StructType(schema.fields.zipWithIndex.flatMap { case (field, idx) =>
      if ((ordinals.isEmpty || ordinalSet.contains(idx)) && isFixedSize(field.dataType)) {
        // For each numeric field, create two fields:
        // 1. Byte marker for null, positive, or negative values
        // 2. The original numeric value in big-endian format
        // Byte type is converted to Int in Avro, which doesn't work for us as Avro
        // uses zig-zag encoding as opposed to big-endian for Ints
        Seq(
          StructField(s"${field.name}_marker", BinaryType, nullable = false),
          field.copy(name = s"${field.name}_value", BinaryType)
        )
      } else {
        Seq(field)
      }
    })
  }

  private def isFixedSize(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: BooleanType | _: ShortType | _: IntegerType | _: LongType |
         _: FloatType | _: DoubleType => true
    case _ => false
  }

  def getTtlColFamilyName(stateName: String): String = {
    "$ttl_" + stateName
  }

  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): Map[String, StateStoreColFamilySchema] = {
    val schemas = mutable.Map[String, StateStoreColFamilySchema]()

    // Add main value state schema
    schemas.put(stateName, StateStoreColFamilySchema(
      stateName,
      keySchemaId = 0,
      keyEncoder.schema,
      valueSchemaId = 0,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema))))

    // Add TTL index if needed
    if (hasTtl) {
      val ttlIndexSchema = StateStoreColFamilySchema(
        getTtlColFamilyName(stateName),
        keySchemaId = 0,
        getTTLRowKeySchema(keyEncoder.schema),
        valueSchemaId = 0,
        StructType(Array(StructField("__empty__", NullType))),
        Some(RangeKeyScanStateEncoderSpec(getTTLRowKeySchema(keyEncoder.schema), Seq(0))))
      schemas.put(ttlIndexSchema.colFamilyName, ttlIndexSchema)
    }

    schemas.toMap
  }

  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): Map[String, StateStoreColFamilySchema] = {
    val schemas = mutable.Map[String, StateStoreColFamilySchema]()

    // Add main list state schema
    schemas.put(stateName, StateStoreColFamilySchema(
      stateName,
      keySchemaId = 0,
      keyEncoder.schema,
      valueSchemaId = 0,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema))))
    // Add row counter schema
    val counterSchema = StateStoreColFamilySchema(
      getRowCounterCFName(stateName), keySchemaId = 0,
      keyEncoder.schema,
      valueSchemaId = 0,
      StructType(Seq(StructField("count", LongType, nullable = false))),
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)))
    schemas.put(counterSchema.colFamilyName, counterSchema)

    // Add TTL-related schemas if needed
    if (hasTtl) {
      // TTL index
      val ttlIndexSchema = StateStoreColFamilySchema(
        getTtlColFamilyName(stateName),
        keySchemaId = 0,
        getTTLRowKeySchema(keyEncoder.schema),
        valueSchemaId = 0,
        StructType(Array(StructField("__empty__", NullType))),
        Some(RangeKeyScanStateEncoderSpec(getTTLRowKeySchema(keyEncoder.schema), Seq(0))))
      schemas.put(ttlIndexSchema.colFamilyName, ttlIndexSchema)

      // Min expiry index
      val minIndexSchema = StateStoreColFamilySchema(
        s"$$min_$stateName",
        keySchemaId = 0,
        keyEncoder.schema,
        valueSchemaId = 0,
        getExpirationMsRowSchema(),
        Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)))
      schemas.put(minIndexSchema.colFamilyName, minIndexSchema)

      // Count index
      val countSchema = StateStoreColFamilySchema(
        s"$$count_$stateName",
        keySchemaId = 0,
        keyEncoder.schema,
        valueSchemaId = 0,
        StructType(Seq(StructField("count", LongType, nullable = false))),
        Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)))
      schemas.put(countSchema.colFamilyName, countSchema)
    }

    schemas.toMap
  }

  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): Map[String, StateStoreColFamilySchema] = {
    val schemas = mutable.Map[String, StateStoreColFamilySchema]()
    val compositeKeySchema = getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)

    // Add main map state schema
    schemas.put(stateName, StateStoreColFamilySchema(
      stateName,
      keySchemaId = 0,
      compositeKeySchema,
      valueSchemaId = 0,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
      Some(userKeyEnc.schema)))

    // Add TTL index if needed
    if (hasTtl) {
      val ttlIndexSchema = StateStoreColFamilySchema(
        getTtlColFamilyName(stateName),
        keySchemaId = 0,
        getTTLRowKeySchema(compositeKeySchema),
        valueSchemaId = 0,
        StructType(Array(StructField("__empty__", NullType))),
        Some(RangeKeyScanStateEncoderSpec(getTTLRowKeySchema(compositeKeySchema), Seq(0))))
      schemas.put(ttlIndexSchema.colFamilyName, ttlIndexSchema)
    }

    schemas.toMap
  }

  def getTimerStateSchema(
      stateName: String,
      keySchema: StructType,
      valSchema: StructType): StateStoreColFamilySchema = {
    StateStoreColFamilySchema(
      stateName,
      keySchemaId = 0,
      keySchema,
      valueSchemaId = 0,
      valSchema,
      Some(PrefixKeyScanStateEncoderSpec(keySchema, 1)))
  }

  def getSecIndexTimerStateSchema(
      stateName: String,
      keySchema: StructType,
      valSchema: StructType): StateStoreColFamilySchema = {
    StateStoreColFamilySchema(
      stateName,
      keySchemaId = 0,
      keySchema,
      valueSchemaId = 0,
      valSchema,
      Some(RangeKeyScanStateEncoderSpec(keySchema, Seq(0))))
  }
}
