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
import org.apache.spark.sql.avro.{AvroDeserializer, AvroOptions, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, StateStoreColFamilySchema}
import org.apache.spark.sql.execution.streaming.state.AvroEncoderSpec
import org.apache.spark.sql.types.StructType

object StateStoreColumnFamilySchemaUtils {

  def apply(initializeAvroSerde: Boolean): StateStoreColumnFamilySchemaUtils =
    new StateStoreColumnFamilySchemaUtils(initializeAvroSerde)
}

class StateStoreColumnFamilySchemaUtils(initializeAvroSerde: Boolean) {

  private def getAvroSerde(
      keySchema: StructType,
      valSchema: StructType,
      compositeKeySchema: Option[StructType] = None): Option[AvroEncoderSpec] = {
    if (initializeAvroSerde) {
      val avroType = SchemaConverters.toAvroType(valSchema)
      val avroOptions = AvroOptions(Map.empty)
      val keyAvroType = SchemaConverters.toAvroType(keySchema)
      val keySer = new AvroSerializer(keySchema, keyAvroType, nullable = false)
      val ser = new AvroSerializer(valSchema, avroType, nullable = false)
      val de = new AvroDeserializer(avroType, valSchema,
        avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
        avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
      val ckSerDe = compositeKeySchema match {
        case Some(schema) =>
          val ckAvroType = SchemaConverters.toAvroType(schema)
          val ckSer = new AvroSerializer(schema, ckAvroType, nullable = false)
          val ckDe = new AvroDeserializer(ckAvroType, schema,
            avroOptions.datetimeRebaseModeInRead, avroOptions.useStableIdForUnionType,
            avroOptions.stableIdPrefixForUnionType, avroOptions.recursiveFieldMaxDepth)
          (Some(ckSer), Some(ckDe))
        case None => (None, None)
      }
      Some(AvroEncoderSpec(keySer, ser, de, ckSerDe._1, ckSerDe._2))
    } else {
      None
    }
  }

  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): StateStoreColFamilySchema = {
   val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
   StateStoreColFamilySchema(
      stateName,
      keyEncoder.schema,
      valSchema,
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)),
      avroEnc = getAvroSerde(keyEncoder.schema, valSchema))
  }

  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): StateStoreColFamilySchema = {
  val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
  StateStoreColFamilySchema(
      stateName,
      keyEncoder.schema,
      valSchema,
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)),
      avroEnc = getAvroSerde(keyEncoder.schema, valSchema))
  }

  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): StateStoreColFamilySchema = {
    // Create the proper wrapping structure for the key schema
    val wrappedKeySchema = new StructType()
      .add("key", keyEncoder.schema)
    val compositeKeySchema = getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
    val valSchema = getValueSchemaWithTTL(valEncoder.schema, hasTtl)
    StateStoreColFamilySchema(
      stateName,
      compositeKeySchema,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
      Some(userKeyEnc.schema),
      avroEnc = getAvroSerde(wrappedKeySchema, valSchema, Some(compositeKeySchema)))
  }

  def getTimerStateSchema(
      stateName: String,
      keySchema: StructType,
      valSchema: StructType): StateStoreColFamilySchema = {
    StateStoreColFamilySchema(
      stateName,
      keySchema,
      valSchema,
      Some(PrefixKeyScanStateEncoderSpec(keySchema, 1)))
  }
}
