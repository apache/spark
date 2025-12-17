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
package org.apache.spark.sql.streaming.util

import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.types.{BinaryType, IntegerType, LongType, NullType, StringType, StructField, StructType}

/**
 * Test utility object providing schema definitions and constants for TTL state tests.
 * The schema is based on ListStateTTLProcessor, MapStateTTLProcessor, ValueStateTTLProcessor
 * defined in the test suite
 */
// TODO: Move the corresponding TTLProcessors to this file
object TTLProcessorUtils {
  /**
   * Case class to hold all metadata for a column family.
   */
  case class ColumnFamilyMetadata(
      keySchema: StructType,
      valueSchema: StructType,
      encoderSpec: KeyStateEncoderSpec,
      useMultipleValuePerKey: Boolean = false)

  // List state column families
  val LIST_STATE = "listState"
  val LIST_STATE_TTL_INDEX = "$ttl_listState"
  val LIST_STATE_MIN = "$min_listState"
  val LIST_STATE_COUNT = "$count_listState"
  val LIST_STATE_ALL: Set[String] = Set(
    LIST_STATE, LIST_STATE_TTL_INDEX, LIST_STATE_MIN, LIST_STATE_COUNT
  )

  // Map state column families
  val MAP_STATE = "mapState"
  val MAP_STATE_TTL_INDEX = "$ttl_mapState"
  val MAP_STATE_ALL: Set[String] = Set(MAP_STATE, MAP_STATE_TTL_INDEX)

  // Value state column families
  val VALUE_STATE = "valueState"
  val VALUE_STATE_TTL_INDEX = "$ttl_valueState"
  val VALUE_STATE_ALL: Set[String] = Set(VALUE_STATE, VALUE_STATE_TTL_INDEX)

  /**
   * @return A map from column family name to (keySchema, valueSchema) tuple
   */
  def getListStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getListStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  /**
   * Returns complete metadata for list state with TTL including encoder specs.
   * @return A map from column family name to ColumnFamilyMetadata
   */
  def getListStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val listStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", groupByKeySchema)
    ))
    val minExpiryValueSchema = StructType(Array(
      StructField("minExpiry", LongType)
    ))
    val countValueSchema = StructType(Array(
      StructField("count", LongType)
    ))
    val dummyValueSchema = StructType(Array(StructField("__dummy__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      LIST_STATE -> ColumnFamilyMetadata(
        groupByKeySchema, listStateValueSchema, encoderSpec, useMultipleValuePerKey = true),
      LIST_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec),
      LIST_STATE_MIN -> ColumnFamilyMetadata(
        groupByKeySchema, minExpiryValueSchema, encoderSpec),
      LIST_STATE_COUNT -> ColumnFamilyMetadata(
        groupByKeySchema, countValueSchema, encoderSpec)
    )
  }

  /**
   * @return A map from column family name to (keySchema, valueSchema) tuple
   */
  def getMapStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getMapStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  /**
   * @return A map from column family name to ColumnFamilyMetadata
   */
  def getMapStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val userKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val compositeKeySchema = StructType(Array(
      StructField("key", groupByKeySchema),
      StructField("userKey", userKeySchema)
    ))
    val mapStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", compositeKeySchema)
    ))
    val dummyValueSchema = StructType(Array(StructField("__empty__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val mapStateEncoderSpec = PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      MAP_STATE -> ColumnFamilyMetadata(
        compositeKeySchema, mapStateValueSchema, mapStateEncoderSpec),
      MAP_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec)
    )
  }

  /**
   * @return A map from column family name to (keySchema, valueSchema) tuple
   */
  def getValueStateTTLSchemas(): Map[String, (StructType, StructType)] = {
    getValueStateTTLSchemasWithMetadata().view.mapValues { metadata =>
      (metadata.keySchema, metadata.valueSchema)
    }.toMap
  }

  /**
   * @return A map from column family name to ColumnFamilyMetadata
   */
  def getValueStateTTLSchemasWithMetadata(): Map[String, ColumnFamilyMetadata] = {
    val groupByKeySchema = StructType(Array(
      StructField("value", StringType, nullable = true)
    ))
    val valueStateValueSchema = StructType(Array(
      StructField("value", StructType(Array(
        StructField("value", IntegerType, nullable = true)
      )), nullable = false),
      StructField("ttlExpirationMs", LongType, nullable = false)
    ))
    val ttlIndexKeySchema = StructType(Array(
      StructField("expirationMs", LongType, nullable = false),
      StructField("elementKey", groupByKeySchema)
    ))
    val dummyValueSchema = StructType(Array(StructField("__empty__", NullType)))
    val defaultValueSchema = StructType(Array(StructField("value", BinaryType, nullable = true)))

    val encoderSpec = NoPrefixKeyStateEncoderSpec(groupByKeySchema)
    val ttlIndexEncoderSpec = RangeKeyScanStateEncoderSpec(ttlIndexKeySchema, Seq(0))

    Map(
      StateStore.DEFAULT_COL_FAMILY_NAME -> ColumnFamilyMetadata(
        groupByKeySchema, defaultValueSchema, encoderSpec),
      VALUE_STATE -> ColumnFamilyMetadata(
        groupByKeySchema, valueStateValueSchema, encoderSpec),
      VALUE_STATE_TTL_INDEX -> ColumnFamilyMetadata(
        ttlIndexKeySchema, dummyValueSchema, ttlIndexEncoderSpec)
    )
  }

  /**
   * Returns select expressions for TTL state column families with standardized column order.
   *
   * @param columnFamilyName The column family name
   * @return Select expressions as a Seq of strings with order (partition_id, key, value)
   */
  def getTTLSelectExpressions(columnFamilyName: String): Seq[String] = {
    columnFamilyName match {
      case MAP_STATE =>
        Seq("partition_id", "STRUCT(key, user_map_key) AS KEY",
          "user_map_value AS value")
      case LIST_STATE =>
        Seq("partition_id", "key", "list_element")
      case _ =>
        Seq("partition_id", "key", "value")
    }
  }
}
