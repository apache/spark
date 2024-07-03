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
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchema.{COMPOSITE_KEY_ROW_SCHEMA, KEY_ROW_SCHEMA}
import org.apache.spark.sql.execution.streaming.state.{ColumnFamilySchema, ColumnFamilySchemaV1, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec}

/**
 * Singleton class to fetch the ColumnFamilySchema for each state type
 */
object ColumnFamilySchemaFactory {

  def getFactory(version: Int): ColumnFamilySchemaFactory = {
    version match {
      case 1 => ColumnFamilySchemaFactoryV1
      case _ => throw new IllegalArgumentException(s"Unsupported version: $version")
    }
  }
}

trait ColumnFamilySchemaFactory {
  def getValueStateSchema[T](
      stateName: String,
      valueEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchema


  def getListStateSchema[T](
      stateName: String,
      valueEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchema


  def getMapStateSchema[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valueEncoder: Encoder[V],
      hasTtl: Boolean): ColumnFamilySchema
}

object ColumnFamilySchemaFactoryV1 extends ColumnFamilySchemaFactory {

  def getValueStateSchema[T](
      stateName: String,
      valueEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    new ColumnFamilySchemaV1(
      stateName,
      KEY_ROW_SCHEMA,
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA),
      false,
      valueEncoder.schema)
  }

  def getListStateSchema[T](
      stateName: String,
      valueEncoder: Encoder[T],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    new ColumnFamilySchemaV1(
      stateName,
      KEY_ROW_SCHEMA,
      NoPrefixKeyStateEncoderSpec(KEY_ROW_SCHEMA),
      true,
      valueEncoder.schema)
  }

  def getMapStateSchema[K, V](
      stateName: String,
      userKeyEnc: Encoder[K],
      valueEncoder: Encoder[V],
      hasTtl: Boolean): ColumnFamilySchemaV1 = {
    new ColumnFamilySchemaV1(
      stateName,
      COMPOSITE_KEY_ROW_SCHEMA,
      PrefixKeyScanStateEncoderSpec(COMPOSITE_KEY_ROW_SCHEMA, 1),
      false,
      valueEncoder.schema,
      Some(userKeyEnc.schema))
  }
}
