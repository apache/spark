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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.execution.streaming.StateTTLSchema.TTL_VALUE_ROW_SCHEMA
import org.apache.spark.sql.execution.streaming.TimerStateUtils.TIMER_VALUE_ROW_SCHEMA
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStoreColFamilySchema}


object StateStoreColumnFamilySchemaUtils {

  def getValueStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): List[StateStoreColFamilySchema] = {
   List(StateStoreColFamilySchema(
      stateName,
      keyEncoder.schema,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)))) ++
        (if (hasTtl) {
            val ttlKeyRowSchema = getSingleKeyTTLRowSchema(keyEncoder.schema)
            List(
              StateStoreColFamilySchema(
                s"_ttl_$stateName",
                ttlKeyRowSchema,
                TTL_VALUE_ROW_SCHEMA,
                Some(RangeKeyScanStateEncoderSpec(ttlKeyRowSchema, Seq(0)))))
        } else {
            Nil
        })
  }

  def getListStateSchema[T](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[T],
      hasTtl: Boolean): List[StateStoreColFamilySchema] = {
    List(
      StateStoreColFamilySchema(
        stateName,
        keyEncoder.schema,
        getValueSchemaWithTTL(valEncoder.schema, hasTtl),
        Some(NoPrefixKeyStateEncoderSpec(keyEncoder.schema)))
    ) ++ (if (hasTtl) {
      val ttlKeyRowSchema = getSingleKeyTTLRowSchema(keyEncoder.schema)
      List(
        StateStoreColFamilySchema(
          s"_ttl_$stateName",
          ttlKeyRowSchema,
          TTL_VALUE_ROW_SCHEMA,
          Some(RangeKeyScanStateEncoderSpec(ttlKeyRowSchema, Seq(0)))))
    } else {
      Nil
    })
  }

  def getMapStateSchema[K, V](
      stateName: String,
      keyEncoder: ExpressionEncoder[Any],
      userKeyEnc: Encoder[K],
      valEncoder: Encoder[V],
      hasTtl: Boolean): List[StateStoreColFamilySchema] = {
    val compositeKeySchema = getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)
    List(StateStoreColFamilySchema(
      stateName,
      compositeKeySchema,
      getValueSchemaWithTTL(valEncoder.schema, hasTtl),
      Some(PrefixKeyScanStateEncoderSpec(compositeKeySchema, 1)),
      Some(userKeyEnc.schema))) ++ (if (hasTtl) {
      val ttlKeyRowSchema = getCompositeKeyTTLRowSchema(
        keyEncoder.schema, userKeyEnc.schema)
      List(
        StateStoreColFamilySchema(
          s"_ttl_$stateName",
          ttlKeyRowSchema,
          TTL_VALUE_ROW_SCHEMA,
          Some(RangeKeyScanStateEncoderSpec(ttlKeyRowSchema, Seq(0)))))
    } else {
      Nil
    })
  }

  def getTimerSchema(
      keyEncoder: ExpressionEncoder[Any],
      timeMode: TimeMode): List[StateStoreColFamilySchema] = {
    val timerCFName = if (timeMode == TimeMode.ProcessingTime) {
      TimerStateUtils.PROC_TIMERS_STATE_NAME
    } else {
      TimerStateUtils.EVENT_TIMERS_STATE_NAME
    }
    val rowEncoder = new TimerKeyEncoder(keyEncoder)
    val schemaForKeyRow = rowEncoder.schemaForKeyRow

    List(
      StateStoreColFamilySchema(
        timerCFName + TimerStateUtils.KEY_TO_TIMESTAMP_CF,
        schemaForKeyRow,
        TIMER_VALUE_ROW_SCHEMA,
        Some(PrefixKeyScanStateEncoderSpec(schemaForKeyRow, 1))
      ),
      StateStoreColFamilySchema(
        timerCFName + TimerStateUtils.TIMESTAMP_TO_KEY_CF,
        rowEncoder.keySchemaForSecIndex,
        TIMER_VALUE_ROW_SCHEMA,
        Some(RangeKeyScanStateEncoderSpec(rowEncoder.keySchemaForSecIndex, Seq(0)))
      )
    )
  }
}
