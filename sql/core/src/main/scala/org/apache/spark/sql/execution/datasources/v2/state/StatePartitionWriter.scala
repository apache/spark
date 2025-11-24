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
package org.apache.spark.sql.execution.datasources.v2.state

import java.io.IOException

import scala.collection.MapView
import scala.collection.immutable.HashMap

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.{StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateSchemaProvider, StateStore, StateStoreConf, StateStoreId, StateStoreProvider, StateStoreProviderId}
import org.apache.spark.sql.types.{NullType, StructField, StructType}


class StatePartitionAllColumnFamiliesWriter(
     storeConf: StateStoreConf,
     hadoopConf: Configuration,
     partition: StateStoreInputPartition,
     columnFamilyToSchemaMap: HashMap[String, (StructType, StructType)],
     keyStateEncoderSpec: KeyStateEncoderSpec,
     stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
     stateSchemaProviderOpt: Option[StateSchemaProvider]) {
  private val placeholderSchema: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val schemaOpt: Option[(StructType, StructType)] = columnFamilyToSchemaMap.get(
      StateStore.DEFAULT_COL_FAMILY_NAME)
    val keySchema: StructType = schemaOpt.map(_._1).getOrElse(placeholderSchema)
    val valueSchema: StructType = schemaOpt.map(_._2).getOrElse(placeholderSchema)

    val useMultipleValuesPerKey = SchemaUtil.checkVariableType(stateVariableInfoOpt,
      StateVariableType.ListState)
    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = false, storeConf, hadoopConf,
      useMultipleValuesPerKey = useMultipleValuesPerKey, stateSchemaProviderOpt)
    provider
  }

  private lazy val stateStore: StateStore = {
    provider.getStore(partition.sourceOptions.batchId + 1, forceSnapshotOnCommit = true)
  }

  private val columnFamilyToKeySchemaLenMap: MapView[String, Int] =
    columnFamilyToSchemaMap.view.mapValues(_._1.length)
  private val columnFamilyToValueSchemaLenMap: MapView[String, Int] =
    columnFamilyToSchemaMap.view.mapValues(_._2.length)

  // Get the partition key schema from the default column family for schema construction
  private val partitionKeySchema = {
    val schemaOpt = columnFamilyToSchemaMap.get(StateStore.DEFAULT_COL_FAMILY_NAME)
    schemaOpt.map(_._1).getOrElse(placeholderSchema)
  }

  private val schema = SchemaUtil.getSourceSchema(
    partition.sourceOptions, partitionKeySchema, placeholderSchema, None, None)

  def write(partition: Iterator[Row] ): Unit = {
    partition.foreach(row => writeRaw(row))
    stateStore.commit()
  }

  private def writeRaw(rawRecord: Row): Unit = {
    val rowConverter = CatalystTypeConverters.createToCatalystConverter(schema)
    val record = rowConverter(rawRecord).asInstanceOf[InternalRow]
      // Validate record schema
      if (record.numFields != 4) {
        throw new IOException(
          s"Invalid record schema: expected 4 fields (partition_key, key_bytes, value_bytes, " +
            s"column_family_name), got ${record.numFields}")
      }

      // Extract raw bytes and column family name from the record
      val keyBytes = record.getBinary(1)
      val valueBytes = record.getBinary(2)
      val colFamilyName = record.getString(3)

      // Reconstruct UnsafeRow objects from the raw bytes
      // The bytes are in UnsafeRow memory format from StatePartitionReaderAllColumnFamilies
      val keyRow = new UnsafeRow(columnFamilyToKeySchemaLenMap(colFamilyName))
      keyRow.pointTo(keyBytes, keyBytes.length)

      val valueRow = new UnsafeRow(columnFamilyToValueSchemaLenMap(colFamilyName))
      valueRow.pointTo(valueBytes, valueBytes.length)

      // Use StateStore API which handles proper RocksDB encoding (version byte, checksums, etc.)
      stateStore.put(keyRow, valueRow, colFamilyName)
  }
}
