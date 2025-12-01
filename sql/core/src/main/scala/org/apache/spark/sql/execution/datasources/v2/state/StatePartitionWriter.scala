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
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateStore, StateStoreConf, StateStoreId, StateStoreProvider, StateStoreProviderId}
import org.apache.spark.sql.types.StructType

/**
 * A writer that can directly write binary data to the streaming state store.
 *
 * This writer expects input rows with the same schema produced by
 * StatePartitionAllColumnFamiliesReader:
 *   (partition_key, key_bytes, value_bytes, column_family_name)
 *
 * The writer creates a fresh (empty) state store instance for the target commit version
 * instead of loading previous partition data. After writing all rows for the partition, it will
 * commit all changes as a snapshot
 */
class StatePartitionAllColumnFamiliesWriter(
     storeConf: StateStoreConf,
     hadoopConf: Configuration,
     partition: StateStoreInputPartition,
     columnFamilyToSchemaMap: HashMap[String, (StructType, StructType)],
     keyStateEncoderSpec: KeyStateEncoderSpec) {

  private val (defaultKeySchema, defaultValueSchema) = {
    columnFamilyToSchemaMap.getOrElse(
      StateStore.DEFAULT_COL_FAMILY_NAME,
      throw new IllegalArgumentException(
        s"Column family ${StateStore.DEFAULT_COL_FAMILY_NAME} not found in schema map")
    )
  }

  private val columnFamilyToKeySchemaLenMap: MapView[String, Int] =
    columnFamilyToSchemaMap.view.mapValues(_._1.length)
  private val columnFamilyToValueSchemaLenMap: MapView[String, Int] =
    columnFamilyToSchemaMap.view.mapValues(_._2.length)

  private val rowConverter = {
     val schema = SchemaUtil.getSourceSchema(
       partition.sourceOptions, defaultKeySchema, defaultValueSchema, None, None)
    CatalystTypeConverters.createToCatalystConverter(schema)
  }

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, defaultKeySchema, defaultValueSchema, keyStateEncoderSpec,
      useColumnFamilies = false, storeConf, hadoopConf,
      useMultipleValuesPerKey = false, stateSchemaProvider = None)
    provider
  }

  private lazy val stateStore: StateStore = {
    val stateStoreCkptId = if (storeConf.enableStateStoreCheckpointIds) {
      Some(java.util.UUID.randomUUID.toString)
    } else {
      None
    }
    val version = partition.sourceOptions.batchId + 1
    // Create empty store to avoid loading old partition data during repartitioning
    // Use loadEmpty=true to create a fresh state store without loading previous versions
    // We create the empty store AT version, and the next commit will
    // produce version + 1
    provider.getStore(
      version,
      stateStoreCkptId,
      forceSnapshotOnCommit = true,
      loadEmpty = true
    )
  }

  // The function that writes and commits data to state store. It takes in rows with schema
  // - partition_key, StructType
  // - key_bytes, BinaryType
  // - value_bytes, BinaryType
  // - column_family_name, StringType
  def put(partition: Iterator[Row]): Unit = {
    partition.foreach(row => putRaw(row))
    stateStore.commit()
  }

  private def putRaw(rawRecord: Row): Unit = {
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
