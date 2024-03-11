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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.state.{ReadStateStore, StateStoreConf, StateStoreId, StateStoreProvider, StateStoreProviderId}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of [[PartitionReaderFactory]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
class StatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    schema: StructType) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new StatePartitionReader(storeConf, hadoopConf,
      partition.asInstanceOf[StateStoreInputPartition], schema)
  }
}

/**
 * An implementation of [[PartitionReader]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
class StatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType) extends PartitionReader[InternalRow] with Logging {

  private val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
  private val valueSchema = SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

  private lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)
    val allStateStoreMetadata = new StateMetadataPartitionReader(
      partition.sourceOptions.stateCheckpointLocation.getParent.toString, hadoopConf)
      .stateMetadata.toArray
    val stateStoreMetadata = allStateStoreMetadata.filter { entry =>
      entry.operatorId == partition.sourceOptions.operatorId &&
        entry.stateStoreName == partition.sourceOptions.storeName
    }
    val numColsPrefixKey = if (stateStoreMetadata.isEmpty) {
      logWarning("Metadata for state store not found, possible cause is this checkpoint " +
        "is created by older version of spark. If the query has session window aggregation, " +
        "the state can't be read correctly and runtime exception will be thrown. " +
        "Run the streaming query in newer spark version to generate state metadata " +
        "can fix the issue.")
      0
    } else {
      require(stateStoreMetadata.length == 1)
      stateStoreMetadata.head.numColsPrefixKey
    }

    StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, numColsPrefixKey,
      useColumnFamilies = false, storeConf, hadoopConf.value, useMultipleValuesPerKey = false)
  }

  private lazy val store: ReadStateStore = {
    provider.getReadStore(partition.sourceOptions.batchId + 1)
  }

  private lazy val iter: Iterator[InternalRow] = {
    store.iterator().map(pair => unifyStateRowPair((pair.key, pair.value)))
  }

  private var current: InternalRow = _

  override def next(): Boolean = {
    if (iter.hasNext) {
      current = iter.next()
      true
    } else {
      current = null
      false
    }
  }

  override def get(): InternalRow = current

  override def close(): Unit = {
    current = null
    store.abort()
    provider.close()
  }

  private def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow)): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition.partition)
    row
  }
}
