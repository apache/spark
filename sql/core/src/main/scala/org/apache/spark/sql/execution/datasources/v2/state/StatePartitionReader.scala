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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class StatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType) extends PartitionReader[InternalRow] {

  private val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
  private val valueSchema = SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

  private lazy val iter = {
    val stateStoreId = StateStoreId(partition.stateCheckpointRootLocation,
      partition.operatorId, partition.partition, partition.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val store = StateStore.get(stateStoreProviderId, keySchema, valueSchema,
      indexOrdinal = None, version = partition.version, storeConf = storeConf,
      hadoopConf = hadoopConf.value)

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
  }

  private def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow)): InternalRow = {
    val row = new GenericInternalRow(2)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row
  }
}
