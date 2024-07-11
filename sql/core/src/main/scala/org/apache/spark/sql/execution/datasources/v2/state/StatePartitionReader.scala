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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataTableEntry
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.StateTypesEncoder
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of [[PartitionReaderFactory]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
class StatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    schema: StructType,
    stateStoreMetadata: Array[StateMetadataTableEntry]) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new StatePartitionReader(storeConf, hadoopConf,
      partition.asInstanceOf[StateStoreInputPartition], schema, stateStoreMetadata)
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
    schema: StructType,
    stateStoreMetadata: Array[StateMetadataTableEntry])
  extends PartitionReader[InternalRow] with Logging {

  private val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
  private val valueSchema = SchemaUtil.getSchemaAsDataType(schema, "value").asInstanceOf[StructType]

  private val hasTWS: Boolean = {
    stateStoreMetadata.toSeq.head.operatorName == "transformWithStateExec"
  }

  private lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)
    val numColsPrefixKey = if (stateStoreMetadata.isEmpty) {
      logWarning("Metadata for state store not found, possible cause is this checkpoint " +
        "is created by older version of spark. If the query has session window aggregation, " +
        "the state can't be read correctly and runtime exception will be thrown. " +
        "Run the streaming query in newer spark version to generate state metadata " +
        "can fix the issue.")
      0
    } else {
      require(stateStoreMetadata.length == 1)
      require(stateStoreMetadata.head.version == 1)
      stateStoreMetadata.head.numColsPrefixKey.get
    }

    // TODO: currently we don't support RangeKeyScanStateEncoderSpec. Support for this will be
    // added in the future along with state metadata changes.
    // Filed JIRA here: https://issues.apache.org/jira/browse/SPARK-47524
    val keyStateEncoderType = if (numColsPrefixKey > 0) {
      PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey)
    } else {
      NoPrefixKeyStateEncoderSpec(keySchema)
    }

    println("I got here, hasTWS: " + hasTWS)
    if (hasTWS) {
      println("Key schema before getting rocksdb provider: " + keySchema)
      println("Value schema before getting rocksdb provider: " + valueSchema)
      println("KeyEncoder schema before getting rocksdb provider: " + keyStateEncoderType)
    }

    StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderType,
      useColumnFamilies = hasTWS, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = false)
  }

  private lazy val store: ReadStateStore = {
    partition.sourceOptions.snapshotStartBatchId match {
      case None =>
        println("I got None here")
        provider.getReadStore(partition.sourceOptions.batchId + 1)

      case Some(snapshotStartBatchId) =>
        println("I got snapshotStartBatchId")
        if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
          throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
            provider.getClass.toString)
        }
        provider.asInstanceOf[SupportsFineGrainedReplay]
          .replayReadStateFromSnapshot(
            snapshotStartBatchId + 1,
            partition.sourceOptions.batchId + 1)
    }
  }

  private lazy val iter: Iterator[InternalRow] = {
    if (hasTWS) {
      val newStore = provider.getStore(partition.sourceOptions.batchId + 1)
      newStore.createColFamilyIfAbsent("countState", keySchema, valueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema), false, false)
      newStore.iterator("countState").map(pair => unifyStateRowPair((pair.key, pair.value)))
    } else {
      store.iterator().map(pair => unifyStateRowPair((pair.key, pair.value)))
    }
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
    if (hasTWS) {

      val valEncoder = Encoders.scalaLong
      val keyencoder = encoderFor(Encoders.STRING)
      val keySerializer = keyencoder.createSerializer()
      val stateTypesEncoder = StateTypesEncoder(keySerializer, valEncoder, "countState")
      val keyRowBytes = pair._1.getBinary(0)
      val reusedValRow = new UnsafeRow(keyencoder.schema.fields.length)
      reusedValRow.pointTo(keyRowBytes, keyRowBytes.length)
      val decodedKeyDes = keyencoder.resolveAndBind().createDeserializer()
      val decodedKeyVal = decodedKeyDes.apply(reusedValRow)
      println("decoded Key obj is this: " + decodedKeyVal)
      val keyRow = InternalRow(decodedKeyVal)
      val data = Array[Any](UTF8String.fromString(decodedKeyVal))
      val srow: InternalRow = new GenericInternalRow(data)
      srow.update(0, decodedKeyVal)
      val getStringObj = srow.getUTF8String(0)
      println("decode key Row, getString on internal Row: " + getStringObj)
      row.update(0, pair._1)

      val decodedvalRow = stateTypesEncoder.decodeValue(pair._2)
      println("decoded Val obj is this: " + decodedvalRow)
      val valRow = InternalRow(decodedvalRow)
      row.update(1, valRow)
    } else {
      println("what is the internal row here: " + pair._1.toSeq(keySchema))
      val internalRow = pair._1.getInt(0)
      println("inside unifyStateRowPair, int: " + internalRow)
      println("inside unifyStateRowPair, length: " + pair._1.numFields())
      row.update(0, pair._1)
      row.update(1, pair._2)
    }
    row.update(2, partition.partition)
    row
  }
}
