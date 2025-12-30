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
package org.apache.spark.sql.execution.streaming.state

import java.util.UUID

import scala.collection.MapView

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.streaming.operators.stateful.StatefulOperatorsUtils
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.StateStoreColumnFamilySchemaUtils
import org.apache.spark.sql.execution.streaming.runtime.StreamingCheckpointConstants.DIR_NAME_STATE
import org.apache.spark.sql.internal.SQLConf

case class StatePartitionWriterColumnFamilyInfo(
  schema: StateStoreColFamilySchema,
  // set this to true if state variable is ListType in TransformWithState
  useMultipleValuesPerKey: Boolean = false)

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
    partitionId: Int,
    targetCpLocation: String,
    operatorId: Int,
    storeName: String,
    currentBatchId: Long,
    colFamilyToWriterInfoMap: Map[String, StatePartitionWriterColumnFamilyInfo],
    operatorName: String,
    schemaProviderOpt: Option[StateSchemaProvider],
    sqlConf: SQLConf) {

  private def isJoinV3Operator(
      operatorName: String, sqlConf: SQLConf): Boolean = {
    operatorName == StatefulOperatorsUtils.SYMMETRIC_HASH_JOIN_EXEC_OP_NAME &&
      sqlConf.getConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION) == 3
  }

  private val defaultSchema = {
    colFamilyToWriterInfoMap.get(StateStore.DEFAULT_COL_FAMILY_NAME) match {
      case Some(info) => info.schema
      case None =>
        // joinV3 operator doesn't have default column family schema
        assert(isJoinV3Operator(operatorName, sqlConf),
          s"Please provide the schema of 'default' column family in StateStoreColFamilySchema" +
            s"for operator $operatorName")
        // Return a dummy StateStoreColFamilySchema if not found
        val placeholderSchema = colFamilyToWriterInfoMap.head._2.schema
        StateStoreColFamilySchema(
          colFamilyName = "__dummy__",
          keySchemaId = 0,
          keySchema = placeholderSchema.keySchema,
          valueSchemaId = 0,
          valueSchema = placeholderSchema.valueSchema,
          keyStateEncoderSpec = Option(NoPrefixKeyStateEncoderSpec(placeholderSchema.keySchema)))
    }
  }

  private val useColumnFamilies = colFamilyToWriterInfoMap.size > 1
  private val columnFamilyToKeySchemaLenMap: MapView[String, Int] =
    colFamilyToWriterInfoMap.view.mapValues(_.schema.keySchema.length)
  private val columnFamilyToValueSchemaLenMap: MapView[String, Int] =
    colFamilyToWriterInfoMap.view.mapValues(_.schema.valueSchema.length)

  protected lazy val provider: StateStoreProvider = {
    val stateCheckpointLocation = new Path(targetCpLocation, DIR_NAME_STATE).toString
    val stateStoreId = StateStoreId(stateCheckpointLocation,
      operatorId, partitionId, storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, UUID.randomUUID())

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, defaultSchema.keySchema, defaultSchema.valueSchema,
      defaultSchema.keyStateEncoderSpec.get,
      useColumnFamilies = useColumnFamilies, storeConf, hadoopConf,
      useMultipleValuesPerKey = false, stateSchemaProvider = schemaProviderOpt)
    provider
  }

  private lazy val stateStore: StateStore = {
    // TODO[SPARK-54590]: Support checkpoint V2 in StatePartitionAllColumnFamiliesWriter
    // Create empty store to avoid loading old partition data since we are rewriting the
    // store e.g. during repartitioning
    // Use loadEmpty=true to create a fresh state store without loading previous versions
    // We create the empty store AT version, and the next commit will
    // produce version + 1
    val store = provider.getStore(
      currentBatchId,
      stateStoreCkptId = None,
      loadEmpty = true
    )
    if (useColumnFamilies) {
      colFamilyToWriterInfoMap.foreach { pair =>
        val colFamilyName = pair._1
        val cfSchema = pair._2.schema
        colFamilyName match {
          case StateStore.DEFAULT_COL_FAMILY_NAME => // createAndInit has registered default
          case _ =>
            require(cfSchema.keyStateEncoderSpec.isDefined,
              s"keyStateEncoderSpec must be defined for column family ${cfSchema.colFamilyName}")
            val isInternal = StateStoreColumnFamilySchemaUtils.isInternalColFamily(colFamilyName)
            store.createColFamilyIfAbsent(
              colFamilyName,
              cfSchema.keySchema,
              cfSchema.valueSchema,
              cfSchema.keyStateEncoderSpec.get,
              pair._2.useMultipleValuesPerKey,
              isInternal)
        }
      }
    }
    store
  }

  // The function that writes and commits data to state store. It takes in rows with schema
  // - partition_key, StructType
  // - key_bytes, BinaryType
  // - value_bytes, BinaryType
  // - column_family_name, StringType
  def write(rows: Iterator[InternalRow]): Unit = {
    try {
      rows.foreach(row => writeRow(row))
      stateStore.commit()
    } finally {
      if (!stateStore.hasCommitted) {
        stateStore.abort()
      }
    }
  }

  private def writeRow(record: InternalRow): Unit = {
    assert(record.numFields == 4,
        s"Invalid record schema: expected 4 fields (partition_key, key_bytes, value_bytes, " +
          s"column_family_name), got ${record.numFields}")

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

    // if a column family useMultipleValuesPerKey (e.g. ListType), we will
    // write with 1 put followed by merge
    if (colFamilyToWriterInfoMap(colFamilyName).useMultipleValuesPerKey &&
        stateStore.keyExists(keyRow, colFamilyName)) {
      stateStore.merge(keyRow, valueRow, colFamilyName)
    } else {
      stateStore.put(keyRow, valueRow, colFamilyName)
    }
  }
}
