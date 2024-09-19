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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericInternalRow, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{StateStoreConf, SymmetricHashJoinStateManager}
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of [[PartitionReaderFactory]] for State Store data source, specifically
 * to build a [[PartitionReader]] for reading the state from stream-stream join.
 */
class StreamStreamJoinStatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    userFacingSchema: StructType,
    stateSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    new StreamStreamJoinStatePartitionReader(storeConf, hadoopConf,
      partition.asInstanceOf[StateStoreInputPartition], userFacingSchema, stateSchema)
  }
}

/**
 * An implementation of [[PartitionReader]] for State Store data source, specifically to read
 * the partition for the state from stream-stream join.
 */
class StreamStreamJoinStatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    userFacingSchema: StructType,
    stateSchema: StructType) extends PartitionReader[InternalRow] with Logging {

  private val keySchema = SchemaUtil.getSchemaAsDataType(stateSchema, "key")
    .asInstanceOf[StructType]
  private val valueSchema = SchemaUtil.getSchemaAsDataType(stateSchema, "value")
    .asInstanceOf[StructType]

  private val userFacingValueSchema = SchemaUtil.getSchemaAsDataType(userFacingSchema, "value")
    .asInstanceOf[StructType]

  private val joinSide: JoinSide = partition.sourceOptions.joinSide match {
    case JoinSideValues.left => LeftSide
    case JoinSideValues.right => RightSide
    case JoinSideValues.none =>
      throw StateDataSourceErrors.internalError("Unexpected join side for stream-stream read!")
  }

  /*
   * This is to handle the difference of schema across state format versions. The major difference
   * is whether we have added new field(s) in addition to the fields from input schema.
   *
   * - version 1: no additional field
   * - version 2: the field "matched" is added to the last
   */
  private val (inputAttributes, formatVersion) = {
    val maybeMatchedColumn = valueSchema.last
    val (fields, version) = {
      if (maybeMatchedColumn.name == "matched" && maybeMatchedColumn.dataType == BooleanType) {
        (valueSchema.dropRight(1), 2)
      } else {
        (valueSchema, 1)
      }
    }

    assert(fields.toArray.sameElements(userFacingValueSchema.fields),
      "Exposed fields should be same with given user facing schema for value! " +
        s"Exposed fields: ${fields.mkString("(", ", ", ")")} / " +
        s"User facing value fields: ${userFacingValueSchema.fields.mkString("(", ", ", ")")}")

    val attrs = fields.map {
      f => AttributeReference(f.name, f.dataType, f.nullable)()
    }
    (attrs, version)
  }

  private var joinStateManager: SymmetricHashJoinStateManager = _

  private lazy val iter = {
    if (joinStateManager == null) {
      val stateInfo = StatefulOperatorStateInfo(
        partition.sourceOptions.stateCheckpointLocation.toString,
        partition.queryId, partition.sourceOptions.operatorId,
        partition.sourceOptions.batchId + 1, -1, None)
      joinStateManager = new SymmetricHashJoinStateManager(
        joinSide,
        inputAttributes,
        joinKeys = DataTypeUtils.toAttributes(keySchema),
        stateInfo = Some(stateInfo),
        storeConf = storeConf,
        hadoopConf = hadoopConf.value,
        partitionId = partition.partition,
        // TODO State store source doesn't support checkpoint ID yet.
        keyToNumValuesCheckpointId = None,
        keyWithIndexToValueCheckpointId = None,
        formatVersion,
        skippedNullValueCount = None,
        useStateStoreCoordinator = false,
        snapshotStartVersion =
          partition.sourceOptions.fromSnapshotOptions.map(_.snapshotStartBatchId + 1)
      )
    }

    // state format 2
    val valueWithMatchedExprs = inputAttributes :+ Literal(true)
    val indexOrdinalInValueWithMatchedRow = inputAttributes.size
    val valueWithMatchedRowGenerator = UnsafeProjection.create(valueWithMatchedExprs,
      inputAttributes)

    joinStateManager.iterator.map { pair =>
      if (formatVersion == 2) {
        val row = valueWithMatchedRowGenerator(pair.value)
        row.setBoolean(indexOrdinalInValueWithMatchedRow, pair.matched)
        unifyStateRowPair(pair.key, row)
      } else { // formatVersion == 1
        unifyStateRowPair(pair.key, pair.value)
      }
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
    if (joinStateManager != null) {
      joinStateManager.abortIfNeeded()
    }
  }

  private def unifyStateRowPair(pair: (UnsafeRow, UnsafeRow)): InternalRow = {
    val row = new GenericInternalRow(3)
    row.update(0, pair._1)
    row.update(1, pair._2)
    row.update(2, partition.partition)
    row
  }
}
