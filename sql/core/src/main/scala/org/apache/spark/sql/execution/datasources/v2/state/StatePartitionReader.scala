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
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.{StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.RecordType.{getRecordTypeAsString, RecordType}
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{NextIterator, SerializableConfiguration}

/**
 * An implementation of [[PartitionReaderFactory]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
class StatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val stateStoreInputPartition = partition.asInstanceOf[StateStoreInputPartition]
    if (stateStoreInputPartition.sourceOptions.readChangeFeed) {
      new StateStoreChangeDataPartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt,
        stateStoreColFamilySchemaOpt)
    } else {
      new StatePartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt,
        stateStoreColFamilySchemaOpt)
    }
  }
}

/**
 * An implementation of [[PartitionReader]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 */
abstract class StatePartitionReaderBase(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema])
  extends PartitionReader[InternalRow] with Logging {
  // Used primarily as a placeholder for the value schema in the context of
  // state variables used within the transformWithState operator.
  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  protected val keySchema = {
    if (SchemaUtil.checkVariableType(stateVariableInfoOpt, StateVariableType.MapState)) {
      SchemaUtil.getCompositeKeySchema(schema, partition.sourceOptions)
    } else {
      SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    }
  }

  protected val valueSchema = if (stateVariableInfoOpt.isDefined) {
    schemaForValueRow
  } else {
    SchemaUtil.getSchemaAsDataType(
      schema, "value").asInstanceOf[StructType]
  }

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useColFamilies = if (stateVariableInfoOpt.isDefined) {
      true
    } else {
      false
    }

    val useMultipleValuesPerKey = SchemaUtil.checkVariableType(stateVariableInfoOpt,
      StateVariableType.ListState)

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = useColFamilies, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = useMultipleValuesPerKey)

    if (useColFamilies) {
      val store = provider.getStore(partition.sourceOptions.batchId + 1)
      require(stateStoreColFamilySchemaOpt.isDefined)
      val stateStoreColFamilySchema = stateStoreColFamilySchemaOpt.get
      require(stateStoreColFamilySchema.keyStateEncoderSpec.isDefined)
      store.createColFamilyIfAbsent(
        stateStoreColFamilySchema.colFamilyName,
        stateStoreColFamilySchema.keySchema,
        stateStoreColFamilySchema.valueSchema,
        stateStoreColFamilySchema.keyStateEncoderSpec.get,
        useMultipleValuesPerKey = useMultipleValuesPerKey)
    }
    provider
  }

  protected val iter: Iterator[InternalRow]

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
    provider.close()
  }
}

/**
 * An implementation of [[StatePartitionReaderBase]] for the normal mode of State Data
 * Source. It reads the the state at a particular batchId.
 */
class StatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt, stateStoreColFamilySchemaOpt) {

  private lazy val store: ReadStateStore = {
    partition.sourceOptions.fromSnapshotOptions match {
      case None => provider.getReadStore(partition.sourceOptions.batchId + 1)

      case Some(fromSnapshotOptions) =>
        if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
          throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
            provider.getClass.toString)
        }
        provider.asInstanceOf[SupportsFineGrainedReplay]
          .replayReadStateFromSnapshot(
            fromSnapshotOptions.snapshotStartBatchId + 1,
            partition.sourceOptions.batchId + 1)
    }
  }

  private def processListStateEntries(stateVarName: String): Iterator[InternalRow] = {
    if (partition.sourceOptions.flattenCollectionTypes) {
      store
        .iterator(stateVarName)
        .flatMap { pair =>
          val key = pair.key
          val result = store.valuesIterator(key, stateVarName)
          result.map { entry =>
            SchemaUtil.unifyStateRowPair((key, entry), partition.partition)
          }
        }
    } else {
      store
        .iterator(stateVarName)
        .map { pair =>
          val key = pair.key
          val result = store.valuesIterator(key, stateVarName)
          var unsafeRowArr: Seq[UnsafeRow] = Seq.empty
          result.foreach { entry =>
            unsafeRowArr = unsafeRowArr :+ entry.copy()
          }
          // convert the list of values to array type
          val arrData = new GenericArrayData(unsafeRowArr.toArray)
          // convert the list of values to a single row
          SchemaUtil.unifyStateRowPairWithMultipleValues((key, arrData), partition.partition)
        }
    }
  }

  private def processValueStateEntries(stateVarName: String): Iterator[InternalRow] = {
    store
      .iterator(stateVarName)
      .map { pair =>
        SchemaUtil.unifyStateRowPair((pair.key, pair.value), partition.partition)
      }
  }

  private def processMapStateEntries(stateVarName: String): Iterator[InternalRow] = {
    SchemaUtil.unifyMapStateRowPair(store.iterator(stateVarName), keySchema, partition.partition,
      partition.sourceOptions)
  }

  override lazy val iter: Iterator[InternalRow] = {
    val stateVarName = stateVariableInfoOpt
      .map(_.stateName).getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME)

    if (stateVariableInfoOpt.isDefined) {
      val stateVariableInfo = stateVariableInfoOpt.get
      val stateVarType = stateVariableInfo.stateVariableType

      stateVarType match {
        case StateVariableType.ValueState =>
          processValueStateEntries(stateVarName)

        case StateVariableType.ListState =>
          processListStateEntries(stateVarName)

        case StateVariableType.MapState =>
          processMapStateEntries(stateVarName)

        case _ =>
          throw new IllegalStateException(
            s"Unsupported state variable type: $stateVarType")
      }
    } else {
      store
        .iterator(stateVarName)
        .map { pair =>
          SchemaUtil.unifyStateRowPair((pair.key, pair.value), partition.partition)
        }
    }
  }

  override def close(): Unit = {
    store.abort()
    super.close()
  }
}

/**
 * An implementation of [[StatePartitionReaderBase]] for the readChangeFeed mode of State Data
 * Source. It reads the change of state over batches of a particular partition.
 */
class StateStoreChangeDataPartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt, stateStoreColFamilySchemaOpt) {

  private lazy val changeDataReader:
    NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] = {
    if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
      throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
        provider.getClass.toString)
    }
    provider.asInstanceOf[SupportsFineGrainedReplay]
      .getStateStoreChangeDataReader(
        partition.sourceOptions.readChangeFeedOptions.get.changeStartBatchId + 1,
        partition.sourceOptions.readChangeFeedOptions.get.changeEndBatchId + 1)
  }

  override lazy val iter: Iterator[InternalRow] = {
    changeDataReader.iterator.map(unifyStateChangeDataRow)
  }

  override def close(): Unit = {
    changeDataReader.closeIfNeeded()
    super.close()
  }

  private def unifyStateChangeDataRow(row: (RecordType, UnsafeRow, UnsafeRow, Long)):
    InternalRow = {
    val result = new GenericInternalRow(5)
    result.update(0, row._4)
    result.update(1, UTF8String.fromString(getRecordTypeAsString(row._1)))
    result.update(2, row._2)
    result.update(3, row._3)
    result.update(4, partition.partition)
    result
  }
}
