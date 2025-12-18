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
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.operators.stateful.join.SymmetricHashJoinStateManager
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.{StateStoreColumnFamilySchemaUtils, StateVariableType, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.RecordType.{getRecordTypeAsString, RecordType}
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.{NextIterator, SerializableConfiguration}

/**
 * Information used specifically by StatePartitionAllColumnFamiliesReader
 * @param colFamilySchemas a set of ColFamilySchema for all column families in an operator.
 *                         The reader relies on this field to read data for all column family
 * @param stateVariableInfos a list of TransformWithStateVariableInfo for state variables
 *                           in TWS operator. The reader relies on this to check variable type
 */
case class AllColumnFamiliesReaderInfo(
    colFamilySchemas: Set[StateStoreColFamilySchema] = Set.empty,
    stateVariableInfos: List[TransformWithStateVariableInfo] = List.empty)

/**
 * An implementation of [[PartitionReaderFactory]] for State data source. This is used to support
 * general read from a state store instance, rather than specific to the operator.
 * @param stateSchemaProviderOpt Optional provider that maintains mapping between schema IDs and
 *                               their corresponding schemas, enabling reading of state data
 *                               written with older schema versions
 */
class StatePartitionReaderFactory(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    joinColFamilyOpt: Option[String],
    allColumnFamiliesReaderInfo: Option[AllColumnFamiliesReaderInfo])
  extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val stateStoreInputPartition = partition.asInstanceOf[StateStoreInputPartition]
    if (stateStoreInputPartition.sourceOptions.internalOnlyReadAllColumnFamilies) {
      require(allColumnFamiliesReaderInfo.isDefined)
      new StatePartitionAllColumnFamiliesReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateStoreColFamilySchemaOpt,
        stateSchemaProviderOpt, allColumnFamiliesReaderInfo.get)
    } else if (stateStoreInputPartition.sourceOptions.readChangeFeed) {
      new StateStoreChangeDataPartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt,
        stateStoreColFamilySchemaOpt, stateSchemaProviderOpt, joinColFamilyOpt)
    } else {
      new StatePartitionReader(storeConf, hadoopConf,
        stateStoreInputPartition, schema, keyStateEncoderSpec, stateVariableInfoOpt,
        stateStoreColFamilySchemaOpt, stateSchemaProviderOpt, joinColFamilyOpt)
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
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    joinColFamilyOpt: Option[String])
  extends PartitionReader[InternalRow] with Logging {
  // Used primarily as a placeholder for the value schema in the context of
  // state variables used within the transformWithState operator.
  private val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  protected val keySchema : StructType = {
    if (SchemaUtil.checkVariableType(stateVariableInfoOpt, StateVariableType.MapState)) {
      SchemaUtil.getCompositeKeySchema(schema, partition.sourceOptions)
    } else if (partition.sourceOptions.internalOnlyReadAllColumnFamilies) {
      require(stateStoreColFamilySchemaOpt.isDefined)
      stateStoreColFamilySchemaOpt.map(_.keySchema).get
    } else {
      SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
    }
  }

  protected val valueSchema : StructType = if (stateVariableInfoOpt.isDefined) {
    schemaForValueRow
  } else if (partition.sourceOptions.internalOnlyReadAllColumnFamilies) {
    require(stateStoreColFamilySchemaOpt.isDefined)
    stateStoreColFamilySchemaOpt.map(_.valueSchema).get
  } else {
    SchemaUtil.getSchemaAsDataType(
      schema, "value").asInstanceOf[StructType]
  }

  protected def getStoreUniqueId(
    operatorStateUniqueIds: Option[Array[Array[String]]]) : Option[String] = {
    SymmetricHashJoinStateManager.getStateStoreCheckpointId(
      storeName = partition.sourceOptions.storeName,
      partitionId = partition.partition,
      stateStoreCkptIds = operatorStateUniqueIds)
  }

  protected def getStartStoreUniqueId: Option[String] = {
    getStoreUniqueId(partition.sourceOptions.startOperatorStateUniqueIds)
  }

  protected def getEndStoreUniqueId: Option[String] = {
    getStoreUniqueId(partition.sourceOptions.endOperatorStateUniqueIds)
  }

  protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)

    val useColFamilies = stateVariableInfoOpt.isDefined || joinColFamilyOpt.isDefined

    val useMultipleValuesPerKey = SchemaUtil.checkVariableType(stateVariableInfoOpt,
      StateVariableType.ListState)

    val provider = StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies = useColFamilies, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = useMultipleValuesPerKey, stateSchemaProviderOpt)

    if (useColFamilies) {
      val store = provider.getStore(
        partition.sourceOptions.batchId + 1,
        getEndStoreUniqueId)
      require(stateStoreColFamilySchemaOpt.isDefined)
      val stateStoreColFamilySchema = stateStoreColFamilySchemaOpt.get
      val isInternal = partition.sourceOptions.readRegisteredTimers ||
        StateStoreColumnFamilySchemaUtils.isTestingInternalColFamily(
          stateStoreColFamilySchema.colFamilyName)
      require(stateStoreColFamilySchema.keyStateEncoderSpec.isDefined)
      store.createColFamilyIfAbsent(
        stateStoreColFamilySchema.colFamilyName,
        stateStoreColFamilySchema.keySchema,
        stateStoreColFamilySchema.valueSchema,
        stateStoreColFamilySchema.keyStateEncoderSpec.get,
        useMultipleValuesPerKey = useMultipleValuesPerKey,
        isInternal = isInternal)
      store.abort()
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
 * Source. It reads the state at a particular batchId.
 */
class StatePartitionReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    joinColFamilyOpt: Option[String])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt, stateStoreColFamilySchemaOpt,
    stateSchemaProviderOpt, joinColFamilyOpt) {

  private lazy val store: ReadStateStore = {
    partition.sourceOptions.fromSnapshotOptions match {
      case None =>
        assert(getStartStoreUniqueId == getEndStoreUniqueId,
          "Start and end store unique IDs must be the same when not reading from snapshot")
        provider.getReadStore(
          partition.sourceOptions.batchId + 1,
          getStartStoreUniqueId
        )

      case Some(fromSnapshotOptions) =>
        if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
          throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
            provider.getClass.toString)
        }
        provider.asInstanceOf[SupportsFineGrainedReplay]
          .replayReadStateFromSnapshot(
            fromSnapshotOptions.snapshotStartBatchId + 1,
            partition.sourceOptions.batchId + 1,
            getStartStoreUniqueId,
            getEndStoreUniqueId)
    }
  }

  override lazy val iter: Iterator[InternalRow] = {
    val colFamilyName = stateStoreColFamilySchemaOpt
      .map(_.colFamilyName).getOrElse(
        joinColFamilyOpt.getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME))

    if (stateVariableInfoOpt.isDefined) {
      val stateVariableInfo = stateVariableInfoOpt.get
      val stateVarType = stateVariableInfo.stateVariableType
      SchemaUtil.processStateEntries(stateVarType, colFamilyName, store,
        keySchema, partition.partition, partition.sourceOptions)
    } else {
      store
        .iterator(colFamilyName)
        .map { pair =>
          SchemaUtil.unifyStateRowPair((pair.key, pair.value), partition.partition)
        }
    }
  }

  override def close(): Unit = {
    store.release()
    super.close()
  }
}

/**
 * An implementation of [[StatePartitionReaderBase]] for reading all column families
 * in binary format. This reader returns raw key and value bytes along with column family names.
 * We are returning key/value bytes because each column family can have different schema
 * It will also return the partition key
 */
class StatePartitionAllColumnFamiliesReader(
    storeConf: StateStoreConf,
    hadoopConf: SerializableConfiguration,
    partition: StateStoreInputPartition,
    schema: StructType,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    defaultStateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    allColumnFamiliesReaderInfo: AllColumnFamiliesReaderInfo)
  extends StatePartitionReaderBase(
    storeConf,
    hadoopConf, partition, schema,
    keyStateEncoderSpec, None,
    defaultStateStoreColFamilySchemaOpt,
    stateSchemaProviderOpt, None) {

  private val stateStoreColFamilySchemas = allColumnFamiliesReaderInfo.colFamilySchemas
  private val stateVariableInfos = allColumnFamiliesReaderInfo.stateVariableInfos

  private def isListType(colFamilyName: String): Boolean = {
    SchemaUtil.checkVariableType(
      stateVariableInfos.find(info => info.stateName == colFamilyName),
      StateVariableType.ListState)
  }

  override protected lazy val provider: StateStoreProvider = {
    val stateStoreId = StateStoreId(partition.sourceOptions.stateCheckpointLocation.toString,
      partition.sourceOptions.operatorId, partition.partition, partition.sourceOptions.storeName)
    val stateStoreProviderId = StateStoreProviderId(stateStoreId, partition.queryId)
    val useColumnFamilies = stateStoreColFamilySchemas.size > 1
    StateStoreProvider.createAndInit(
      stateStoreProviderId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies, storeConf, hadoopConf.value,
      useMultipleValuesPerKey = false, stateSchemaProviderOpt)
  }

  private def checkAllColFamiliesExist(
      colFamilyNames: List[String],
      stateStore: StateStore
    ): Unit = {
    // Filter out DEFAULT column family from validation for two reasons:
    // 1. Some operators (e.g., stream-stream join v3) don't include DEFAULT in their schema
    //    because the underlying RocksDB creates "default" column family automatically
    // 2. The default column family schema is handled separately via
    //    defaultStateStoreColFamilySchemaOpt, so no need to verify it here
    val actualCFs = colFamilyNames.toSet.filter(_ != StateStore.DEFAULT_COL_FAMILY_NAME)
    val expectedCFs = stateStore.allColumnFamilyNames
      .filter(_ != StateStore.DEFAULT_COL_FAMILY_NAME)

    // Validation: All column families found in the checkpoint must be declared in the schema.
    // It's acceptable if some schema CFs are not in expectedCFs - this just means those
    // column families have no data yet in the checkpoint
    // (they'll be created during registration).
    // However, if the checkpoint contains CFs not in the schema, it indicates a mismatch.
    require(expectedCFs.subsetOf(actualCFs),
      s"Some column families are present in the state store but missing in the metadata. " +
        s"Column families in state store but not in metadata: ${expectedCFs.diff(actualCFs)}")
  }

  // Use a single store instance for both registering column families and iteration.
  // We cannot abort and then get a read store because abort() invalidates the loaded version,
  // causing getReadStore() to reload from checkpoint and clear the column family registrations.
  private lazy val store: StateStore = {
    assert(getStartStoreUniqueId == getEndStoreUniqueId,
      "Start and end store unique IDs must be the same when reading all column families")
    val stateStore = provider.getStore(
      partition.sourceOptions.batchId + 1,
      getStartStoreUniqueId
    )

    // Register all column families from the schema
    if (stateStoreColFamilySchemas.size > 1) {
      checkAllColFamiliesExist(stateStoreColFamilySchemas.map(_.colFamilyName).toList, stateStore)
      stateStoreColFamilySchemas.foreach { cfSchema =>
        cfSchema.colFamilyName match {
          case StateStore.DEFAULT_COL_FAMILY_NAME => // createAndInit has registered default
          case _ =>
            val isInternal =
              StateStoreColumnFamilySchemaUtils.isInternalColFamily(cfSchema.colFamilyName)
            val useMultipleValuesPerKey = isListType(cfSchema.colFamilyName)
            require(cfSchema.keyStateEncoderSpec.isDefined,
              s"keyStateEncoderSpec must be defined for column family ${cfSchema.colFamilyName}")
            stateStore.createColFamilyIfAbsent(
              cfSchema.colFamilyName,
              cfSchema.keySchema,
              cfSchema.valueSchema,
              cfSchema.keyStateEncoderSpec.get,
              useMultipleValuesPerKey,
              isInternal)
        }
      }
    }
    stateStore
  }

  override lazy val iter: Iterator[InternalRow] = {
    // Iterate all column families and concatenate results
    stateStoreColFamilySchemas.iterator.flatMap { cfSchema =>
      if (isListType(cfSchema.colFamilyName)) {
        store.iterator(cfSchema.colFamilyName).flatMap(
          pair =>
            store.valuesIterator(pair.key, cfSchema.colFamilyName).map {
              value =>
                SchemaUtil.unifyStateRowPairAsRawBytes((pair.key, value), cfSchema.colFamilyName)
            }
        )
      } else {
        store.iterator(cfSchema.colFamilyName).map { pair =>
          SchemaUtil.unifyStateRowPairAsRawBytes(
            (pair.key, pair.value), cfSchema.colFamilyName)
        }
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
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider],
    joinColFamilyOpt: Option[String])
  extends StatePartitionReaderBase(storeConf, hadoopConf, partition, schema,
    keyStateEncoderSpec, stateVariableInfoOpt, stateStoreColFamilySchemaOpt,
    stateSchemaProviderOpt, joinColFamilyOpt) {

  private lazy val changeDataReader:
    NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)] = {
    if (!provider.isInstanceOf[SupportsFineGrainedReplay]) {
      throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
        provider.getClass.toString)
    }

    val colFamilyNameOpt = if (stateVariableInfoOpt.isDefined) {
      Some(stateVariableInfoOpt.get.stateName)
    } else if (joinColFamilyOpt.isDefined) {
      Some(joinColFamilyOpt.get)
    } else {
      None
    }

    provider.asInstanceOf[SupportsFineGrainedReplay]
      .getStateStoreChangeDataReader(
        partition.sourceOptions.readChangeFeedOptions.get.changeStartBatchId + 1,
        partition.sourceOptions.readChangeFeedOptions.get.changeEndBatchId + 1,
        colFamilyNameOpt,
        getEndStoreUniqueId)
  }

  override lazy val iter: Iterator[InternalRow] = {
    if (SchemaUtil.checkVariableType(stateVariableInfoOpt, StateVariableType.MapState)) {
      val groupingKeySchema = SchemaUtil.getSchemaAsDataType(
        keySchema, "key"
      ).asInstanceOf[StructType]
      val userKeySchema = SchemaUtil.getSchemaAsDataType(
        keySchema, "userKey"
      ).asInstanceOf[StructType]
      changeDataReader.iterator.map { entry =>
        val groupingKey = entry._2.get(0, groupingKeySchema).asInstanceOf[UnsafeRow]
        val userMapKey = entry._2.get(1, userKeySchema).asInstanceOf[UnsafeRow]
        createFlattenedRowForMapState(entry._4, entry._1,
          groupingKey, userMapKey, entry._3, partition.partition)
      }
    } else {
      changeDataReader.iterator.map(unifyStateChangeDataRow)
    }
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

  private def createFlattenedRowForMapState(
      batchId: Long,
      recordType: RecordType,
      groupingKey: UnsafeRow,
      userKey: UnsafeRow,
      userValue: UnsafeRow,
      partition: Int): InternalRow = {
    val result = new GenericInternalRow(6)
    result.update(0, batchId)
    result.update(1, UTF8String.fromString(getRecordTypeAsString(recordType)))
    result.update(2, groupingKey)
    result.update(3, userKey)
    result.update(4, userValue)
    result.update(5, partition)
    result
  }
}
