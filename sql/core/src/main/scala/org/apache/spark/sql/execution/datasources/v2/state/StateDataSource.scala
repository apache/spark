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

import java.util
import java.util.UUID

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.DataSourceOptions
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.{JoinSideValues, READ_REGISTERED_TIMERS, STATE_VAR_NAME}
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.metadata.{StateMetadataPartitionReader, StateMetadataTableEntry}
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.{OffsetSeqMetadata, StreamingQueryCheckpointMetadata, TimerStateUtils, TransformWithStateOperatorProperties, TransformWithStateVariableInfo}
import org.apache.spark.sql.execution.streaming.StreamingCheckpointConstants.DIR_NAME_STATE
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{InMemoryStateSchemaProvider, KeyStateEncoderSpec, NoPrefixKeyStateEncoderSpec, PrefixKeyScanStateEncoderSpec, StateSchemaCompatibilityChecker, StateSchemaMetadata, StateSchemaProvider, StateStore, StateStoreColFamilySchema, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.streaming.TimeMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of [[TableProvider]] with [[DataSourceRegister]] for State Store data source.
 */
class StateDataSource extends TableProvider with DataSourceRegister with Logging {
  private lazy val session: SparkSession = SparkSession.active

  private lazy val hadoopConf: Configuration = session.sessionState.newHadoopConf()

  private lazy val serializedHadoopConf = new SerializableConfiguration(hadoopConf)

  // Seq of operator names who uses state schema v3 and TWS related options.
  // This Seq was used in checks before reading state schema files.
  private val twsShortNameSeq = Seq(
    "transformWithStateExec",
    "transformWithStateInPandasExec",
    "transformWithStateInPySparkExec"
  )

  override def shortName(): String = "statestore"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, properties)
    val stateConf = buildStateStoreConf(sourceOptions.resolvedCpLocation, sourceOptions.batchId)
    val stateStoreReaderInfo: StateStoreReaderInfo = getStoreMetadataAndRunChecks(sourceOptions)

    // The key state encoder spec should be available for all operators except stream-stream joins
    val keyStateEncoderSpec = if (stateStoreReaderInfo.keyStateEncoderSpecOpt.isDefined) {
      stateStoreReaderInfo.keyStateEncoderSpecOpt.get
    } else {
      val keySchema = SchemaUtil.getSchemaAsDataType(schema, "key").asInstanceOf[StructType]
      NoPrefixKeyStateEncoderSpec(keySchema)
    }

    new StateTable(session, schema, sourceOptions, stateConf, keyStateEncoderSpec,
      stateStoreReaderInfo.transformWithStateVariableInfoOpt,
      stateStoreReaderInfo.stateStoreColFamilySchemaOpt,
      stateStoreReaderInfo.stateSchemaProviderOpt)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, options)

    val stateStoreReaderInfo: StateStoreReaderInfo = getStoreMetadataAndRunChecks(sourceOptions)

    val stateCheckpointLocation = sourceOptions.stateCheckpointLocation
    try {
      // SPARK-51779 TODO: Support stream-stream joins with virtual column families
      val (keySchema, valueSchema) = sourceOptions.joinSide match {
        case JoinSideValues.left =>
          StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
            sourceOptions.operatorId, LeftSide)

        case JoinSideValues.right =>
          StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
            sourceOptions.operatorId, RightSide)

        case JoinSideValues.none =>
          // we should have the schema for the state store if joinSide is none
          require(stateStoreReaderInfo.stateStoreColFamilySchemaOpt.isDefined)
          val resultSchema = stateStoreReaderInfo.stateStoreColFamilySchemaOpt.get
          (resultSchema.keySchema, resultSchema.valueSchema)
      }

      SchemaUtil.getSourceSchema(sourceOptions, keySchema,
        valueSchema,
        stateStoreReaderInfo.transformWithStateVariableInfoOpt,
        stateStoreReaderInfo.stateStoreColFamilySchemaOpt)
    } catch {
      case NonFatal(e) =>
        throw StateDataSourceErrors.failedToReadStateSchema(sourceOptions, e)
    }
  }

  override def supportsExternalMetadata(): Boolean = false

  private def buildStateStoreConf(checkpointLocation: String, batchId: Long): StateStoreConf = {
    val offsetLog = new StreamingQueryCheckpointMetadata(session, checkpointLocation).offsetLog
    offsetLog.get(batchId) match {
      case Some(value) =>
        val metadata = value.metadata.getOrElse(
          throw StateDataSourceErrors.offsetMetadataLogUnavailable(batchId, checkpointLocation)
        )

        val clonedSqlConf = session.sessionState.conf.clone()
        OffsetSeqMetadata.setSessionConf(metadata, clonedSqlConf)
        StateStoreConf(clonedSqlConf)

      case _ =>
        throw StateDataSourceErrors.offsetLogUnavailable(batchId, checkpointLocation)
    }
  }

  private def runStateVarChecks(
      sourceOptions: StateSourceOptions,
      stateStoreMetadata: Array[StateMetadataTableEntry]): Unit = {
    if (sourceOptions.stateVarName.isDefined || sourceOptions.readRegisteredTimers) {
      // Perform checks for transformWithState operator in case state variable name is provided
      require(stateStoreMetadata.size == 1)
      val opMetadata = stateStoreMetadata.head
      if (!twsShortNameSeq.contains(opMetadata.operatorName)) {
        // if we are trying to query state source with state variable name, then the operator
        // should be transformWithState
        val errorMsg = "Providing state variable names is only supported with the " +
          s"transformWithState operator. Found operator=${opMetadata.operatorName}. " +
          s"Please remove this option and re-run the query."
        throw StateDataSourceErrors.invalidOptionValue(STATE_VAR_NAME, errorMsg)
      }

      // if the operator is transformWithState, but the operator properties are empty, then
      // the user has not defined any state variables for the operator
      val operatorProperties = opMetadata.operatorPropertiesJson
      if (operatorProperties.isEmpty) {
        throw StateDataSourceErrors.invalidOptionValue(STATE_VAR_NAME,
          "No state variable names are defined for the transformWithState operator")
      }

      val twsOperatorProperties = TransformWithStateOperatorProperties.fromJson(operatorProperties)
      val timeMode = twsOperatorProperties.timeMode
      if (sourceOptions.readRegisteredTimers && timeMode == TimeMode.None().toString) {
        throw StateDataSourceErrors.invalidOptionValue(READ_REGISTERED_TIMERS,
          "Registered timers are not available in TimeMode=None.")
      }

      // if the state variable is not one of the defined/available state variables, then we
      // fail the query
      val stateVarName = if (sourceOptions.readRegisteredTimers) {
        TimerStateUtils.getTimerStateVarNames(timeMode)._1
      } else {
        sourceOptions.stateVarName.get
      }

      val stateVars = twsOperatorProperties.stateVariables
      val stateVarInfo = stateVars.filter(stateVar => stateVar.stateName == stateVarName)
      if (stateVarInfo.size != 1) {
        throw StateDataSourceErrors.invalidOptionValue(STATE_VAR_NAME,
          s"State variable $stateVarName is not defined for the transformWithState operator.")
      }
    } else {
      // if the operator is transformWithState, then a state variable argument is mandatory
      if (stateStoreMetadata.size == 1 &&
        twsShortNameSeq.contains(stateStoreMetadata.head.operatorName)) {
        throw StateDataSourceErrors.requiredOptionUnspecified("stateVarName")
      }
    }
  }

  private def getStateStoreMetadata(stateSourceOptions: StateSourceOptions):
    Array[StateMetadataTableEntry] = {
    val allStateStoreMetadata = new StateMetadataPartitionReader(
      stateSourceOptions.stateCheckpointLocation.getParent.toString,
      serializedHadoopConf, stateSourceOptions.batchId).stateMetadata.toArray
    val stateStoreMetadata = allStateStoreMetadata.filter { entry =>
      entry.operatorId == stateSourceOptions.operatorId &&
        entry.stateStoreName == stateSourceOptions.storeName
    }
    stateStoreMetadata
  }

  private def getStoreMetadataAndRunChecks(sourceOptions: StateSourceOptions):
    StateStoreReaderInfo = {
    val storeMetadata = getStateStoreMetadata(sourceOptions)
    runStateVarChecks(sourceOptions, storeMetadata)
    var keyStateEncoderSpecOpt: Option[KeyStateEncoderSpec] = None
    var stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema] = None
    var transformWithStateVariableInfoOpt: Option[TransformWithStateVariableInfo] = None
    var stateSchemaProvider: Option[StateSchemaProvider] = None
    var timeMode: String = TimeMode.None.toString

    if (sourceOptions.joinSide == JoinSideValues.none) {
      var stateVarName = sourceOptions.stateVarName
        .getOrElse(StateStore.DEFAULT_COL_FAMILY_NAME)

      // Read the schema file path from operator metadata version v2 onwards
      // for the transformWithState operator
      val oldSchemaFilePaths = if (storeMetadata.length > 0 && storeMetadata.head.version == 2
        && twsShortNameSeq.exists(storeMetadata.head.operatorName.contains)) {
        val storeMetadataEntry = storeMetadata.head
        val operatorProperties = TransformWithStateOperatorProperties.fromJson(
          storeMetadataEntry.operatorPropertiesJson)
        timeMode = operatorProperties.timeMode

        if (sourceOptions.readRegisteredTimers) {
          stateVarName = TimerStateUtils.getTimerStateVarNames(timeMode)._1
        }

        val stateVarInfoList = operatorProperties.stateVariables
          .filter(stateVar => stateVar.stateName == stateVarName)
        require(stateVarInfoList.size == 1, s"Failed to find unique state variable info " +
          s"for state variable $stateVarName in operator ${sourceOptions.operatorId}")
        val stateVarInfo = stateVarInfoList.head
        transformWithStateVariableInfoOpt = Some(stateVarInfo)
        val schemaFilePaths = storeMetadataEntry.stateSchemaFilePaths
        val stateSchemaMetadata = StateSchemaMetadata.createStateSchemaMetadata(
          sourceOptions.stateCheckpointLocation.toString,
          hadoopConf,
          schemaFilePaths
        )
        stateSchemaProvider = Some(new InMemoryStateSchemaProvider(stateSchemaMetadata))
        schemaFilePaths.map(new Path(_))
      } else {
        None
      }.toList

      try {
        // Read the actual state schema from the provided path for v2 or from the dedicated path
        // for v1
        val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
        val stateCheckpointLocation = sourceOptions.stateCheckpointLocation
        val storeId = new StateStoreId(stateCheckpointLocation.toString, sourceOptions.operatorId,
          partitionId, sourceOptions.storeName)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())
        val manager = new StateSchemaCompatibilityChecker(providerId, hadoopConf,
          oldSchemaFilePaths = oldSchemaFilePaths)
        val stateSchema = manager.readSchemaFile()

        // Based on the version and read schema, populate the keyStateEncoderSpec used for
        // reading the column families
        val resultSchema = stateSchema.filter(_.colFamilyName == stateVarName).head
        keyStateEncoderSpecOpt = Some(getKeyStateEncoderSpec(resultSchema, storeMetadata))
        stateStoreColFamilySchemaOpt = Some(resultSchema)
      } catch {
        case NonFatal(ex) =>
          throw StateDataSourceErrors.failedToReadStateSchema(sourceOptions, ex)
      }
    }

    StateStoreReaderInfo(
      keyStateEncoderSpecOpt,
      stateStoreColFamilySchemaOpt,
      transformWithStateVariableInfoOpt,
      stateSchemaProvider
    )
  }

  private def getKeyStateEncoderSpec(
      colFamilySchema: StateStoreColFamilySchema,
      storeMetadata: Array[StateMetadataTableEntry]): KeyStateEncoderSpec = {
    // If operator metadata is not found, then log a warning and continue with using the no-prefix
    // key state encoder
    val keyStateEncoderSpec = if (storeMetadata.length == 0) {
      logWarning("Metadata for state store not found, possible cause is this checkpoint " +
        "is created by older version of spark. If the query has session window aggregation, " +
        "the state can't be read correctly and runtime exception will be thrown. " +
        "Run the streaming query in newer spark version to generate state metadata " +
        "can fix the issue.")
      NoPrefixKeyStateEncoderSpec(colFamilySchema.keySchema)
    } else {
      require(storeMetadata.length == 1)
      val storeMetadataEntry = storeMetadata.head
      // if version has metadata info, then use numColsPrefixKey as specified
      if (storeMetadataEntry.version == 1 && storeMetadataEntry.numColsPrefixKey == 0) {
        NoPrefixKeyStateEncoderSpec(colFamilySchema.keySchema)
      } else if (storeMetadataEntry.version == 1 && storeMetadataEntry.numColsPrefixKey > 0) {
        PrefixKeyScanStateEncoderSpec(colFamilySchema.keySchema,
          storeMetadataEntry.numColsPrefixKey)
      } else if (storeMetadataEntry.version == 2) {
        // for version 2, we have the encoder spec recorded to the state schema file. so we just
        // use that directly
        require(colFamilySchema.keyStateEncoderSpec.isDefined)
        colFamilySchema.keyStateEncoderSpec.get
      } else {
        throw StateDataSourceErrors.internalError(s"Failed to read " +
          s"key state encoder spec for operator=${storeMetadataEntry.operatorId}")
      }
    }
    keyStateEncoderSpec
  }
}

case class FromSnapshotOptions(
    snapshotStartBatchId: Long,
    snapshotPartitionId: Int)

case class ReadChangeFeedOptions(
    changeStartBatchId: Long,
    changeEndBatchId: Long
)

case class StateSourceOptions(
    resolvedCpLocation: String,
    batchId: Long,
    operatorId: Int,
    storeName: String,
    joinSide: JoinSideValues,
    readChangeFeed: Boolean,
    fromSnapshotOptions: Option[FromSnapshotOptions],
    readChangeFeedOptions: Option[ReadChangeFeedOptions],
    stateVarName: Option[String],
    readRegisteredTimers: Boolean,
    flattenCollectionTypes: Boolean) {
  def stateCheckpointLocation: Path = new Path(resolvedCpLocation, DIR_NAME_STATE)

  override def toString: String = {
    var desc = s"StateSourceOptions(checkpointLocation=$resolvedCpLocation, batchId=$batchId, " +
      s"operatorId=$operatorId, storeName=$storeName, joinSide=$joinSide, " +
      s"stateVarName=${stateVarName.getOrElse("None")}, +" +
      s"flattenCollectionTypes=$flattenCollectionTypes"
    if (fromSnapshotOptions.isDefined) {
      desc += s", snapshotStartBatchId=${fromSnapshotOptions.get.snapshotStartBatchId}"
      desc += s", snapshotPartitionId=${fromSnapshotOptions.get.snapshotPartitionId}"
    }
    if (readChangeFeedOptions.isDefined) {
      desc += s", changeStartBatchId=${readChangeFeedOptions.get.changeStartBatchId}"
      desc += s", changeEndBatchId=${readChangeFeedOptions.get.changeEndBatchId}"
    }
    desc + ")"
  }
}

object StateSourceOptions extends DataSourceOptions {
  val PATH = newOption("path")
  val BATCH_ID = newOption("batchId")
  val OPERATOR_ID = newOption("operatorId")
  val STORE_NAME = newOption("storeName")
  val JOIN_SIDE = newOption("joinSide")
  val SNAPSHOT_START_BATCH_ID = newOption("snapshotStartBatchId")
  val SNAPSHOT_PARTITION_ID = newOption("snapshotPartitionId")
  val READ_CHANGE_FEED = newOption("readChangeFeed")
  val CHANGE_START_BATCH_ID = newOption("changeStartBatchId")
  val CHANGE_END_BATCH_ID = newOption("changeEndBatchId")
  val STATE_VAR_NAME = newOption("stateVarName")
  val READ_REGISTERED_TIMERS = newOption("readRegisteredTimers")
  val FLATTEN_COLLECTION_TYPES = newOption("flattenCollectionTypes")

  object JoinSideValues extends Enumeration {
    type JoinSideValues = Value
    val left, right, none = Value
  }

  def apply(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      properties: util.Map[String, String]): StateSourceOptions = {
    apply(sparkSession, hadoopConf, new CaseInsensitiveStringMap(properties))
  }

  def apply(
      sparkSession: SparkSession,
      hadoopConf: Configuration,
      options: CaseInsensitiveStringMap): StateSourceOptions = {
    val checkpointLocation = Option(options.get(PATH)).orElse {
      throw StateDataSourceErrors.requiredOptionUnspecified(PATH)
    }.get

    val operatorId = Option(options.get(OPERATOR_ID)).map(_.toInt)
      .orElse(Some(0)).get

    if (operatorId < 0) {
      throw StateDataSourceErrors.invalidOptionValueIsNegative(OPERATOR_ID)
    }

    val storeName = Option(options.get(STORE_NAME))
      .map(_.trim)
      .getOrElse(StateStoreId.DEFAULT_STORE_NAME)

    if (storeName.isEmpty) {
      throw StateDataSourceErrors.invalidOptionValueIsEmpty(STORE_NAME)
    }

    // Check if the state variable name is provided. Used with the transformWithState operator.
    val stateVarName = Option(options.get(STATE_VAR_NAME))
      .map(_.trim)

    val readRegisteredTimers = try {
      Option(options.get(READ_REGISTERED_TIMERS))
        .map(_.toBoolean).getOrElse(false)
    } catch {
      case _: IllegalArgumentException =>
        throw StateDataSourceErrors.invalidOptionValue(READ_REGISTERED_TIMERS,
          "Boolean value is expected")
    }

    if (readRegisteredTimers && stateVarName.isDefined) {
      throw StateDataSourceErrors.conflictOptions(Seq(READ_REGISTERED_TIMERS, STATE_VAR_NAME))
    }

    val flattenCollectionTypes = try {
      Option(options.get(FLATTEN_COLLECTION_TYPES))
        .map(_.toBoolean).getOrElse(true)
    } catch {
      case _: IllegalArgumentException =>
        throw StateDataSourceErrors.invalidOptionValue(FLATTEN_COLLECTION_TYPES,
          "Boolean value is expected")
    }

    val joinSide = try {
      Option(options.get(JOIN_SIDE))
        .map(JoinSideValues.withName).getOrElse(JoinSideValues.none)
    } catch {
      case _: NoSuchElementException =>
        throw StateDataSourceErrors.invalidOptionValue(JOIN_SIDE,
          s"Valid values are ${JoinSideValues.values.mkString(",")}")
    }

    if (joinSide != JoinSideValues.none && storeName != StateStoreId.DEFAULT_STORE_NAME) {
      throw StateDataSourceErrors.conflictOptions(Seq(JOIN_SIDE, STORE_NAME))
    }

    val resolvedCpLocation = resolvedCheckpointLocation(hadoopConf, checkpointLocation)

    var batchId = Option(options.get(BATCH_ID)).map(_.toLong)

    val snapshotStartBatchId = Option(options.get(SNAPSHOT_START_BATCH_ID)).map(_.toLong)
    val snapshotPartitionId = Option(options.get(SNAPSHOT_PARTITION_ID)).map(_.toInt)

    val readChangeFeed = Option(options.get(READ_CHANGE_FEED)).exists(_.toBoolean)

    val changeStartBatchId = Option(options.get(CHANGE_START_BATCH_ID)).map(_.toLong)
    var changeEndBatchId = Option(options.get(CHANGE_END_BATCH_ID)).map(_.toLong)

    var fromSnapshotOptions: Option[FromSnapshotOptions] = None
    var readChangeFeedOptions: Option[ReadChangeFeedOptions] = None

    if (readChangeFeed) {
      if (joinSide != JoinSideValues.none) {
        throw StateDataSourceErrors.conflictOptions(Seq(JOIN_SIDE, READ_CHANGE_FEED))
      }
      if (batchId.isDefined) {
        throw StateDataSourceErrors.conflictOptions(Seq(BATCH_ID, READ_CHANGE_FEED))
      }
      if (snapshotStartBatchId.isDefined) {
        throw StateDataSourceErrors.conflictOptions(Seq(SNAPSHOT_START_BATCH_ID, READ_CHANGE_FEED))
      }
      if (snapshotPartitionId.isDefined) {
        throw StateDataSourceErrors.conflictOptions(Seq(SNAPSHOT_PARTITION_ID, READ_CHANGE_FEED))
      }

      if (changeStartBatchId.isEmpty) {
        throw StateDataSourceErrors.requiredOptionUnspecified(CHANGE_START_BATCH_ID)
      }
      changeEndBatchId = Some(
        changeEndBatchId.getOrElse(getLastCommittedBatch(sparkSession, resolvedCpLocation)))

      // changeStartBatchId and changeEndBatchId must all be defined at this point
      if (changeStartBatchId.get < 0) {
        throw StateDataSourceErrors.invalidOptionValueIsNegative(CHANGE_START_BATCH_ID)
      }
      if (changeEndBatchId.get < changeStartBatchId.get) {
        throw StateDataSourceErrors.invalidOptionValue(CHANGE_END_BATCH_ID,
          s"$CHANGE_END_BATCH_ID cannot be smaller than $CHANGE_START_BATCH_ID. " +
          s"Please check the input to $CHANGE_END_BATCH_ID, or if you are using its default " +
          s"value, make sure that $CHANGE_START_BATCH_ID is less than ${changeEndBatchId.get}.")
      }

      batchId = Some(changeEndBatchId.get)

      readChangeFeedOptions = Option(
        ReadChangeFeedOptions(changeStartBatchId.get, changeEndBatchId.get))
    } else {
      if (changeStartBatchId.isDefined) {
        throw StateDataSourceErrors.invalidOptionValue(CHANGE_START_BATCH_ID,
            s"Only specify this option when $READ_CHANGE_FEED is set to true.")
      }
      if (changeEndBatchId.isDefined) {
        throw StateDataSourceErrors.invalidOptionValue(CHANGE_END_BATCH_ID,
          s"Only specify this option when $READ_CHANGE_FEED is set to true.")
      }

      batchId = Some(batchId.getOrElse(getLastCommittedBatch(sparkSession, resolvedCpLocation)))

      if (batchId.get < 0) {
        throw StateDataSourceErrors.invalidOptionValueIsNegative(BATCH_ID)
      }
      if (snapshotStartBatchId.exists(_ < 0)) {
        throw StateDataSourceErrors.invalidOptionValueIsNegative(SNAPSHOT_START_BATCH_ID)
      } else if (snapshotStartBatchId.exists(_ > batchId.get)) {
        throw StateDataSourceErrors.invalidOptionValue(
          SNAPSHOT_START_BATCH_ID, s"value should be less than or equal to ${batchId.get}")
      }
      if (snapshotPartitionId.exists(_ < 0)) {
        throw StateDataSourceErrors.invalidOptionValueIsNegative(SNAPSHOT_PARTITION_ID)
      }
      // both snapshotPartitionId and snapshotStartBatchId are required at the same time, because
      // each partition may have different checkpoint status
      if (snapshotPartitionId.isDefined && snapshotStartBatchId.isEmpty) {
        throw StateDataSourceErrors.requiredOptionUnspecified(SNAPSHOT_START_BATCH_ID)
      } else if (snapshotPartitionId.isEmpty && snapshotStartBatchId.isDefined) {
        throw StateDataSourceErrors.requiredOptionUnspecified(SNAPSHOT_PARTITION_ID)
      }

      if (snapshotStartBatchId.isDefined && snapshotPartitionId.isDefined) {
        fromSnapshotOptions = Some(
          FromSnapshotOptions(snapshotStartBatchId.get, snapshotPartitionId.get))
      }
    }

    StateSourceOptions(
      resolvedCpLocation, batchId.get, operatorId, storeName, joinSide,
      readChangeFeed, fromSnapshotOptions, readChangeFeedOptions,
      stateVarName, readRegisteredTimers, flattenCollectionTypes)
  }

  private def resolvedCheckpointLocation(
      hadoopConf: Configuration,
      checkpointLocation: String): String = {
    val checkpointPath = new Path(checkpointLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

  private def getLastCommittedBatch(session: SparkSession, checkpointLocation: String): Long = {
    val commitLog = new StreamingQueryCheckpointMetadata(session, checkpointLocation).commitLog
    commitLog.getLatest() match {
      case Some((lastId, _)) => lastId
      case None => throw StateDataSourceErrors.committedBatchUnavailable(checkpointLocation)
    }
  }
}

// Case class to store information around the key state encoder, col family schema and
// operator specific state used primarily for the transformWithState operator.
case class StateStoreReaderInfo(
    keyStateEncoderSpecOpt: Option[KeyStateEncoderSpec],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    transformWithStateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateSchemaProviderOpt: Option[StateSchemaProvider]
)
