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

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import org.apache.spark.sql.catalyst.DataSourceOptions
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog, OffsetSeqMetadata}
import org.apache.spark.sql.execution.streaming.StreamingCheckpointConstants.{DIR_NAME_COMMITS, DIR_NAME_OFFSETS, DIR_NAME_STATE}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

/**
 * An implementation of [[TableProvider]] with [[DataSourceRegister]] for State Store data source.
 */
class StateDataSource extends TableProvider with DataSourceRegister {
  private lazy val session: SparkSession = SparkSession.active

  private lazy val hadoopConf: Configuration = session.sessionState.newHadoopConf()

  private lazy val serializedHadoopConf = new SerializableConfiguration(hadoopConf)

  override def shortName(): String = "statestore"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, properties)
    val stateConf = buildStateStoreConf(sourceOptions.resolvedCpLocation, sourceOptions.batchId)
    // Read the operator metadata once to see if we can find the information for prefix scan
    // encoder used in session window aggregation queries.
    val allStateStoreMetadata = new StateMetadataPartitionReader(
      sourceOptions.stateCheckpointLocation.getParent.toString, serializedHadoopConf)
      .stateMetadata.toArray
    val stateStoreMetadata = allStateStoreMetadata.filter { entry =>
      entry.operatorId == sourceOptions.operatorId &&
        entry.stateStoreName == sourceOptions.storeName
    }

    new StateTable(session, schema, sourceOptions, stateConf, stateStoreMetadata)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, options)

    val stateCheckpointLocation = sourceOptions.stateCheckpointLocation
    try {
      val (keySchema, valueSchema) = sourceOptions.joinSide match {
        case JoinSideValues.left =>
          StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
            sourceOptions.operatorId, LeftSide)

        case JoinSideValues.right =>
          StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
            sourceOptions.operatorId, RightSide)

        case JoinSideValues.none =>
          val storeId = new StateStoreId(stateCheckpointLocation.toString, sourceOptions.operatorId,
            partitionId, sourceOptions.storeName)
          val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())
          val manager = new StateSchemaCompatibilityChecker(providerId, hadoopConf)
          val stateSchema = manager.readSchemaFile().head
          (stateSchema.keySchema, stateSchema.valueSchema)
      }

      if (sourceOptions.readChangeFeed) {
        new StructType()
          .add("batch_id", LongType)
          .add("change_type", StringType)
          .add("key", keySchema)
          .add("value", valueSchema)
          .add("partition_id", IntegerType)
      } else {
        new StructType()
          .add("key", keySchema)
          .add("value", valueSchema)
          .add("partition_id", IntegerType)
      }

    } catch {
      case NonFatal(e) =>
        throw StateDataSourceErrors.failedToReadStateSchema(sourceOptions, e)
    }
  }

  private def buildStateStoreConf(checkpointLocation: String, batchId: Long): StateStoreConf = {
    val offsetLog = new OffsetSeqLog(session,
      new Path(checkpointLocation, DIR_NAME_OFFSETS).toString)
    offsetLog.get(batchId) match {
      case Some(value) =>
        val metadata = value.metadata.getOrElse(
          throw StateDataSourceErrors.offsetMetadataLogUnavailable(batchId, checkpointLocation)
        )

        val clonedRuntimeConf = new RuntimeConfig(session.sessionState.conf.clone())
        OffsetSeqMetadata.setSessionConf(metadata, clonedRuntimeConf)
        StateStoreConf(clonedRuntimeConf.sqlConf)

      case _ =>
        throw StateDataSourceErrors.offsetLogUnavailable(batchId, checkpointLocation)
    }
  }

  override def supportsExternalMetadata(): Boolean = false
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
    readChangeFeedOptions: Option[ReadChangeFeedOptions]) {
  def stateCheckpointLocation: Path = new Path(resolvedCpLocation, DIR_NAME_STATE)

  override def toString: String = {
    var desc = s"StateSourceOptions(checkpointLocation=$resolvedCpLocation, batchId=$batchId, " +
      s"operatorId=$operatorId, storeName=$storeName, joinSide=$joinSide"
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
      readChangeFeed, fromSnapshotOptions, readChangeFeedOptions)
  }

  private def resolvedCheckpointLocation(
      hadoopConf: Configuration,
      checkpointLocation: String): String = {
    val checkpointPath = new Path(checkpointLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

  private def getLastCommittedBatch(session: SparkSession, checkpointLocation: String): Long = {
    val commitLog = new CommitLog(session,
      new Path(checkpointLocation, DIR_NAME_COMMITS).toString)
    commitLog.getLatest() match {
      case Some((lastId, _)) => lastId
      case None => throw StateDataSourceErrors.committedBatchUnavailable(checkpointLocation)
    }
  }
}
