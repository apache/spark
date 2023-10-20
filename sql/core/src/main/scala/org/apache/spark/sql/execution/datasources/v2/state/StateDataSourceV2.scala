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
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.v2.state.StateDataSourceV2.JoinSideValues.JoinSideValues
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog, OffsetSeqMetadata}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An implementation of [[TableProvider]] with [[DataSourceRegister]] for State Store data source.
 */
class StateDataSourceV2 extends TableProvider with DataSourceRegister {
  import StateDataSourceV2._

  private lazy val session: SparkSession = SparkSession.active

  private lazy val hadoopConf: Configuration = session.sessionState.newHadoopConf()

  override def shortName(): String = "statestore"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, properties)
    val stateConf = buildStateStoreConf(sourceOptions.resolvedCpLocation, sourceOptions.batchId)
    new StateTable(session, schema, sourceOptions, stateConf)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val sourceOptions = StateSourceOptions.apply(session, hadoopConf, options)
    if (sourceOptions.joinSide != JoinSideValues.none &&
        sourceOptions.storeName != StateStoreId.DEFAULT_STORE_NAME) {
      throw new IllegalArgumentException(s"The options '$PARAM_JOIN_SIDE' and " +
        s"'$PARAM_STORE_NAME' cannot be specified together. Please specify either one.")
    }

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
          manager.readSchemaFile()
      }

      new StructType()
        .add("key", keySchema)
        .add("value", valueSchema)
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException("Fail to read the state schema. Either the file " +
          s"does not exist, or the file is corrupted. options: $sourceOptions", e)
    }
  }

  private def buildStateStoreConf(checkpointLocation: String, batchId: Long): StateStoreConf = {
    val offsetLog = new OffsetSeqLog(session, new Path(checkpointLocation, "offsets").toString)
    offsetLog.get(batchId) match {
      case Some(value) =>
        val metadata = value.metadata.getOrElse(
          throw new IllegalStateException(s"Metadata is not available for offset log for " +
            s"$batchId, checkpoint location $checkpointLocation")
        )

        val clonedRuntimeConf = new RuntimeConfig(session.sessionState.conf.clone())
        OffsetSeqMetadata.setSessionConf(metadata, clonedRuntimeConf)
        StateStoreConf(clonedRuntimeConf.sqlConf)

      case _ =>
        throw new IllegalStateException(s"The offset log for $batchId does not exist, " +
          s"checkpoint location $checkpointLocation")
    }
  }

  override def supportsExternalMetadata(): Boolean = false
}

object StateDataSourceV2 {
  val PARAM_PATH = "path"
  val PARAM_BATCH_ID = "batchId"
  val PARAM_OPERATOR_ID = "operatorId"
  val PARAM_STORE_NAME = "storeName"
  val PARAM_JOIN_SIDE = "joinSide"

  object JoinSideValues extends Enumeration {
    type JoinSideValues = Value
    val left, right, none = Value
  }

  case class StateSourceOptions(
      resolvedCpLocation: String,
      batchId: Long,
      operatorId: Int,
      storeName: String,
      joinSide: JoinSideValues) {
    def stateCheckpointLocation: Path = new Path(resolvedCpLocation, "state")
  }

  object StateSourceOptions {
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
      val checkpointLocation = Option(options.get(PARAM_PATH)).orElse {
        throw new IllegalArgumentException(s"'$PARAM_PATH' must be specified.")
      }.get

      val resolvedCpLocation = resolvedCheckpointLocation(hadoopConf, checkpointLocation)

      val batchId = Option(options.get(PARAM_BATCH_ID)).map(_.toLong).orElse {
        Some(getLastCommittedBatch(sparkSession, resolvedCpLocation))
      }.get

      if (batchId < 0) {
        throw new IllegalArgumentException(s"'${PARAM_BATCH_ID} cannot be negative.")
      }

      val operatorId = Option(options.get(PARAM_OPERATOR_ID)).map(_.toInt)
        .orElse(Some(0)).get

      if (operatorId < 0) {
        throw new IllegalArgumentException(s"'${PARAM_OPERATOR_ID} cannot be negative.")
      }

      val storeName = Option(options.get(PARAM_STORE_NAME))
        .getOrElse(StateStoreId.DEFAULT_STORE_NAME)

      val joinSide = try {
        Option(options.get(PARAM_JOIN_SIDE))
          .map(JoinSideValues.withName).getOrElse(JoinSideValues.none)
      } catch {
        case _: NoSuchElementException =>
          // convert to IllegalArgumentException
          throw new IllegalArgumentException(s"Incorrect value of the option " +
            s"'$PARAM_JOIN_SIDE'. Valid values are ${JoinSideValues.values.mkString(",")}")
      }

      if (joinSide != JoinSideValues.none && storeName != StateStoreId.DEFAULT_STORE_NAME) {
        throw new IllegalArgumentException(s"The options '$PARAM_JOIN_SIDE' and " +
          s"'$PARAM_STORE_NAME' cannot be specified together. Please specify either one.")
      }

      StateSourceOptions(resolvedCpLocation, batchId, operatorId, storeName, joinSide)
    }

    private def resolvedCheckpointLocation(
        hadoopConf: Configuration,
        checkpointLocation: String): String = {
      val checkpointPath = new Path(checkpointLocation)
      val fs = checkpointPath.getFileSystem(hadoopConf)
      checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
    }

    private def getLastCommittedBatch(session: SparkSession, checkpointLocation: String): Long = {
      val commitLog = new CommitLog(session, new Path(checkpointLocation, "commits").toString)
      commitLog.getLatest() match {
        case Some((lastId, _)) => lastId
        case None => throw new IllegalStateException("No committed batch found, " +
          s"checkpoint location: $checkpointLocation")
      }
    }
  }
}
