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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, RuntimeConfig, SparkSession}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.{CommitLog, OffsetSeqLog, OffsetSeqMetadata}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.{LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.state.{StateSchemaCompatibilityChecker, StateStore, StateStoreConf, StateStoreId, StateStoreProviderId}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StateDataSourceV2 extends TableProvider with DataSourceRegister {
  import StateDataSourceV2._

  private lazy val session: SparkSession = SparkSession.active

  private lazy val hadoopConf: Configuration = session.sessionState.newHadoopConf()

  override def shortName(): String = "statestore"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val checkpointLocation = Option(properties.get(PARAM_PATH)).orElse {
      throw new AnalysisException(s"'$PARAM_PATH' must be specified.")
    }.get

    val resolvedCpLocation = resolvedCheckpointLocation(checkpointLocation)

    val batchId = Option(properties.get(PARAM_BATCH_ID)).map(_.toLong).orElse {
      Some(getLastCommittedBatch(resolvedCpLocation))
    }.get

    val operatorId = Option(properties.get(PARAM_OPERATOR_ID)).map(_.toInt)
      .orElse(Some(0)).get

    val storeName = Option(properties.get(PARAM_STORE_NAME))
      .getOrElse(StateStoreId.DEFAULT_STORE_NAME)

    val joinSide = Option(properties.get(PARAM_JOIN_SIDE))
      .map(JoinSideValues.withName).getOrElse(JoinSideValues.none)

    val stateConf = buildStateStoreConf(resolvedCpLocation, batchId)

    val stateCheckpointLocation = new Path(resolvedCpLocation, "state")
    new StateTable(session, schema, stateCheckpointLocation.toString, batchId, operatorId,
      storeName, joinSide, stateConf)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val checkpointLocation = Option(options.get(PARAM_PATH)).orElse {
      throw new AnalysisException(s"'$PARAM_PATH' must be specified.")
    }.get

    val resolvedCpLocation = resolvedCheckpointLocation(checkpointLocation)

    val operatorId = Option(options.get(PARAM_OPERATOR_ID)).map(_.toInt)
      .orElse(Some(0)).get

    val partitionId = StateStore.PARTITION_ID_TO_CHECK_SCHEMA
    val storeName = Option(options.get(PARAM_STORE_NAME))
      .getOrElse(StateStoreId.DEFAULT_STORE_NAME)

    val joinSide = Option(options.get(PARAM_JOIN_SIDE))
      .map(JoinSideValues.withName).getOrElse(JoinSideValues.none)

    if (joinSide != JoinSideValues.none && storeName != StateStoreId.DEFAULT_STORE_NAME) {
      throw new IllegalArgumentException(s"The options '$PARAM_JOIN_SIDE' and " +
        s"'$PARAM_STORE_NAME' cannot be specified together. Please specify either one.")
    }

    val stateCheckpointLocation = new Path(resolvedCpLocation, "state")
    val (keySchema, valueSchema) = joinSide match {
      case JoinSideValues.left =>
        StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
          operatorId, LeftSide)

      case JoinSideValues.right =>
        StreamStreamJoinStateHelper.readKeyValueSchema(session, stateCheckpointLocation.toString,
          operatorId, RightSide)

      case JoinSideValues.none =>
        val storeId = new StateStoreId(stateCheckpointLocation.toString, operatorId, partitionId,
          storeName)
        val providerId = new StateStoreProviderId(storeId, UUID.randomUUID())
        val manager = new StateSchemaCompatibilityChecker(providerId, hadoopConf)
        manager.readSchemaFile()
    }

    new StructType()
      .add("key", keySchema)
      .add("value", valueSchema)
  }

  private def resolvedCheckpointLocation(checkpointLocation: String): String = {
    val checkpointPath = new Path(checkpointLocation)
    val fs = checkpointPath.getFileSystem(hadoopConf)
    checkpointPath.makeQualified(fs.getUri, fs.getWorkingDirectory).toUri.toString
  }

  private def buildStateStoreConf(checkpointLocation: String, batchId: Long): StateStoreConf = {
    val offsetLog = new OffsetSeqLog(session, new Path(checkpointLocation, "offsets").toString)
    offsetLog.get(batchId) match {
      case Some(value) =>
        val metadata = value.metadata.getOrElse(
          throw new IllegalStateException(s"Metadata is not available for offset log for $batchId")
        )

        val clonedRuntimeConf = new RuntimeConfig(session.sessionState.conf.clone())
        OffsetSeqMetadata.setSessionConf(metadata, clonedRuntimeConf)
        StateStoreConf(clonedRuntimeConf.sqlConf)

      case _ =>
        throw new AnalysisException(s"The offset log for $batchId does not exist")
    }
  }

  private def getLastCommittedBatch(checkpointLocation: String): Long = {
    val commitLog = new CommitLog(session, new Path(checkpointLocation, "commits").toString)
    commitLog.getLatest() match {
      case Some((lastId, _)) => lastId
      case None => throw new AnalysisException("No committed batch found.")
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
}
