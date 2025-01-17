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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{MetadataColumn, SupportsMetadataColumns, SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.v2.state.StateSourceOptions.JoinSideValues
import org.apache.spark.sql.execution.datasources.v2.state.utils.SchemaUtil
import org.apache.spark.sql.execution.streaming.TransformWithStateVariableInfo
import org.apache.spark.sql.execution.streaming.state.{KeyStateEncoderSpec, StateSchemaProvider, StateStoreColFamilySchema, StateStoreConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * An implementation of [[Table]] with [[SupportsRead]] for State Store data source.
 * @param stateSchemaProviderOpt Optional provider that maintains mapping between schema IDs and
 *                               their corresponding schemas, enabling reading of state data
 *                               written with older schema versions
 */
class StateTable(
    session: SparkSession,
    override val schema: StructType,
    sourceOptions: StateSourceOptions,
    stateConf: StateStoreConf,
    keyStateEncoderSpec: KeyStateEncoderSpec,
    stateVariableInfoOpt: Option[TransformWithStateVariableInfo],
    stateStoreColFamilySchemaOpt: Option[StateStoreColFamilySchema],
    stateSchemaProviderOpt: Option[StateSchemaProvider])
  extends Table with SupportsRead with SupportsMetadataColumns {

  import StateTable._

  if (!SchemaUtil.isValidSchema(sourceOptions, schema, stateVariableInfoOpt)) {
    throw StateDataSourceErrors.internalError(
      s"Invalid schema is provided. Provided schema: $schema for " +
        s"checkpoint location: ${sourceOptions.stateCheckpointLocation} , operatorId: " +
        s"${sourceOptions.operatorId} , storeName: ${sourceOptions.storeName}, " +
        s"joinSide: ${sourceOptions.joinSide}")
  }

  override def name(): String = {
    var desc = s"StateTable " +
      s"[stateCkptLocation=${sourceOptions.stateCheckpointLocation}]" +
      s"[batchId=${sourceOptions.batchId}][operatorId=${sourceOptions.operatorId}]" +
      s"[storeName=${sourceOptions.storeName}]"

    if (sourceOptions.joinSide != JoinSideValues.none) {
      desc += s"[joinSide=${sourceOptions.joinSide}]"
    }
    sourceOptions.fromSnapshotOptions match {
      case Some(fromSnapshotOptions) =>
        desc += s"[snapshotStartBatchId=${fromSnapshotOptions.snapshotStartBatchId}]"
        desc += s"[snapshotPartitionId=${fromSnapshotOptions.snapshotPartitionId}]"
      case _ =>
    }
    sourceOptions.readChangeFeedOptions match {
      case Some(fromSnapshotOptions) =>
        desc += s"[changeStartBatchId=${fromSnapshotOptions.changeStartBatchId}"
        desc += s"[changeEndBatchId=${fromSnapshotOptions.changeEndBatchId}"
      case _ =>
    }
    desc
  }

  override def capabilities(): util.Set[TableCapability] = CAPABILITY

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new StateScanBuilder(session, schema, sourceOptions, stateConf, keyStateEncoderSpec,
      stateVariableInfoOpt, stateStoreColFamilySchemaOpt, stateSchemaProviderOpt)

  override def properties(): util.Map[String, String] = Map.empty[String, String].asJava

  override def metadataColumns(): Array[MetadataColumn] = Array.empty
}

/**
 * Companion object for StateTable class to place constants and nested objects.
 * Currently storing capability of the table and the definition of metadata column(s).
 */
object StateTable {
  private val CAPABILITY = Set(TableCapability.BATCH_READ).asJava
}
