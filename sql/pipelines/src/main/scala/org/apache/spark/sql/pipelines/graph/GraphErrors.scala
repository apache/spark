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

package org.apache.spark.sql.pipelines.graph

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.pipelines.common.DatasetType
import org.apache.spark.sql.types.StructType

/** Collection of errors that can be thrown during graph resolution / analysis. */
object GraphErrors {

  def pipelineLocalDatasetNotDefinedError(datasetName: String): SparkException = {
    // TODO: this should be an internal error, as we never expect this to happen
    new SparkException(
      errorClass = "PIPELINE_LOCAL_DATASET_NOT_DEFINED",
      messageParameters = Map("datasetName" -> datasetName),
      cause = null
    )
  }

  /**
   * Throws when the catalog or schema name in the "USE CATALOG | SCHEMA" command is invalid
   *
   * @param command string "USE CATALOG" or "USE SCHEMA"
   * @param name the invalid catalog or schema name
   * @param reason the reason why the name is invalid
   */
  def invalidNameInUseCommandError(
      command: String,
      name: String,
      reason: String
  ): SparkException = {
    new SparkException(
      errorClass = "INVALID_NAME_IN_USE_COMMAND",
      messageParameters = Map("command" -> command, "name" -> name, "reason" -> reason),
      cause = null
    )
  }

  def unresolvedTablePath(identifier: TableIdentifier): SparkException = {
    new SparkException(
      errorClass = "UNRESOLVED_TABLE_PATH",
      messageParameters = Map("identifier" -> identifier.toString),
      cause = null
    )
  }

  def incompatibleUserSpecifiedAndInferredSchemasError(
      tableIdentifier: TableIdentifier,
      datasetType: DatasetType,
      specifiedSchema: StructType,
      inferredSchema: StructType,
      cause: Option[Throwable] = None
  ): AnalysisException = {
    val streamingTableHint =
      if (datasetType == DatasetType.STREAMING_TABLE) {
        s""""
           |Streaming tables are stateful and remember data that has already been
           |processed. If you want to recompute the table from scratch, please full refresh
           |the table.
              """.stripMargin
      } else {
        ""
      }

    new AnalysisException(
      errorClass = "USER_SPECIFIED_AND_INFERRED_SCHEMA_NOT_COMPATIBLE",
      messageParameters = Map(
        "tableName" -> tableIdentifier.unquotedString,
        "streamingTableHint" -> streamingTableHint,
        "specifiedSchema" -> specifiedSchema.treeString,
        "inferredDataSchema" -> inferredSchema.treeString
      ),
      cause = Option(cause.orNull)
    )
  }

  def unableToInferSchemaError(
      tableIdentifier: TableIdentifier,
      inferredSchema: StructType,
      incompatibleSchema: StructType,
      cause: Option[Throwable] = None
  ): AnalysisException = {
    new AnalysisException(
      errorClass = "UNABLE_TO_INFER_PIPELINE_TABLE_SCHEMA",
      messageParameters = Map(
        "tableName" -> tableIdentifier.unquotedString,
        "inferredDataSchema" -> inferredSchema.treeString,
        "incompatibleDataSchema" -> incompatibleSchema.treeString
      ),
      cause = Option(cause.orNull)
    )
  }

  def persistedViewReadsFromTemporaryView(
      persistedViewIdentifier: TableIdentifier,
      temporaryViewIdentifier: TableIdentifier): AnalysisException = {
    new AnalysisException(
      "PERSISTED_VIEW_READS_FROM_TEMPORARY_VIEW",
      Map(
        "persistedViewName" -> persistedViewIdentifier.toString,
        "temporaryViewName" -> temporaryViewIdentifier.toString
      )
    )
  }
}
