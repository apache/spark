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

package org.apache.spark.sql.execution.command

import org.apache.spark.internal.LogKeys._
import org.apache.spark.internal.MDC
import org.apache.spark.sql.{AnalysisException, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{CTEInChildren, CTERelationDef, LogicalPlan, WithCTE}
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources._

/**
 * A command used to write the result of a query to a directory.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   INSERT OVERWRITE DIRECTORY (path=STRING)?
 *   USING format OPTIONS ([option1_name "option1_value", option2_name "option2_value", ...])
 *   SELECT ...
 * }}}
 *
 * @param storage storage format used to describe how the query result is stored.
 * @param provider the data source type to be used
 * @param query the logical plan representing data to write to
 * @param overwrite whether overwrites existing directory
 */
case class InsertIntoDataSourceDirCommand(
    storage: CatalogStorageFormat,
    provider: String,
    query: LogicalPlan,
    overwrite: Boolean) extends LeafRunnableCommand with CTEInChildren {

  override def innerChildren: Seq[LogicalPlan] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(storage.locationUri.nonEmpty, "Directory path is required")
    assert(provider.nonEmpty, "Data source is required")

    // Create the relation based on the input logical plan: `query`.
    val pathOption = storage.locationUri.map("path" -> CatalogUtils.URIToString(_))

    val dataSource = DataSource(
      sparkSession,
      className = provider,
      options = storage.properties ++ pathOption,
      catalogTable = None)

    val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
    if (!isFileFormat) {
      throw QueryExecutionErrors.onlySupportDataSourcesProvidingFileFormatError(
        dataSource.providingClass.toString)
    }

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    try {
      sparkSession.sessionState.executePlan(dataSource.planForWriting(saveMode, query)).toRdd
    } catch {
      case ex: AnalysisException =>
        logError(log"Failed to write to directory ${MDC(URI, storage.locationUri.toString)}", ex)
        throw ex
    }

    Seq.empty[Row]
  }

  override def withCTEDefs(cteDefs: Seq[CTERelationDef]): LogicalPlan = {
    copy(query = WithCTE(query, cteDefs))
  }
}
