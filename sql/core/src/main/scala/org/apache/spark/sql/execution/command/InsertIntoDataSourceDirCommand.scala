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

import java.util.UUID

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2Utils, OverwriteByExpressionExec, WriteToDataSourceV2Exec}
import org.apache.spark.sql.sources.v2.TableProvider
import org.apache.spark.sql.sources.v2.writer.SupportsSaveMode

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
abstract class InsertIntoDataSourceDirCommand(
    storage: CatalogStorageFormat,
    provider: String,
    query: LogicalPlan,
    overwrite: Boolean) extends RunnableCommand {

  protected override def innerChildren: Seq[QueryPlan[_]] = query :: Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(storage.locationUri.nonEmpty, "Directory path is required")
    assert(provider.nonEmpty, "Data source is required")

    // Create the relation based on the input logical plan: `query`.
    val pathOption = storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    write(sparkSession, pathOption)
    Seq.empty[Row]
  }

  protected def write(sparkSession: SparkSession, pathOption: Option[(String, String)]): Unit
}

object InsertIntoDataSourceDirCommand {
  def apply(
      storage: CatalogStorageFormat,
      provider: String,
      query: LogicalPlan,
      overwrite: Boolean): InsertIntoDataSourceDirCommand = {
    val sparkSession = SparkSession.getActiveSession.get
    // Create the relation based on the input logical plan: `query`.
    val pathOption = storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
    if (DataSourceV2Utils.shouldWriteWithV2(
      sparkSession, Some(query.schema), provider, pathOption.toMap)) {
      InsertIntoDataSourceV2DirCommand
    }
  }
}
/**
 * Write the result of a query to a data source V1 directory.
 */
case class InsertIntoDataSourceV1DirCommand(
    storage: CatalogStorageFormat,
    provider: String,
    query: LogicalPlan,
    overwrite: Boolean)
  extends InsertIntoDataSourceDirCommand(storage, provider, query, overwrite) {

  override protected def write(
      sparkSession: SparkSession,
      pathOption: Option[(String, String)]): Unit = {
    val dataSource = DataSource(
      sparkSession,
      className = provider,
      options = storage.properties ++ pathOption,
      catalogTable = None)

    val isFileFormat = classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
    if (!isFileFormat) {
      throw new SparkException(
        "Only Data Sources providing FileFormat are supported: " + dataSource.providingClass)
    }

    val saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.ErrorIfExists
    try {
      sparkSession.sessionState.executePlan(dataSource.planForWriting(saveMode, query)).toRdd
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to directory " + storage.locationUri.toString, ex)
        throw ex
    }
  }
}

/**
 * Write the result of a query to a data source V2 directory.
 */
case class InsertIntoDataSourceV2DirCommand(
    storage: CatalogStorageFormat,
    provider: String,
    query: LogicalPlan,
    overwrite: Boolean)
  extends InsertIntoDataSourceDirCommand(storage, provider, query, overwrite) {

  override protected def write(
      sparkSession: SparkSession,
      pathOption: Option[(String, String)]): Unit = {
    import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

    val cls = DataSourceV2Utils.isV2Source(sparkSession, provider)
    val tableProvider = cls.get.getConstructor().newInstance().asInstanceOf[TableProvider]
    val dsOptions = DataSourceV2Utils.
      extractSessionConfigs(tableProvider, sparkSession.sessionState.conf, pathOption.toMap)
    val data = sparkSession.sessionState.executePlan(query).executedPlan
    val writeTable = DataSourceV2Utils.
      getBatchWriteTable(sparkSession, Option(data.schema), cls.get, dsOptions)
    val writeExec = if (overwrite) {
      val relation = DataSourceV2Relation.create(writeTable.get, dsOptions)
      OverwriteByExpressionExec(relation.table.asWritable, Array.empty, dsOptions, data)
    } else {
      writeTable.get.newWriteBuilder(dsOptions) match {
        case writeBuilder: SupportsSaveMode =>
          val write = writeBuilder.mode(SaveMode.ErrorIfExists)
            .withQueryId(UUID.randomUUID().toString)
            .withInputDataSchema(data.schema)
            .buildForBatch()
          WriteToDataSourceV2Exec(write, data)
        case _ => throw new SparkException(
          "Insert into data source dir should support save mode or overwrite")
      }
    }
    writeExec.execute().count()
  }
}

