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

package org.apache.spark.sql.execution.datasources

import java.util.Locale

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, V1CreateTablePlan}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DDLUtils, LeafRunnableCommand}
import org.apache.spark.sql.execution.command.ViewHelper.createTemporaryViewRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Utils
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types._

/**
 * Create a table and optionally insert some data into it. Note that this plan is unresolved and
 * has to be replaced by the concrete implementations during analysis.
 *
 * @param tableDesc the metadata of the table to be created.
 * @param mode the data writing mode
 * @param query an optional logical plan representing data to write into the created table.
 */
case class CreateTable(
    tableDesc: CatalogTable,
    mode: SaveMode,
    query: Option[LogicalPlan]) extends LogicalPlan with V1CreateTablePlan {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def children: Seq[LogicalPlan] = query.toSeq
  override def output: Seq[Attribute] = Seq.empty
  override lazy val resolved: Boolean = false

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan =
    copy(query = if (query.isDefined) Some(newChildren.head) else None)

  /**
   * Identifies the underlying table's location is qualified or absent.
   *
   * @return true if the location is absolute or absent, false otherwise.
   */
  def locationQualifiedOrAbsent: Boolean = {
    tableDesc.storage.locationUri.map(_.isAbsolute).getOrElse(true)
  }
}

/**
 * Create or replace a local/global temporary view with given data source.
 */
case class CreateTempViewUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    replace: Boolean,
    global: Boolean,
    provider: String,
    options: Map[String, String]) extends LeafRunnableCommand {

  if (tableIdent.database.isDefined) {
    throw QueryCompilationErrors.cannotSpecifyDatabaseForTempViewError(tableIdent)
  }

  override def argString(maxFields: Int): String = {
    s"[tableIdent:$tableIdent " +
      userSpecifiedSchema.map(_.toString() + " ").getOrElse("") +
      s"replace:$replace " +
      s"provider:$provider " +
      conf.redactOptions(options)
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (provider.toLowerCase(Locale.ROOT) == DDLUtils.HIVE_PROVIDER) {
      throw QueryCompilationErrors.cannotCreateTempViewUsingHiveDataSourceError()
    }

    val catalog = sparkSession.sessionState.catalog
    val unresolvedPlan = DataSource.lookupDataSourceV2(provider, sparkSession.sessionState.conf)
      .flatMap { tblProvider =>
        DataSourceV2Utils.loadV2Source(sparkSession, tblProvider, userSpecifiedSchema,
          CaseInsensitiveMap(options), provider)
      }.getOrElse {
        val dataSource = DataSource(
          sparkSession,
          userSpecifiedSchema = userSpecifiedSchema,
          className = provider,
          options = options)
        LogicalRelation(dataSource.resolveRelation())
      }
    val analyzedPlan = sparkSession.sessionState.analyzer.execute(unresolvedPlan)

    if (global) {
      val db = sparkSession.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
      val viewIdent = TableIdentifier(tableIdent.table, Option(db))
      val viewDefinition = createTemporaryViewRelation(
        viewIdent,
        sparkSession,
        replace,
        catalog.getRawGlobalTempView,
        originalText = None,
        analyzedPlan,
        aliasedPlan = analyzedPlan,
        referredTempFunctions = Seq.empty)
      catalog.createGlobalTempView(tableIdent.table, viewDefinition, replace)
    } else {
      val viewDefinition = createTemporaryViewRelation(
        tableIdent,
        sparkSession,
        replace,
        catalog.getRawTempView,
        originalText = None,
        analyzedPlan,
        aliasedPlan = analyzedPlan,
        referredTempFunctions = Seq.empty)
      catalog.createTempView(tableIdent.table, viewDefinition, replace)
    }

    Seq.empty[Row]
  }
}

case class RefreshResource(path: String)
  extends LeafRunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.refreshByPath(path)
    Seq.empty[Row]
  }
}
