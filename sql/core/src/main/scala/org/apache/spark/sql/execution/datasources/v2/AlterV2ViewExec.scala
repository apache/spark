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

package org.apache.spark.sql.execution.datasources.v2

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, SchemaBinding, SchemaCompensation, SchemaEvolution, SchemaTypeEvolution, SchemaUnsupported, TableAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable.VIEW_SCHEMA_MODE
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, TableCatalog, TableInfo}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CommandUtils, ViewHelper}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Utils

/**
 * Shared bits for the v2 ALTER VIEW ... AS execs. Loads the existing view once via
 * `existingInfo` and uses its properties to preserve user-set properties, comment, collation,
 * and schema-binding mode when constructing the replacement `TableInfo`. A v2 identifier that
 * does not resolve to a [[MetadataOnlyTable]] is rejected — the connector contract for catalogs
 * with `SUPPORTS_VIEW` is to round-trip `MetadataOnlyTable` from `loadTable`.
 *
 * `generateViewProperties` (invoked from `buildTableInfo`) strips the transient view keys
 * (SQL configs, query column names, referred-temp names) from the inherited properties and
 * re-emits them from the current session, matching v1 `AlterViewAsCommand.alterPermanentView`.
 */
private[v2] trait V2AlterViewPreparation extends V2ViewPreparation {
  protected lazy val existingInfo: TableInfo = {
    val table = try {
      catalog.loadTable(identifier)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(catalog.name(), identifier)
    }
    table match {
      case mot: MetadataOnlyTable => mot.getTableInfo
      case other =>
        // SUPPORTS_VIEW requires catalogs to round-trip MetadataOnlyTable; getting
        // anything else back is a catalog contract violation.
        throw SparkException.internalError(
          s"Expected MetadataOnlyTable from $catalog for $identifier, " +
            s"got ${other.getClass.getName}")
    }
  }

  private def existingProp(key: String): Option[String] =
    Option(existingInfo.properties.get(key))

  // ALTER VIEW ... AS does not accept a user column list.
  override def userSpecifiedColumns: Seq[(String, Option[String])] = Seq.empty
  override def comment: Option[String] = existingProp(TableCatalog.PROP_COMMENT)
  override def collation: Option[String] = existingProp(TableCatalog.PROP_COLLATION)
  // Strip reserved keys; those become first-class `TableInfo` / `CatalogTable` fields or are
  // re-emitted by `buildTableInfo` (view text, current-catalog-namespace, comment, collation).
  // User TBLPROPERTIES and view.sqlConfig.* / view.query.out.* / view.referredTempNames /
  // view.schemaMode pass through — generateViewProperties handles their cleanup + re-emit.
  override def userProperties: Map[String, String] =
    existingInfo.properties.asScala.toMap -- CatalogV2Util.TABLE_RESERVED_PROPERTIES

  override def viewSchemaMode: ViewSchemaMode = {
    existingProp(VIEW_SCHEMA_MODE) match {
      case Some(s) if s == SchemaBinding.toString => SchemaBinding
      case Some(s) if s == SchemaEvolution.toString => SchemaEvolution
      case Some(s) if s == SchemaTypeEvolution.toString => SchemaTypeEvolution
      case Some(s) if s == SchemaCompensation.toString => SchemaCompensation
      case _ => SchemaUnsupported
    }
  }
}

/**
 * Non-atomic ALTER VIEW for a plain [[TableCatalog]]: load existing, build replacement,
 * check cyclic reference, uncache, drop, create. Between drop and create the view does not
 * exist — catalogs that need atomicity should also implement [[StagingTableCatalog]].
 */
case class AlterV2ViewExec(
    catalog: TableCatalog,
    identifier: Identifier,
    originalText: String,
    query: LogicalPlan,
    referredTempFunctions: Seq[String]) extends V2AlterViewPreparation {

  override protected def run(): Seq[InternalRow] = {
    // Force the lazy to load before building; surfaces NoSuchTableException as a proper error.
    val _ = existingInfo
    val info = buildTableInfo()
    ViewHelper.checkCyclicViewReference(query, Seq(legacyName), legacyName)
    CommandUtils.uncacheTableOrView(session, legacyName)
    catalog.dropTable(identifier)
    try {
      catalog.createTable(identifier, info)
    } catch {
      case _: TableAlreadyExistsException => throw viewAlreadyExists()
    }
    Seq.empty
  }
}

/**
 * Atomic ALTER VIEW for a [[StagingTableCatalog]]: uses `stageReplace` + commit so the view
 * metadata swap is atomic against concurrent readers. `stageReplace` throws
 * [[NoSuchTableException]] when the view does not exist; we surface that as the standard
 * no-such-table error.
 */
case class AtomicAlterV2ViewExec(
    catalog: StagingTableCatalog,
    identifier: Identifier,
    originalText: String,
    query: LogicalPlan,
    referredTempFunctions: Seq[String]) extends V2AlterViewPreparation {

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    val _ = existingInfo
    val info = buildTableInfo()
    ViewHelper.checkCyclicViewReference(query, Seq(legacyName), legacyName)
    CommandUtils.uncacheTableOrView(session, legacyName)
    val staged: StagedTable = try {
      catalog.stageReplace(identifier, info)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(catalog.name(), identifier)
    }
    Utils.tryWithSafeFinallyAndFailureCallbacks({
      DataSourceV2Utils.commitStagedChanges(sparkContext, staged, metrics)
    })(catchBlock = {
      staged.abortStagedChanges()
    })
    Seq.empty
  }
}
