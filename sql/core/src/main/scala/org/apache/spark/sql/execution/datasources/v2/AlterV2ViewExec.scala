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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, ResolvedIdentifier, TableAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, MetadataOnlyTable, StagedTable, StagingTableCatalog, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.Utils

/**
 * Shared bits for the v2 ALTER VIEW ... AS execs. Loads the existing view once via
 * `existingTable` and uses its properties to preserve user-set properties, comment, collation,
 * and schema-binding mode when constructing the replacement `TableInfo`. A racing DDL between
 * analysis and exec can change the target out from under us (dropped, or replaced with a
 * non-view table); in that case we surface a regular no-such-table / not-a-view analysis
 * error rather than propagating a stale analyzer decision.
 *
 * `generateViewProperties` (invoked from `buildTableInfo`) strips the transient view keys
 * (SQL configs, query column names, referred-temp names) from the inherited properties and
 * re-emits them from the current session, matching v1 `AlterViewAsCommand.alterPermanentView`.
 */
private[v2] trait V2AlterViewPreparation extends V2ViewPreparation {
  protected lazy val existingTable: MetadataOnlyTable = {
    val table = try {
      catalog.loadTable(identifier)
    } catch {
      case _: NoSuchTableException =>
        throw QueryCompilationErrors.noSuchTableError(catalog.name(), identifier)
    }
    table match {
      case mot: MetadataOnlyTable if isViewTable(mot) => mot
      case _ =>
        // Analyzer verified this was a view, but a racing DDL (drop + recreate as a
        // non-view table, or a catalog that now returns a different Table subclass for this
        // identifier) can invalidate that. Surface as a user-facing error.
        throw QueryCompilationErrors.expectViewNotTableError(
          (catalog.name() +: identifier.asMultipartIdentifier).toSeq,
          cmd = "ALTER VIEW ... AS",
          suggestAlternative = false,
          t = this)
    }
  }

  // Carry the existing view's full property map forward. Keys the ALTER actually changes are
  // overwritten downstream: view text + PROP_TABLE_TYPE via `withViewText`, comment / collation
  // via `withComment` / `withCollation`, view.sqlConfig.* / view.query.out.* /
  // view.referredTempNames re-emitted by `generateViewProperties`, and
  // PROP_VIEW_CURRENT_CATALOG_AND_NAMESPACE re-emitted by the v2 encoder inside
  // `buildTableInfo`. Everything else -- notably PROP_OWNER and view.schemaMode -- flows
  // through unchanged, matching v1 `AlterViewAsCommand.alterPermanentView`'s `viewMeta.copy`
  // semantics.
  protected lazy val existingProps: Map[String, String] =
    existingTable.getTableInfo.properties.asScala.toMap

  private def existingProp(key: String): Option[String] = existingProps.get(key)

  // ALTER VIEW ... AS does not accept a user column list.
  override def userSpecifiedColumns: Seq[(String, Option[String])] = Seq.empty
  override def comment: Option[String] = existingProp(TableCatalog.PROP_COMMENT)
  override def collation: Option[String] = existingProp(TableCatalog.PROP_COLLATION)
  override def userProperties: Map[String, String] = existingProps

  // Read the schema binding mode directly from the properties map; shares decoding with
  // the v1 path via `CatalogTable.viewSchemaModeFromProperties` (honors
  // viewSchemaBindingEnabled and the same default when the property is absent).
  override def viewSchemaMode: ViewSchemaMode =
    CatalogTable.viewSchemaModeFromProperties(existingProps)

  /**
   * Force-evaluate `existingTable` so `NoSuchTableException` / `expectViewNotTableError`
   * surfaces before any other work (e.g. `buildTableInfo`, uncache, drop). The result is
   * intentionally discarded; call this purely for its side effect of materializing the
   * lazy val.
   */
  protected def requireExistingView(): Unit = existingTable
}

/**
 * Non-atomic ALTER VIEW for a plain [[TableCatalog]]: load existing, build replacement,
 * check cyclic reference, uncache, drop, create. Between drop and create the view does not
 * exist -- catalogs that need atomicity should also implement [[StagingTableCatalog]].
 */
case class AlterV2ViewExec(
    catalog: TableCatalog,
    identifier: Identifier,
    originalText: String,
    query: LogicalPlan) extends V2AlterViewPreparation {

  override protected def run(): Seq[InternalRow] = {
    requireExistingView()
    val info = buildTableInfo()
    // Cyclic reference detection is done at analysis time in CheckViewReferences.
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
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
    query: LogicalPlan) extends V2AlterViewPreparation {

  override val metrics: Map[String, SQLMetric] =
    DataSourceV2Utils.commitMetrics(sparkContext, catalog)

  override protected def run(): Seq[InternalRow] = {
    requireExistingView()
    val info = buildTableInfo()
    // Cyclic reference detection is done at analysis time in CheckViewReferences.
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
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
