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
import org.apache.spark.sql.catalyst.analysis.{NoSuchViewException, ResolvedIdentifier, ViewSchemaMode}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, ViewCatalog, ViewInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CommandUtils

/**
 * Shared bits for the v2 ALTER VIEW ... AS exec. Loads the existing view once via
 * `existingView` and uses it to preserve user-set TBLPROPERTIES, comment, collation, owner,
 * and schema binding mode when constructing the replacement [[ViewInfo]]. A racing DDL between
 * analysis and exec can change the target out from under us (dropped, or replaced with a
 * non-view table); in that case we surface a regular no-such-view / not-a-view analysis error
 * rather than propagating a stale analyzer decision.
 *
 * Transient fields (SQL configs, query column names) are re-captured from the
 * current session by [[V2ViewPreparation.buildViewInfo]], matching v1
 * `AlterViewAsCommand.alterPermanentView`. PROP_OWNER and user TBLPROPERTIES flow through
 * unchanged.
 */
private[v2] trait V2AlterViewPreparation extends V2ViewPreparation {
  protected lazy val existingView: ViewInfo = try {
    catalog.loadView(identifier)
  } catch {
    case _: NoSuchViewException =>
      // Race: the view disappeared after analysis. Surface no-such-view, or
      // expect-view-not-table if a colliding non-view table appeared in a mixed catalog.
      catalog match {
        case tc: TableCatalog if tc.tableExists(identifier) =>
          throw QueryCompilationErrors.expectViewNotTableError(
            (catalog.name() +: identifier.asMultipartIdentifier).toSeq,
            cmd = "ALTER VIEW ... AS",
            suggestAlternative = false,
            t = this)
        case _ =>
          throw new NoSuchViewException(identifier)
      }
  }

  protected lazy val existingProps: Map[String, String] =
    existingView.properties.asScala.toMap

  private def existingProp(key: String): Option[String] = existingProps.get(key)

  // ALTER VIEW ... AS does not accept a user column list.
  override def userSpecifiedColumns: Seq[(String, Option[String])] = Seq.empty
  override def comment: Option[String] = existingProp(TableCatalog.PROP_COMMENT)
  override def collation: Option[String] = existingProp(TableCatalog.PROP_COLLATION)
  // Preserve the existing view's owner (v1-parity with AlterViewAsCommand's viewMeta.copy,
  // which leaves `owner` untouched). If the existing view has no PROP_OWNER, pass it through
  // as None so the replacement ViewInfo also has no owner.
  override def owner: Option[String] = existingProp(TableCatalog.PROP_OWNER)
  override def userProperties: Map[String, String] = existingProps

  // Preserve the existing view's schema binding mode. Reuse `viewSchemaModeFromProperties`
  // for a v1-identical decode -- it honors `viewSchemaBindingEnabled` and defaults missing
  // values to SchemaBinding. We feed the typed `ViewInfo.schemaMode` String in via a
  // single-key map so the decode logic stays in one place.
  override def viewSchemaMode: ViewSchemaMode =
    CatalogTable.viewSchemaModeFromProperties(
      Option(existingView.schemaMode)
        .map(CatalogTable.VIEW_SCHEMA_MODE -> _)
        .toMap)

  /**
   * Force-evaluate `existingView` so `NoSuchViewException` / `expectViewNotTableError`
   * surfaces before any other work (e.g. `buildViewInfo`, uncache, replace). The result is
   * intentionally discarded; call this purely for its side effect of materializing the
   * lazy val.
   */
  protected def requireExistingView(): Unit = existingView
}

/**
 * Physical plan node for ALTER VIEW ... AS on a v2 [[ViewCatalog]]. Dispatches to
 * [[ViewCatalog#replaceView]], which is contractually atomic.
 */
case class AlterV2ViewExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    originalText: String,
    query: LogicalPlan) extends V2AlterViewPreparation {

  override protected def run(): Seq[InternalRow] = {
    requireExistingView()
    val info = buildViewInfo()
    // Cyclic reference detection is done at analysis time in CheckViewReferences.
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
    catalog.replaceView(identifier, info)
    Seq.empty
  }
}
