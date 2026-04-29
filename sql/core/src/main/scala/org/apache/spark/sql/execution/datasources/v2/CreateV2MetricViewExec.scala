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

import org.apache.spark.sql.catalyst.{CurrentUserContext, InternalRow}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, SchemaUnsupported, ViewAlreadyExistsException, ViewSchemaMode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{DependencyList, Identifier, TableCatalog, TableSummary, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.CommandUtils

/**
 * Physical plan node for `CREATE VIEW ... WITH METRICS` on a v2 [[ViewCatalog]]. Mirrors
 * [[CreateV2ViewExec]]'s flag handling and cross-type collision decoding via the shared
 * [[V2ViewPreparation]] trait, and adds metric-view-specific bits (typed
 * [[DependencyList]] view-dependencies and `PROP_TABLE_TYPE = METRIC_VIEW`) through the
 * trait's optional hooks.
 *
 * Routed by [[DataSourceV2Strategy]] from
 * [[org.apache.spark.sql.execution.command.CreateMetricViewCommand]] when the resolved
 * catalog is a non-session v2 catalog.
 */
case class CreateV2MetricViewExec(
    catalog: ViewCatalog,
    identifier: Identifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    userProperties: Map[String, String],
    originalText: String,
    query: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    deps: Option[DependencyList]) extends V2ViewPreparation {

  // Metric views don't carry a default-collation override.
  override def collation: Option[String] = None

  // CREATE stamps the current user, matching the v1 metric-view path (which goes through
  // ViewHelper.prepareTable -> CatalogTable.owner default) and CreateV2ViewExec.
  override def owner: Option[String] = Some(CurrentUserContext.getCurrentUser)

  // Metric views always have schema-mode UNSUPPORTED (mirroring the v1 path which passes
  // SchemaUnsupported into ViewHelper.prepareTable).
  override def viewSchemaMode: ViewSchemaMode = SchemaUnsupported

  override protected def viewDependencies: Option[DependencyList] = deps

  override protected def tableType: Option[String] =
    Some(TableSummary.METRIC_VIEW_TABLE_TYPE)

  override protected def run(): Seq[InternalRow] = {
    // CREATE VIEW IF NOT EXISTS short-circuit, identical to CreateV2ViewExec: skip the
    // `buildViewInfo` work when a view already sits at the ident. The mixed-catalog
    // "table at ident" no-op is handled in the catch block below.
    if (allowExisting && catalog.viewExists(identifier)) return Seq.empty

    val info = buildViewInfo()
    try {
      if (replace) {
        CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, identifier))
        catalog.createOrReplaceView(identifier, info)
      } else {
        catalog.createView(identifier, info)
      }
    } catch {
      case _: ViewAlreadyExistsException =>
        // Catalog refused: decode whether a table sits at the ident (cross-type collision)
        // or a view (race for plain CREATE / OR REPLACE), and emit the precise error -- or
        // no-op for IF NOT EXISTS. Same shape as CreateV2ViewExec.
        val isTable = catalog match {
          case tc: TableCatalog => tc.tableExists(identifier)
          case _ => false
        }
        if (isTable) {
          if (!allowExisting) {
            throw QueryCompilationErrors.unsupportedCreateOrReplaceViewOnTableError(
              fullNameParts, replace)
          }
          // CREATE VIEW IF NOT EXISTS over a table is a no-op (v1 parity).
        } else if (!allowExisting) {
          throw viewAlreadyExists()
        }
        // else: a view appeared between our viewExists probe and createView; IF NOT EXISTS
        // semantics make this a no-op.
    }
    Seq.empty
  }
}
