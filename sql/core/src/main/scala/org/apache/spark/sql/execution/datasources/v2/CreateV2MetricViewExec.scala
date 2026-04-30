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

import org.apache.spark.sql.catalyst.CurrentUserContext
import org.apache.spark.sql.catalyst.analysis.{SchemaUnsupported, ViewSchemaMode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{DependencyList, Identifier, TableSummary, ViewCatalog}

/**
 * Physical plan node for `CREATE VIEW ... WITH METRICS` on a v2 [[ViewCatalog]]. Inherits the
 * shared CREATE-side `run()` (viewExists short-circuit, OR REPLACE, cross-type collision
 * decoding) from [[V2CreateViewPreparation]]; only supplies the metric-view-specific bits
 * (no collation, schema-mode UNSUPPORTED, typed view dependencies, `PROP_TABLE_TYPE =
 * METRIC_VIEW`) via the [[V2ViewPreparation]] hooks.
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
    deps: Option[DependencyList]) extends V2CreateViewPreparation {

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

  // The analyzer attaches `metric_view.type` / `metric_view.expr` keys to each output
  // attribute's metadata; `aliasPlan`'s default re-projection drops them when the user
  // supplies a column-rename clause. Mirror v1 `ViewHelper.prepareTable(isMetricView = true)`
  // by retaining metadata across the rename.
  override protected def retainColumnMetadata: Boolean = true
}
