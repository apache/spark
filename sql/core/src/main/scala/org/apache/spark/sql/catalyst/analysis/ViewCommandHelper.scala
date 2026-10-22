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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.{DropView, LogicalPlan}
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{DropTableCommand, DropTempViewCommand}

/**
 * Rewrites resolved view commands to their session-catalog commands in [[ResolveSessionCatalog]].
 * Views in a [[ViewCatalog]] keep the V2 command.
 */
object ViewCommandHelper {

  /**
   * Rewrites a resolved [[DropView]] to [[DropTempViewCommand]] for a temporary view or to
   * [[DropTableCommand]] for a session-catalog view, and fails for a catalog that cannot host
   * views. Returns `dropView` itself if its target is in a [[ViewCatalog]] or is not resolved.
   */
  def rewrite(dropView: DropView): LogicalPlan = dropView match {
    // Session-qualified temp view: same execution path as FakeSystemCatalog DROP VIEW.
    case DropView(ResolvedTempView(ident, _), ifExists) =>
      DropTempViewCommand(ident, ifExists)

    case DropView(DropViewInSessionCatalog(ident), ifExists) =>
      DropTableCommand(ident, ifExists, isView = true, purge = false)

    // ViewCatalog catalogs fall through to `DataSourceV2Strategy`, which routes DROP VIEW to
    // `ViewCatalog.dropView` (this also covers METRIC_VIEW since metric views are persisted
    // through the same ViewCatalog interface). Other non-session catalogs get
    // `MISSING_CATALOG_ABILITY.VIEWS`, matching the error raised from `CheckViewReferences` for
    // CREATE/ALTER VIEW and from the analyzer gate on UnresolvedView.
    case DropView(ResolvedIdentifier(catalog, ident), ifExists)
        if !catalog.isInstanceOf[ViewCatalog] =>
      if (catalog == FakeSystemCatalog) {
        DropTempViewCommand(ident, ifExists)
      } else {
        throw QueryCompilationErrors.missingCatalogViewsAbilityError(catalog)
      }

    case _ =>
      dropView
  }

  private object DropViewInSessionCatalog
    extends ResolvedIdentifierInSessionCatalog("DROP", "VIEW")
}
