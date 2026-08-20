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

import org.apache.spark.sql.catalyst.plans.logical.{CreateView, DropView, LogicalPlan}
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, RelationCatalog, V1View, ViewCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Util.isSessionCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.{CreateViewCommand, DropTableCommand, DropTempViewCommand}

/**
 * Rewrites resolved view commands to their session-catalog commands in [[ResolveSessionCatalog]].
 * Native views in a [[ViewCatalog]] keep the V2 command, including in a custom session catalog.
 */
object ViewCommandHelper {

  /**
   * Rewrites a resolved [[DropView]] to [[DropTempViewCommand]] for a temporary view or to
   * [[DropTableCommand]] for a V1 session-catalog view, matching temporary views first. Fails if a
   * V1 session-catalog target's namespace does not have exactly one part.
   *
   * For remaining resolved targets outside a [[ViewCatalog]], [[FakeSystemCatalog]] uses
   * [[DropTempViewCommand]], sharing the execution path for session-qualified temporary views.
   * Other catalogs fail with `MISSING_CATALOG_ABILITY.VIEWS`, matching `CheckViewReferences` for
   * CREATE/ALTER VIEW and the analyzer gate on [[UnresolvedView]].
   *
   * Returns `dropView` itself for native [[ViewCatalog]] targets or unresolved targets.
   * For native view catalogs, `DataSourceV2Strategy` routes DROP VIEW to
   * `ViewCatalog.dropView`, including metric views persisted through the same interface.
   */
  def rewrite(dropView: DropView): LogicalPlan = dropView match {
    case DropView(ResolvedTempView(ident, _), ifExists) =>
      DropTempViewCommand(ident, ifExists)

    case DropView(DropViewInSessionCatalog(ident), ifExists) =>
      DropTableCommand(ident, ifExists, isView = true, purge = false)

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

  /**
   * Lowers a V1 session-catalog [[CreateView]] to [[CreateViewCommand]], leaving other targets
   * intact. Fails if a V1 session-catalog target's namespace does not have exactly one part.
   * Analysis-only state is not transferred: HandleSpecialCommand marks the new command analyzed.
   */
  def rewriteCreateView(createView: CreateView): LogicalPlan = createView match {
    case CreateView(CreateViewInSessionCatalog(ident), userSpecifiedColumns, comment,
        collation, properties, originalText, query, allowExisting, replace, viewSchemaMode,
        _, _, _) =>
      CreateViewCommand(
        name = ident,
        userSpecifiedColumns = userSpecifiedColumns,
        comment = comment,
        collation = collation,
        properties = properties,
        originalText = originalText,
        plan = query,
        allowExisting = allowExisting,
        replace = replace,
        viewType = PersistedView,
        viewSchemaMode = viewSchemaMode)

    case _ =>
      createView
  }

  private[analysis] def useV1ViewCommands(catalog: CatalogPlugin): Boolean = {
    isSessionCatalog(catalog) && !catalog.isInstanceOf[ViewCatalog]
  }

  private def isDelegatedV1View(catalog: CatalogPlugin, ident: Identifier): Boolean = {
    catalog match {
      case relationCatalog: RelationCatalog if isSessionCatalog(catalog) =>
        try {
          relationCatalog.loadRelation(ident).isInstanceOf[V1View]
        } catch {
          case _: NoSuchNamespaceException | _: NoSuchTableException => false
        }
      case _ => false
    }
  }

  private object DropViewInSessionCatalog
    extends ResolvedIdentifierInSessionCatalog(
      "DROP", "VIEW",
      (catalog, ident) => useV1ViewCommands(catalog) || isDelegatedV1View(catalog, ident))

  private object CreateViewInSessionCatalog
    extends ResolvedIdentifierInSessionCatalog(
      "CREATE", "VIEW", (catalog, _) => useV1ViewCommands(catalog))
}
