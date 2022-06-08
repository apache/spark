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

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog, ViewChange}
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Resolves the catalog of the name parts for table/view/function/namespace.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedIdentifier(CatalogAndIdentifier(catalog, identifier)) =>
      ResolvedIdentifier(catalog, identifier)
    case s @ ShowTables(UnresolvedNamespace(Seq()), _, _) =>
      s.copy(namespace = ResolvedNamespace(currentCatalog, catalogManager.currentNamespace))
    case s @ ShowTableExtended(UnresolvedNamespace(Seq()), _, _, _) =>
      s.copy(namespace = ResolvedNamespace(currentCatalog, catalogManager.currentNamespace))
    case s @ ShowViews(UnresolvedNamespace(Seq()), _, _) =>
      s.copy(namespace = ResolvedNamespace(currentCatalog, catalogManager.currentNamespace))
    case s @ ShowFunctions(UnresolvedNamespace(Seq()), _, _, _, _) =>
      s.copy(namespace = ResolvedNamespace(currentCatalog, catalogManager.currentNamespace))
    case a @ AnalyzeTables(UnresolvedNamespace(Seq()), _) =>
      a.copy(namespace = ResolvedNamespace(currentCatalog, catalogManager.currentNamespace))
    case UnresolvedNamespace(Seq()) =>
      ResolvedNamespace(currentCatalog, Seq.empty[String])
    case UnresolvedNamespace(CatalogAndNamespace(catalog, ns)) =>
      ResolvedNamespace(catalog, ns)

    case DescribeRelation(ResolvedV2View(_, ident, view), _, isExtended, _) =>
      DescribeV2View(V2ViewDescription(ident.quoted, view), isExtended)

    case ShowCreateTable(ResolvedV2View(_, ident, view), _, _) =>
      ShowCreateV2View(V2ViewDescription(ident.quoted, view))

    case ShowTableProperties(ResolvedV2View(_, ident, view), propertyKeys, _) =>
      ShowV2ViewProperties(V2ViewDescription(ident.quoted, view), propertyKeys)

    case SetViewProperties(ResolvedV2View(catalog, ident, _), props) =>
      val changes = props.map {
        case (property, value) => ViewChange.setProperty(property, value)
      }.toSeq
      AlterV2View(catalog, ident, changes)

    case UnsetViewProperties(ResolvedV2View(catalog, ident, _), propertyKeys, ifExists) =>
      if (!ifExists) {
        val view = catalog.loadView(ident)
        propertyKeys.filterNot(view.properties.containsKey).foreach { property =>
          QueryCompilationErrors.cannotUnsetNonExistentViewProperty(ident, property)
        }
      }
      val changes = propertyKeys.map(ViewChange.removeProperty)
      AlterV2View(catalog, ident, changes)

    case RenameTable(ResolvedV2View(oldCatalog, oldIdent, _),
        NonSessionCatalogAndIdentifier(newCatalog, newIdent), true) =>
      if (oldCatalog.name != newCatalog.name) {
        QueryCompilationErrors.cannotMoveViewBetweenCatalogs(
          oldCatalog.name, newCatalog.name)
      }
      RenameV2View(oldCatalog, oldIdent, newIdent)

    case RefreshTable(ResolvedV2View(catalog, ident, _)) =>
      RefreshView(catalog, ident)

    case DropView(ResolvedV2View(catalog, ident, _), ifExists) =>
      DropV2View(catalog, ident, ifExists)
  }

  object NonSessionCatalogAndTable {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case NonSessionCatalogAndIdentifier(catalog, ident) =>
        Some(catalog -> ident.asMultipartIdentifier)
      case _ => None
    }
  }
}
