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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog}

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v2 commands if the resolved catalog is not the session catalog.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case AlterViewSetPropertiesStatement(
         NonSessionCatalogAndTable(catalog, tbl), props) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tbl.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewUnsetPropertiesStatement(
         NonSessionCatalogAndTable(catalog, tbl), keys, ifExists) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tbl.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case RenameTableStatement(NonSessionCatalogAndTable(catalog, oldName), newNameParts, isView) =>
      if (isView) {
        throw new AnalysisException("Renaming view is not supported in v2 catalogs.")
      }
      RenameTable(catalog.asTableCatalog, oldName.asIdentifier, newNameParts.asIdentifier)

    case DescribeColumnStatement(
         NonSessionCatalogAndTable(catalog, tbl), colNameParts, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case c @ CreateTableStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      CreateV2Table(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        ignoreIfExists = c.ifNotExists)

    case c @ CreateTableAsSelectStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      CreateTableAsSelect(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options,
        ignoreIfExists = c.ifNotExists)

    case RefreshTableStatement(NonSessionCatalogAndTable(catalog, tbl)) =>
      RefreshTable(catalog.asTableCatalog, tbl.asIdentifier)

    case c @ ReplaceTableStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      ReplaceTable(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        orCreate = c.orCreate)

    case c @ ReplaceTableAsSelectStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _) =>
      ReplaceTableAsSelect(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options,
        orCreate = c.orCreate)

    case DropTableStatement(NonSessionCatalogAndTable(catalog, tbl), ifExists, _) =>
      DropTable(catalog.asTableCatalog, tbl.asIdentifier, ifExists)

    case DropViewStatement(NonSessionCatalogAndTable(catalog, viewName), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${viewName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case c @ CreateNamespaceStatement(CatalogAndNamespace(catalog, ns), _, _)
        if !isSessionCatalog(catalog) =>
      CreateNamespace(catalog.asNamespaceCatalog, ns, c.ifNotExists, c.properties)

    case UseStatement(isNamespaceSet, nameParts) =>
      if (isNamespaceSet) {
        SetCatalogAndNamespace(catalogManager, None, Some(nameParts))
      } else {
        val CatalogAndNamespace(catalog, ns) = nameParts
        val namespace = if (ns.nonEmpty) Some(ns) else None
        SetCatalogAndNamespace(catalogManager, Some(catalog.name()), namespace)
      }

    case ShowCurrentNamespaceStatement() =>
      ShowCurrentNamespace(catalogManager)
  }

  object NonSessionCatalogAndTable {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case NonSessionCatalogAndIdentifier(catalog, ident) =>
        Some(catalog -> ident.asMultipartIdentifier)
      case _ => None
    }
  }
}
