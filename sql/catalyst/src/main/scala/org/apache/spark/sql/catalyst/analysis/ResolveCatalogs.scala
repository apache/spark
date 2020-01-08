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
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog, SupportsNamespaces, TableCatalog, TableChange}

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v2 commands if the resolved catalog is not the session catalog.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case AlterTableAddColumnsStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), cols) =>
      val changes = cols.map { col =>
        TableChange.addColumn(
          col.name.toArray,
          col.dataType,
          true,
          col.comment.orNull,
          col.position.orNull)
      }
      createAlterTable(nameParts, catalog, tbl, changes)

    case AlterTableAlterColumnStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), colName, dataType, comment, pos) =>
      val colNameArray = colName.toArray
      val typeChange = dataType.map { newDataType =>
        TableChange.updateColumnType(colNameArray, newDataType, true)
      }
      val commentChange = comment.map { newComment =>
        TableChange.updateColumnComment(colNameArray, newComment)
      }
      val positionChange = pos.map { newPosition =>
        TableChange.updateColumnPosition(colNameArray, newPosition)
      }
      createAlterTable(
        nameParts,
        catalog,
        tbl,
        typeChange.toSeq ++ commentChange ++ positionChange)

    case AlterTableRenameColumnStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), col, newName) =>
      val changes = Seq(TableChange.renameColumn(col.toArray, newName))
      createAlterTable(nameParts, catalog, tbl, changes)

    case AlterTableDropColumnsStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), cols) =>
      val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
      createAlterTable(nameParts, catalog, tbl, changes)

    case AlterTableSetPropertiesStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }.toSeq
      createAlterTable(nameParts, catalog, tbl, changes)

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetPropertiesStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      createAlterTable(nameParts, catalog, tbl, changes)

    case AlterTableSetLocationStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), partitionSpec, newLoc) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException(
          "ALTER TABLE SET LOCATION does not support partition for v2 tables.")
      }
      val changes = Seq(TableChange.setProperty(TableCatalog.PROP_LOCATION, newLoc))
      createAlterTable(nameParts, catalog, tbl, changes)

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

    case AlterNamespaceSetPropertiesStatement(
        NonSessionCatalogAndNamespace(catalog, ns), properties) =>
      AlterNamespaceSetProperties(catalog, ns, properties)

    case AlterNamespaceSetLocationStatement(
        NonSessionCatalogAndNamespace(catalog, ns), location) =>
      AlterNamespaceSetProperties(catalog, ns,
        Map(SupportsNamespaces.PROP_LOCATION -> location))

    case RenameTableStatement(NonSessionCatalogAndTable(catalog, oldName), newNameParts, isView) =>
      if (isView) {
        throw new AnalysisException("Renaming view is not supported in v2 catalogs.")
      }
      RenameTable(catalog.asTableCatalog, oldName.asIdentifier, newNameParts.asIdentifier)

    case DescribeTableStatement(
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESCRIBE TABLE does not support partition for v2 tables.")
      }
      val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tbl.asIdentifier)
      DescribeTable(r, isExtended)

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
        writeOptions = c.options.filterKeys(_ != "path"),
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
        writeOptions = c.options.filterKeys(_ != "path"),
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

    case DropNamespaceStatement(NonSessionCatalogAndNamespace(catalog, ns), ifExists, cascade) =>
      DropNamespace(catalog, ns, ifExists, cascade)

    case DescribeNamespaceStatement(NonSessionCatalogAndNamespace(catalog, ns), extended) =>
      DescribeNamespace(catalog, ns, extended)

    case ShowNamespacesStatement(NonSessionCatalogAndNamespace(catalog, ns), pattern) =>
      ShowNamespaces(catalog, ns, pattern)

    case ShowTablesStatement(NonSessionCatalogAndNamespace(catalog, ns), pattern) =>
      ShowTables(catalog.asTableCatalog, ns, pattern)

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

    case ShowTablePropertiesStatement(
      nameParts @ NonSessionCatalogAndTable(catalog, tbl), propertyKey) =>
      val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tbl.asIdentifier)
      ShowTableProperties(r, propertyKey)
  }

  object NonSessionCatalogAndTable {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case NonSessionCatalogAndIdentifier(catalog, ident) =>
        Some(catalog -> ident.asMultipartIdentifier)
      case _ => None
    }
  }

  object NonSessionCatalogAndNamespace {
    def unapply(resolved: ResolvedNamespace): Option[(SupportsNamespaces, Seq[String])] =
      if (!isSessionCatalog(resolved.catalog)) {
        Some(resolved.catalog -> resolved.namespace)
      } else {
        None
      }
  }
}
