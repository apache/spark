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
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, LookupCatalog, TableChange}

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
         nameParts @ NonSessionCatalog(catalog, tableName), cols) =>
      val changes = cols.map { col =>
        TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
      }
      createAlterTable(nameParts, catalog, tableName, changes)

    case AlterTableAlterColumnStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), colName, dataType, comment) =>
      val typeChange = dataType.map { newDataType =>
        TableChange.updateColumnType(colName.toArray, newDataType, true)
      }
      val commentChange = comment.map { newComment =>
        TableChange.updateColumnComment(colName.toArray, newComment)
      }
      createAlterTable(nameParts, catalog, tableName, typeChange.toSeq ++ commentChange)

    case AlterTableRenameColumnStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), col, newName) =>
      val changes = Seq(TableChange.renameColumn(col.toArray, newName))
      createAlterTable(nameParts, catalog, tableName, changes)

    case AlterTableDropColumnsStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), cols) =>
      val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
      createAlterTable(nameParts, catalog, tableName, changes)

    case AlterTableSetPropertiesStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }.toSeq
      createAlterTable(nameParts, catalog, tableName, changes)

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetPropertiesStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      createAlterTable(nameParts, catalog, tableName, changes)

    case AlterTableSetLocationStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), newLoc) =>
      val changes = Seq(TableChange.setProperty("location", newLoc))
      createAlterTable(nameParts, catalog, tableName, changes)

    case AlterViewSetPropertiesStatement(
         NonSessionCatalog(catalog, tableName), props) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tableName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewUnsetPropertiesStatement(
         NonSessionCatalog(catalog, tableName), keys, ifExists) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tableName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case DeleteFromStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), tableAlias, condition) =>
      val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tableName.asIdentifier)
      val aliased = tableAlias.map(SubqueryAlias(_, r)).getOrElse(r)
      DeleteFromTable(aliased, condition)

    case u @ UpdateTableStatement(
         nameParts @ CatalogAndIdentifierParts(catalog, tableName), _, _, _, _) =>
      val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tableName.asIdentifier)
      val aliased = u.tableAlias.map(SubqueryAlias(_, r)).getOrElse(r)
      val columns = u.columns.map(UnresolvedAttribute(_))
      UpdateTable(aliased, columns, u.values, u.condition)

    case DescribeTableStatement(
         nameParts @ NonSessionCatalog(catalog, tableName), partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESCRIBE TABLE does not support partition for v2 tables.")
      }
      val r = UnresolvedV2Relation(nameParts, catalog.asTableCatalog, tableName.asIdentifier)
      DescribeTable(r, isExtended)

    case DescribeColumnStatement(
         NonSessionCatalog(catalog, tableName), colNameParts, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")

    case c @ CreateTableStatement(
         NonSessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      CreateV2Table(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        ignoreIfExists = c.ifNotExists)

    case c @ CreateTableAsSelectStatement(
         NonSessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      CreateTableAsSelect(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options.filterKeys(_ != "path"),
        ignoreIfExists = c.ifNotExists)

    case c @ ReplaceTableStatement(
         NonSessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      ReplaceTable(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        orCreate = c.orCreate)

    case c @ ReplaceTableAsSelectStatement(
         NonSessionCatalog(catalog, tableName), _, _, _, _, _, _, _, _, _) =>
      ReplaceTableAsSelect(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options.filterKeys(_ != "path"),
        orCreate = c.orCreate)

    case DropTableStatement(NonSessionCatalog(catalog, tableName), ifExists, _) =>
      DropTable(catalog.asTableCatalog, tableName.asIdentifier, ifExists)

    case DropViewStatement(NonSessionCatalog(catalog, viewName), _) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${viewName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case c @ CreateNamespaceStatement(NonSessionCatalog(catalog, nameParts), _, _) =>
      CreateNamespace(
        catalog.asNamespaceCatalog,
        nameParts,
        c.ifNotExists,
        c.properties)

    case ShowNamespacesStatement(Some(CatalogAndNamespace(catalog, namespace)), pattern) =>
      ShowNamespaces(catalog.asNamespaceCatalog, namespace, pattern)

    case ShowNamespacesStatement(None, pattern) =>
      ShowNamespaces(currentCatalog.asNamespaceCatalog, None, pattern)

    case ShowTablesStatement(Some(NonSessionCatalog(catalog, nameParts)), pattern) =>
      ShowTables(catalog.asTableCatalog, nameParts, pattern)

    case ShowTablesStatement(None, pattern) if !isSessionCatalog(currentCatalog) =>
      ShowTables(currentCatalog.asTableCatalog, catalogManager.currentNamespace, pattern)

    case UseStatement(isNamespaceSet, nameParts) =>
      if (isNamespaceSet) {
        SetCatalogAndNamespace(catalogManager, None, Some(nameParts))
      } else {
        val CatalogAndNamespace(catalog, namespace) = nameParts
        SetCatalogAndNamespace(catalogManager, Some(catalog.name()), namespace)
      }
  }

  object NonSessionCatalog {
    def unapply(nameParts: Seq[String]): Option[(CatalogPlugin, Seq[String])] = nameParts match {
      case CatalogAndIdentifierParts(catalog, parts) if !isSessionCatalog(catalog) =>
        Some(catalog -> parts)
      case _ => None
    }
  }
}
