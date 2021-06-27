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
         nameParts @ NonSessionCatalogAndTable(catalog, tbl), cols) =>
      cols.foreach(c => failNullType(c.dataType))
      val changes = cols.map { col =>
        TableChange.addColumn(
          col.name.toArray,
          col.dataType,
          col.nullable,
          col.comment.orNull,
          col.position.orNull)
      }
      createAlterTable(nameParts, catalog, tbl, changes)

    case AlterTableReplaceColumnsStatement(
        nameParts @ NonSessionCatalogAndTable(catalog, tbl), cols) =>
      cols.foreach(c => failNullType(c.dataType))
      val changes: Seq[TableChange] = loadTable(catalog, tbl.asIdentifier) match {
        case Some(table) =>
          // REPLACE COLUMNS deletes all the existing columns and adds new columns specified.
          val deleteChanges = table.schema.fieldNames.map { name =>
            TableChange.deleteColumn(Array(name))
          }
          val addChanges = cols.map { col =>
            TableChange.addColumn(
              col.name.toArray,
              col.dataType,
              col.nullable,
              col.comment.orNull,
              col.position.orNull)
          }
          deleteChanges ++ addChanges
        case None => Seq()
      }
      createAlterTable(nameParts, catalog, tbl, changes)

    case c @ CreateTableStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _, _, _) =>
      assertNoNullTypeInSchema(c.tableSchema)
      CreateV2Table(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c),
        ignoreIfExists = c.ifNotExists)

    case c @ CreateTableAsSelectStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _, _, _, _) =>
      if (c.asSelect.resolved) {
        assertNoNullTypeInSchema(c.asSelect.schema)
      }
      CreateTableAsSelect(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c),
        writeOptions = c.writeOptions,
        ignoreIfExists = c.ifNotExists)

    case c @ ReplaceTableStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _, _) =>
      assertNoNullTypeInSchema(c.tableSchema)
      ReplaceTable(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c),
        orCreate = c.orCreate)

    case c @ ReplaceTableAsSelectStatement(
         NonSessionCatalogAndTable(catalog, tbl), _, _, _, _, _, _, _, _, _, _, _) =>
      if (c.asSelect.resolved) {
        assertNoNullTypeInSchema(c.asSelect.schema)
      }
      ReplaceTableAsSelect(
        catalog.asTableCatalog,
        tbl.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c),
        writeOptions = c.writeOptions,
        orCreate = c.orCreate)

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
