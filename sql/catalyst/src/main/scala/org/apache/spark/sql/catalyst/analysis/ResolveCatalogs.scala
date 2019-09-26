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
import org.apache.spark.sql.catalyst.plans.logical.{CreateTableAsSelect, CreateV2Table, DropTable, LogicalPlan, ReplaceTable, ReplaceTableAsSelect, ShowNamespaces, ShowTables}
import org.apache.spark.sql.catalyst.plans.logical.sql.{CreateTableAsSelectStatement, CreateTableStatement, DropTableStatement, DropViewStatement, ReplaceTableAsSelectStatement, ReplaceTableStatement, ShowNamespacesStatement, ShowTablesStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v2 commands if the resolved catalog is not the session catalog.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case c @ CreateTableStatement(
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if !isSessionCatalog(catalog) =>
      CreateV2Table(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        ignoreIfExists = c.ifNotExists)

    case c @ CreateTableAsSelectStatement(
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if !isSessionCatalog(catalog) =>
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
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if !isSessionCatalog(catalog) =>
      ReplaceTable(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        c.tableSchema,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        orCreate = c.orCreate)

    case c @ ReplaceTableAsSelectStatement(
         CatalogAndIdentifierParts(catalog, tableName), _, _, _, _, _, _, _, _, _)
         if !isSessionCatalog(catalog) =>
      ReplaceTableAsSelect(
        catalog.asTableCatalog,
        tableName.asIdentifier,
        // convert the bucket spec and add it as a transform
        c.partitioning ++ c.bucketSpec.map(_.asTransform),
        c.asSelect,
        convertTableProperties(c.properties, c.options, c.location, c.comment, c.provider),
        writeOptions = c.options.filterKeys(_ != "path"),
        orCreate = c.orCreate)

    case DropTableStatement(
         CatalogAndIdentifierParts(c, tableName), ifExists, purge) if !isSessionCatalog(c) =>
      DropTable(c.asTableCatalog, tableName.asIdentifier, ifExists)

    case DropViewStatement(
         CatalogAndIdentifierParts(c, viewName), _) if !isSessionCatalog(c) =>
      throw new AnalysisException(
        s"Can not specify catalog `${c.name}` for view ${viewName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case ShowNamespacesStatement(
         Some(CatalogAndIdentifierParts(c, identParts)), pattern) if !isSessionCatalog(c) =>
      val namespace = if (identParts.isEmpty) None else Some(identParts)
      ShowNamespaces(c.asNamespaceCatalog, namespace, pattern)

    // TODO (SPARK-29014): we should check if the current catalog is not session catalog here.
    case ShowNamespacesStatement(None, pattern) if defaultCatalog.isDefined =>
      ShowNamespaces(defaultCatalog.get.asNamespaceCatalog, None, pattern)

    case ShowTablesStatement(
         Some(CatalogAndIdentifierParts(c, identParts)), pattern) if !isSessionCatalog(c) =>
      ShowTables(c.asTableCatalog, identParts, pattern)

    // TODO (SPARK-29014): we should check if the current catalog is not session catalog here.
    case ShowTablesStatement(None, pattern) if defaultCatalog.isDefined =>
      ShowTables(defaultCatalog.get.asTableCatalog, catalogManager.currentNamespace, pattern)
  }
}
