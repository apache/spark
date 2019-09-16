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
import org.apache.spark.sql.catalyst.plans.logical.{DropTable, LogicalPlan, ShowNamespaces, ShowTables}
import org.apache.spark.sql.catalyst.plans.logical.sql.{DropTableStatement, DropViewStatement, ShowNamespacesStatement, ShowTablesStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog}

/**
 * Resolves catalogs from the multi-part identifiers in SQL statements, and convert the statements
 * to the corresponding v2 commands if the resolved catalog is not the session catalog.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util.isSessionCatalog

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case DropTableStatement(
         CatalogAndRestNameParts(c, restNameParts), ifExists, purge) if !isSessionCatalog(c) =>
      DropTable(c.asTableCatalog, restNameParts.asIdentifier, ifExists)

    case DropViewStatement(
         CatalogAndRestNameParts(c, restNameParts), _) if !isSessionCatalog(c) =>
      throw new AnalysisException(
        s"Can not specify catalog `${c.name}` for view ${restNameParts.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case ShowNamespacesStatement(
         Some(CatalogAndRestNameParts(c, restNameParts)), pattern) if !isSessionCatalog(c) =>
      val namespace = if (restNameParts.isEmpty) None else Some(restNameParts)
      ShowNamespaces(c.asNamespaceCatalog, namespace, pattern)

    // TODO (SPARK-29014): we should check if the current catalog is not session catalog here.
    case ShowNamespacesStatement(None, pattern) if defaultCatalog.isDefined =>
      ShowNamespaces(defaultCatalog.get.asNamespaceCatalog, None, pattern)

    case ShowTablesStatement(
         Some(CatalogAndRestNameParts(c, restNameParts)), pattern) if !isSessionCatalog(c) =>
      ShowTables(c.asTableCatalog, restNameParts, pattern)

    // TODO (SPARK-29014): we should check if the current catalog is not session catalog here.
    case ShowTablesStatement(None, pattern) if defaultCatalog.isDefined =>
      ShowTables(defaultCatalog.get.asTableCatalog, catalogManager.currentNamespace, pattern)
  }
}
