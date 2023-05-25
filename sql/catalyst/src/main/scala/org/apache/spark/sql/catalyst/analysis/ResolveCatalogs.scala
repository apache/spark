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
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier, LookupCatalog}

/**
 * Resolves the catalog of the name parts for table/view/function/namespace.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedIdentifier(nameParts, allowTemp) =>
      if (allowTemp && catalogManager.v1SessionCatalog.isTempView(nameParts)) {
        val ident = Identifier.of(nameParts.dropRight(1).toArray, nameParts.last)
        ResolvedIdentifier(FakeSystemCatalog, ident)
      } else {
        val CatalogAndIdentifier(catalog, identifier) = nameParts
        ResolvedIdentifier(catalog, identifier)
      }
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
  }
}
