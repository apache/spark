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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, LookupCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

case class ResolveNamespaceAndTable(catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UnresolvedNamespace(Nil) =>
      ResolvedNamespace(currentCatalog.asNamespaceCatalog, Nil)

    case UnresolvedNamespace(CatalogAndNamespace(catalog, ns)) =>
      ResolvedNamespace(catalog.asNamespaceCatalog, ns)

    case u @ UnresolvedV2Table(CatalogAndIdentifier(catalog, ident)) =>
      CatalogV2Util.loadTable(catalog, ident).map { _ =>
        ResolvedV2Table(catalog.asTableCatalog, ident)
      }.getOrElse(u)
  }
}
