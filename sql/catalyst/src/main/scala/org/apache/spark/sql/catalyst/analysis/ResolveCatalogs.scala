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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogPlugin, Identifier, LookupCatalog, SupportsNamespaces}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.util.ArrayImplicits._

/**
 * Resolves the catalog of the name parts for table/view/function/namespace.
 */
class ResolveCatalogs(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    // We only support temp variables for now and the system catalog is not properly implemented
    // yet. We need to resolve `UnresolvedIdentifier` for variable commands specially.
    case c @ CreateVariable(UnresolvedIdentifier(nameParts, _), _, _) =>
      val resolved = resolveVariableName(nameParts)
      c.copy(name = resolved)
    case d @ DropVariable(UnresolvedIdentifier(nameParts, _), _) =>
      val resolved = resolveVariableName(nameParts)
      d.copy(name = resolved)

    case UnresolvedIdentifier(nameParts, allowTemp) =>
      if (allowTemp && catalogManager.v1SessionCatalog.isTempView(nameParts)) {
        val ident = Identifier.of(nameParts.dropRight(1).toArray, nameParts.last)
        ResolvedIdentifier(FakeSystemCatalog, ident)
      } else {
        val CatalogAndIdentifier(catalog, identifier) = nameParts
        ResolvedIdentifier(catalog, identifier)
      }

    case CurrentNamespace =>
      ResolvedNamespace(currentCatalog, catalogManager.currentNamespace.toImmutableArraySeq)
    case UnresolvedNamespace(Seq(), fetchMetadata) =>
      resolveNamespace(currentCatalog, Seq.empty[String], fetchMetadata)
    case UnresolvedNamespace(CatalogAndNamespace(catalog, ns), fetchMetadata) =>
      resolveNamespace(catalog, ns, fetchMetadata)
  }

  private def resolveNamespace(
      catalog: CatalogPlugin,
      ns: Seq[String],
      fetchMetadata: Boolean): ResolvedNamespace = {
    catalog match {
      case supportsNS: SupportsNamespaces if fetchMetadata =>
        ResolvedNamespace(
          catalog,
          ns,
          supportsNS.loadNamespaceMetadata(ns.toArray).asScala.toMap)
      case _ =>
        ResolvedNamespace(catalog, ns)
    }
  }

  private def resolveVariableName(nameParts: Seq[String]): ResolvedIdentifier = {
    def ident: Identifier = Identifier.of(Array(CatalogManager.SESSION_NAMESPACE), nameParts.last)
    if (nameParts.length == 1) {
      ResolvedIdentifier(FakeSystemCatalog, ident)
    } else if (nameParts.length == 2) {
      if (nameParts.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
        ResolvedIdentifier(FakeSystemCatalog, ident)
      } else {
        throw QueryCompilationErrors.unresolvedVariableError(
          nameParts, Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE))
      }
    } else if (nameParts.length == 3) {
      if (nameParts(0).equalsIgnoreCase(CatalogManager.SYSTEM_CATALOG_NAME) &&
        nameParts(1).equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
        ResolvedIdentifier(FakeSystemCatalog, ident)
      } else {
        throw QueryCompilationErrors.unresolvedVariableError(
          nameParts, Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE))
      }
    } else {
      throw QueryCompilationErrors.unresolvedVariableError(
        nameParts, Seq(CatalogManager.SYSTEM_CATALOG_NAME, CatalogManager.SESSION_NAMESPACE))
    }
  }
}
