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

import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{View => V2View, _}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Substitute persisted views in parsed plans with parsed view sql text.
 */
case class ViewSubstitution(
    catalogManager: CatalogManager,
    sqlParser: ParserInterface) extends Rule[LogicalPlan] with LookupCatalog {

  def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case u @ UnresolvedRelation(nameParts, _, _) if isTempView(nameParts) =>
      u
    case u @ UnresolvedRelation(parts @ NonSessionCatalogAndIdentifier(catalog, ident), _, _)
        if !isSQLOnFile(parts) =>
      CatalogV2Util.loadView(catalog, ident)
          .map(createViewRelation(ident, _))
          .getOrElse(u)
  }

  private def isTempView(parts: Seq[String]): Boolean =
    catalogManager.v1SessionCatalog.isTempView(parts)

  private def isSQLOnFile(parts: Seq[String]): Boolean = parts match {
    case Seq(_, path) if path.contains("/") => true
    case _ => false
  }

  private def createViewRelation(ident: Identifier, view: V2View): LogicalPlan = {
    if (!catalogManager.isCatalogRegistered(view.currentCatalog)) {
      throw QueryCompilationErrors.invalidViewCurrentCatalog(
        view.currentCatalog, ident.asMultipartIdentifier)
    }

    val child = try {
      sqlParser.parsePlan(view.query)
    } catch {
      case _: ParseException =>
        throw QueryCompilationErrors.invalidViewText(view.query, ident.quoted)
    }

    val desc = view
    val catalogAndNamespace = Option(view.currentCatalog)
        .map(_ +: view.currentNamespace.toSeq)
    val qualifiedChild = catalogAndNamespace match {
      case None =>
        // Views from Spark 2.2 or prior do not store catalog or namespace,
        // however its sql text should already be fully qualified.
        child
      case Some(catalogAndNamespace) =>
        // Substitute CTEs within the view before qualifying table identifiers
        qualifyTableIdentifiers(CTESubstitution.apply(child), catalogAndNamespace)
    }

    // The relation is a view, so we wrap the relation by:
    // 1. Add a [[View]] operator over the relation to keep track of the view desc;
    // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
    SubqueryAlias(ident.quoted, View(V2ViewDescription(ident, desc), false, qualifiedChild))
  }

  /**
   * Qualify table identifiers with default catalog and namespace if necessary.
   */
  private def qualifyTableIdentifiers(
      child: LogicalPlan,
      catalogAndNamespace: Seq[String]): LogicalPlan =
    child transform {
      case u @ UnresolvedRelation(Seq(table), _, _) =>
        u.copy(multipartIdentifier = catalogAndNamespace :+ table)
      case u @ UnresolvedRelation(parts, _, _)
        if !catalogManager.isCatalogRegistered(parts.head) =>
        u.copy(multipartIdentifier = catalogAndNamespace.head +: parts)
    }
}
