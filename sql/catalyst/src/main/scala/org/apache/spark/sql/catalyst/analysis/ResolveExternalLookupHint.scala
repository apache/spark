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

import java.util.Locale

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Expression, StringLiteral}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupHint, LogicalPlan,
  SubqueryAlias, UnresolvedHint, UnresolvedWith, View}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_HINT
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryErrorsBase}

/**
 * Resolves the `EXTERNAL_LOOKUP` hint to [[ExternalLookupHint]] markers. As with join strategy
 * hints, a hint without parameters marks the plan it is attached to, and a hint with relation
 * names, given as identifiers or strings, marks each named relation.
 */
object ResolveExternalLookupHint extends Rule[LogicalPlan] with QueryErrorsBase {
  private val HINT_NAME = "EXTERNAL_LOOKUP"

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.resolveOperatorsUpWithPruning(_.containsPattern(UNRESOLVED_HINT), ruleId) {
      case UnresolvedHint(name, parameters, child)
          if name.toUpperCase(Locale.ROOT) == HINT_NAME =>
        resolveHint(parameters, child)
    }

  private def resolveHint(parameters: Seq[Expression], child: LogicalPlan): LogicalPlan = {
    val relationNames = mutable.ArrayBuffer.empty[Seq[String]]
    parameters.foreach {
      // As in ResolveJoinStrategyHints: the Dataset API, PySpark and Spark Connect pass a relation
      // name as a string, and SQL as an identifier.
      case StringLiteral(name) =>
        relationNames += UnresolvedAttribute.parseAttributeName(name)
      case attr: UnresolvedAttribute =>
        relationNames += attr.nameParts
      case other =>
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "UNSUPPORTED_PARAMETER", Map("parameter" -> toSQLExpr(other)))
    }
    if (relationNames.isEmpty) {
      ExternalLookupHint(child)
    } else {
      // As with join strategy hints, a name marks every relation that it matches. `matched` only
      // tells which names matched nothing.
      val matched = mutable.HashSet.empty[Seq[String]]
      def matches(identifier: Seq[String]): Boolean = {
        val names = relationNames.filter { name =>
          ResolveHints.matchedIdentifier(name, identifier, conf.resolver)
        }
        matched ++= names
        names.nonEmpty
      }
      // Mirrors the target scoping of ResolveHints.ResolveJoinStrategyHints: a name matches an
      // unaliased (possibly multipart) table or an alias, and does not reach into a view,
      // a WITH clause, or an aliased subquery.
      def applyHint(plan: LogicalPlan): LogicalPlan = plan match {
        case relation: UnresolvedRelation if matches(relation.multipartIdentifier) =>
          ExternalLookupHint(relation)
        case alias: SubqueryAlias
            if matches(alias.identifier.qualifier :+ alias.identifier.name) =>
          ExternalLookupHint(alias)
        case _: View | _: UnresolvedWith | _: SubqueryAlias => plan
        case _ => plan.mapChildren(applyHint)
      }
      val rewritten = applyHint(child)
      // As with join strategy hints, a name may be repeated.
      val missing = relationNames.distinct.filterNot(matched.contains)
      if (missing.nonEmpty) {
        throw QueryCompilationErrors.invalidExternalLookupHintError(
          "RELATION_NOT_FOUND", Map("relations" -> missing.map(toSQLId).mkString(", ")))
      }
      rewritten
    }
  }
}
