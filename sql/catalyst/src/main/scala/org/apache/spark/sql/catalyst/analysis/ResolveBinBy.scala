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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinBy, LogicalPlan, UnresolvedBinBy}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_BIN_BY
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedBinBy]] into [[BinBy]]: looks up column references against the child's
 * output, validates types and foldability, and captures the session local time zone for the
 * physical execution to use.
 */
object ResolveBinBy extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsWithPruning(
      _.containsPattern(UNRESOLVED_BIN_BY), ruleId) {
    case _: UnresolvedBinBy if !SQLConf.get.getConf(SQLConf.BIN_BY_ENABLED) =>
      throw QueryCompilationErrors.binByDisabledError()
    case b: UnresolvedBinBy if !readyToResolve(b) => b
    case b: UnresolvedBinBy => resolve(b)
  }

  // `binWidthExpr` must be fully resolved before validation can proceed. The optional
  // `originExpr` must also be resolved when present; an absent `ALIGN TO` clause defaults to
  // `1970-01-01 00:00:00` and bypasses user-facing foldability and type checks. The
  // range/distribute column references can stay as `UnresolvedAttribute`.
  private def readyToResolve(b: UnresolvedBinBy): Boolean = {
    b.childrenResolved && b.binWidthExpr.resolved && b.originExpr.forall(_.resolved)
  }

  private def resolve(b: UnresolvedBinBy): LogicalPlan = {
    val resolver = SQLConf.get.resolver
    val child = b.child

    val rangeStart = resolveColumn(b.rangeStartCol, child, resolver)
    val rangeEnd = resolveColumn(b.rangeEndCol, child, resolver)
    val distributeAttributes = b.distributeColumns.map(c => resolveColumn(c, child, resolver))

    val parameters = BinByResolution.validateAndComputeParameters(
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      distributeAttributes = distributeAttributes,
      binWidthExpr = b.binWidthExpr,
      originExpr = b.originExpr)

    val appendedAttributes =
      BinByResolution.appendedAttributesWithAliases(parameters.rangeType, b.outputAliases)
    val scaledDistributeColumns = BinByResolution.scaledDistributeAttributes(distributeAttributes)

    BinBy(
      binWidthMicros = parameters.binWidthMicros,
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      originMicros = parameters.originMicros,
      distributeColumns = distributeAttributes,
      scaledDistributeColumns = scaledDistributeColumns,
      appendedAttributes = appendedAttributes,
      child = child,
      timeZoneId = parameters.timeZoneId)
  }

  private def resolveColumn(
      expr: Expression,
      child: LogicalPlan,
      resolver: Resolver): Attribute = expr match {
    case u: UnresolvedAttribute =>
      // Still unresolved here means genuinely absent: ResolveReferences rewrites resolvable refs.
      child.resolve(u.nameParts, resolver) match {
        case Some(a: Attribute) => a
        case _ =>
          throw QueryCompilationErrors.unresolvedColumnError(
            u.name,
            proposal = StringUtils.orderSuggestedIdentifiersBySimilarity(
              u.name,
              candidates = child.output.map(attribute => attribute.qualifier :+ attribute.name)))
      }
    case a: Attribute => a
    case alias: Alias if alias.resolved =>
      // A resolved nested ref (e.g. `struct.field`) exists but isn't a top-level column.
      throw QueryCompilationErrors.binByRequiresTopLevelColumnError(alias.child)
    case other =>
      // Genuinely unexpected; the grammar restricts these positions to multipartIdentifier.
      throw SparkException.internalError(
        s"Unexpected expression in BIN BY column position: $other")
  }
}
