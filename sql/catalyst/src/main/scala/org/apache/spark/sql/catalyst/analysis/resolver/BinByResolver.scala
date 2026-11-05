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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.sql.catalyst.analysis.BinByResolution
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{BinBy, LogicalPlan, UnresolvedBinBy}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

/**
 * Resolves [[UnresolvedBinBy]] into [[BinBy]].
 */
class BinByResolver(operatorResolver: Resolver, expressionResolver: ExpressionResolver)
    extends TreeNodeResolver[UnresolvedBinBy, LogicalPlan] {
  private val scopes: NameScopeStack = operatorResolver.getNameScopes
  private val expressionIdAssigner = expressionResolver.getExpressionIdAssigner

  /**
   * Resolves an [[UnresolvedBinBy]] into a [[BinBy]] in a single pass:
   *  1. Resolve the child operator.
   *  2. Resolve the `BIN WIDTH` expression, the range columns, the `ALIGN TO` expression, and the
   *     distribute columns against the child's output.
   *  3. Validate types/foldability and fold the bin width and origin to micros via
   *     [[BinByResolution]].
   *  4. Assign unique expression ids to the scaled DISTRIBUTE columns and the appended output
   *     attributes.
   *  5. Overwrite the current scope's output with the resolved operator's output: each DISTRIBUTE
   *     input replaced in place by its scaled produced attribute, followed by the appended
   *     attributes.
   *  6. Drop the consumed DISTRIBUTE inputs from the scope's hidden output.
   *
   * For example, the query:
   *
   * {{{
   *   SELECT * FROM VALUES
   *     (TIMESTAMP_NTZ'2024-01-01 00:00:00', TIMESTAMP_NTZ'2024-01-01 01:00:00', 1.0D)
   *     AS t(ts_start, ts_end, value)
   *   BIN BY (RANGE ts_start TO ts_end BIN WIDTH INTERVAL '5' MINUTE DISTRIBUTE UNIFORM (value))
   * }}}
   *
   * has the unresolved plan:
   *
   * {{{
   *   'BinBy INTERVAL '05' MINUTE, 'ts_start, 'ts_end, None, ['value], BinByOutputAliases()
   *   +- SubqueryAlias t
   *      +- LocalRelation [ts_start#0, ts_end#1, value#2]
   * }}}
   *
   * and resolves to:
   *
   * {{{
   *   BinBy 300000000, ts_start#0, ts_end#1, 0, [value#2], [value#6],
   *     [bin_start#3, bin_end#4, bin_distribute_ratio#5], None
   *   +- SubqueryAlias t
   *      +- LocalRelation [ts_start#0, ts_end#1, value#2]
   * }}}
   */
  override def resolve(unresolvedBinBy: UnresolvedBinBy): LogicalPlan = {
    checkBinByEnabled()

    val resolvedChild = operatorResolver.resolve(unresolvedBinBy.child)
    val partiallyResolvedBinBy = unresolvedBinBy.copy(child = resolvedChild)

    val resolvedBinWidthExpr = expressionResolver.resolveExpressionTreeInOperator(
      unresolvedExpression = unresolvedBinBy.binWidthExpr,
      parentOperator = partiallyResolvedBinBy
    )
    val rangeStart = resolveColumn(unresolvedBinBy.rangeStartCol, partiallyResolvedBinBy)
    val rangeEnd = resolveColumn(unresolvedBinBy.rangeEndCol, partiallyResolvedBinBy)
    val resolvedOriginExpr = unresolvedBinBy.originExpr.map { origin =>
      expressionResolver.resolveExpressionTreeInOperator(
        unresolvedExpression = origin,
        parentOperator = partiallyResolvedBinBy
      )
    }
    val distributeAttributes =
      unresolvedBinBy.distributeColumns.map(resolveColumn(_, partiallyResolvedBinBy))

    val parameters = BinByResolution.validateAndComputeParameters(
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      distributeAttributes = distributeAttributes,
      binWidthExpr = resolvedBinWidthExpr,
      originExpr = resolvedOriginExpr
    )

    val scaledDistributeColumns =
      BinByResolution.scaledDistributeAttributes(distributeAttributes).map { attribute =>
        expressionIdAssigner.mapExpression(attribute, allowUpdatesForAttributeReferences = true)
      }

    val appendedAttributes =
      BinByResolution
        .appendedAttributesWithAliases(parameters.rangeType, unresolvedBinBy.outputAliases)
        .map { attribute =>
          expressionIdAssigner.mapExpression(attribute, allowUpdatesForAttributeReferences = true)
        }

    val resolvedBinBy = BinBy(
      binWidthMicros = parameters.binWidthMicros,
      rangeStart = rangeStart,
      rangeEnd = rangeEnd,
      originMicros = parameters.originMicros,
      distributeColumns = distributeAttributes,
      scaledDistributeColumns = scaledDistributeColumns,
      appendedAttributes = appendedAttributes,
      child = resolvedChild,
      timeZoneId = parameters.timeZoneId
    )

    val distributeReplacements =
      AttributeMap(distributeAttributes.zip(scaledDistributeColumns))
    val childOutput = scopes.current.output.map { attribute =>
      distributeReplacements.getOrElse(attribute, attribute)
    }

    scopes.overwriteOutputAndExtendHiddenOutput(
      output = childOutput ++ appendedAttributes,
      retainQualifiedAccessOnlyOutput = true
    )
    removeConsumedDistributeInputsFromHiddenOutput(distributeAttributes)
    resolvedBinBy
  }

  private def checkBinByEnabled(): Unit = {
    if (!conf.getConf(SQLConf.BIN_BY_ENABLED)) {
      throw QueryCompilationErrors.binByDisabledError()
    }
  }

  /**
   * Resolves a BIN BY column reference to its [[Attribute]]. A missing column raises
   * `UNRESOLVED_COLUMN` / `AMBIGUOUS_REFERENCE`; a nested reference resolves to a bare extract
   * expression, which is rejected as not top-level with the full path reported.
   */
  private def resolveColumn(expression: Expression, parentOperator: LogicalPlan): Attribute =
    expressionResolver.resolveExpressionTreeInOperator(
      unresolvedExpression = expression,
      parentOperator = parentOperator
    ) match {
      case attribute: Attribute => attribute
      case nestedReference =>
        throw QueryCompilationErrors.binByRequiresTopLevelColumnError(nestedReference)
    }

  /**
   * Removes the consumed DISTRIBUTE input columns from the current scope's hidden output so a
   * reference resolves to their same-named scaled replacements instead of matching both.
   */
  private def removeConsumedDistributeInputsFromHiddenOutput(
      distributeAttributes: Seq[Attribute]): Unit = {
    val distributeInputIds = distributeAttributes.map(_.exprId).toSet
    scopes.overwriteCurrent(
      hiddenOutput = Some(scopes.current.hiddenOutput.filterNot { attribute =>
        distributeInputIds.contains(attribute.exprId)
      })
    )
  }
}
