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

import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.ByteType

/**
 * Object used to transform grouping analytics (CUBE/ROLLUP/GROUPING SETS) operations
 * into [[Expand]] and [[Aggregate]] operators.
 */
object GroupingAnalyticsTransformer extends SQLConfHelper with AliasHelper {

  /**
   * Marks an [[Aggregate]] that [[constructGrandTotalAggregate]] produced by lowering a grand-total
   * `GROUP BY GROUPING SETS (())` (or the equivalent empty `CUBE()`/`ROLLUP()`) to a global
   * aggregate. Such an aggregate has no grouping expressions, so it is otherwise indistinguishable
   * from a plain no-`GROUP BY` aggregate; the tag lets [[collectGroupingExpressions]] tell them
   * apart so a grouping function in HAVING/ORDER BY folds to the constant 0 over the former but is
   * still rejected over the latter (matching the SELECT-list path).
   */
  val GRAND_TOTAL_AGGREGATE_TAG = TreeNodeTag[Unit]("grandTotalAggregate")

  /**
   * Transform a grouping analytics operation (CUBE/ROLLUP/GROUPING SETS) into an [[Expand]]
   * followed by an [[Aggregate]] operator.
   *
   * The transformation works by:
   * 1. Creating aliases for all group by expressions to prevent null values set by [[Expand]]
   *    from being used in aggregates instead of the original values.
   * 2. Creating an [[Expand]] operator that generates rows for each grouping set, with
   *    appropriate null values for expressions not in the current grouping set.
   * 3. Creating an [[Aggregate]] operator that aggregates the expanded rows, using the
   *    grouping attributes and replacing grouping functions ([[GROUPING]], [[GROUPING_ID]]).
   *
   * For example, for a query:
   *
   * {{{
   *   SELECT col1 FROM values(1) GROUP BY grouping sets ((col1), ());
   * }}}
   *
   * Arguments would be:
   *  - groupByExpressions: [col1#0]
   *  - selectedGroupByExpressions: [[col1#0], []]
   *  - aggregationExpressions: [col1#0]
   *  - child: LocalRelation [col1#0]
   *
   * The result of [[GroupingAnalyticsTransformer]] invocation would be:
   *
   * {{{
   *   Aggregate [col1#3, spark_grouping_id#2], [col1#3]
   *   +- Expand [[col1#0, col1#1, 0], [col1#0, null, 1]], [col1#0, col1#3, spark_grouping_id#2]
   *      +- Project [col1#0, col1#0 AS col1#1]
   *         +- LocalRelation [col1#0]
   * }}}
   *
   * The grand-total-only case `GROUP BY GROUPING SETS (())` (and the equivalent empty `CUBE()` /
   * `ROLLUP()`) is the exception: instead of an [[Expand]] it is lowered to a global [[Aggregate]]
   * with no grouping expressions (see [[shouldLowerToGrandTotalAggregate]] and
   * [[constructGrandTotalAggregate]]), so it returns one row over empty input, like an aggregation
   * with no GROUP BY clause.
   *
   * @param newAlias Function to create new aliases, takes expression, optional name, and optional
   *                 qualifier
   * @param childOutput The output attributes of the child plan
   * @param groupByExpressions The original group by expressions
   * @param selectedGroupByExpressions The selected group by expressions for each grouping set
   * @param child The child logical plan
   * @param aggregationExpressions The aggregation expressions
   * @return The transformed logical plan with Expand and Aggregate operators
   */
  def apply(
      newAlias: (Expression, Option[String], Seq[String]) => Alias,
      childOutput: Seq[Attribute],
      groupByExpressions: Seq[Expression],
      selectedGroupByExpressions: Seq[Seq[Expression]],
      child: LogicalPlan,
      aggregationExpressions: Seq[NamedExpression]): Aggregate = {

    if (shouldLowerToGrandTotalAggregate(groupByExpressions, selectedGroupByExpressions)) {
      constructGrandTotalAggregate(newAlias, aggregationExpressions, child)
    } else {
      val groupByAliases = constructGroupByAlias(newAlias, groupByExpressions)

      val gid = AttributeReference(VirtualColumn.groupingIdName, GroupingID.dataType, false)()
      val expand = constructExpand(
        selectedGroupByExpressions = selectedGroupByExpressions,
        child = child,
        groupByAliases = groupByAliases,
        gid = gid,
        childOutput = childOutput
      )
      val groupingAttributes = expand.output.drop(childOutput.length)

      val aggregations = constructAggregateExpressions(
        newAlias = newAlias,
        groupByExpressions = groupByExpressions,
        aggregations = aggregationExpressions,
        groupByAliases = groupByAliases,
        groupingAttributes = groupingAttributes,
        gid = gid
      )

      Aggregate(
        groupingExpressions = groupingAttributes,
        aggregateExpressions = aggregations,
        child = expand
      )
    }
  }

  /**
   * Whether a grouping-set spec is the grand-total-only case `GROUP BY GROUPING SETS (())` (and
   * the equivalent empty `CUBE()`/`ROLLUP()`) that [[apply]] lowers to a global [[Aggregate]]:
   * no leading group-by expressions and a single empty grouping set, with
   * [[SQLConf.LOWER_EMPTY_GROUPING_SET_TO_GLOBAL_AGGREGATE]] enabled. This is the
   * pre-lowering decision; [[isLoweredToGrandTotalAggregate]] detects the same case post-lowering.
   */
  def shouldLowerToGrandTotalAggregate(
      groupByExpressions: Seq[Expression],
      selectedGroupByExpressions: Seq[Seq[Expression]]): Boolean = {
    conf.getConf(SQLConf.LOWER_EMPTY_GROUPING_SET_TO_GLOBAL_AGGREGATE) &&
      groupByExpressions.isEmpty &&
      selectedGroupByExpressions.length == 1 &&
      selectedGroupByExpressions.head.isEmpty
  }

  /**
   * Whether a lowered [[Aggregate]] is the grand total produced by
   * [[shouldLowerToGrandTotalAggregate]]: its resolved grouping expressions (without the
   * `spark_grouping_id` key, as returned by [[collectGroupingExpressions]]) are empty, with the
   * flag enabled. The flag is part of the check because with it off the same grand total is
   * lowered via [[Expand]], whose `spark_grouping_id` key must still be referenced.
   *
   * Only the single-empty-grouping-set grand total reaches here with empty grouping expressions:
   * [[collectGroupingExpressions]] returns empty only for the [[GRAND_TOTAL_AGGREGATE_TAG]]-marked
   * global aggregate. A multi-empty-set spec such as `GROUP BY GROUPING SETS ((), ())` stays on the
   * [[Expand]] path with a `spark_grouping_id` key (and, being a duplicated grouping set, a
   * `_gen_grouping_pos` key), so its collected grouping expressions are non-empty and it is
   * unaffected by this method.
   */
  def isLoweredToGrandTotalAggregate(groupingExpressions: Seq[Expression]): Boolean = {
    conf.getConf(SQLConf.LOWER_EMPTY_GROUPING_SET_TO_GLOBAL_AGGREGATE) &&
      groupingExpressions.isEmpty
  }

  /**
   * The grouping id to substitute for grouping()/grouping_id() over a lowered grouping-analytics
   * [[Aggregate]]: the constant 0 for a grand total ([[isLoweredToGrandTotalAggregate]]), otherwise
   * the unresolved `spark_grouping_id` attribute produced by [[Expand]].
   */
  def groupingIdExpression(groupingExpressions: Seq[Expression]): Expression = {
    if (isLoweredToGrandTotalAggregate(groupingExpressions)) {
      Literal.default(GroupingID.dataType)
    } else {
      VirtualColumn.groupingIdAttribute
    }
  }

  /**
   * Build a global [[Aggregate]] (with no grouping expressions) for the grand-total-only case
   * `GROUP BY GROUPING SETS (())`.
   *
   * A single empty grouping set is a grand total, semantically identical to an aggregation with no
   * GROUP BY clause. Lowering it via [[Expand]] would group by `spark_grouping_id` and so emit zero
   * rows over empty input instead of one; a global [[Aggregate]] returns one (grand total) row,
   * matching the GROUP BY-less form and the SQL standard.
   *
   * Any [[GroupingID]] in the aggregation expressions evaluates to the constant `0` (the only
   * active grouping set is the empty one), and [[Grouping]] over a non-grouping column is rejected,
   * both reusing [[replaceGroupingFunction]] with a literal grouping id in place of the
   * Expand-produced `spark_grouping_id` attribute.
   */
  private def constructGrandTotalAggregate(
      newAlias: (Expression, Option[String], Seq[String]) => Alias,
      aggregationExpressions: Seq[NamedExpression],
      child: LogicalPlan): Aggregate = {
    val groupingId = Literal.default(GroupingID.dataType)
    val aggregations = aggregationExpressions.map { expression =>
      replaceGroupingFunction(
        expression = expression,
        groupByExpressions = Seq.empty,
        gid = groupingId,
        newAlias = newAlias
      ).asInstanceOf[NamedExpression]
    }
    val aggregate = Aggregate(
      groupingExpressions = Seq.empty,
      aggregateExpressions = aggregations,
      child = child
    )
    aggregate.setTagValue(GRAND_TOTAL_AGGREGATE_TAG, ())
    aggregate
  }

  /**
   * Replace [[GROUPING]] and [[GROUPING_ID]] functions with expressions that extract bits from
   * the grouping ID attribute to determine which grouping set is active.
   */
  def replaceGroupingFunction(
      expression: Expression,
      groupByExpressions: Seq[Expression],
      gid: Expression,
      newAlias: (Expression, Option[String], Seq[String]) => Alias): Expression = {
    val canonicalizedGroupByExpressions = groupByExpressions.map(_.canonicalized)

    expression transform {
      case groupingId: GroupingID =>
        if (groupingId.groupByExprs.isEmpty ||
          groupingId.groupByExprs.map(_.canonicalized) == canonicalizedGroupByExpressions) {
          newAlias(gid, Some(toPrettySQL(groupingId)), Seq.empty)
        } else {
          throw QueryCompilationErrors.groupingIDMismatchError(groupingId, groupByExpressions)
        }
      case grouping @ Grouping(column: Expression) =>
        val index = groupByExpressions.indexWhere(_.semanticEquals(column))
        if (index >= 0) {
          newAlias(
            Cast(
              BitwiseAnd(
                ShiftRight(gid, Literal(groupByExpressions.length - 1 - index)),
                Literal(1L)
              ),
              ByteType
            ).withTimeZone(conf.sessionLocalTimeZone),
            Some(toPrettySQL(grouping)),
            Seq.empty
          )
        } else {
          throw QueryCompilationErrors.groupingColInvalidError(column, groupByExpressions)
        }
    }
  }

  /**
   * Collect the last grouping expression since the provided [[Aggregate]] should have grouping id
   * as the last grouping key.
   *
   * A grand-total `GROUP BY GROUPING SETS (())` is lowered to a global [[Aggregate]] with no
   * grouping expressions (see [[apply]]), so there is no `spark_grouping_id` key and no grouping
   * columns; return an empty sequence for it. That case is recognized by the
   * [[GRAND_TOTAL_AGGREGATE_TAG]] marker rather than by emptiness alone, so a plain no-`GROUP BY`
   * aggregate (also empty grouping expressions, but never lowered from a grouping set) still throws
   * `groupingMustWithGroupingSetsOrCubeOrRollupError`, matching the SELECT-list path where a
   * leftover grouping function fails `UNSUPPORTED_GROUPING_EXPRESSION` in CheckAnalysis.
   */
  def collectGroupingExpressions(aggregate: Aggregate): Seq[Expression] = {
    if (aggregate.groupingExpressions.isEmpty) {
      if (aggregate.getTagValue(GRAND_TOTAL_AGGREGATE_TAG).isEmpty) {
        throw QueryCompilationErrors.groupingMustWithGroupingSetsOrCubeOrRollupError()
      }
      Seq.empty
    } else {
      val gid = aggregate.groupingExpressions.last
      gid match {
        case attributeReference: AttributeReference =>
          if (attributeReference.name != VirtualColumn.groupingIdName) {
            throw QueryCompilationErrors.groupingMustWithGroupingSetsOrCubeOrRollupError()
          }
        case _ =>
          throw QueryCompilationErrors.groupingMustWithGroupingSetsOrCubeOrRollupError()
      }

      aggregate.groupingExpressions.take(aggregate.groupingExpressions.length - 1)
    }
  }

  /**
   * Create new aliases for all group by expressions to prevent null values set by [[Expand]]
   * from being used in aggregates instead of original values.
   */
  private def constructGroupByAlias(
      newAlias: (Expression, Option[String], Seq[String]) => Alias,
      groupByExpressions: Seq[Expression]): Seq[Alias] = {
    groupByExpressions.map {
      case namedExpression: NamedExpression =>
        newAlias(namedExpression, Some(namedExpression.name), namedExpression.qualifier)
      case other =>
        newAlias(other, Some(toPrettySQL(other)), Seq.empty)
    }
  }

  /**
   * Construct [[Expand]] operator with grouping sets. Adjusts nullability of grouping attributes
   * based on whether they appear in all grouping sets.
   */
  private def constructExpand(
      selectedGroupByExpressions: Seq[Seq[Expression]],
      child: LogicalPlan,
      groupByAliases: Seq[Alias],
      gid: Attribute,
      childOutput: Seq[Attribute]): Expand = {
    val expandedAttributes = groupByAliases.map { alias =>
      val aliasAttribute = alias.toAttribute
      if (selectedGroupByExpressions.exists(!_.contains(alias.child))) {
        aliasAttribute.withNullability(true)
      } else {
        aliasAttribute
      }
    }

    val groupingSetsAttributes = selectedGroupByExpressions.map { groupingSetExprs =>
      groupingSetExprs.map { expression =>
        val alias = groupByAliases
          .find(_.child.semanticEquals(expression))
          .getOrElse(
            throw QueryCompilationErrors.selectExprNotInGroupByError(expression, groupByAliases)
          )
        expandedAttributes.find(_.semanticEquals(alias.toAttribute)).getOrElse(alias.toAttribute)
      }
    }

    Expand(
      groupingSetsAttrs = groupingSetsAttributes,
      groupByAliases = groupByAliases,
      groupByAttrs = expandedAttributes,
      gid = gid,
      child = child,
      childOutputOpt = Some(childOutput)
    )
  }

  /**
   * Construct new aggregate expressions by replacing grouping functions with appropriate
   * expressions and mapping group by expressions to expanded attributes.
   */
  private def constructAggregateExpressions(
      newAlias: (Expression, Option[String], Seq[String]) => Alias,
      groupByExpressions: Seq[Expression],
      aggregations: Seq[NamedExpression],
      groupByAliases: Seq[Alias],
      groupingAttributes: Seq[Expression],
      gid: Attribute): Seq[NamedExpression] = {
    val aggregationsWithReplacedGroupingFunctions = aggregations
      .map { expression =>
        replaceGroupingFunction(
          expression = expression,
          groupByExpressions = groupByExpressions,
          gid = gid,
          newAlias = newAlias
        )
      }

    val aggregationsWithExtractedAttributes = aggregationsWithReplacedGroupingFunctions
      .map { expression =>
        replaceExpressions(
          expression = expression,
          groupByAliases = groupByAliases,
          groupingAttributes = groupingAttributes
        )
      }

    aggregationsWithExtractedAttributes.map { expression =>
      expression.asInstanceOf[NamedExpression]
    }
  }

  /**
   * Replace group by expressions with their corresponding expanded attributes from the
   * [[Expand]] operator output. Leaves aggregate expressions unchanged.
   */
  private def replaceExpressions(
      expression: Expression,
      groupByAliases: Seq[Alias],
      groupingAttributes: Seq[Expression]): Expression = expression match {
    case aggregateExpression: AggregateExpression => aggregateExpression
    case other =>
      val index = groupByAliases.indexWhere(_.child.semanticEquals(other))
      if (index == -1) {
        other.mapChildren(replaceExpressions(_, groupByAliases, groupingAttributes))
      } else {
        groupingAttributes(index)
      }
  }
}
