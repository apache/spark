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
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  AliasHelper,
  Attribute,
  AttributeSet,
  Cast,
  EmptyRow,
  EqualNullSafe,
  Expression,
  ExtractValue,
  If,
  In,
  Literal,
  NamedExpression
}
import org.apache.spark.sql.catalyst.expressions.aggregate.{
  AggregateExpression,
  AggregateFunction,
  ApproximatePercentile,
  First,
  Last,
  PivotFirst
}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StringType

/**
 * Object used to transform [[Pivot]] node into a [[Project]] or [[Aggregate]] (based on the tree
 * structure below the [[Pivot]]).
 */
object PivotTransformer extends AliasHelper with SQLConfHelper {

  /**
   * Transform a pivot operation into an [[Aggregate]] or a combination of [[Aggregate]]s and
   * [[Project]] operators.
   *
   *  1. Check all pivot values are literal and match pivot column data type.
   *
   *  2. Deduce group-by expressions. Group-by expressions coming from SQL are implicit and need to
   *     be deduced by filtering out pivot column and aggregate references from child output.
   *     In case of:
   *     {{{
   *       SELECT year, region, q1, q2, q3, q4
   *       FROM sales
   *       PIVOT (sum(sales) AS sales
   *         FOR quarter
   *         IN (1 AS q1, 2 AS q2, 3 AS q3, 4 AS q4));
   *     }}}
   *     where table `sales` has `year`, `quarter`, `region`, `sales` as columns.
   *     In this example: pivot column would be `quarter`, aggregate would be `sales` and because
   *     of that, `year` and `region` would be grouping expressions.
   *
   *  3. Choose between two execution strategies based on aggregate data types:
   *
   *     a) If all aggregates support [[PivotFirst]] data types (fast path):
   *        Since evaluating `pivotValues` `IF` statements for each input row can get slow, use an
   *        alternate plan that instead uses two steps of aggregation:
   *        - First aggregation: group by original grouping expressions + pivot column, compute
   *          aggregates
   *        - Second aggregation: group by original grouping expressions only, use [[PivotFirst]]
   *          to extract values for each pivot value
   *        - Final projection: extract individual pivot outputs using [[ExtractValue]]
   *
   *     b) Otherwise (standard path):
   *        Create a single [[Aggregate]] with filtered aggregates for each pivot value. For each
   *        aggregate and pivot value combination:
   *        - Wrap aggregate children with `If(pivotColumn == pivotValue, expr, null)` expressions.
   *        - Handle special cases for [[First]], [[Last]], and [[ApproximatePercentile]] which
   *          have specific semantics around null handling.
   */
  def apply(
      child: LogicalPlan,
      pivotValues: Seq[Expression],
      pivotColumn: Expression,
      groupByExpressionsOpt: Option[Seq[NamedExpression]],
      aggregates: Seq[Expression],
      childOutput: Seq[Attribute],
      newAlias: (Expression, Option[String]) => Alias): LogicalPlan = {
    val evalPivotValues = pivotValues.map { value =>
      val foldable = trimAliases(value).foldable
      if (!foldable) {
        throw QueryCompilationErrors.nonLiteralPivotValError(value)
      }
      if (!Cast.canCast(value.dataType, pivotColumn.dataType)) {
        throw QueryCompilationErrors.pivotValDataTypeMismatchError(value, pivotColumn)
      }
      Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
    }
    val groupByExpressions = groupByExpressionsOpt.getOrElse {
      val pivotColumnAndAggregatesRefs = pivotColumn.references ++ AttributeSet(aggregates)
      childOutput.filterNot(pivotColumnAndAggregatesRefs.contains)
    }
    if (aggregates.forall(aggregate => PivotFirst.supportsDataType(aggregate.dataType))) {
      val namedAggExps: Seq[NamedExpression] = aggregates.map { aggregate =>
        newAlias(aggregate, Some(aggregate.sql))
      }
      val namedPivotCol = pivotColumn match {
        case namedExpression: NamedExpression => namedExpression
        case _ =>
          newAlias(pivotColumn, Some("__pivot_col"))
      }
      // SPARK-55569: Collapse non-matching pivot column values to null so the firstAgg produces
      // at most one group per (groupBy-key, null) instead of one group per distinct non-matching
      // value. PivotFirst ignores the null group, but the groupBy key is preserved in secondAgg,
      // keeping the semantics identical to the un-optimized plan. This reduces the number of
      // groups when the table has many distinct values outside the pivot IN list.
      // Skip when the pivot column contains an aggregate (SPARK-24722) or when a NULL pivot
      // value is present (non-matching rows would merge with legitimate null rows).
      val hasNullPivotValue =
        evalPivotValues.contains(null)
      val canCollapse =
        !pivotColumn.exists(_.isInstanceOf[AggregateFunction]) &&
        !hasNullPivotValue
      val effectivePivotCol = if (canCollapse) {
        val castPivotExprs = pivotValues.map { value =>
          Cast(value, pivotColumn.dataType,
            Some(conf.sessionLocalTimeZone))
        }
        val nonNullExprs = castPivotExprs
          .zip(evalPivotValues)
          .collect { case (expr, v) if v != null => expr }
        val matchesPivotValue = if (nonNullExprs.nonEmpty) {
          In(pivotColumn, nonNullExprs)
        } else {
          Literal.FalseLiteral
        }
        newAlias(
          If(matchesPivotValue, pivotColumn,
            Literal(null, pivotColumn.dataType)),
          Some(namedPivotCol match {
            case ne: NamedExpression => ne.name
            case _ => "__pivot_col"
          }))
      } else {
        namedPivotCol
      }
      val extendedGroupingExpressions =
        groupByExpressions :+ effectivePivotCol
      val firstAgg =
        Aggregate(extendedGroupingExpressions,
          extendedGroupingExpressions ++ namedAggExps, child)
      val pivotAggregates = namedAggExps.map { a =>
        newAlias(
          PivotFirst(effectivePivotCol.toAttribute,
            a.toAttribute, evalPivotValues)
            .toAggregateExpression(),
          Some("__pivot_" + a.sql)
        )
      }
      val groupByExpressionsAttributes = groupByExpressions.map(_.toAttribute)
      val secondAgg =
        Aggregate(
          groupByExpressionsAttributes,
          groupByExpressionsAttributes ++ pivotAggregates,
          firstAgg
        )
      val pivotAggregatesAttributes = pivotAggregates.map(_.toAttribute)
      val pivotOutputs = pivotValues.zipWithIndex.flatMap {
        case (value, i) =>
          aggregates.zip(pivotAggregatesAttributes).map {
            case (aggregate, pivotAtt) =>
              newAlias(
                ExtractValue(pivotAtt, Literal(i), conf.resolver),
                Some(outputName(value, aggregate, isSingleAggregate = aggregates.size == 1))
              )
          }
      }
      Project(groupByExpressionsAttributes ++ pivotOutputs, secondAgg)
    } else {
      val pivotAggregates: Seq[NamedExpression] = pivotValues.flatMap { value =>
        aggregates.map { aggregate =>
          val filteredAggregate = aggregate
            .transformDown {
              case First(expression, _) =>
                First(createIfExpression(expression, pivotColumn, value), true)
              case Last(expression, _) =>
                Last(createIfExpression(expression, pivotColumn, value), true)
              case approximatePercentile: ApproximatePercentile =>
                approximatePercentile.withNewChildren(
                  createIfExpression(approximatePercentile.first, pivotColumn, value) ::
                  approximatePercentile.second ::
                  approximatePercentile.third ::
                  Nil
                )
              case aggregateFunction: AggregateFunction =>
                aggregateFunction.withNewChildren(aggregateFunction.children.map { child =>
                  createIfExpression(child, pivotColumn, value)
                })
            }
            .transform {
              // TODO: Don't construct the physical container until after analysis.
              case aggregateExpression: AggregateExpression =>
                aggregateExpression.copy(resultId = NamedExpression.newExprId)
            }
          newAlias(
            filteredAggregate,
            Some(outputName(value, aggregate, isSingleAggregate = aggregates.size == 1))
          )
        }
      }
      Aggregate(groupByExpressions, groupByExpressions ++ pivotAggregates, child)
    }
  }

  private def outputName(
      value: Expression,
      aggregate: Expression,
      isSingleAggregate: Boolean): String = {
    val stringValue = value match {
      case namedExpression: NamedExpression => namedExpression.name
      case _ =>
        val utf8Value =
          Cast(value, StringType, Some(conf.sessionLocalTimeZone)).eval(EmptyRow)
        Option(utf8Value).map(_.toString).getOrElse("null")
    }
    if (isSingleAggregate) {
      stringValue
    } else {
      val suffix = aggregate match {
        case namedExpression: NamedExpression => namedExpression.name
        case _ => toPrettySQL(aggregate)
      }
      stringValue + "_" + suffix
    }
  }

  private def createIfExpression(
      expression: Expression,
      pivotColumn: Expression,
      value: Expression) = {
    If(
      EqualNullSafe(
        pivotColumn,
        Cast(value, pivotColumn.dataType, Some(conf.sessionLocalTimeZone))
      ),
      expression,
      Literal(null)
    )
  }
}
