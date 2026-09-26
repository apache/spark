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
  AttributeReference,
  AttributeSet,
  Cast,
  Coalesce,
  EmptyRow,
  EqualNullSafe,
  Expression,
  ExtractValue,
  If,
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
import org.apache.spark.sql.internal.SQLConf
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
      val extendedGroupingExpressions = groupByExpressions :+ namedPivotCol
      val firstAgg =
        Aggregate(extendedGroupingExpressions, extendedGroupingExpressions ++ namedAggExps, child)
      val pivotAggregates = namedAggExps.map { a =>
        newAlias(
          PivotFirst(namedPivotCol.toAttribute, a.toAttribute, evalPivotValues)
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
      // When the flag is disabled, supply no empty-input defaults: empty buckets stay NULL and no
      // Coalesce is added, so the pivoted columns stay nullable.
      val aggregateEmptyInputDefaults: Seq[Option[Expression]] =
        if (conf.getConf(SQLConf.PIVOT_EMPTY_BUCKET_RETURNS_AGGREGATE_DEFAULT)) {
          aggregates.map(aggregateEmptyInputDefault)
        } else {
          Seq.fill(aggregates.size)(None)
        }
      val pivotOutputs = pivotValues.zipWithIndex.flatMap {
        case (value, i) =>
          aggregates.zip(pivotAggregatesAttributes).zip(aggregateEmptyInputDefaults).map {
            case ((aggregate, pivotAtt), emptyInputDefault) =>
              val extractedValue = ExtractValue(pivotAtt, Literal(i), conf.resolver)
              val withEmptyInputDefault = emptyInputDefault match {
                case Some(default) => Coalesce(Seq(extractedValue, default))
                case None => extractedValue
              }
              newAlias(
                withEmptyInputDefault,
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

  /**
   * Empty-input default for a pivot aggregate to coalesce into its extracted value, or `None` to
   * leave the value unchanged. The fast path's [[PivotFirst]] leaves an unmatched pivot category's
   * slot unset, so the caller wraps the result in a [[Coalesce]] to recover the value the slow path
   * produces on an empty bucket (`count` -> 0; `sum`/`avg`/`min`/`max` -> NULL).
   *
   * Returned unevaluated so later constant folding defers a default that throws under ANSI (e.g.
   * `count(v1) / count(v2)` -> `0 / 0`) to runtime, where [[Coalesce]] only evaluates it for
   * actually-empty buckets -- matching the slow path. Mirrors
   * `RewriteCorrelatedScalarSubquery.evalAggExprOnZeroTups`.
   */
  private def aggregateEmptyInputDefault(aggregate: Expression): Option[Expression] = {
    trimAliases(aggregate) match {
      // Bare aggregate: use its published default result (count -> 0, sum/avg/min/max -> None).
      case AggregateExpression(aggregateFunction, _, _, _, _) =>
        aggregateFunction.defaultResult
      // Composite over aggregate(s): substitute each aggregate/attribute with its empty-input
      // value. Return None for a non-foldable default or a literal NULL (nothing to coalesce in). A
      // default that only folds to NULL (e.g. sum(x) + 1) is still returned; its Coalesce evaluates
      // to NULL on an empty bucket, which is the correct result.
      case other =>
        val default = other.transform {
          case AggregateExpression(aggregateFunction, _, _, _, _) =>
            aggregateFunction.defaultResult.getOrElse(
              Literal.create(null, aggregateFunction.dataType))
          case attribute: AttributeReference =>
            Literal.create(null, attribute.dataType)
        }
        default match {
          case _ if !trimAliases(default).foldable => None
          case Literal(null, _) => None
          case _ => Some(default)
        }
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
