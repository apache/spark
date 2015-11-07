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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Expand, Aggregate, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{IntegerType, StructType, MapType, ArrayType}

/**
 * Utility functions used by the query planner to convert our plan to new aggregation code path.
 */
object Utils {
  // Right now, we do not support complex types in the grouping key schema.
  private def supportsGroupingKeySchema(aggregate: Aggregate): Boolean = {
    val hasComplexTypes = aggregate.groupingExpressions.map(_.dataType).exists {
      case array: ArrayType => true
      case map: MapType => true
      case struct: StructType => true
      case _ => false
    }

    !hasComplexTypes
  }

  private def doConvert(plan: LogicalPlan): Option[Aggregate] = plan match {
    case p: Aggregate if supportsGroupingKeySchema(p) =>
      val converted = MultipleDistinctRewriter.rewrite(p.transformExpressionsDown {
        case expressions.Average(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Average(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Count(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Count(child),
            mode = aggregate.Complete,
            isDistinct = false)

        // We do not support multiple COUNT DISTINCT columns for now.
        case expressions.CountDistinct(children) if children.length == 1 =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Count(children.head),
            mode = aggregate.Complete,
            isDistinct = true)

        case expressions.First(child, ignoreNulls) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.First(child, ignoreNulls),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Kurtosis(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Kurtosis(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Last(child, ignoreNulls) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Last(child, ignoreNulls),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Max(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Max(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Min(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Min(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Skewness(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Skewness(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.StddevPop(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.StddevPop(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.StddevSamp(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.StddevSamp(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.Sum(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Sum(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.SumDistinct(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Sum(child),
            mode = aggregate.Complete,
            isDistinct = true)

        case expressions.Corr(left, right) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.Corr(left, right),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.ApproxCountDistinct(child, rsd) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.HyperLogLogPlusPlus(child, rsd),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.VariancePop(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.VariancePop(child),
            mode = aggregate.Complete,
            isDistinct = false)

        case expressions.VarianceSamp(child) =>
          aggregate.AggregateExpression2(
            aggregateFunction = aggregate.VarianceSamp(child),
            mode = aggregate.Complete,
            isDistinct = false)
      })

      // Check if there is any expressions.AggregateExpression1 left.
      // If so, we cannot convert this plan.
      val hasAggregateExpression1 = converted.aggregateExpressions.exists { expr =>
        // For every expressions, check if it contains AggregateExpression1.
        expr.find {
          case agg: expressions.AggregateExpression1 => true
          case other => false
        }.isDefined
      }

      // Check if there are multiple distinct columns.
      // TODO remove this.
      val aggregateExpressions = converted.aggregateExpressions.flatMap { expr =>
        expr.collect {
          case agg: AggregateExpression2 => agg
        }
      }.toSet.toSeq
      val functionsWithDistinct = aggregateExpressions.filter(_.isDistinct)
      val hasMultipleDistinctColumnSets =
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          true
        } else {
          false
        }

      if (!hasAggregateExpression1 && !hasMultipleDistinctColumnSets) Some(converted) else None

    case other => None
  }

  def checkInvalidAggregateFunction2(aggregate: Aggregate): Unit = {
    // If the plan cannot be converted, we will do a final round check to see if the original
    // logical.Aggregate contains both AggregateExpression1 and AggregateExpression2. If so,
    // we need to throw an exception.
    val aggregateFunction2s = aggregate.aggregateExpressions.flatMap { expr =>
      expr.collect {
        case agg: AggregateExpression2 => agg.aggregateFunction
      }
    }.distinct
    if (aggregateFunction2s.nonEmpty) {
      // For functions implemented based on the new interface, prepare a list of function names.
      val invalidFunctions = {
        if (aggregateFunction2s.length > 1) {
          s"${aggregateFunction2s.tail.map(_.nodeName).mkString(",")} " +
            s"and ${aggregateFunction2s.head.nodeName} are"
        } else {
          s"${aggregateFunction2s.head.nodeName} is"
        }
      }
      val errorMessage =
        s"${invalidFunctions} implemented based on the new Aggregate Function " +
          s"interface and it cannot be used with functions implemented based on " +
          s"the old Aggregate Function interface."
      throw new AnalysisException(errorMessage)
    }
  }

  def tryConvert(plan: LogicalPlan): Option[Aggregate] = plan match {
    case p: Aggregate =>
      val converted = doConvert(p)
      if (converted.isDefined) {
        converted
      } else {
        checkInvalidAggregateFunction2(p)
        None
      }
    case other => None
  }
}

/**
 * This rule rewrites an aggregate query with multiple distinct clauses into an expanded double
 * aggregation in which the regular aggregation expressions and every distinct clause is aggregated
 * in a separate group. The results are then combined in a second aggregate.
 *
 * TODO Expression cannocalization
 * TODO Eliminate foldable expressions from distinct clauses.
 * TODO This eliminates all distinct expressions. We could safely pass one to the aggregate
 *      operator. Perhaps this is a good thing? It is much simpler to plan later on...
 */
object MultipleDistinctRewriter extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case a: Aggregate => rewrite(a)
    case p => p
  }

  def rewrite(a: Aggregate): Aggregate = {

    // Collect all aggregate expressions.
    val aggExpressions = a.aggregateExpressions.flatMap { e =>
      e.collect {
        case ae: AggregateExpression2 => ae
      }
    }

    // Extract distinct aggregate expressions.
    val distinctAggGroups = aggExpressions
      .filter(_.isDistinct)
      .groupBy(_.aggregateFunction.children.toSet)

    // Only continue to rewrite if there is more than one distinct group.
    if (distinctAggGroups.size > 1) {
      // Create the attributes for the grouping id and the group by clause.
      val gid = new AttributeReference("gid", IntegerType, false)()
      val groupByMap = a.groupingExpressions.collect {
        case ne: NamedExpression => ne -> ne.toAttribute
        case e => e -> new AttributeReference(e.prettyName, e.dataType, e.nullable)()
      }
      val groupByAttrs = groupByMap.map(_._2)

      // Functions used to modify aggregate functions and their inputs.
      def evalWithinGroup(id: Literal, e: Expression) = If(EqualTo(gid, id), e, nullify(e))
      def patchAggregateFunctionChildren(
          af: AggregateFunction2,
          id: Literal,
          attrs: Map[Expression, Expression]): AggregateFunction2 = {
        af.withNewChildren(af.children.map { case afc =>
          evalWithinGroup(id, attrs(afc))
        }).asInstanceOf[AggregateFunction2]
      }

      // Setup unique distinct aggregate children.
      val distinctAggChildren = distinctAggGroups.keySet.flatten.toSeq
      val distinctAggChildAttrMap = distinctAggChildren.map(expressionAttributePair).toMap
      val distinctAggChildAttrs = distinctAggChildAttrMap.values.toSeq

      // Setup expand & aggregate operators for distinct aggregate expressions.
      val distinctAggOperatorMap = distinctAggGroups.toSeq.zipWithIndex.map {
        case ((group, expressions), i) =>
          val id = Literal(i + 1)

          // Expand projection
          val projection = distinctAggChildren.map {
            case e if group.contains(e) => e
            case e => nullify(e)
          } :+ id

          // Final aggregate
          val operators = expressions.map { e =>
            val af = e.aggregateFunction
            val naf = patchAggregateFunctionChildren(af, id, distinctAggChildAttrMap)
            (e, e.copy(aggregateFunction = naf, isDistinct = false))
          }

          (projection, operators)
      }

      // Setup expand for the 'regular' aggregate expressions.
      val regularAggExprs = aggExpressions.filter(!_.isDistinct)
      val regularAggChildren = regularAggExprs.flatMap(_.aggregateFunction.children).distinct
      val regularAggChildAttrMap = regularAggChildren.map(expressionAttributePair).toMap

      // Setup aggregates for 'regular' aggregate expressions.
      val regularGroupId = Literal(0)
      val regularAggOperatorMap = regularAggExprs.map { e =>
        // Perform the actual aggregation in the initial aggregate.
        val af = patchAggregateFunctionChildren(
          e.aggregateFunction,
          regularGroupId,
          regularAggChildAttrMap)
        val a = Alias(e.copy(aggregateFunction = af), e.toString)()

        // Get the result of the first aggregate in the last aggregate.
        val b = AggregateExpression2(
          aggregate.First(evalWithinGroup(regularGroupId, a.toAttribute), Literal(true)),
          mode = Complete,
          isDistinct = false)

        // Some aggregate functions (COUNT) have the special property that they can return a
        // non-null result without any input. We need to make sure we return a result in this case.
        val c = af.defaultResult match {
          case Some(lit) => Coalesce(Seq(b, lit))
          case None => b
        }

        (e, a, c)
      }

      // Construct the regular aggregate input projection only if we need one.
      val regularAggProjection = if (regularAggExprs.nonEmpty) {
        Seq(a.groupingExpressions ++
          distinctAggChildren.map(nullify) ++
          Seq(regularGroupId) ++
          regularAggChildren)
      } else {
        Seq.empty[Seq[Expression]]
      }

      // Construct the distinct aggregate input projections.
      val regularAggNulls = regularAggChildren.map(nullify)
      val distinctAggProjections = distinctAggOperatorMap.map {
        case (projection, _) =>
          a.groupingExpressions ++
            projection ++
            regularAggNulls
      }

      // Construct the expand operator.
      val expand = Expand(
        regularAggProjection ++ distinctAggProjections,
        groupByAttrs ++ distinctAggChildAttrs ++ Seq(gid) ++ regularAggChildAttrMap.values.toSeq,
        a.child)

      // Construct the first aggregate operator. This de-duplicates the all the children of
      // distinct operators, and applies the regular aggregate operators.
      val firstAggregateGroupBy = groupByAttrs ++ distinctAggChildAttrs :+ gid
      val firstAggregate = Aggregate(
        firstAggregateGroupBy,
        firstAggregateGroupBy ++ regularAggOperatorMap.map(_._2),
        expand)

      // Construct the second aggregate
      val transformations: Map[Expression, Expression] =
        (distinctAggOperatorMap.flatMap(_._2) ++
          regularAggOperatorMap.map(e => (e._1, e._3))).toMap

      val patchedAggExpressions = a.aggregateExpressions.map { e =>
        e.transformDown {
          case e: Expression =>
            // The same GROUP BY clauses can have different forms (different names for instance) in
            // the groupBy and aggregate expressions of an aggregate. This makes a map lookup
            // tricky. So we do a linear search for a semantically equal group by expression.
            groupByMap
              .find(ge => e.semanticEquals(ge._1))
              .map(_._2)
              .getOrElse(transformations.getOrElse(e, e))
        }.asInstanceOf[NamedExpression]
      }
      Aggregate(groupByAttrs, patchedAggExpressions, firstAggregate)
    } else {
      a
    }
  }

  private def nullify(e: Expression) = Literal.create(null, e.dataType)

  private def expressionAttributePair(e: Expression) =
    // We are creating a new reference here instead of reusing the attribute in case of a
    // NamedExpression. This is done to prevent collisions between distinct and regular aggregate
    // children, in this case attribute reuse causes the input of the regular aggregate to bound to
    // the (nulled out) input of the distinct aggregate.
    e -> new AttributeReference(e.prettyName, e.dataType, true)()
}
