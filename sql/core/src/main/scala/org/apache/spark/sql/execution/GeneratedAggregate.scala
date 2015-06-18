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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class AggregateEvaluation(
    schema: Seq[Attribute],
    initialValues: Seq[Expression],
    update: Seq[Expression],
    result: Expression)

trait GeneratedAggregate {
  self: SparkPlan =>

  val groupingExpressions: Seq[Expression]
  val aggregateExpressions: Seq[NamedExpression]
  val unsafeEnabled: Boolean
  val child: SparkPlan

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  @transient protected lazy val aggregatesToCompute = aggregateExpressions.flatMap { a =>
    a.collect { case agg: AggregateExpression => agg}
  }

  @transient protected lazy val namedGroups = groupingExpressions.zipWithIndex.map {
    case (ne: NamedExpression, _) => (ne, ne.toAttribute)
    case (e, i) => (e, Alias(e, s"GroupingExpr$i")().toAttribute)
  }

  // If you add any new function support, please add tests in org.apache.spark.sql.SQLQuerySuite
  // (in test "aggregation with codegen").
  @transient protected lazy val computeFunctions = aggregatesToCompute.map {
    case c @ Count(expr) =>
      // If we're evaluating UnscaledValue(x), we can do Count on x directly, since its
      // UnscaledValue will be null if and only if x is null; helps with Average on decimals
      val toCount = expr match {
        case UnscaledValue(e) => e
        case _ => expr
      }
      val currentCount = AttributeReference("currentCount", LongType, nullable = false)()
      val initialValue = Literal(0L)
      val updateFunction = If(IsNotNull(toCount), Add(currentCount, Literal(1L)), currentCount)
      val result = currentCount

      AggregateEvaluation(currentCount :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

    case s @ Sum(expr) =>
      val calcType =
        expr.dataType match {
          case DecimalType.Fixed(_, _) =>
            DecimalType.Unlimited
          case _ =>
            expr.dataType
        }

      val currentSum = AttributeReference("currentSum", calcType, nullable = true)()
      val initialValue = Literal.create(null, calcType)

      // Coalesce avoids double calculation...
      // but really, common sub expression elimination would be better....
      val zero = Cast(Literal(0), calcType)
      val updateFunction = Coalesce(
        Add(
          Coalesce(currentSum :: zero :: Nil),
          Cast(expr, calcType)
        ) :: currentSum :: zero :: Nil)
      val result =
        expr.dataType match {
          case DecimalType.Fixed(_, _) =>
            Cast(currentSum, s.dataType)
          case _ => currentSum
        }

      AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

    case cs @ CombineSum(expr) =>
      val calcType =
        expr.dataType match {
          case DecimalType.Fixed(_, _) =>
            DecimalType.Unlimited
          case _ =>
            expr.dataType
        }

      val currentSum = AttributeReference("currentSum", calcType, nullable = true)()
      val initialValue = Literal.create(null, calcType)

      // Coalesce avoids double calculation...
      // but really, common sub expression elimination would be better....
      val zero = Cast(Literal(0), calcType)
      // If we're evaluating UnscaledValue(x), we can do Count on x directly, since its
      // UnscaledValue will be null if and only if x is null; helps with Average on decimals
      val actualExpr = expr match {
        case UnscaledValue(e) => e
        case _ => expr
      }
      // partial sum result can be null only when no input rows present
      val updateFunction = If(
        IsNotNull(actualExpr),
        Coalesce(
          Add(
            Coalesce(currentSum :: zero :: Nil),
            Cast(expr, calcType)) :: currentSum :: zero :: Nil),
        currentSum)

      val result =
        expr.dataType match {
          case DecimalType.Fixed(_, _) =>
            Cast(currentSum, cs.dataType)
          case _ => currentSum
        }

      AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

    case m @ Max(expr) =>
      val currentMax = AttributeReference("currentMax", expr.dataType, nullable = true)()
      val initialValue = Literal.create(null, expr.dataType)
      val updateMax = MaxOf(currentMax, expr)

      AggregateEvaluation(
        currentMax :: Nil,
        initialValue :: Nil,
        updateMax :: Nil,
        currentMax)

    case m @ Min(expr) =>
      val currentMin = AttributeReference("currentMin", expr.dataType, nullable = true)()
      val initialValue = Literal.create(null, expr.dataType)
      val updateMin = MinOf(currentMin, expr)

      AggregateEvaluation(
        currentMin :: Nil,
        initialValue :: Nil,
        updateMin :: Nil,
        currentMin)

    case CollectHashSet(Seq(expr)) =>
      val set =
        AttributeReference("hashSet", new OpenHashSetUDT(expr.dataType), nullable = false)()
      val initialValue = NewSet(expr.dataType)
      val addToSet = AddItemToSet(expr, set)

      AggregateEvaluation(
        set :: Nil,
        initialValue :: Nil,
        addToSet :: Nil,
        set)

    case CombineSetsAndCount(inputSet) =>
      val elementType = inputSet.dataType.asInstanceOf[OpenHashSetUDT].elementType
      val set =
        AttributeReference("hashSet", new OpenHashSetUDT(elementType), nullable = false)()
      val initialValue = NewSet(elementType)
      val collectSets = CombineSets(set, inputSet)

      AggregateEvaluation(
        set :: Nil,
        initialValue :: Nil,
        collectSets :: Nil,
        CountSet(set))

    case o => sys.error(s"$o can't be codegened.")
  }

  @transient protected lazy val computationSchema = computeFunctions.flatMap(_.schema)

  @transient protected lazy val resultMap: Map[TreeNodeRef, Expression] =
    aggregatesToCompute.zip(computeFunctions).map {
      case (agg, func) => new TreeNodeRef(agg) -> func.result
    }.toMap

  // The set of expressions that produce the final output given the aggregation buffer and the
  // grouping expressions.
  @transient protected lazy val resultExpressions = aggregateExpressions.map(_.transform {
    case e: Expression if resultMap.contains(new TreeNodeRef(e)) => resultMap(new TreeNodeRef(e))
    case e: Expression =>
      namedGroups.collectFirst {
        case (expr, attr) if expr semanticEquals e => attr
      }.getOrElse(e)
  })
}
