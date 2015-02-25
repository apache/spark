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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.trees._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.types._

case class AggregateEvaluation(
    schema: Seq[Attribute],
    initialValues: Seq[Expression],
    update: Seq[Expression],
    result: Expression)

/**
 * :: DeveloperApi ::
 * Alternate version of aggregation that leverages projection and thus code generation.
 * Aggregations are converted into a set of projections from a aggregation buffer tuple back onto
 * itself. Currently only used for simple aggregations like SUM, COUNT, or AVERAGE are supported.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class GeneratedAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output = aggregateExpressions.map(_.toAttribute)

  override def execute() = {
    val aggregatesToCompute = aggregateExpressions.flatMap { a =>
      a.collect { case agg: AggregateExpression => agg}
    }

    val computeFunctions = aggregatesToCompute.map {
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
        val initialValue = Literal(null, calcType)

        // Coalasce avoids double calculation...
        // but really, common sub expression elimination would be better....
        val zero = Cast(Literal(0), calcType)
        val updateFunction = Coalesce(
          Add(Coalesce(currentSum :: zero :: Nil), Cast(expr, calcType)) :: currentSum :: Nil)
        val result =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              Cast(currentSum, s.dataType)
            case _ => currentSum
          }

        AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case a @ Average(expr) =>
        val calcType =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              DecimalType.Unlimited
            case _ =>
              expr.dataType
          }

        val currentCount = AttributeReference("currentCount", LongType, nullable = false)()
        val currentSum = AttributeReference("currentSum", calcType, nullable = false)()
        val initialCount = Literal(0L)
        val initialSum = Cast(Literal(0L), calcType)

        // If we're evaluating UnscaledValue(x), we can do Count on x directly, since its
        // UnscaledValue will be null if and only if x is null; helps with Average on decimals
        val toCount = expr match {
          case UnscaledValue(e) => e
          case _ => expr
        }

        val updateCount = If(IsNotNull(toCount), Add(currentCount, Literal(1L)), currentCount)
        val updateSum = Coalesce(Add(Cast(expr, calcType), currentSum) :: currentSum :: Nil)

        val result =
          expr.dataType match {
            case DecimalType.Fixed(_, _) =>
              If(EqualTo(currentCount, Literal(0L)),
                Literal(null, a.dataType),
                Cast(Divide(
                  Cast(currentSum, DecimalType.Unlimited),
                  Cast(currentCount, DecimalType.Unlimited)), a.dataType))
            case _ =>
              If(EqualTo(currentCount, Literal(0L)),
                Literal(null, a.dataType),
                Divide(Cast(currentSum, a.dataType), Cast(currentCount, a.dataType)))
          }

        AggregateEvaluation(
          currentCount :: currentSum :: Nil,
          initialCount :: initialSum :: Nil,
          updateCount :: updateSum :: Nil,
          result
        )

      case m @ Max(expr) =>
        val currentMax = AttributeReference("currentMax", expr.dataType, nullable = true)()
        val initialValue = Literal(null, expr.dataType)
        val updateMax = MaxOf(currentMax, expr)

        AggregateEvaluation(
          currentMax :: Nil,
          initialValue :: Nil,
          updateMax :: Nil,
          currentMax)

      case CollectHashSet(Seq(expr)) =>
        val set = AttributeReference("hashSet", ArrayType(expr.dataType), nullable = false)()
        val initialValue = NewSet(expr.dataType)
        val addToSet = AddItemToSet(expr, set)

        AggregateEvaluation(
          set :: Nil,
          initialValue :: Nil,
          addToSet :: Nil,
          set)

      case CombineSetsAndCount(inputSet) =>
        val ArrayType(inputType, _) = inputSet.dataType
        val set = AttributeReference("hashSet", inputSet.dataType, nullable = false)()
        val initialValue = NewSet(inputType)
        val collectSets = CombineSets(set, inputSet)

        AggregateEvaluation(
          set :: Nil,
          initialValue :: Nil,
          collectSets :: Nil,
          CountSet(set))
    }

    val computationSchema = computeFunctions.flatMap(_.schema)

    val resultMap: Map[TreeNodeRef, Expression] =
      aggregatesToCompute.zip(computeFunctions).map {
        case (agg, func) => new TreeNodeRef(agg) -> func.result
      }.toMap

    val namedGroups = groupingExpressions.zipWithIndex.map {
      case (ne: NamedExpression, _) => (ne, ne)
      case (e, i) => (e, Alias(e, s"GroupingExpr$i")())
    }

    val groupMap: Map[Expression, Attribute] =
      namedGroups.map { case (k, v) => k -> v.toAttribute}.toMap

    // The set of expressions that produce the final output given the aggregation buffer and the
    // grouping expressions.
    val resultExpressions = aggregateExpressions.map(_.transform {
      case e: Expression if resultMap.contains(new TreeNodeRef(e)) => resultMap(new TreeNodeRef(e))
      case e: Expression if groupMap.contains(e) => groupMap(e)
    })

    child.execute().mapPartitions { iter =>
      // Builds a new custom class for holding the results of aggregation for a group.
      val initialValues = computeFunctions.flatMap(_.initialValues)
      val newAggregationBuffer = newProjection(initialValues, child.output)
      log.info(s"Initial values: ${initialValues.mkString(",")}")

      // A projection that computes the group given an input tuple.
      val groupProjection = newProjection(groupingExpressions, child.output)
      log.info(s"Grouping Projection: ${groupingExpressions.mkString(",")}")

      // A projection that is used to update the aggregate values for a group given a new tuple.
      // This projection should be targeted at the current values for the group and then applied
      // to a joined row of the current values with the new input row.
      val updateExpressions = computeFunctions.flatMap(_.update)
      val updateSchema = computeFunctions.flatMap(_.schema) ++ child.output
      val updateProjection = newMutableProjection(updateExpressions, updateSchema)()
      log.info(s"Update Expressions: ${updateExpressions.mkString(",")}")

      // A projection that produces the final result, given a computation.
      val resultProjectionBuilder =
        newMutableProjection(
          resultExpressions,
          (namedGroups.map(_._2.toAttribute) ++ computationSchema).toSeq)
      log.info(s"Result Projection: ${resultExpressions.mkString(",")}")

      val joinedRow = new JoinedRow3

      if (groupingExpressions.isEmpty) {
        // TODO: Codegening anything other than the updateProjection is probably over kill.
        val buffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
        var currentRow: Row = null
        updateProjection.target(buffer)

        while (iter.hasNext) {
          currentRow = iter.next()
          updateProjection(joinedRow(buffer, currentRow))
        }

        val resultProjection = resultProjectionBuilder()
        Iterator(resultProjection(buffer))
      } else {
        val buffers = new java.util.HashMap[Row, MutableRow]()

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupProjection(currentRow)
          var currentBuffer = buffers.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregationBuffer(EmptyRow).asInstanceOf[MutableRow]
            buffers.put(currentGroup, currentBuffer)
          }
          // Target the projection at the current aggregation buffer and then project the updated
          // values.
          updateProjection.target(currentBuffer)(joinedRow(currentBuffer, currentRow))
        }

        new Iterator[Row] {
          private[this] val resultIterator = buffers.entrySet.iterator()
          private[this] val resultProjection = resultProjectionBuilder()

          def hasNext = resultIterator.hasNext

          def next() = {
            val currentGroup = resultIterator.next()
            resultProjection(joinedRow(currentGroup.getKey, currentGroup.getValue))
          }
        }
      }
    }
  }
}
