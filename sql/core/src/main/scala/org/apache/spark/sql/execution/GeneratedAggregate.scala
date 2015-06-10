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

import org.apache.spark.TaskContext
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
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
 * @param unsafeEnabled whether to allow Unsafe-based aggregation buffers to be used.
 * @param child the input data source.
 */
@DeveloperApi
case class GeneratedAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    unsafeEnabled: Boolean,
    child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution: Seq[Distribution] =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  protected override def doExecute(): RDD[Row] = {
    val aggregatesToCompute = aggregateExpressions.flatMap { a =>
      a.collect { case agg: AggregateExpression => agg}
    }

    // If you add any new function support, please add tests in org.apache.spark.sql.SQLQuerySuite
    // (in test "aggregation with codegen").
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

    val aggregationBufferSchema: StructType = StructType.fromAttributes(computationSchema)

    val groupKeySchema: StructType = {
      val fields = groupingExpressions.zipWithIndex.map { case (expr, idx) =>
        // This is a dummy field name
        StructField(idx.toString, expr.dataType, expr.nullable)
      }
      StructType(fields)
    }

    val schemaSupportsUnsafe: Boolean = {
      UnsafeFixedWidthAggregationMap.supportsAggregationBufferSchema(aggregationBufferSchema) &&
        UnsafeFixedWidthAggregationMap.supportsGroupKeySchema(groupKeySchema)
    }

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
      } else if (unsafeEnabled && schemaSupportsUnsafe) {
        log.info("Using Unsafe-based aggregator")
        val aggregationMap = new UnsafeFixedWidthAggregationMap(
          newAggregationBuffer(EmptyRow),
          aggregationBufferSchema,
          groupKeySchema,
          TaskContext.get.taskMemoryManager(),
          1024 * 16, // initial capacity
          false // disable tracking of performance metrics
        )

        while (iter.hasNext) {
          val currentRow: Row = iter.next()
          val groupKey: Row = groupProjection(currentRow)
          val aggregationBuffer = aggregationMap.getAggregationBuffer(groupKey)
          updateProjection.target(aggregationBuffer)(joinedRow(aggregationBuffer, currentRow))
        }

        new Iterator[Row] {
          private[this] val mapIterator = aggregationMap.iterator()
          private[this] val resultProjection = resultProjectionBuilder()

          def hasNext: Boolean = mapIterator.hasNext

          def next(): Row = {
            val entry = mapIterator.next()
            val result = resultProjection(joinedRow(entry.key, entry.value))
            if (hasNext) {
              result
            } else {
              // This is the last element in the iterator, so let's free the buffer. Before we do,
              // though, we need to make a defensive copy of the result so that we don't return an
              // object that might contain dangling pointers to the freed memory
              val resultCopy = result.copy()
              aggregationMap.free()
              resultCopy
            }
          }
        }
      } else {
        if (unsafeEnabled) {
          log.info("Not using Unsafe-based aggregator because it is not supported for this schema")
        }
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

          def hasNext: Boolean = resultIterator.hasNext

          def next(): Row = {
            val currentGroup = resultIterator.next()
            resultProjection(joinedRow(currentGroup.getKey, currentGroup.getValue))
          }
        }
      }
    }
  }
}
