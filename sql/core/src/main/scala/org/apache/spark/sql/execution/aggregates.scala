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

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.types._

/**
 * Attempt to rewrite aggregate to be more efficient.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
case class HashAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(@transient sqlContext: SQLContext)
  extends UnaryNode with NoBind {

  private def sc = sqlContext.sparkContext

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

  override def otherCopyArgs = sqlContext :: Nil

  def output = aggregateExpressions.map(_.toAttribute)

  def execute() = {
    val aggregatesToCompute = aggregateExpressions.flatMap { a =>
      a.collect { case agg: AggregateExpression => agg }
    }

    // Move these into expressions... have fall back that uses standard aggregate interface.
    val computeFunctions = aggregatesToCompute.map {
      case c @ Count(expr) =>
        val currentCount = AttributeReference("currentCount", LongType, true)()
        val initialValue = Literal(0L)
        val updateFunction = If(IsNotNull(expr), Add(currentCount, Literal(1L)), currentCount)
        val result = currentCount

        AggregateEvaluation(currentCount :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case Sum(expr) =>
        val currentSum = AttributeReference("currentSum", expr.dataType, true)()
        val initialValue = Cast(Literal(0L), expr.dataType)

        // Coalasce avoids double calculation...
        // but really, common sub expression elimination would be better....
        val updateFunction = Coalesce(Add(expr, currentSum) :: currentSum :: Nil)
        val result = currentSum

        AggregateEvaluation(currentSum :: Nil, initialValue :: Nil, updateFunction :: Nil, result)

      case a @ Average(expr) =>
        val currentCount = AttributeReference("currentCount", LongType, true)()
        val currentSum = AttributeReference("currentSum", expr.dataType, true)()
        val initialCount = Literal(0L)
        val initialSum = Cast(Literal(0L), expr.dataType)
        val updateCount = If(IsNotNull(expr), Add(currentCount, Literal(1L)), currentCount)
        val updateSum = Coalesce(Add(expr, currentSum) :: currentSum :: Nil)

        val result = Divide(Cast(currentSum, DoubleType), Cast(currentCount, DoubleType))

        AggregateEvaluation(
          currentCount :: currentSum :: Nil,
          initialCount :: initialSum :: Nil,
          updateCount :: updateSum :: Nil,
          result
        )

        /*
      case otherAggregate =>
        val ref =
          AttributeReference("aggregateFunction", otherAggregate.dataType, otherAggregate.nullable)

        AggregateEvaluation(
          ref :: Nil,
          ScalaUdf(() => otherAggregate.newInstance, NullType, ),
      )
      */
    }

    @transient val computationSchema = computeFunctions.flatMap(_.schema)
    @transient lazy val newComputationBuffer =
      newProjection(computeFunctions.flatMap(_.initialValues), child.output)
    @transient lazy val updateProjectionBuilder =
      newMutableProjection(
        computeFunctions.flatMap(_.update),
        computeFunctions.flatMap(_.schema) ++ child.output)
    @transient lazy val groupProjection = newProjection(groupingExpressions, child.output)

    @transient val resultMap = aggregatesToCompute.zip(computeFunctions).map {
      case (agg, func) => agg.id -> func.result
    }.toMap

    val namedGroups = groupingExpressions.zipWithIndex.map {
      case (ne: NamedExpression, _) => (ne, ne)
      case (e, i) => (e, Alias(e, s"GroupingExpr$i")())
    }
    val groupMap = namedGroups.map {case (k,v) => k -> v.toAttribute }.toMap

    @transient val resultExpressions = aggregateExpressions.map(_.transform {
      case e: Expression if resultMap.contains(e.id) => resultMap(e.id)
      case e: Expression if groupMap.contains(e) => groupMap(e)
    })
    @transient lazy val resultProjectionBuilder =
      newMutableProjection(
        resultExpressions,
        (namedGroups.map(_._2.toAttribute) ++ computationSchema).toSeq)

    child.execute().mapPartitions { iter =>
      // TODO: Skip hashmap for no grouping exprs...
      @transient val buffers = new java.util.HashMap[Row, MutableRow]()
      @transient val updateProjection = updateProjectionBuilder()
      @transient val joinedRow = new JoinedRow

      var currentRow: Row = null
      while(iter.hasNext) {
        currentRow = iter.next()
        val currentGroup = groupProjection(currentRow)
        var currentBuffer = buffers.get(currentGroup)
        if(currentBuffer == null) {
          currentBuffer = newComputationBuffer(EmptyRow).asInstanceOf[MutableRow]
          buffers.put(currentGroup, currentBuffer)
        }
        updateProjection.target(currentBuffer)(joinedRow(currentBuffer, currentRow))
      }

      @transient val resultIterator = buffers.entrySet.iterator()
      @transient val resultProjection = resultProjectionBuilder()
      new Iterator[Row] {
        def hasNext = resultIterator.hasNext
        def next() = {
          val currentGroup = resultIterator.next()
          resultProjection(joinedRow(currentGroup.getKey, currentGroup.getValue))
        }
      }
    }
  }
}

case class AggregateEvaluation(
    schema: Seq[Attribute],
    initialValues: Seq[Expression],
    update: Seq[Expression],
    result: Expression)
