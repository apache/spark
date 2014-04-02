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

import java.util.HashMap

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._

/**
 * Groups input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param partial if true then aggregation is done partially on local data without shuffling to
 *                ensure all values where `groupingExpressions` are equal are present.
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
case class Aggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(@transient sc: SparkContext)
  extends UnaryNode with NoBind {

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

  override def otherCopyArgs = sc :: Nil

  def output = aggregateExpressions.map(_.toAttribute)

  case class ComputedAggregate(
      unbound: AggregateExpression,        // Unbound aggregate used for result substitution
      aggregate: AggregateExpression,      // A bound copy of this aggregate used to create a buffer
      resultAttribute: AttributeReference) // An attribute used to refer to the result of this agg

  // A list of aggregates that need to be computed for each group.
  @transient
  lazy val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output).asInstanceOf[AggregateExpression],
          AttributeReference(s"aggResult:$a", a.dataType, nullable = true)())
    }
  }.toArray

  // The schema of the result of all aggregate evaluations
  @transient
  lazy val computedSchema = computedAggregates.map(_.resultAttribute)

  // Creates a new aggregate buffer for a group.
  def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.size)
    var i = 0
    while(i < computedAggregates.size) {
      buffer(i) = computedAggregates(i).aggregate.newInstance
      i += 1
    }
    buffer
  }

  // Named attributes used to substitute grouping attributes into the final result.
  @transient
  lazy val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  // A map of substitutions that are used to insert the aggregate expressions and grouping
  // expression into the final result expression.
  @transient
  lazy val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute} ++ namedGroups).toMap

  // Substituted version of aggregateExpressions expressions which are used to compute final
  // output rows given a group and the result of all aggregate computations.
  @transient
  lazy val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.size) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new Projection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.size)

        var i = 0
        while (i < buffer.size) {
          aggregateResults(i) = buffer(i).apply(EmptyRow)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, Array[AggregateFunction]]
        val groupingProjection = new MutableProjection(groupingExpressions, child.output)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.size) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[Row] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.size)
          private[this] val resultProjection =
            new MutableProjection(resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          final def hasNext: Boolean = hashTableIter.hasNext

          final def next(): Row = {
            val currentEntry = hashTableIter.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = 0
            while (i < currentBuffer.size) {
              aggregateResults(i) = currentBuffer(i).apply(EmptyRow)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}
