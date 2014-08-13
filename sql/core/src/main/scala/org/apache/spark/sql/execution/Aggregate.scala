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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{SparkEnv, SparkContext}
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.collection.ExternalAppendOnlyMap

trait Aggregate{

  self: SparkPlan =>

  val partial: Boolean
  val groupingExpressions: Seq[Expression]
  val aggregateExpressions: Seq[NamedExpression]
  val child: SparkPlan

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

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output

  override def output = aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(
      unbound: AggregateExpression,
      aggregate: AggregateExpression,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, childOutput),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  def newAggregateBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computedAggregates.length)
    var i = 0
    while (i < computedAggregates.length) {
      buffer(i) = computedAggregates(i).aggregate.newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  private[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  private[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  private[this] val resultExpressions = aggregateExpressions.map { agg =>
    agg.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  def aggregateNoGrouping() = {
    child.execute().mapPartitions { iter =>
      val buffer = newAggregateBuffer()
      var currentRow: Row = null
      while (iter.hasNext) {
        currentRow = iter.next()
        var i = 0
        while (i < buffer.length) {
          buffer(i).update(currentRow)
          i += 1
        }
      }
      val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
      val aggregateResults = new GenericMutableRow(computedAggregates.length)

      var i = 0
      while (i < buffer.length) {
        aggregateResults(i) = buffer(i).eval(EmptyRow)
        i += 1
      }

      Iterator(resultProjection(aggregateResults))
    }
  }

  def resultRow(iter: Iterator[(Row,Array[AggregateFunction])]) = {
    new Iterator[Row] {
      private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
      private[this] val resultProjection =
        new InterpretedMutableProjection(
          resultExpressions, computedSchema ++ namedGroups.map(_._2))
      private[this] val joinedRow = new JoinedRow

      override final def hasNext: Boolean = iter.hasNext
      override final def next(): Row = {
        val currentEntry = iter.next()
        val currentGroup = currentEntry._1
        val currentBuffer = currentEntry._2

        var i = 0
        while (i < currentBuffer.length) {
          // Evaluating an aggregate buffer returns the result.  No row is required since we
          // already added all rows in the group using update.
          aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
          i += 1
        }
        resultProjection(joinedRow(aggregateResults, currentGroup))
      }
    }
  }

}

case class OnHeapAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with Aggregate{

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      aggregateNoGrouping()
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new HashMap[Row, Array[AggregateFunction]]
        val groupingProjection =
          new InterpretedMutableProjection(groupingExpressions, childOutput)

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
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }
        val iterPair = new Iterator[(Row, Array[AggregateFunction])] {
          private[this] val hashTableIter = hashTable.entrySet().iterator()
          override final def hasNext: Boolean = hashTableIter.hasNext

          override final def next(): (Row, Array[AggregateFunction]) = {
            val currentEntry = hashTableIter.next()
            (currentEntry.getKey, currentEntry.getValue)
          }
        }
        resultRow(iterPair)
      }
    }
  }

}

case class ExternalAggregate(
    partial: Boolean,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with Aggregate{

  override def execute() = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      aggregateNoGrouping()
    } else {
      child.execute().mapPartitions { iter =>
        val groupingProjection =
          new InterpretedMutableProjection(groupingExpressions, childOutput)

        val createCombiner = (v: Row) =>{
          val c = newAggregateBuffer()
          var i = 0
          while (i < c.length) {
            c(i).update(v)
            i += 1
          }
          c
        }
        val mergeValue = (c: Array[AggregateFunction], v: Row) => {
          var i = 0
          while (i < c.length) {
            c(i).update(v)
            i += 1
          }
          c
        }
        val mergeCombiners = (c1: Array[AggregateFunction], c2: Array[AggregateFunction]) => {
          var i = 0
          while (i < c1.length) {
            c1(i).merge(c2(i))
            i += 1
          }
          c1
        }
        val combiners = new ExternalAppendOnlyMap[Row, Row, Array[AggregateFunction]](
          createCombiner, mergeValue, mergeCombiners)
        while (iter.hasNext) {
          val row = iter.next()
          combiners.insert(groupingProjection(row).copy(), row)
        }
        resultRow(combiners.iterator)
      }
    }
  }

}