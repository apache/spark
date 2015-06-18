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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}

/**
 * :: DeveloperApi ::
 * SortMerge group input data by `groupingExpressions` and computes the `aggregateExpressions` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param aggregateExpressions expressions that are computed for each group.
 * @param child the input data source.
 */
@DeveloperApi
case class SortMergeAggregate(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingExpressions) :: Nil

  // this is to manually construct an ordering that can be used to compare keys
  private val keyOrdering: RowOrdering = RowOrdering.forSchema(groupingExpressions.map(_.dataType))

  override def outputOrdering: Seq[SortOrder] = requiredOrders(groupingExpressions)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(groupingExpressions) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] =
    groupingExpressions.map(SortOrder(_, Ascending))

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
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction] = {
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

  protected override def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      new Iterator[InternalRow] {

        private[this] var currentElement: InternalRow = _
        private[this] var nextElement: InternalRow = _
        private[this] var currentKey: InternalRow = _
        private[this] var nextKey: InternalRow = _
        private[this] val groupingProjection =
          new InterpretedMutableProjection(groupingExpressions, child.output)
        private[this] var currentBuffer: Array[AggregateFunction] = _
        private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
        private[this] val resultProjection =
          new InterpretedMutableProjection(
            resultExpressions, computedSchema ++ namedGroups.map(_._2))
        private[this] val joinedRow = new JoinedRow4

        initialize()

        private def initialize() = {
          if (iter.hasNext) {
            currentElement = iter.next()
            currentKey = groupingProjection(currentElement).copy()
          } else {
            currentElement = null
          }
        }

        override final def hasNext: Boolean = {
          if (currentElement != null) {
            currentBuffer = newAggregateBuffer()
            var i = 0
            while (i < currentBuffer.length) {
              currentBuffer(i).update(currentElement)
              i += 1
            }
            var stop: Boolean = false
            while (!stop) {
              if (iter.hasNext) {
                nextElement = iter.next()
                nextKey = groupingProjection(nextElement).copy()
                stop = keyOrdering.compare(currentKey, nextKey) != 0
                if (!stop) {
                  var i = 0
                  while (i < currentBuffer.length) {
                    currentBuffer(i).update(nextElement)
                    i += 1
                  }
                }
              } else {
                nextElement = null
                stop = true
              }
            }
            true
          } else {
            false
          }
        }

        override final def next(): InternalRow = {
          val currentGroup = currentKey
          currentElement = nextElement
          currentKey = nextKey
          var i = 0
          while (i < currentBuffer.length) {
            aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
            i += 1
          }
          resultProjection(joinedRow(aggregateResults, currentGroup))
        }
      }
    }

  }

}
