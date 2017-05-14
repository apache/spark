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

package org.apache.spark.sql.execution.local

import java.util.HashMap

import org.apache.spark.sql.SQLConf
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._

case class AggregateNode(
    conf: SQLConf,
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LocalNode) extends UnaryLocalNode(conf) {

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this aggregate, used for result substitution.
   * @param aggregate A bound copy of this aggregate used to create a new aggregation buffer.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  private case class ComputedAggregate(
      unbound: AggregateExpression1,
      aggregate: AggregateExpression1,
      resultAttribute: AttributeReference)

  /** A list of aggregates that need to be computed for each group. */
  private[this] val computedAggregates = aggregateExpressions.flatMap { agg =>
    agg.collect {
      case a: AggregateExpression1 =>
        ComputedAggregate(
          a,
          BindReferences.bindReference(a, child.output),
          AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
    }
  }.toArray

  /** The schema of the result of all aggregate evaluations */
  private[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  private[this] def newAggregateBuffer(): Array[AggregateFunction1] = {
    val buffer = new Array[AggregateFunction1](computedAggregates.length)
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

  private[this] var iterator: Iterator[InternalRow] = _
  private[this] var currentRow: InternalRow = _

  override def output: Seq[Attribute] = aggregateExpressions.map(_.toAttribute)

  override def open(): Unit = {
    child.open()
    if (groupingExpressions.isEmpty) {
      val buffer = newAggregateBuffer()
      var currentRow: InternalRow = null
      while (child.next()) {
        currentRow = child.fetch()
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

      iterator = Iterator(resultProjection(aggregateResults))
    } else {
      val hashTable = new HashMap[InternalRow, Array[AggregateFunction1]]
      val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)

      var currentRow: InternalRow = null
      while (child.next()) {
        currentRow = child.fetch()
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

      iterator = new Iterator[InternalRow] {
        private[this] val hashTableIter = hashTable.entrySet().iterator()
        private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
        private[this] val resultProjection =
          new InterpretedMutableProjection(
            resultExpressions, computedSchema ++ namedGroups.map(_._2))
        private[this] val joinedRow = new JoinedRow

        override final def hasNext: Boolean = hashTableIter.hasNext

        override final def next(): InternalRow = {
          val currentEntry = hashTableIter.next()
          val currentGroup = currentEntry.getKey
          val currentBuffer = currentEntry.getValue

          var i = 0
          while (i < currentBuffer.length) {
            // Evaluating an aggregate buffer returns the result. No row is required since we
            // already added all rows in the group using update.
            aggregateResults(i) = currentBuffer(i).eval(EmptyRow)
            i += 1
          }
          resultProjection(joinedRow(aggregateResults, currentGroup))
        }
      }
    }
  }

  override def next(): Boolean = {
    if (iterator.hasNext) {
      currentRow = iterator.next()
      true
    } else {
      false
    }
  }

  override def fetch(): InternalRow = currentRow

  override def close(): Unit = child.close()
}
