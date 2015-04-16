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

package org.apache.spark.sql.execution.aggregate2

import java.util.{HashSet=>JHashSet, Set=>JSet}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

import org.apache.spark.util.collection.OpenHashMap

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical._

sealed class BufferSeens(var buffer: MutableRow, var seens: Array[JSet[Any]] = null) {
  def this() {
    this(new GenericMutableRow(0), null)
  }

  def withBuffer(row: MutableRow): BufferSeens = {
    this.buffer = row
    this
  }

  def withSeens(seens: Array[JSet[Any]]): BufferSeens = {
    this.seens = seens
    this
  }
}

sealed trait Aggregate {
  self: Product =>
  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output

  def initializedAndGetAggregates(mode: Mode, aggregates: Seq[AggregateExpression2]): Array[AggregateExpression2] = {
    var pos = 0

    aggregates.map { ae =>
      ae.initial(mode)

      // we connect all of the aggregation buffers in a single Row,
      // and "BIND" the attribute references in a Hack way.
      val bufferDataTypes = ae.bufferDataType
      ae.initialize(for (i <- 0 until bufferDataTypes.length) yield {
        BoundReference(pos + i, bufferDataTypes(i), true)
      })
      pos += bufferDataTypes.length

      ae
    }.toArray
  }

  // This is provided by SparkPlan
  def child: SparkPlan

  def bufferSchema(aggregates: Seq[AggregateExpression2]): Seq[Attribute] =
    aggregates.zipWithIndex.flatMap { case (ca, idx) =>
      ca.bufferDataType.zipWithIndex.map { case (dt, i) =>
        AttributeReference(s"aggr.${idx}_$i", dt)().toAttribute }
    }
}

sealed trait PostShuffle extends Aggregate {
  self: Product =>

  def computedAggregates(aggregateExpressions: Seq[NamedExpression]): Seq[AggregateExpression2] = {
    aggregateExpressions.flatMap { expr =>
      expr.collect {
        case ae: AggregateExpression2 => ae
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `projection` for each
 * group.
 *
 * @param groupingExpressions the attributes represent the output of the groupby expressions
 * @param unboundAggregateExpressions Unbound Aggregate Function List.
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[NamedExpression],
    unboundAggregateExpressions: Seq[AggregateExpression2],
    child: SparkPlan)
  extends UnaryNode with Aggregate {

  val aggregateExpressions: Seq[AggregateExpression2] = unboundAggregateExpressions.map {
    BindReferences.bindReference(_, childOutput)
  }

  override def requiredChildDistribution = UnspecifiedDistribution :: Nil
  override def output = bufferSchema(aggregateExpressions) ++ groupingExpressions.map(_.toAttribute)

  /**
   * Create Iterator for the in-memory hash map.
   */
  private[this] def createIterator(
                                    functions: Array[AggregateExpression2],
                                    iterator: Iterator[BufferSeens]) = {
    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keybuffer = iterator.next()
        var idx = 0
        while (idx < functions.length) {
          functions(idx).terminatePartial(keybuffer.buffer)
          idx += 1
        }

        keybuffer.buffer
      }
    }
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = initializedAndGetAggregates(PARTIAL1, aggregateExpressions)

      val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)
      val results = new OpenHashMap[Row, BufferSeens]()
      while (iter.hasNext) {
        val currentRow = iter.next()

        val keys = groupByProjection(currentRow)
        results(keys) match {
          case null =>
            val buffer = new GenericMutableRow(output.length)
            var idx = 0
            while (idx < aggregates.length) {
              val ae = aggregates(idx)
              ae.reset(buffer)
              ae.update(currentRow, buffer, null)
              idx += 1
            }
            var idx2 = 0
            while (idx2 < keys.length) {
              buffer(idx) = keys(idx2)
              idx2 += 1
              idx += 1
            }

            results(keys.copy()) = new BufferSeens(buffer, null)
          case inputbuffer =>
            var idx = 0
            while (idx < aggregates.length) {
              val ae = aggregates(idx)
              ae.update(currentRow, inputbuffer.buffer, null)
              idx += 1
            }
        }
      }

      createIterator(aggregates, results.iterator.map(_._2))
    }
  }
}

case class AggregatePostShuffle(
    groupingExpressions: Seq[Attribute],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output = aggregateExpressions.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = initializedAndGetAggregates(FINAL, computedAggregates(aggregateExpressions))

      val finalProjection = new InterpretedMutableProjection(aggregateExpressions, childOutput)

      val results = new OpenHashMap[Row, BufferSeens]()
      val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

      while (iter.hasNext) {
        val currentRow = iter.next()
        val keys = groupByProjection(currentRow)
        results(keys) match {
          case null =>
            // TODO currentRow seems most likely a MutableRow
            val buffer = currentRow.makeMutable()
            results(keys.copy()) = new BufferSeens(buffer, null)
          case pair =>
            var idx = 0
            while (idx < aggregates.length) {
              val ae = aggregates(idx)
              ae.merge(currentRow, pair.buffer)
              idx += 1
            }
        }
      }

      results.iterator.map(it => finalProjection(it._2.buffer))
    }
  }
}

// TODO Currently even if only a single DISTINCT exists in the aggregate expressions, we will
// not do partial aggregation (aggregating before shuffling), all of the data have to be shuffled
// to the reduce side and do aggregation directly, this probably causes the performance regression
// for Aggregation Function like CountDistinct etc.
case class DistinctAggregate(
    groupingExpressions: Seq[NamedExpression],
    unboundAggregateExpressions: Seq[NamedExpression],
    rewrittenAggregateExpressions: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output = rewrittenAggregateExpressions.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  val aggregateExpressions: Seq[NamedExpression] = unboundAggregateExpressions.map {
    BindReferences.bindReference(_, childOutput)
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = initializedAndGetAggregates(COMPLETE, computedAggregates(aggregateExpressions))

      val outputSchema: Seq[Attribute] = bufferSchema(aggregates) ++ groupingExpressions.map(_.toAttribute)

      initializedAndGetAggregates(COMPLETE, computedAggregates(rewrittenAggregateExpressions))
      val finalProjection = new InterpretedMutableProjection(rewrittenAggregateExpressions, outputSchema)

      val results = new OpenHashMap[Row, BufferSeens]()
      val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

      while (iter.hasNext) {
        val currentRow = iter.next()

        val keys = groupByProjection(currentRow)
        results(keys) match {
          case null =>
            val buffer = new GenericMutableRow(aggregates.length + keys.length)
            val seens = new Array[JSet[Any]](aggregates.length)

            var idx = 0
            while (idx < aggregates.length) {
              val ae = aggregates(idx)
              ae.reset(buffer)

              if (ae.distinct) {
                val seen = new JHashSet[Any]()
                seens(idx) = seen
              }

              ae.update(currentRow, buffer, seens(idx))
              idx += 1
            }
            var idx2 = 0
            while (idx2 < keys.length) {
              buffer(idx) = keys(idx2)
              idx2 += 1
              idx += 1
            }
            results(keys.copy()) = new BufferSeens(buffer, seens)

          case bufferSeens =>
            var idx = 0
            while (idx < aggregates.length) {
              val ae = aggregates(idx)
              ae.update(currentRow, bufferSeens.buffer, bufferSeens.seens(idx))

              idx += 1
            }
        }
      }

      results.iterator.map(it => finalProjection(it._2.buffer))
    }
  }
}
