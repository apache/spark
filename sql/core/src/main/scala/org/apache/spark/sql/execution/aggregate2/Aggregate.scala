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

import java.util.{HashSet => JHashSet, Set => JSet}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

import org.apache.spark.util.collection.OpenHashMap

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical._

// A class of the Aggregate buffer & Seen Set pair
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

// A MutableRow for AggregateBuffers and GroupingExpression Values
sealed class BufferAndKey(leftLen: Int, rightLen: Int)
  extends GenericMutableRow(leftLen + rightLen) {

  def this(leftLen: Int, keys: Row) = {
    this(leftLen, keys.length)
    // copy the keys to the last
    var idx = leftLen
    var idx2 = 0
    while (idx2 < keys.length) {
      this.values(idx) = keys(idx2)
      idx2 += 1
      idx += 1
    }
  }
}

sealed trait Aggregate {
  self: Product =>
  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output

  // initialize the aggregate functions, this will be called in the beginning of every partition
  // data processing
  def initializedAndGetAggregates(
      mode: Mode,
      aggregates: Seq[AggregateExpression2])
  : Array[AggregateExpression2] = {
    var pos = 0

    aggregates.map { ae =>
      ae.initial(mode)

      // We connect all of the aggregation buffers in a single Row,
      // and "BIND" the attribute references in a Hack way, as we believe
      // the Pre/Post Shuffle Aggregate are actually tightly coupled
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

  // The schema of the aggregate buffers, as we lines those buffers
  // in a single row.
  def bufferSchemaFromAggregate(aggregates: Seq[AggregateExpression2]): Seq[Attribute] =
    aggregates.zipWithIndex.flatMap { case (ca, idx) =>
      ca.bufferDataType.zipWithIndex.map { case (dt, i) =>
        // the attribute names is useless here, as we bind the attribute
        // in a hack way, see [[initializedAndGetAggregates]]
        AttributeReference(s"aggr.${idx}_$i", dt)().toAttribute }
    }
}

sealed trait PostShuffle extends Aggregate {
  self: Product =>

  // extract the aggregate function from the projection
  def computedAggregates(projectionList: Seq[Expression]): Seq[AggregateExpression2] = {
    projectionList.flatMap { expr =>
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
 * @param groupingExpressions the attributes represent the output of the grouping expressions
 * @param originalProjection Unbound Aggregate Function List.
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[NamedExpression],
    originalProjection: Seq[AggregateExpression2],
    child: SparkPlan)
  extends UnaryNode with Aggregate {

  private val aggregateExpressions: Seq[AggregateExpression2] = originalProjection.map {
    BindReferences.bindReference(_, childOutput)
  }

  private val buffersSchema = bufferSchemaFromAggregate(aggregateExpressions)
  override def requiredChildDistribution: Seq[Distribution] = UnspecifiedDistribution :: Nil
  override val output: Seq[Attribute] = buffersSchema ++ groupingExpressions.map(_.toAttribute)

  /**
   * Create Iterator for the in-memory hash map.
   */
  private[this] def createIterator(
      functions: Array[AggregateExpression2],
      iterator: Iterator[BufferSeens]) = {
    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keyBuffer = iterator.next()
        var idx = 0
        while (idx < functions.length) {
          // terminatedPartial is for Hive UDAF, we
          // provide an opportunity to transform its internal aggregate buffer into
          // the catalyst data.
          functions(idx).terminatePartial(keyBuffer.buffer)
          idx += 1
        }

        keyBuffer.buffer
      }
    }
  }

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    if (groupingExpressions.length == 0) {
      child.execute().mapPartitions { iter =>
        // the input is every single row
        val aggregates = initializedAndGetAggregates(PARTIAL1, aggregateExpressions)
        // without group by keys
        val buffer = new GenericMutableRow(buffersSchema.length)
        var idx = 0
        while (idx < aggregates.length) {
          val ae = aggregates(idx)
          ae.reset(buffer)
          idx += 1
        }

        while (iter.hasNext) {
          val currentRow = iter.next()
          var idx = 0
          while (idx < aggregates.length) {
            val ae = aggregates(idx)
            ae.update(currentRow, buffer, null)
            idx += 1
          }
        }

        createIterator(aggregates, Iterator(new BufferSeens().withBuffer(buffer)))
      }
    } else {
      child.execute().mapPartitions { iter =>
        // the input is every single row
        val aggregates = initializedAndGetAggregates(PARTIAL1, aggregateExpressions)

        val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        val results = new OpenHashMap[Row, BufferSeens]()
        while (iter.hasNext) {
          val currentRow = iter.next()

          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              val buffer = new BufferAndKey(buffersSchema.length, keys)
              // update the aggregate buffers
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                ae.reset(buffer)
                ae.update(currentRow, buffer, null)
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

        // The output is the (Aggregate Buffers + Grouping Expression Values)
        createIterator(aggregates, results.iterator.map(_._2))
      }
    }
  }
}

case class AggregatePostShuffle(
    groupingExpressions: Seq[Attribute],
    rewrittenProjection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output: Seq[Attribute] = rewrittenProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    if (groupingExpressions.length == 0) {
      child.execute().mapPartitions { iter =>
        // The input Row in the format of (AggregateBuffers)
        val aggregates =
          initializedAndGetAggregates(FINAL, computedAggregates(rewrittenProjection))
        val finalProjection = new InterpretedMutableProjection(rewrittenProjection, childOutput)

        val buffer = new GenericMutableRow(childOutput.length)
        var idx = 0
        while (idx < aggregates.length) {
          val ae = aggregates(idx)
          ae.reset(buffer)
          idx += 1
        }

        while (iter.hasNext) {
          val currentRow = iter.next()

          var idx = 0
          while (idx < aggregates.length) {
            val ae = aggregates(idx)
            ae.merge(currentRow, buffer)
            idx += 1
          }
        }

        Iterator(finalProjection(buffer))
      }
    } else {
      child.execute().mapPartitions { iter =>
        // The input Row in the format of (AggregateBuffers + GroupingExpression Values)
        val aggregates =
          initializedAndGetAggregates(FINAL, computedAggregates(rewrittenProjection))
        val finalProjection = new InterpretedMutableProjection(rewrittenProjection, childOutput)

        val results = new OpenHashMap[Row, BufferSeens]()
        val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        while (iter.hasNext) {
          val currentRow = iter.next()
          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              // TODO actually what we need to copy is the grouping expression values
              // as the aggregate buffer will be reset.
              val buffer = currentRow.makeMutable()
              // The reason why need to reset it first and merge with the input row is,
              // in Hive UDAF, we need to provide an opportunity that the buffer can be the
              // custom type, Otherwise, HIVE UDAF will wrap/unwrap in every merge() method
              // calls, which is every expensive
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                ae.reset(buffer)
                ae.merge(currentRow, buffer)
                idx += 1
              }
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

        // final Project is simple a rewrite version of output expression list
        // which will project as the final output
        results.iterator.map { it => finalProjection(it._2.buffer)}
      }
    }
  }
}

// TODO Currently even if only a single DISTINCT exists in the aggregate expressions, we will
// not do partial aggregation (aggregating before shuffling), all of the data have to be shuffled
// to the reduce side and do aggregation directly, this probably causes the performance regression
// for Aggregation Function like CountDistinct etc.
case class DistinctAggregate(
    groupingExpressions: Seq[NamedExpression],
    originalProjection: Seq[NamedExpression],
    rewrittenProjection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output: Seq[Attribute] = rewrittenProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  // binding the expression, which takes the child's output as input
  private val aggregateExpressions: Seq[Expression] = originalProjection.map {
    BindReferences.bindReference(_: Expression, childOutput)
  }

  override def doExecute(): RDD[Row] = attachTree(this, "execute") {
    if (groupingExpressions.length == 0) {
      child.execute().mapPartitions { iter =>
        // initialize the aggregate functions for input rows
        // (update/terminatePartial will be called)
        val aggregates =
          initializedAndGetAggregates(COMPLETE, computedAggregates(aggregateExpressions))

        val buffersSchema = bufferSchemaFromAggregate(aggregates)

        // initialize the aggregate functions for the final output (merge/terminate will be called)
        initializedAndGetAggregates(COMPLETE, computedAggregates(rewrittenProjection))
        // binding the output projection, which takes the aggregate buffer and grouping keys
        // as the input row.
        val finalProjection = new InterpretedMutableProjection(rewrittenProjection, buffersSchema)

        val buffer = new GenericMutableRow(buffersSchema.length)
        val seens = new Array[JSet[Any]](aggregates.length)

        // reset the aggregate buffer
        var idx = 0
        while (idx < aggregates.length) {
          val ae = aggregates(idx)
          ae.reset(buffer)

          if (ae.distinct) {
            seens(idx) = new JHashSet[Any]()
          }

          idx += 1
        }
        val ibs = new BufferSeens().withBuffer(buffer).withSeens(seens)

        // update the aggregate buffer
        while (iter.hasNext) {
          val currentRow = iter.next()

          var idx = 0
          while (idx < aggregates.length) {
            val ae = aggregates(idx)
            ae.update(currentRow, buffer, seens(idx))

            idx += 1
          }
        }

        // only single output for non grouping keys case
        Iterator(finalProjection(ibs.buffer))
      }
    } else {
      child.execute().mapPartitions { iter =>
        // initialize the aggregate functions for input rows
        // (update will be called)
        val aggregates =
          initializedAndGetAggregates(COMPLETE, computedAggregates(aggregateExpressions))

        val buffersSchema = bufferSchemaFromAggregate(aggregates)
        val outputSchema = buffersSchema ++ groupingExpressions.map(_.toAttribute)

        // initialize the aggregate functions for the final output
        // (merge/terminate will be called)
        initializedAndGetAggregates(COMPLETE, computedAggregates(rewrittenProjection))
        // binding the output projection, which takes the aggregate buffer and grouping keys
        // as the input row.
        val finalProjection = new InterpretedMutableProjection(rewrittenProjection, outputSchema)

        val results = new OpenHashMap[Row, BufferSeens]()
        val groupByProjection = new InterpretedMutableProjection(groupingExpressions, childOutput)

        while (iter.hasNext) {
          val currentRow = iter.next()

          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              val buffer = new BufferAndKey(buffersSchema.length, keys)
              val seens = new Array[JSet[Any]](aggregates.length)

              // update the aggregate buffers
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
}
