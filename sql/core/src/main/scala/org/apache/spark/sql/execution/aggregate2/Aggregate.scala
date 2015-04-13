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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}

import org.apache.spark.util.collection.{OpenHashSet, OpenHashMap}

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.plans.physical._

/**
 * An aggregate that needs to be computed for each row in a group.
 *
 * @param aggregate AggregateExpression2, associated with the function
 * @param substitution A MutableLiteral used to refer to the result of this aggregate in the final
 *                        output.
 */
sealed case class AggregateFunctionBind(
    aggregate: AggregateExpression2,
    substitution: MutableLiteral)

sealed class InputBufferSeens(
    var input: Row, //
    var buffer: MutableRow,
    var seens: Array[OpenHashSet[Any]] = null) {
  def this() {
    this(new GenericMutableRow(0), null)
  }

  def withInput(row: Row): InputBufferSeens = {
    this.input = row
    this
  }

  def withBuffer(row: MutableRow): InputBufferSeens = {
    this.buffer = row
    this
  }

  def withSeens(seens: Array[OpenHashSet[Any]]): InputBufferSeens = {
    this.seens = seens
    this
  }
}

sealed trait Aggregate {
  self: Product =>
  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  val childOutput = child.output
  val isGlobalAggregation = groupingExpressions.isEmpty

  def computedAggregates: Array[AggregateExpression2] = {
    boundProjection.flatMap { expr =>
      expr.collect {
        case ae: AggregateExpression2 => ae
      }
    }
  }.toArray

  // This is a hack, instead of relying on the BindReferences for the aggregation
  // buffer schema in PostShuffle, we have a strong protocols which represented as the
  // BoundReferences in PostShuffle for aggregation buffer.
  @transient lazy val bufferSchema: Array[AttributeReference] =
    computedAggregates.zipWithIndex.flatMap { case (ca, idx) =>
      ca.bufferDataType.zipWithIndex.map { case (dt, i) =>
        AttributeReference(s"aggr.${idx}_$i", dt)() }
    }.toArray

  // The tuples of aggregate expressions with information
  // (AggregateExpression2, Aggregate Function, Placeholder of AggregateExpression2 result)
  @transient lazy val aggregateFunctionBinds: Array[AggregateFunctionBind] = {
    var pos = 0
    computedAggregates.map { ae =>
      ae.initial(mode)

      // we connect all of the aggregation buffers in a single Row,
      // and "BIND" the attribute references in a Hack way.
      val bufferDataTypes = ae.bufferDataType
      ae.initialize(for (i <- 0 until bufferDataTypes.length) yield {
        BoundReference(pos + i, bufferDataTypes(i), true)
      })
      pos += bufferDataTypes.length

      AggregateFunctionBind(ae, MutableLiteral(null, ae.dataType))
    }
  }

  @transient lazy val groupByProjection = if (groupingExpressions.isEmpty) {
    InterpretedMutableProjection(Nil)
  } else {
    new InterpretedMutableProjection(groupingExpressions, childOutput)
  }

  // Indicate which stage we are running into
  def mode: Mode
  // This is provided by SparkPlan
  def child: SparkPlan
  // Group By Key Expressions
  def groupingExpressions: Seq[Expression]
  // Bounded Projection
  def boundProjection: Seq[NamedExpression]
}

sealed trait PreShuffle extends Aggregate {
  self: Product =>

  def boundProjection: Seq[NamedExpression] = projection.map {
    case a: Attribute => // Attribute will be converted into BoundReference
      Alias(
        BindReferences.bindReference(a: Expression, childOutput), a.name)(a.exprId, a.qualifiers)
    case a: NamedExpression => BindReferences.bindReference(a, childOutput)
  }

  // The expression list for output, this is the unbound expressions
  def projection: Seq[NamedExpression]
}

sealed trait PostShuffle extends Aggregate {
  self: Product =>
  /**
   * Substituted version of boundProjection expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  @transient lazy val finalExpressions = {
    val resultMap = aggregateFunctionBinds.map { ae => ae.aggregate -> ae.substitution }.toMap
    boundProjection.map { agg =>
      agg.transform {
        case e: AggregateExpression2 if resultMap.contains(e) => resultMap(e)
      }
    }
  }.map(e => {BindReferences.bindReference(e: Expression, childOutput)})

  @transient lazy val finalProjection = new InterpretedMutableProjection(finalExpressions)

  def aggregateFunctionBinds: Array[AggregateFunctionBind]

  def createIterator(
      aggregates: Array[AggregateExpression2],
      iterator: Iterator[InputBufferSeens]) = {
    val substitutions = aggregateFunctionBinds.map(_.substitution)

    new Iterator[Row] {
      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keybuffer = iterator.next()

        var idx = 0
        while (idx < aggregates.length) {
          // substitute the AggregateExpression2 value
          substitutions(idx).value = aggregates(idx).terminate(keybuffer.buffer)
          idx += 1
        }

        finalProjection(keybuffer.input)
      }
    }
  }
}

/**
 * :: DeveloperApi ::
 * Groups input data by `groupingExpressions` and computes the `projection` for each
 * group.
 *
 * @param groupingExpressions expressions that are evaluated to determine grouping.
 * @param projection expressions that are computed for each group.
 * @param namedGroupingAttributes the attributes represent the output of the groupby expressions
 * @param child the input data source.
 */
@DeveloperApi
case class AggregatePreShuffle(
    groupingExpressions: Seq[Expression],
    projection: Seq[NamedExpression],
    namedGroupingAttributes: Seq[Attribute],
    child: SparkPlan)
  extends UnaryNode with PreShuffle {

  override def requiredChildDistribution = UnspecifiedDistribution :: Nil

  override def output = bufferSchema.map(_.toAttribute) ++ namedGroupingAttributes

  override def mode: Mode = PARTIAL1 // iterate & terminalPartial will be called

  /**
   * Create Iterator for the in-memory hash map.
   */
  private[this] def createIterator(
      functions: Array[AggregateExpression2],
      iterator: Iterator[InputBufferSeens]) = {
    new Iterator[Row] {
      private[this] val joinedRow = new JoinedRow

      override final def hasNext: Boolean = iterator.hasNext

      override final def next(): Row = {
        val keybuffer = iterator.next()
        var idx = 0
        while (idx < functions.length) {
          functions(idx).terminatePartial(keybuffer.buffer)
          idx += 1
        }

        joinedRow(keybuffer.buffer, keybuffer.input).copy()
      }
    }
  }

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = aggregateFunctionBinds.map(_.aggregate)

      if (groupingExpressions.isEmpty) {
        // without group by keys
        val buffer = new GenericMutableRow(bufferSchema.length)
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
            ae.update(ae.eval(currentRow), buffer)
            idx += 1
          }
        }

        createIterator(aggregates, Iterator(new InputBufferSeens().withBuffer(buffer)))
      } else {
        val results = new OpenHashMap[Row, InputBufferSeens]()
        while (iter.hasNext) {
          val currentRow = iter.next()

          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              val buffer = new GenericMutableRow(bufferSchema.length)
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                val value = ae.eval(currentRow)
                // TODO distinctLike? We need to store the "seen" for
                // AggregationExpression that distinctLike=true
                // This is a trade off between memory & computing
                ae.reset(buffer)
                ae.update(value, buffer)
                idx += 1
              }

              val copies = keys.copy()
              results(copies) = new InputBufferSeens(copies, buffer)
            case inputbuffer =>
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                ae.update(ae.eval(currentRow), inputbuffer.buffer)
                idx += 1
              }

          }
        }

        createIterator(aggregates, results.iterator.map(_._2))
      }
    }
  }
}

case class AggregatePostShuffle(
    groupingExpressions: Seq[Expression],
    boundProjection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PostShuffle {

  override def output = boundProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def mode: Mode = FINAL // merge & terminate will be called

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = aggregateFunctionBinds.map(_.aggregate)
      if (groupingExpressions.isEmpty) {
        val buffer = new GenericMutableRow(bufferSchema.length)
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

        createIterator(aggregates, Iterator(new InputBufferSeens().withBuffer(buffer)))
      } else {
        val results = new OpenHashMap[Row, InputBufferSeens]()
        while (iter.hasNext) {
          val currentRow = iter.next()
          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              val buffer = new GenericMutableRow(bufferSchema.length)
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                ae.reset(buffer)
                ae.merge(currentRow, buffer)
                idx += 1
              }
              results(keys.copy()) = new InputBufferSeens(currentRow.copy(), buffer)
            case pair =>
              var idx = 0
              while (idx < aggregates.length) {
                val ae = aggregates(idx)
                ae.merge(currentRow, pair.buffer)
                idx += 1
              }
          }
        }

        createIterator(aggregates, results.iterator.map(_._2))
      }
    }
  }
}

// TODO Currently even if only a single DISTINCT exists in the aggregate expressions, we will
// not do partial aggregation (aggregating before shuffling), all of the data have to be shuffled
// to the reduce side and do aggregation directly, this probably causes the performance regression
// for Aggregation Function like CountDistinct etc.
case class DistinctAggregate(
    groupingExpressions: Seq[Expression],
    projection: Seq[NamedExpression],
    child: SparkPlan) extends UnaryNode with PreShuffle with PostShuffle {
  override def output = boundProjection.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = if (groupingExpressions == Nil) {
    AllTuples :: Nil
  } else {
    ClusteredDistribution(groupingExpressions) :: Nil
  }

  override def mode: Mode = COMPLETE // iterate() & terminate() will be called

  override def execute() = attachTree(this, "execute") {
    child.execute().mapPartitions { iter =>
      val aggregates = aggregateFunctionBinds.map(_.aggregate)
      if (groupingExpressions.isEmpty) {
        val buffer = new GenericMutableRow(bufferSchema.length)
        // TODO save the memory only for those DISTINCT aggregate expressions
        val seens = new Array[OpenHashSet[Any]](aggregateFunctionBinds.length)

        var idx = 0
        while (idx < aggregateFunctionBinds.length) {
          val ae = aggregates(idx)
          ae.reset(buffer)

          if (ae.distinct) {
            seens(idx) = new OpenHashSet[Any]()
          }

          idx += 1
        }
        val ibs = new InputBufferSeens().withBuffer(buffer).withSeens(seens)

        while (iter.hasNext) {
          val currentRow = iter.next()

          var idx = 0
          while (idx < aggregateFunctionBinds.length) {
            val ae = aggregates(idx)
            val value = ae.eval(currentRow)

            if (ae.distinct) {
              if (value != null && !seens(idx).contains(value)) {
                ae.update(value, buffer)
                seens(idx).add(value)
              }
            } else {
              ae.update(value, buffer)
            }
            idx += 1
          }
        }

        createIterator(aggregates, Iterator(ibs))
      } else {
        val results = new OpenHashMap[Row, InputBufferSeens]()

        while (iter.hasNext) {
          val currentRow = iter.next()

          val keys = groupByProjection(currentRow)
          results(keys) match {
            case null =>
              val buffer = new GenericMutableRow(bufferSchema.length)
              // TODO save the memory only for those DISTINCT aggregate expressions
              val seens = new Array[OpenHashSet[Any]](aggregateFunctionBinds.length)

              var idx = 0
              while (idx < aggregateFunctionBinds.length) {
                val ae = aggregates(idx)
                val value = ae.eval(currentRow)
                ae.reset(buffer)
                ae.update(value, buffer)

                if (ae.distinct) {
                  val seen = new OpenHashSet[Any]()
                  if (value != null) {
                    seen.add(value)
                  }
                  seens.update(idx, seen)
                }

                idx += 1
              }
              results(keys.copy()) = new InputBufferSeens(currentRow.copy(), buffer, seens)

            case inputBufferSeens =>
              var idx = 0
              while (idx < aggregateFunctionBinds.length) {
                val ae = aggregates(idx)
                val value = ae.eval(currentRow)

                if (ae.distinct) {
                  if (value != null && !inputBufferSeens.seens(idx).contains(value)) {
                    ae.update(value, inputBufferSeens.buffer)
                    inputBufferSeens.seens(idx).add(value)
                  }
                } else {
                  ae.update(value, inputBufferSeens.buffer)
                }
                idx += 1
              }
          }
        }

        createIterator(aggregates, results.iterator.map(_._2))
      }
    }
  }
}
