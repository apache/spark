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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate2._
import org.apache.spark.sql.types.NullType

import scala.collection.mutable.ArrayBuffer

private[sql] abstract class SortAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends Iterator[InternalRow] {

  ///////////////////////////////////////////////////////////////////////////
  // Static fields for this iterator
  ///////////////////////////////////////////////////////////////////////////

  protected val aggregateFunctions: Array[AggregateFunction2] = {
    var bufferOffset = initialBufferOffset
    val functions = new Array[AggregateFunction2](aggregateExpressions.length)
    var i = 0
    while (i < aggregateExpressions.length) {
      val func = aggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences = aggregateExpressions(i).mode match {
        case Partial | Complete if !func.isInstanceOf[AlgebraicAggregate] =>
          // We need to create BoundReferences if the function is not an
          // AlgebraicAggregate (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributes)
        case _ => func
      }
      // Set bufferOffset for this function. It is important that setting bufferOffset
      // happens after all potential bindReference operations because bindReference
      // will create a new instance of the function.
      funcWithBoundReferences.bufferOffset = bufferOffset
      bufferOffset += funcWithBoundReferences.bufferSchema.length
      functions(i) = funcWithBoundReferences
      i += 1
    }
    functions
  }

  // All non-algebraic aggregate functions.
  protected val nonAlgebraicAggregateFunctions: Array[AggregateFunction2] = {
    aggregateFunctions.collect {
      case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
    }.toArray
  }

  // Positions of those non-algebraic aggregate functions in aggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are non-algebraic aggregate functions.
  // nonAlgebraicAggregateFunctionPositions will be [1, 2].
  protected val nonAlgebraicAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: AlgebraicAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // This is used to project expressions for the grouping expressions.
  protected val groupGenerator =
    newMutableProjection(groupingExpressions, inputAttributes)()

  // The underlying buffer shared by all aggregate functions.
  protected val buffer: MutableRow = {
    // The number of elements of the underlying buffer of this operator.
    // All aggregate functions are sharing this underlying buffer and they find their
    // buffer values through bufferOffset.
    var size = initialBufferOffset
    var i = 0
    while (i < aggregateFunctions.length) {
      size += aggregateFunctions(i).bufferSchema.length
      i += 1
    }
    new GenericMutableRow(size)
  }

  protected val joinedRow = new JoinedRow4

  protected val offsetExpressions = Seq.fill(initialBufferOffset)(NoOp)

  // This projection is used to initialize buffer values for all AlgebraicAggregates.
  protected val algebraicInitialProjection = {
    val initExpressions = offsetExpressions ++ aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.initialValues
      case agg: AggregateFunction2 => NoOp :: Nil
    }
    newMutableProjection(initExpressions, Nil)().target(buffer)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Mutable states
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  protected var currentGroupingKey: InternalRow = _
  // The partition key of next partition.
  protected var nextGroupingKey: InternalRow = _
  // The first row of next partition.
  protected var firstRowInNextGroup: InternalRow = _
  // Indicates if we has new group of rows to process.
  protected var hasNewGroup: Boolean = true

  ///////////////////////////////////////////////////////////////////////////
  // Private methods
  ///////////////////////////////////////////////////////////////////////////

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(): Unit = {
    algebraicInitialProjection(EmptyRow)
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  private def initialize(): Unit = {
    if (inputIter.hasNext) {
      initializeBuffer()
      val currentRow = inputIter.next().copy()
      // partitionGenerator is a mutable projection. Since we need to track nextGroupingKey,
      // we are making a copy at here.
      nextGroupingKey = groupGenerator(currentRow).copy()
      firstRowInNextGroup = currentRow
    } else {
      // This iter is an empty one.
      hasNewGroup = false
    }
  }

  /** Processes rows in the current group. It will stop when it find a new group. */
  private def processCurrentGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(firstRowInNextGroup)
    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    while (inputIter.hasNext && !findNextPartition) {
      val currentRow = inputIter.next()
      // Get the grouping key based on the grouping expressions.
      // For the below compare method, we do not need to make a copy of groupingKey.
      val groupingKey = groupGenerator(currentRow)
      // Check if the current row belongs the current input row.
      currentGroupingKey.equals(groupingKey)

      if (currentGroupingKey == groupingKey) {
        processRow(currentRow)
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      hasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = hasNewGroup

  override final def next(): InternalRow = {
    if (hasNext) {
      // Process the current group.
      processCurrentGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput()
      // Initilize buffer values for the next group.
      initializeBuffer()

      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Methods that need to be implemented
  ///////////////////////////////////////////////////////////////////////////

  protected def initialBufferOffset: Int

  protected def processRow(row: InternalRow): Unit

  protected def generateOutput(): InternalRow

  ///////////////////////////////////////////////////////////////////////////
  // Initialize this iterator
  ///////////////////////////////////////////////////////////////////////////

  initialize()
}


class PartialSortAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends SortAggregationIterator(
    groupingExpressions,
    aggregateExpressions,
    newMutableProjection,
    inputAttributes,
    inputIter) {

  // This projection is used to update buffer values for all AlgebraicAggregates.
  private val algebraicUpdateProjection = {
    val bufferSchema = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.bufferAttributes
      case agg: AggregateFunction2 => agg.bufferAttributes
    }
    val updateExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.updateExpressions
      case agg: AggregateFunction2 => NoOp :: Nil
    }
    newMutableProjection(updateExpressions, bufferSchema ++ inputAttributes)().target(buffer)
  }

  override protected def initialBufferOffset: Int = 0

  /** Processes the current input row. */
  override protected def processRow(row: InternalRow): Unit = {
    // The new row is still in the current group.
    algebraicUpdateProjection(joinedRow(buffer, row))
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).update(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    // We just output the grouping columns and the buffer.
    joinedRow(currentGroupingKey, buffer).copy()
  }
}

class FinalSortAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression2],
    aggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends SortAggregationIterator(
    groupingExpressions,
    aggregateExpressions,
    newMutableProjection,
    inputAttributes,
    inputIter) {

  // The result of aggregate functions. It is only used when aggregate functions' modes
  // are Final.
  private val aggregateResult: MutableRow = new GenericMutableRow(aggregateAttributes.length)

  // The projection used to generate the output rows of this operator.
  // This is only used when we are generating final results of aggregate functions.
  private val resultProjection =
    newMutableProjection(
      resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateAttributes)()

  // When we merge buffers (for mode PartialMerge or Final), the input rows start with
  // values for grouping expressions. So, when we construct our buffer for this
  // aggregate function, the size of the buffer matches the number of values in the
  // input rows. To simplify the code for code-gen, we need create some dummy
  // attributes and expressions for these grouping expressions.
  private val offsetAttributes =
    Seq.fill(initialBufferOffset)(AttributeReference("offset", NullType)())

  // This projection is used to merge buffer values for all AlgebraicAggregates.
  private val algebraicMergeProjection = {
    val bufferSchemata =
      offsetAttributes ++ aggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.bufferAttributes
        case agg: AggregateFunction2 => agg.bufferAttributes
      } ++ offsetAttributes ++ aggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.cloneBufferAttributes
        case agg: AggregateFunction2 => agg.cloneBufferAttributes
      }
    val mergeExpressions = offsetExpressions ++ aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => NoOp :: Nil
    }

    newMutableProjection(mergeExpressions, bufferSchemata)()
  }

  // This projection is used to evaluate all AlgebraicAggregates.
  private val algebraicEvalProjection = {
    val bufferSchemata =
      offsetAttributes ++ aggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.bufferAttributes
        case agg: AggregateFunction2 => agg.bufferAttributes
      } ++ offsetAttributes ++ aggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.cloneBufferAttributes
        case agg: AggregateFunction2 => agg.cloneBufferAttributes
      }
    val evalExpressions = aggregateFunctions.map {
      case ae: AlgebraicAggregate => ae.evaluateExpression
      case agg: AggregateFunction2 => NoOp
    }

    newMutableProjection(evalExpressions, bufferSchemata)()
  }

  override protected def initialBufferOffset: Int = groupingExpressions.length

  override protected def processRow(row: InternalRow): Unit = {
    // The new row is still in the current group.
    algebraicMergeProjection.target(buffer)(joinedRow(buffer, row))
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).merge(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    algebraicEvalProjection.target(aggregateResult)(buffer)
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      aggregateResult.update(
        nonAlgebraicAggregateFunctionPositions(i),
        nonAlgebraicAggregateFunctions(i).eval(buffer))
      i += 1
    }
    resultProjection(joinedRow(currentGroupingKey, aggregateResult))
  }
}
