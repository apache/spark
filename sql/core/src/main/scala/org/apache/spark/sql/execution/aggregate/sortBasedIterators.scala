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

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.NullType

import scala.collection.mutable.ArrayBuffer

/**
 * An iterator used to evaluate aggregate functions. It assumes that input rows
 * are already grouped by values of `groupingExpressions`.
 */
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
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
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
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          func.inputBufferOffset = inputBufferOffset
          inputBufferOffset += func.bufferSchema.length
          func
      }
      // Set mutableBufferOffset for this function. It is important that setting
      // mutableBufferOffset happens after all potential bindReference operations
      // because bindReference will create a new instance of the function.
      funcWithBoundReferences.mutableBufferOffset = mutableBufferOffset
      mutableBufferOffset += funcWithBoundReferences.bufferSchema.length
      functions(i) = funcWithBoundReferences
      i += 1
    }
    functions
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

  // All non-algebraic aggregate functions.
  protected val nonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    nonAlgebraicAggregateFunctionPositions.map(aggregateFunctions)

  // This is used to project expressions for the grouping expressions.
  protected val groupGenerator =
    newMutableProjection(groupingExpressions, inputAttributes)()

  // The underlying buffer shared by all aggregate functions.
  protected val buffer: MutableRow = {
    // The number of elements of the underlying buffer of this operator.
    // All aggregate functions are sharing this underlying buffer and they find their
    // buffer values through bufferOffset.
    // var size = 0
    // var i = 0
    // while (i < aggregateFunctions.length) {
    //  size += aggregateFunctions(i).bufferSchema.length
    //  i += 1
    // }
    new GenericMutableRow(aggregateFunctions.map(_.bufferSchema.length).sum)
  }

  protected val joinedRow = new JoinedRow

  // This projection is used to initialize buffer values for all AlgebraicAggregates.
  protected val algebraicInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.initialValues
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
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

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(): Unit = {
    algebraicInitialProjection(EmptyRow)
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  protected def initialize(): Unit = {
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

  ///////////////////////////////////////////////////////////////////////////
  // Private methods
  ///////////////////////////////////////////////////////////////////////////

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

  /** The initial input buffer offset for `inputBufferOffset` of an [[AggregateFunction2]]. */
  protected def initialInputBufferOffset: Int

  /** The function used to process an input row. */
  protected def processRow(row: InternalRow): Unit

  /** The function used to generate the result row. */
  protected def generateOutput(): InternalRow

  ///////////////////////////////////////////////////////////////////////////
  // Initialize this iterator
  ///////////////////////////////////////////////////////////////////////////

  initialize()
}

/**
 * An iterator used to do partial aggregations (for those aggregate functions with mode Partial).
 * It assumes that input rows are already grouped by values of `groupingExpressions`.
 * The format of its output rows is:
 * |groupingExpr1|...|groupingExprN|aggregationBuffer1|...|aggregationBufferN|
 */
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
    val bufferSchema = aggregateFunctions.flatMap(_.bufferAttributes)
    val updateExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.updateExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }
    newMutableProjection(updateExpressions, bufferSchema ++ inputAttributes)().target(buffer)
  }

  override protected def initialInputBufferOffset: Int = 0

  override protected def processRow(row: InternalRow): Unit = {
    // Process all algebraic aggregate functions.
    algebraicUpdateProjection(joinedRow(buffer, row))
    // Process all non-algebraic aggregate functions.
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).update(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    // We just output the grouping expressions and the underlying buffer.
    joinedRow(currentGroupingKey, buffer).copy()
  }
}

/**
 * An iterator used to do partial merge aggregations (for those aggregate functions with mode
 * PartialMerge). It assumes that input rows are already grouped by values of
 * `groupingExpressions`.
 * The format of its input rows is:
 * |groupingExpr1|...|groupingExprN|aggregationBuffer1|...|aggregationBufferN|
 *
 * The format of its internal buffer is:
 * |aggregationBuffer1|...|aggregationBufferN|
 *
 * The format of its output rows is:
 * |groupingExpr1|...|groupingExprN|aggregationBuffer1|...|aggregationBufferN|
 */
class PartialMergeSortAggregationIterator(
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

  // This projection is used to merge buffer values for all AlgebraicAggregates.
  private val algebraicMergeProjection = {
    val mergeInputSchema =
      aggregateFunctions.flatMap(_.bufferAttributes) ++
        groupingExpressions.map(_.toAttribute) ++
        aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }

    newMutableProjection(mergeExpressions, mergeInputSchema)()
  }

  override protected def initialInputBufferOffset: Int = groupingExpressions.length

  override protected def processRow(row: InternalRow): Unit = {
    // Process all algebraic aggregate functions.
    algebraicMergeProjection.target(buffer)(joinedRow(buffer, row))
    // Process all non-algebraic aggregate functions.
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).merge(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    // We output grouping expressions and aggregation buffers.
    joinedRow(currentGroupingKey, buffer).copy()
  }
}

/**
 * An iterator used to do final aggregations (for those aggregate functions with mode
 * Final). It assumes that input rows are already grouped by values of
 * `groupingExpressions`.
 * The format of its input rows is:
 * |groupingExpr1|...|groupingExprN|aggregationBuffer1|...|aggregationBufferN|
 *
 * The format of its internal buffer is:
 * |aggregationBuffer1|...|aggregationBufferN|
 *
 * The format of its output rows is represented by the schema of `resultExpressions`.
 */
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

  // The result of aggregate functions.
  private val aggregateResult: MutableRow = new GenericMutableRow(aggregateAttributes.length)

  // The projection used to generate the output rows of this operator.
  // This is only used when we are generating final results of aggregate functions.
  private val resultProjection =
    newMutableProjection(
      resultExpressions, groupingExpressions.map(_.toAttribute) ++ aggregateAttributes)()

  // This projection is used to merge buffer values for all AlgebraicAggregates.
  private val algebraicMergeProjection = {
    val mergeInputSchema =
      aggregateFunctions.flatMap(_.bufferAttributes) ++
        groupingExpressions.map(_.toAttribute) ++
        aggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions = aggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.mergeExpressions
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }

    newMutableProjection(mergeExpressions, mergeInputSchema)()
  }

  // This projection is used to evaluate all AlgebraicAggregates.
  private val algebraicEvalProjection = {
    val bufferSchemata = aggregateFunctions.flatMap(_.bufferAttributes)
    val evalExpressions = aggregateFunctions.map {
      case ae: AlgebraicAggregate => ae.evaluateExpression
      case agg: AggregateFunction2 => NoOp
    }

    newMutableProjection(evalExpressions, bufferSchemata)()
  }

  override protected def initialInputBufferOffset: Int = groupingExpressions.length

  override def initialize(): Unit = {
    if (inputIter.hasNext) {
      initializeBuffer()
      val currentRow = inputIter.next().copy()
      // partitionGenerator is a mutable projection. Since we need to track nextGroupingKey,
      // we are making a copy at here.
      nextGroupingKey = groupGenerator(currentRow).copy()
      firstRowInNextGroup = currentRow
    } else {
      if (groupingExpressions.isEmpty) {
        // If there is no grouping expression, we need to generate a single row as the output.
        initializeBuffer()
        // Right now, the buffer only contains initial buffer values. Because
        // merging two buffers with initial values will generate a row that
        // still store initial values. We set the currentRow as the copy of the current buffer.
        // Because input aggregation buffer has initialInputBufferOffset extra values at the
        // beginning, we create a dummy row for this part.
        val currentRow =
          joinedRow(new GenericInternalRow(initialInputBufferOffset), buffer).copy()
        nextGroupingKey = groupGenerator(currentRow).copy()
        firstRowInNextGroup = currentRow
      } else {
        // This iter is an empty one.
        hasNewGroup = false
      }
    }
  }

  override protected def processRow(row: InternalRow): Unit = {
    // Process all algebraic aggregate functions.
    algebraicMergeProjection.target(buffer)(joinedRow(buffer, row))
    // Process all non-algebraic aggregate functions.
    var i = 0
    while (i < nonAlgebraicAggregateFunctions.length) {
      nonAlgebraicAggregateFunctions(i).merge(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    // Generate results for all algebraic aggregate functions.
    algebraicEvalProjection.target(aggregateResult)(buffer)
    // Generate results for all non-algebraic aggregate functions.
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

/**
 * An iterator used to do both final aggregations (for those aggregate functions with mode
 * Final) and complete aggregations (for those aggregate functions with mode Complete).
 * It assumes that input rows are already grouped by values of `groupingExpressions`.
 * The format of its input rows is:
 * |groupingExpr1|...|groupingExprN|col1|...|colM|aggregationBuffer1|...|aggregationBufferN|
 * col1 to colM are columns used by aggregate functions with Complete mode.
 * aggregationBuffer1 to aggregationBufferN are buffers used by aggregate functions with
 * Final mode.
 *
 * The format of its internal buffer is:
 * |aggregationBuffer1|...|aggregationBuffer(N+M)|
 * For aggregation buffers, first N aggregation buffers are used by N aggregate functions with
 * mode Final. Then, the last M aggregation buffers are used by M aggregate functions with mode
 * Complete.
 *
 * The format of its output rows is represented by the schema of `resultExpressions`.
 */
class FinalAndCompleteSortAggregationIterator(
    override protected val initialInputBufferOffset: Int,
    groupingExpressions: Seq[NamedExpression],
    finalAggregateExpressions: Seq[AggregateExpression2],
    finalAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow])
  extends SortAggregationIterator(
    groupingExpressions,
    // TODO: document the ordering
    finalAggregateExpressions ++ completeAggregateExpressions,
    newMutableProjection,
    inputAttributes,
    inputIter) {

  // The result of aggregate functions.
  private val aggregateResult: MutableRow =
    new GenericMutableRow(completeAggregateAttributes.length + finalAggregateAttributes.length)

  // The projection used to generate the output rows of this operator.
  // This is only used when we are generating final results of aggregate functions.
  private val resultProjection = {
    val inputSchema =
      groupingExpressions.map(_.toAttribute) ++
        finalAggregateAttributes ++
        completeAggregateAttributes
    newMutableProjection(resultExpressions, inputSchema)()
  }

  // All aggregate functions with mode Final.
  private val finalAggregateFunctions: Array[AggregateFunction2] = {
    val functions = new Array[AggregateFunction2](finalAggregateExpressions.length)
    var i = 0
    while (i < finalAggregateExpressions.length) {
      functions(i) = aggregateFunctions(i)
      i += 1
    }
    functions
  }

  // All non-algebraic aggregate functions with mode Final.
  private val finalNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    finalAggregateFunctions.collect {
      case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
    }

  // All aggregate functions with mode Complete.
  private val completeAggregateFunctions: Array[AggregateFunction2] = {
    val functions = new Array[AggregateFunction2](completeAggregateExpressions.length)
    var i = 0
    while (i < completeAggregateExpressions.length) {
      functions(i) = aggregateFunctions(finalAggregateFunctions.length + i)
      i += 1
    }
    functions
  }

  // All non-algebraic aggregate functions with mode Complete.
  private val completeNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    completeAggregateFunctions.collect {
      case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
    }

  // This projection is used to merge buffer values for all AlgebraicAggregates with mode
  // Final.
  private val finalAlgebraicMergeProjection = {
    // The first initialInputBufferOffset values of the input aggregation buffer is
    // for grouping expressions and distinct columns.
    val groupingAttributesAndDistinctColumns = inputAttributes.take(initialInputBufferOffset)

    val completeOffsetExpressions =
      Seq.fill(completeAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)

    val mergeInputSchema =
      finalAggregateFunctions.flatMap(_.bufferAttributes) ++
        completeAggregateFunctions.flatMap(_.bufferAttributes) ++
        groupingAttributesAndDistinctColumns ++
        finalAggregateFunctions.flatMap(_.cloneBufferAttributes)
    val mergeExpressions =
      finalAggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.mergeExpressions
        case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
      } ++ completeOffsetExpressions
    newMutableProjection(mergeExpressions, mergeInputSchema)()
  }

  // This projection is used to update buffer values for all AlgebraicAggregates with mode
  // Complete.
  private val completeAlgebraicUpdateProjection = {
    // We do not touch buffer values of aggregate functions with the Final mode.
    val finalOffsetExpressions =
      Seq.fill(finalAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)

    val bufferSchema =
      finalAggregateFunctions.flatMap(_.bufferAttributes) ++
        completeAggregateFunctions.flatMap(_.bufferAttributes)
    val updateExpressions =
      finalOffsetExpressions ++ completeAggregateFunctions.flatMap {
        case ae: AlgebraicAggregate => ae.updateExpressions
        case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
      }
    newMutableProjection(updateExpressions, bufferSchema ++ inputAttributes)().target(buffer)
  }

  // This projection is used to evaluate all AlgebraicAggregates.
  private val algebraicEvalProjection = {
    val bufferSchemata = aggregateFunctions.flatMap(_.bufferAttributes)
    val evalExpressions = aggregateFunctions.map {
      case ae: AlgebraicAggregate => ae.evaluateExpression
      case agg: AggregateFunction2 => NoOp
    }

    newMutableProjection(evalExpressions, bufferSchemata)()
  }

  override def initialize(): Unit = {
    if (inputIter.hasNext) {
      initializeBuffer()
      val currentRow = inputIter.next().copy()
      // partitionGenerator is a mutable projection. Since we need to track nextGroupingKey,
      // we are making a copy at here.
      nextGroupingKey = groupGenerator(currentRow).copy()
      firstRowInNextGroup = currentRow
    } else {
      if (groupingExpressions.isEmpty) {
        // If there is no grouping expression, we need to generate a single row as the output.
        initializeBuffer()
        // Right now, the buffer only contains initial buffer values. Because
        // merging two buffers with initial values will generate a row that
        // still store initial values. We set the currentRow as the copy of the current buffer.
        // Because input aggregation buffer has initialInputBufferOffset extra values at the
        // beginning, we create a dummy row for this part.
        val currentRow =
          joinedRow(new GenericInternalRow(initialInputBufferOffset), buffer).copy()
        nextGroupingKey = groupGenerator(currentRow).copy()
        firstRowInNextGroup = currentRow
      } else {
        // This iter is an empty one.
        hasNewGroup = false
      }
    }
  }

  override protected def processRow(row: InternalRow): Unit = {
    val input = joinedRow(buffer, row)
    // For all aggregate functions with mode Complete, update buffers.
    completeAlgebraicUpdateProjection(input)
    var i = 0
    while (i < completeNonAlgebraicAggregateFunctions.length) {
      completeNonAlgebraicAggregateFunctions(i).update(buffer, row)
      i += 1
    }

    // For all aggregate functions with mode Final, merge buffers.
    finalAlgebraicMergeProjection.target(buffer)(input)
    i = 0
    while (i < finalNonAlgebraicAggregateFunctions.length) {
      finalNonAlgebraicAggregateFunctions(i).merge(buffer, row)
      i += 1
    }
  }

  override protected def generateOutput(): InternalRow = {
    // Generate results for all algebraic aggregate functions.
    algebraicEvalProjection.target(aggregateResult)(buffer)
    // Generate results for all non-algebraic aggregate functions.
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
