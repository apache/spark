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

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.unsafe.KVIterator

import scala.collection.mutable.ArrayBuffer

/**
 * The base class of [[SortBasedAggregationIterator]] and [[UnsafeHybridAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 *    is used to generate result.
 */
abstract class AggregationIterator(
    groupingKeyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    outputsUnsafeRows: Boolean)
  extends Iterator[InternalRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  // An Seq of all AggregateExpressions.
  // It is important that all AggregateExpressions with the mode Partial, PartialMerge or Final
  // are at the beginning of the allAggregateExpressions.
  protected val allAggregateExpressions =
    nonCompleteAggregateExpressions ++ completeAggregateExpressions

  require(
    allAggregateExpressions.map(_.mode).distinct.length <= 2,
    s"$allAggregateExpressions are not supported becuase they have more than 2 distinct modes.")

  /**
   * The distinct modes of AggregateExpressions. Right now, we can handle the following mode:
   *  - Partial-only: all AggregateExpressions have the mode of Partial;
   *  - PartialMerge-only: all AggregateExpressions have the mode of PartialMerge);
   *  - Final-only: all AggregateExpressions have the mode of Final;
   *  - Final-Complete: some AggregateExpressions have the mode of Final and
   *    others have the mode of Complete;
   *  - Complete-only: nonCompleteAggregateExpressions is empty and we have AggregateExpressions
   *    with mode Complete in completeAggregateExpressions; and
   *  - Grouping-only: there is no AggregateExpression.
   */
  protected val aggregationMode: (Option[AggregateMode], Option[AggregateMode]) =
    nonCompleteAggregateExpressions.map(_.mode).distinct.headOption ->
      completeAggregateExpressions.map(_.mode).distinct.headOption

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected val allAggregateFunctions: Array[AggregateFunction2] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
    val functions = new Array[AggregateFunction2](allAggregateExpressions.length)
    var i = 0
    while (i < allAggregateExpressions.length) {
      val func = allAggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences = allAggregateExpressions(i).mode match {
        case Partial | Complete if !func.isInstanceOf[AlgebraicAggregate] =>
          // We need to create BoundReferences if the function is not an
          // AlgebraicAggregate (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, valueAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          func.withNewInputBufferOffset(inputBufferOffset)
          inputBufferOffset += func.bufferSchema.length
          func
      }
      // Set mutableBufferOffset for this function. It is important that setting
      // mutableBufferOffset happens after all potential bindReference operations
      // because bindReference will create a new instance of the function.
      funcWithBoundReferences.withNewMutableBufferOffset(mutableBufferOffset)
      mutableBufferOffset += funcWithBoundReferences.bufferSchema.length
      functions(i) = funcWithBoundReferences
      i += 1
    }
    functions
  }

  // Positions of those non-algebraic aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are non-algebraic aggregate functions.
  // nonAlgebraicAggregateFunctionPositions will be [1, 2].
  private[this] val allNonAlgebraicAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < allAggregateFunctions.length) {
      allAggregateFunctions(i) match {
        case agg: AlgebraicAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All AggregateFunctions functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteAggregateFunctions: Array[AggregateFunction2] =
    allAggregateFunctions.take(nonCompleteAggregateExpressions.length)

  // All non-algebraic aggregate functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
    nonCompleteAggregateFunctions.collect {
      case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
    }

  // The projection used to initialize buffer values for all AlgebraicAggregates.
  private[this] val algebraicInitialProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: AlgebraicAggregate => ae.initialValues
      case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // All non-Algebraic AggregateFunctions.
  private[this] val allNonAlgebraicAggregateFunctions =
    allNonAlgebraicAggregateFunctionPositions.map(allAggregateFunctions)

  ///////////////////////////////////////////////////////////////////////////
  // Methods and fields used by sub-classes.
  ///////////////////////////////////////////////////////////////////////////

  // Initializing functions used to process a row.
  protected val processRow: (MutableRow, InternalRow) => Unit = {
    val rowToBeProcessed = new JoinedRow
    val aggregationBufferSchema = allAggregateFunctions.flatMap(_.bufferAttributes)
    aggregationMode match {
      // Partial-only
      case (Some(Partial), None) =>
        val updateExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: AlgebraicAggregate => ae.updateExpressions
          case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
        }
        val algebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          algebraicUpdateProjection.target(currentBuffer)
          // Process all algebraic aggregate functions.
          algebraicUpdateProjection(rowToBeProcessed(currentBuffer, row))
          // Process all non-algebraic aggregate functions.
          var i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }
        }

      // PartialMerge-only or Final-only
      case (Some(PartialMerge), None) | (Some(Final), None) =>
        val inputAggregationBufferSchema = if (initialInputBufferOffset == 0) {
          // If initialInputBufferOffset, the input value does not contain
          // grouping keys.
          // This part is pretty hacky.
          allAggregateFunctions.flatMap(_.cloneBufferAttributes).toSeq
        } else {
          groupingKeyAttributes ++ allAggregateFunctions.flatMap(_.cloneBufferAttributes)
        }
        // val inputAggregationBufferSchema =
        //  groupingKeyAttributes ++
        //    allAggregateFunctions.flatMap(_.cloneBufferAttributes)
        val mergeExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: AlgebraicAggregate => ae.mergeExpressions
          case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
        }
        // This projection is used to merge buffer values for all AlgebraicAggregates.
        val algebraicMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferSchema ++ inputAggregationBufferSchema)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          // Process all algebraic aggregate functions.
          algebraicMergeProjection.target(currentBuffer)(rowToBeProcessed(currentBuffer, row))
          // Process all non-algebraic aggregate functions.
          var i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Final-Complete
      case (Some(Final), Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction2] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All non-algebraic aggregate functions with mode Complete.
        val completeNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
          completeAggregateFunctions.collect {
            case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
          }

        // The first initialInputBufferOffset values of the input aggregation buffer is
        // for grouping expressions and distinct columns.
        val groupingAttributesAndDistinctColumns = valueAttributes.take(initialInputBufferOffset)

        val completeOffsetExpressions =
          Seq.fill(completeAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)
        // We do not touch buffer values of aggregate functions with the Final mode.
        val finalOffsetExpressions =
          Seq.fill(nonCompleteAggregateFunctions.map(_.bufferAttributes.length).sum)(NoOp)

        val mergeInputSchema =
          aggregationBufferSchema ++
            groupingAttributesAndDistinctColumns ++
            nonCompleteAggregateFunctions.flatMap(_.cloneBufferAttributes)
        val mergeExpressions =
          nonCompleteAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.mergeExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          } ++ completeOffsetExpressions
        val finalAlgebraicMergeProjection =
          newMutableProjection(mergeExpressions, mergeInputSchema)()

        val updateExpressions =
          finalOffsetExpressions ++ completeAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          }
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeNonAlgebraicAggregateFunctions.length) {
            completeNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }

          // For all aggregate functions with mode Final, merge buffers.
          finalAlgebraicMergeProjection.target(currentBuffer)(input)
          i = 0
          while (i < nonCompleteNonAlgebraicAggregateFunctions.length) {
            nonCompleteNonAlgebraicAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Complete-only
      case (None, Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction2] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All non-algebraic aggregate functions with mode Complete.
        val completeNonAlgebraicAggregateFunctions: Array[AggregateFunction2] =
          completeAggregateFunctions.collect {
            case func: AggregateFunction2 if !func.isInstanceOf[AlgebraicAggregate] => func
          }

        val updateExpressions =
          completeAggregateFunctions.flatMap {
            case ae: AlgebraicAggregate => ae.updateExpressions
            case agg: AggregateFunction2 => Seq.fill(agg.bufferAttributes.length)(NoOp)
          }
        val completeAlgebraicUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeAlgebraicUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeNonAlgebraicAggregateFunctions.length) {
            completeNonAlgebraicAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }
        }

      // Grouping only.
      case (None, None) => (currentBuffer: MutableRow, row: InternalRow) => {}

      case other =>
        sys.error(
          s"Could not evaluate ${nonCompleteAggregateExpressions} because we do not " +
            s"support evaluate modes $other in this iterator.")
    }
  }

  // Initializing the function used to generate the output row.
  protected val generateOutput: (InternalRow, MutableRow) => InternalRow = {
    val rowToBeEvaluated = new JoinedRow
    val safeOutoutRow = new GenericMutableRow(resultExpressions.length)
    val mutableOutput = if (outputsUnsafeRows) {
      UnsafeProjection.create(resultExpressions.map(_.dataType).toArray).apply(safeOutoutRow)
    } else {
      safeOutoutRow
    }

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        // Because we cannot copy a joinedRow containing a UnsafeRow (UnsafeRow does not
        // support generic getter), we create a mutable projection to output the
        // JoinedRow(currentGroupingKey, currentBuffer)
        val bufferSchema = nonCompleteAggregateFunctions.flatMap(_.bufferAttributes)
        val resultProjection =
          newMutableProjection(
            groupingKeyAttributes ++ bufferSchema,
            groupingKeyAttributes ++ bufferSchema)()
        resultProjection.target(mutableOutput)

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          resultProjection(rowToBeEvaluated(currentGroupingKey, currentBuffer))
          // rowToBeEvaluated(currentGroupingKey, currentBuffer)
        }

      // Final-only, Complete-only and Final-Complete: every output row contains values representing
      // resultExpressions.
      case (Some(Final), None) | (Some(Final) | None, Some(Complete)) =>
        val bufferSchemata =
          allAggregateFunctions.flatMap(_.bufferAttributes)
        val evalExpressions = allAggregateFunctions.map {
          case ae: AlgebraicAggregate => ae.evaluateExpression
          case agg: AggregateFunction2 => NoOp
        }
        val algebraicEvalProjection = newMutableProjection(evalExpressions, bufferSchemata)()
        val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes
        // TODO: Use unsafe row.
        val aggregateResult = new GenericMutableRow(aggregateResultSchema.length)
        val resultProjection =
          newMutableProjection(
            resultExpressions, groupingKeyAttributes ++ aggregateResultSchema)()
        resultProjection.target(mutableOutput)

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          // Generate results for all algebraic aggregate functions.
          algebraicEvalProjection.target(aggregateResult)(currentBuffer)
          // Generate results for all non-algebraic aggregate functions.
          var i = 0
          while (i < allNonAlgebraicAggregateFunctions.length) {
            aggregateResult.update(
              allNonAlgebraicAggregateFunctionPositions(i),
              allNonAlgebraicAggregateFunctions(i).eval(currentBuffer))
            i += 1
          }
          resultProjection(rowToBeEvaluated(currentGroupingKey, aggregateResult))
        }

      // Grouping-only: we only output values of grouping expressions.
      case (None, None) =>
        val resultProjection =
          newMutableProjection(resultExpressions, groupingKeyAttributes)()
        resultProjection.target(mutableOutput)

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          resultProjection(currentGroupingKey)
        }

      case other =>
        sys.error(
          s"Could not evaluate ${nonCompleteAggregateExpressions} because we do not " +
            s"support evaluate modes $other in this iterator.")
    }
  }

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: MutableRow): Unit = {
    algebraicInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allNonAlgebraicAggregateFunctions.length) {
      allNonAlgebraicAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   */
  protected def newBuffer: MutableRow
}

object AggregationIterator {
  def kvIterator(
    groupingExpressions: Seq[NamedExpression],
    newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
    inputAttributes: Seq[Attribute],
    inputIter: Iterator[InternalRow]): KVIterator[InternalRow, InternalRow] = {
    new KVIterator[InternalRow, InternalRow] {
      private[this] val groupingKeyGenerator = newProjection(groupingExpressions, inputAttributes)

      private[this] var groupingKey: InternalRow = _

      private[this] var value: InternalRow = _

      override def next(): Boolean = {
        if (inputIter.hasNext) {
          // Read the next input row.
          val inputRow = inputIter.next()
          // Get groupingKey based on groupingExpressions.
          groupingKey = groupingKeyGenerator(inputRow)
          // The value is the inputRow.
          value = inputRow
          true
        } else {
          false
        }
      }

      override def getKey(): InternalRow = {
        groupingKey
      }

      override def getValue(): InternalRow = {
        value
      }

      override def close(): Unit = {
        // Do nothing
      }
    }
  }

  def unsafeKVIterator(
      groupingExpressions: Seq[NamedExpression],
      inputAttributes: Seq[Attribute],
      inputIter: Iterator[InternalRow]): KVIterator[UnsafeRow, InternalRow] = {
    new KVIterator[UnsafeRow, InternalRow] {
      private[this] val groupingKeyGenerator =
        UnsafeProjection.create(groupingExpressions, inputAttributes)

      private[this] var groupingKey: UnsafeRow = _

      private[this] var value: InternalRow = _

      override def next(): Boolean = {
        if (inputIter.hasNext) {
          // Read the next input row.
          val inputRow = inputIter.next()
          // Get groupingKey based on groupingExpressions.
          groupingKey = groupingKeyGenerator.apply(inputRow)
          // The value is the inputRow.
          value = inputRow
          true
        } else {
          false
        }
      }

      override def getKey(): UnsafeRow = {
        groupingKey
      }

      override def getValue(): InternalRow = {
        value
      }

      override def close(): Unit = {
        // Do nothing
      }
    }
  }
}
