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

import scala.collection.mutable.ArrayBuffer

/**
 * The base class of [[SortBasedAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 *    is used to generate result.
 */
abstract class AggregationIterator(
    groupingKeyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute],
    nonCompleteAggregateExpressions: Seq[AggregateExpression],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression],
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
  protected val allAggregateFunctions: Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
    val functions = new Array[AggregateFunction](allAggregateExpressions.length)
    var i = 0
    while (i < allAggregateExpressions.length) {
      val func = allAggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = allAggregateExpressions(i).mode match {
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, valueAttributes)
        case _ =>
          // We only need to set inputBufferOffset for aggregate functions with mode
          // PartialMerge and Final.
          val updatedFunc = func match {
            case function: ImperativeAggregate =>
              function.withNewInputAggBufferOffset(inputBufferOffset)
            case function => function
          }
          inputBufferOffset += func.aggBufferSchema.length
          updatedFunc
      }
      val funcWithUpdatedAggBufferOffset = funcWithBoundReferences match {
        case function: ImperativeAggregate =>
          // Set mutableBufferOffset for this function. It is important that setting
          // mutableBufferOffset happens after all potential bindReference operations
          // because bindReference will create a new instance of the function.
          function.withNewMutableAggBufferOffset(mutableBufferOffset)
        case function => function
      }
      mutableBufferOffset += funcWithUpdatedAggBufferOffset.aggBufferSchema.length
      functions(i) = funcWithUpdatedAggBufferOffset
      i += 1
    }
    functions
  }

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  private[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
    val positions = new ArrayBuffer[Int]()
    var i = 0
    while (i < allAggregateFunctions.length) {
      allAggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // All AggregateFunctions functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteAggregateFunctions: Array[AggregateFunction] =
    allAggregateFunctions.take(nonCompleteAggregateExpressions.length)

  // All imperative aggregate functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteImperativeAggregateFunctions: Array[ImperativeAggregate] =
    nonCompleteAggregateFunctions.collect { case func: ImperativeAggregate => func }

  // The projection used to initialize buffer values for all expression-based aggregates.
  private[this] val expressionAggInitialProjection = {
    val initExpressions = allAggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // All imperative AggregateFunctions.
  private[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(allAggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  ///////////////////////////////////////////////////////////////////////////
  // Methods and fields used by sub-classes.
  ///////////////////////////////////////////////////////////////////////////

  // Initializing functions used to process a row.
  protected val processRow: (MutableRow, InternalRow) => Unit = {
    val rowToBeProcessed = new JoinedRow
    val aggregationBufferSchema = allAggregateFunctions.flatMap(_.aggBufferAttributes)
    aggregationMode match {
      // Partial-only
      case (Some(Partial), None) =>
        val updateExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: DeclarativeAggregate => ae.updateExpressions
          case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }
        val expressionAggUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          expressionAggUpdateProjection.target(currentBuffer)
          // Process all expression-based aggregate functions.
          expressionAggUpdateProjection(rowToBeProcessed(currentBuffer, row))
          // Process all imperative aggregate functions.
          var i = 0
          while (i < nonCompleteImperativeAggregateFunctions.length) {
            nonCompleteImperativeAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }
        }

      // PartialMerge-only or Final-only
      case (Some(PartialMerge), None) | (Some(Final), None) =>
        val inputAggregationBufferSchema = if (initialInputBufferOffset == 0) {
          // If initialInputBufferOffset, the input value does not contain
          // grouping keys.
          // This part is pretty hacky.
          allAggregateFunctions.flatMap(_.inputAggBufferAttributes).toSeq
        } else {
          groupingKeyAttributes ++ allAggregateFunctions.flatMap(_.inputAggBufferAttributes)
        }
        // val inputAggregationBufferSchema =
        //  groupingKeyAttributes ++
        //    allAggregateFunctions.flatMap(_.cloneBufferAttributes)
        val mergeExpressions = nonCompleteAggregateFunctions.flatMap {
          case ae: DeclarativeAggregate => ae.mergeExpressions
          case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
        }
        // This projection is used to merge buffer values for all expression-based aggregates.
        val expressionAggMergeProjection =
          newMutableProjection(
            mergeExpressions,
            aggregationBufferSchema ++ inputAggregationBufferSchema)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          // Process all expression-based aggregate functions.
          expressionAggMergeProjection.target(currentBuffer)(rowToBeProcessed(currentBuffer, row))
          // Process all imperative aggregate functions.
          var i = 0
          while (i < nonCompleteImperativeAggregateFunctions.length) {
            nonCompleteImperativeAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Final-Complete
      case (Some(Final), Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All imperative aggregate functions with mode Complete.
        val completeImperativeAggregateFunctions: Array[ImperativeAggregate] =
          completeAggregateFunctions.collect { case func: ImperativeAggregate => func }

        // The first initialInputBufferOffset values of the input aggregation buffer is
        // for grouping expressions and distinct columns.
        val groupingAttributesAndDistinctColumns = valueAttributes.take(initialInputBufferOffset)

        val completeOffsetExpressions =
          Seq.fill(completeAggregateFunctions.map(_.aggBufferAttributes.length).sum)(NoOp)
        // We do not touch buffer values of aggregate functions with the Final mode.
        val finalOffsetExpressions =
          Seq.fill(nonCompleteAggregateFunctions.map(_.aggBufferAttributes.length).sum)(NoOp)

        val mergeInputSchema =
          aggregationBufferSchema ++
            groupingAttributesAndDistinctColumns ++
            nonCompleteAggregateFunctions.flatMap(_.inputAggBufferAttributes)
        val mergeExpressions =
          nonCompleteAggregateFunctions.flatMap {
            case ae: DeclarativeAggregate => ae.mergeExpressions
            case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
          } ++ completeOffsetExpressions
        val finalExpressionAggMergeProjection =
          newMutableProjection(mergeExpressions, mergeInputSchema)()

        val updateExpressions =
          finalOffsetExpressions ++ completeAggregateFunctions.flatMap {
            case ae: DeclarativeAggregate => ae.updateExpressions
            case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
          }
        val completeExpressionAggUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeExpressionAggUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeImperativeAggregateFunctions.length) {
            completeImperativeAggregateFunctions(i).update(currentBuffer, row)
            i += 1
          }

          // For all aggregate functions with mode Final, merge buffers.
          finalExpressionAggMergeProjection.target(currentBuffer)(input)
          i = 0
          while (i < nonCompleteImperativeAggregateFunctions.length) {
            nonCompleteImperativeAggregateFunctions(i).merge(currentBuffer, row)
            i += 1
          }
        }

      // Complete-only
      case (None, Some(Complete)) =>
        val completeAggregateFunctions: Array[AggregateFunction] =
          allAggregateFunctions.takeRight(completeAggregateExpressions.length)
        // All imperative aggregate functions with mode Complete.
        val completeImperativeAggregateFunctions: Array[ImperativeAggregate] =
          completeAggregateFunctions.collect { case func: ImperativeAggregate => func }

        val updateExpressions =
          completeAggregateFunctions.flatMap {
            case ae: DeclarativeAggregate => ae.updateExpressions
            case agg: AggregateFunction => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
          }
        val completeExpressionAggUpdateProjection =
          newMutableProjection(updateExpressions, aggregationBufferSchema ++ valueAttributes)()

        (currentBuffer: MutableRow, row: InternalRow) => {
          val input = rowToBeProcessed(currentBuffer, row)
          // For all aggregate functions with mode Complete, update buffers.
          completeExpressionAggUpdateProjection.target(currentBuffer)(input)
          var i = 0
          while (i < completeImperativeAggregateFunctions.length) {
            completeImperativeAggregateFunctions(i).update(currentBuffer, row)
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
    val safeOutputRow = new SpecificMutableRow(resultExpressions.map(_.dataType))
    val mutableOutput = if (outputsUnsafeRows) {
      UnsafeProjection.create(resultExpressions.map(_.dataType).toArray).apply(safeOutputRow)
    } else {
      safeOutputRow
    }

    aggregationMode match {
      // Partial-only or PartialMerge-only: every output row is basically the values of
      // the grouping expressions and the corresponding aggregation buffer.
      case (Some(Partial), None) | (Some(PartialMerge), None) =>
        // Because we cannot copy a joinedRow containing a UnsafeRow (UnsafeRow does not
        // support generic getter), we create a mutable projection to output the
        // JoinedRow(currentGroupingKey, currentBuffer)
        val bufferSchema = nonCompleteAggregateFunctions.flatMap(_.aggBufferAttributes)
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
          allAggregateFunctions.flatMap(_.aggBufferAttributes)
        val evalExpressions = allAggregateFunctions.map {
          case ae: DeclarativeAggregate => ae.evaluateExpression
          case agg: AggregateFunction => NoOp
        }
        val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferSchemata)()
        val aggregateResultSchema = nonCompleteAggregateAttributes ++ completeAggregateAttributes
        // TODO: Use unsafe row.
        val aggregateResult = new SpecificMutableRow(aggregateResultSchema.map(_.dataType))
        expressionAggEvalProjection.target(aggregateResult)
        val resultProjection =
          newMutableProjection(
            resultExpressions, groupingKeyAttributes ++ aggregateResultSchema)()
        resultProjection.target(mutableOutput)

        (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
          // Generate results for all expression-based aggregate functions.
          expressionAggEvalProjection(currentBuffer)
          // Generate results for all imperative aggregate functions.
          var i = 0
          while (i < allImperativeAggregateFunctions.length) {
            aggregateResult.update(
              allImperativeAggregateFunctionPositions(i),
              allImperativeAggregateFunctions(i).eval(currentBuffer))
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
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allImperativeAggregateFunctions.length) {
      allImperativeAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }

  /**
   * Creates a new aggregation buffer and initializes buffer values
   * for all aggregate functions.
   */
  protected def newBuffer: MutableRow
}
