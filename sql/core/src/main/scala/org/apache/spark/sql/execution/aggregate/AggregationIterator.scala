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
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection),
    outputsUnsafeRows: Boolean)
  extends Iterator[InternalRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  require(
    aggregateExpressions.map(_.mode).distinct.length <= 2,
    s"$aggregateExpressions are not supported becuase they have more than 2 distinct modes.")

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
  protected val aggregateFunctions: Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
    val functions = new Array[AggregateFunction](aggregateExpressions.length)
    var i = 0
    while (i < aggregateExpressions.length) {
      val func = aggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = aggregateExpressions(i).mode match {
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
    while (i < aggregateFunctions.length) {
      aggregateFunctions(i) match {
        case agg: DeclarativeAggregate =>
        case _ => positions += i
      }
      i += 1
    }
    positions.toArray
  }

  // The projection used to initialize buffer values for all expression-based aggregates.
  private[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
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
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  ///////////////////////////////////////////////////////////////////////////
  // Methods and fields used by sub-classes.
  ///////////////////////////////////////////////////////////////////////////

  // Initializing functions used to process a row.
  protected val processRow: (MutableRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    val aggregationBufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
    val modes = aggregateExpressions.map(_.mode).distinct
    if (aggregateExpressions.nonEmpty) {
      val inputAggregationBufferSchema = if (initialInputBufferOffset == 0) {
        // If initialInputBufferOffset, the input value does not contain
        // grouping keys.
        // This part is pretty hacky.
        aggregateFunctions.flatMap(_.inputAggBufferAttributes).toSeq
      } else {
        groupingKeyAttributes ++ aggregateFunctions.flatMap(_.inputAggBufferAttributes)
      }
      val mergeExpressions = aggregateFunctions.zipWithIndex.flatMap {
        case (ae: DeclarativeAggregate, i) =>
          aggregateExpressions(i).mode match {
            case Partial | Complete => ae.updateExpressions
            case PartialMerge | Final => ae.mergeExpressions
          }
        case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
      }
      val updateFunctions = aggregateFunctions.zipWithIndex.collect {
        case (ae: ImperativeAggregate, i) =>
          aggregateExpressions(i).mode match {
            case Partial | Complete =>
              (buffer: MutableRow, row: InternalRow) => ae.update(buffer, row)
            case PartialMerge | Final =>
              (buffer: MutableRow, row: InternalRow) => ae.merge(buffer, row)
          }
      }
      // This projection is used to merge buffer values for all expression-based aggregates.
      val expressionAggMergeProjection =
        newMutableProjection(mergeExpressions, aggregationBufferSchema ++ valueAttributes)()

      (currentBuffer: MutableRow, row: InternalRow) => {
        // Process all expression-based aggregate functions.
        expressionAggMergeProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
        // Process all imperative aggregate functions.
        var i = 0
        while (i < updateFunctions.length) {
          updateFunctions(i)(currentBuffer, row)
          i += 1
        }
      }
    } else {
      // Grouping only.
      (currentBuffer: MutableRow, row: InternalRow) => {}
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
    val modes = aggregateExpressions.map(_.mode).distinct
    if (!modes.contains(Final) && !modes.contains(Complete)) {
      // Because we cannot copy a joinedRow containing a UnsafeRow (UnsafeRow does not
      // support generic getter), we create a mutable projection to output the
      // JoinedRow(currentGroupingKey, currentBuffer)
      val bufferSchema = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val resultProjection =
        newMutableProjection(
          groupingKeyAttributes ++ bufferSchema,
          groupingKeyAttributes ++ bufferSchema)()
      resultProjection.target(mutableOutput)
      (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
        resultProjection(rowToBeEvaluated(currentGroupingKey, currentBuffer))
      }
    } else if (aggregateExpressions.nonEmpty) {
      // Final-only, Complete-only and Final-Complete: every output row contains values representing
      // resultExpressions.
      val bufferSchemata = aggregateFunctions.flatMap(_.aggBufferAttributes)
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferSchemata)()
      val aggregateResultSchema = aggregateAttributes
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
    } else {
      // Grouping-only: we only output values of grouping expressions.
      val resultProjection =
        newMutableProjection(resultExpressions, groupingKeyAttributes)()
      resultProjection.target(mutableOutput)

      (currentGroupingKey: InternalRow, currentBuffer: MutableRow) => {
        resultProjection(currentGroupingKey)
      }
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
