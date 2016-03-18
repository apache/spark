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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._

/**
 * The base class of [[SortBasedAggregationIterator]] and [[TungstenAggregationIterator]].
 * It mainly contains two parts:
 * 1. It initializes aggregate functions.
 * 2. It creates two functions, `processRow` and `generateOutput` based on [[AggregateMode]] of
 *    its aggregate functions. `processRow` is the function to handle an input. `generateOutput`
 *    is used to generate result.
 */
abstract class AggregationIterator(
<<<<<<< HEAD
    groupingExpressions: Seq[NamedExpression],
    inputAttributes: Seq[Attribute],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
=======
    groupingKeyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute],
    nonCompleteAggregateExpressions: Seq[AggregateExpression],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression],
    completeAggregateAttributes: Seq[Attribute],
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    newMutableProjection: (Seq[Expression], Seq[Attribute]) => (() => MutableProjection))
  extends Iterator[UnsafeRow] with Logging {

  ///////////////////////////////////////////////////////////////////////////
  // Initializing functions.
  ///////////////////////////////////////////////////////////////////////////

  /**
    * The following combinations of AggregationMode are supported:
    * - Partial
    * - PartialMerge (for single distinct)
    * - Partial and PartialMerge (for single distinct)
    * - Final
    * - Complete (for SortBasedAggregate with functions that does not support Partial)
    * - Final and Complete (currently not used)
    *
    * TODO: AggregateMode should have only two modes: Update and Merge, AggregateExpression
    * could have a flag to tell it's final or not.
    */
  {
    val modes = aggregateExpressions.map(_.mode).distinct.toSet
    require(modes.size <= 2,
      s"$aggregateExpressions are not supported because they have more than 2 distinct modes.")
    require(modes.subsetOf(Set(Partial, PartialMerge)) || modes.subsetOf(Set(Final, Complete)),
      s"$aggregateExpressions can't have Partial/PartialMerge and Final/Complete in the same time.")
  }

  // Initialize all AggregateFunctions by binding references if necessary,
  // and set inputBufferOffset and mutableBufferOffset.
<<<<<<< HEAD
  protected def initializeAggregateFunctions(
      expressions: Seq[AggregateExpression],
      startingInputBufferOffset: Int): Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = startingInputBufferOffset
    val functions = new Array[AggregateFunction](expressions.length)
    var i = 0
    while (i < expressions.length) {
      val func = expressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = expressions(i).mode match {
=======
  protected val allAggregateFunctions: Array[AggregateFunction] = {
    var mutableBufferOffset = 0
    var inputBufferOffset: Int = initialInputBufferOffset
    val functions = new Array[AggregateFunction](allAggregateExpressions.length)
    var i = 0
    while (i < allAggregateExpressions.length) {
      val func = allAggregateExpressions(i).aggregateFunction
      val funcWithBoundReferences: AggregateFunction = allAggregateExpressions(i).mode match {
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
        case Partial | Complete if func.isInstanceOf[ImperativeAggregate] =>
          // We need to create BoundReferences if the function is not an
          // expression-based aggregate function (it does not support code-gen) and the mode of
          // this function is Partial or Complete because we will call eval of this
          // function's children in the update method of this aggregate function.
          // Those eval calls require BoundReferences to work.
          BindReferences.bindReference(func, inputAttributes)
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

  protected val aggregateFunctions: Array[AggregateFunction] =
    initializeAggregateFunctions(aggregateExpressions, initialInputBufferOffset)

  // Positions of those imperative aggregate functions in allAggregateFunctions.
  // For example, we have func1, func2, func3, func4 in aggregateFunctions, and
  // func2 and func3 are imperative aggregate functions.
  // ImperativeAggregateFunctionPositions will be [1, 2].
  protected[this] val allImperativeAggregateFunctionPositions: Array[Int] = {
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

<<<<<<< HEAD
=======
  // All AggregateFunctions functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteAggregateFunctions: Array[AggregateFunction] =
    allAggregateFunctions.take(nonCompleteAggregateExpressions.length)

  // All imperative aggregate functions with mode Partial, PartialMerge, or Final.
  private[this] val nonCompleteImperativeAggregateFunctions: Array[ImperativeAggregate] =
    nonCompleteAggregateFunctions.collect { case func: ImperativeAggregate => func }

>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
  // The projection used to initialize buffer values for all expression-based aggregates.
  protected[this] val expressionAggInitialProjection = {
    val initExpressions = aggregateFunctions.flatMap {
      case ae: DeclarativeAggregate => ae.initialValues
      // For the positions corresponding to imperative aggregate functions, we'll use special
      // no-op expressions which are ignored during projection code-generation.
      case i: ImperativeAggregate => Seq.fill(i.aggBufferAttributes.length)(NoOp)
    }
    newMutableProjection(initExpressions, Nil)()
  }

  // All imperative AggregateFunctions.
  protected[this] val allImperativeAggregateFunctions: Array[ImperativeAggregate] =
    allImperativeAggregateFunctionPositions
      .map(aggregateFunctions)
      .map(_.asInstanceOf[ImperativeAggregate])

  // Initializing functions used to process a row.
<<<<<<< HEAD
  protected def generateProcessRow(
      expressions: Seq[AggregateExpression],
      functions: Seq[AggregateFunction],
      inputAttributes: Seq[Attribute]): (MutableRow, InternalRow) => Unit = {
    val joinedRow = new JoinedRow
    if (expressions.nonEmpty) {
      val mergeExpressions = functions.zipWithIndex.flatMap {
        case (ae: DeclarativeAggregate, i) =>
          expressions(i).mode match {
            case Partial | Complete => ae.updateExpressions
            case PartialMerge | Final => ae.mergeExpressions
          }
        case (agg: AggregateFunction, _) => Seq.fill(agg.aggBufferAttributes.length)(NoOp)
      }
      val updateFunctions = functions.zipWithIndex.collect {
        case (ae: ImperativeAggregate, i) =>
          expressions(i).mode match {
            case Partial | Complete =>
              (buffer: MutableRow, row: InternalRow) => ae.update(buffer, row)
            case PartialMerge | Final =>
              (buffer: MutableRow, row: InternalRow) => ae.merge(buffer, row)
=======
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
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
          }
      }
      // This projection is used to merge buffer values for all expression-based aggregates.
      val aggregationBufferSchema = functions.flatMap(_.aggBufferAttributes)
      val updateProjection =
        newMutableProjection(mergeExpressions, aggregationBufferSchema ++ inputAttributes)()

      (currentBuffer: MutableRow, row: InternalRow) => {
        // Process all expression-based aggregate functions.
        updateProjection.target(currentBuffer)(joinedRow(currentBuffer, row))
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

<<<<<<< HEAD
  protected val processRow: (MutableRow, InternalRow) => Unit =
    generateProcessRow(aggregateExpressions, aggregateFunctions, inputAttributes)
=======
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
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3

  protected val groupingProjection: UnsafeProjection =
    UnsafeProjection.create(groupingExpressions, inputAttributes)
  protected val groupingAttributes = groupingExpressions.map(_.toAttribute)

  // Initializing the function used to generate the output row.
  protected def generateResultProjection(): (UnsafeRow, MutableRow) => UnsafeRow = {
    val joinedRow = new JoinedRow
    val modes = aggregateExpressions.map(_.mode).distinct
    val bufferAttributes = aggregateFunctions.flatMap(_.aggBufferAttributes)
    if (modes.contains(Final) || modes.contains(Complete)) {
      val evalExpressions = aggregateFunctions.map {
        case ae: DeclarativeAggregate => ae.evaluateExpression
        case agg: AggregateFunction => NoOp
      }
      val aggregateResult = new SpecificMutableRow(aggregateAttributes.map(_.dataType))
      val expressionAggEvalProjection = newMutableProjection(evalExpressions, bufferAttributes)()
      expressionAggEvalProjection.target(aggregateResult)

      val resultProjection =
        UnsafeProjection.create(resultExpressions, groupingAttributes ++ aggregateAttributes)

      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
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
        resultProjection(joinedRow(currentGroupingKey, aggregateResult))
      }
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      val resultProjection = UnsafeProjection.create(
        groupingAttributes ++ bufferAttributes,
        groupingAttributes ++ bufferAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(joinedRow(currentGroupingKey, currentBuffer))
      }
    } else {
      // Grouping-only: we only output values based on grouping expressions.
      val resultProjection = UnsafeProjection.create(resultExpressions, groupingAttributes)
      (currentGroupingKey: UnsafeRow, currentBuffer: MutableRow) => {
        resultProjection(currentGroupingKey)
      }
    }
  }

  protected val generateOutput: (UnsafeRow, MutableRow) => UnsafeRow =
    generateResultProjection()

  /** Initializes buffer values for all aggregate functions. */
  protected def initializeBuffer(buffer: MutableRow): Unit = {
    expressionAggInitialProjection.target(buffer)(EmptyRow)
    var i = 0
    while (i < allImperativeAggregateFunctions.length) {
      allImperativeAggregateFunctions(i).initialize(buffer)
      i += 1
    }
  }
}
