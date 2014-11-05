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

package org.apache.spark.sql.execution

import java.util.HashMap

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.errors._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.InterpretedMutableProjection
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.SortPartitions


/**
 * :: DeveloperApi ::
 * Groups input data by `partitionExpressions` and computes the `computeExpressions` for each
 * group.
 * @param partitionExpressions expressions that are evaluated to determine partition.
 * @param functionExpressions expressions that are computed for each partition.
 * @param child the input data source.
 */
@DeveloperApi
case class WindowFunction(
  partitionExpressions: Seq[Expression],
  functionExpressions: Seq[NamedExpression],
  child: SparkPlan)
  extends UnaryNode {

  override def requiredChildDistribution =
    if (partitionExpressions == Nil) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionExpressions) :: Nil
    }

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.
  private[this] val childOutput = child.output

  override def output = functionExpressions.map(_.toAttribute)

  /** A list of functions that need to be computed for each partition. */
  private[this] val computeExpressions = new ArrayBuffer[AggregateExpression]

  private[this] val otherExpressions = new ArrayBuffer[NamedExpression]

  functionExpressions.foreach { sel =>
    sel.collect {
      case func: AggregateExpression => computeExpressions += func
      case other: NamedExpression if (!other.isInstanceOf[Alias]) => otherExpressions += other
    }
  }

  private[this] val functionAttributes = computeExpressions.map { func =>
    func -> AttributeReference(s"funcResult:$func", func.dataType, func.nullable)()}

  /** The schema of the result of all evaluations */
  private[this] val resultAttributes =
    otherExpressions.map(_.toAttribute) ++ functionAttributes.map(_._2)

  private[this] val resultMap =
    (otherExpressions.map { other => other -> other.toAttribute } ++ functionAttributes
    ).toMap


  private[this] val resultExpressions = functionExpressions.map { sel =>
    sel.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  private[this] val sortExpressions =
    if (child.isInstanceOf[SortPartitions]) {
      child.asInstanceOf[SortPartitions].sortExpressions
    }
    else if (child.isInstanceOf[Sort]) {
      child.asInstanceOf[Sort].sortOrder
    }
    else null

  /** Creates a new function buffer for a partition. */
  private[this] def newFunctionBuffer(): Array[AggregateFunction] = {
    val buffer = new Array[AggregateFunction](computeExpressions.length)
    var i = 0
    while (i < computeExpressions.length) {
      val baseExpr = BindReferences.bindReference(computeExpressions(i), childOutput)
      baseExpr.windowFrame = computeExpressions(i).windowFrame
      buffer(i) = baseExpr.newInstance()
      i += 1
    }
    buffer
  }

  private[this] def computeFunctions(rows: CompactBuffer[Row]): Array[Iterator[Any]] = {
    val aggrFunctions = newFunctionBuffer()
    val functionResults = new Array[Iterator[Any]](aggrFunctions.length)
    var i = 0
    while (i < aggrFunctions.length) {
      val aggrFunction = aggrFunctions(i)
      val base = aggrFunction.base
      if (base.windowFrame == null) {
        if (sortExpressions != null) {
          if (aggrFunction.dataType.isInstanceOf[ArrayType]) {
            rows.foreach(aggrFunction.update)
            functionResults(i) = aggrFunction.eval(EmptyRow).asInstanceOf[Seq[Any]].iterator
          } else {
            functionResults(i) = rows.map(row => {
              aggrFunction.update(row)
              aggrFunction.eval(EmptyRow)
            }).iterator
          }
        } else {
          rows.foreach(aggrFunction.update)
          functionResults(i) = aggrFunction.eval(EmptyRow) match {
            case r: Seq[_] => r.iterator
            case other => (0 to rows.size - 1).map(r => other).iterator
          }
        }

      } else {
        functionResults(i) =
          if (base.windowFrame.frameType == "ROWS_FRAME") rowsWindowFunction(base, rows).iterator
          else valueWindowFunction(base, rows).iterator
      }
      i += 1
    }
    functionResults
  }

  private[this] def rowsWindowFunction(base: AggregateExpression,
    rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    val rangeResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {

      val windowFrame = base.windowFrame
      var start =
        if (windowFrame.preceding == Int.MaxValue) 0
        else rowIndex - windowFrame.preceding
      if (start < 0) start = 0
      var end =
        if (windowFrame.following == Int.MaxValue) {
          rows.size - 1
        } else {
          rowIndex + windowFrame.following
        }
      if (end > rows.size - 1) end = rows.size - 1

      // new aggregate function
      val aggr = base.newInstance()
      (start to end).foreach(i => aggr.update(rows(i)))

      rangeResults += aggr.eval(EmptyRow)
      rowIndex += 1
    }
    rangeResults
  }

  private[this] def valueWindowFunction(base: AggregateExpression,
    rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    val windowFrame = base.windowFrame

    // rande only support 1 order
    val sortExpression = BindReferences.bindReference(sortExpressions.head, childOutput)

    val preceding = sortExpression.child.dataType match {
      case IntegerType => Literal(windowFrame.preceding)
      case LongType => Literal(windowFrame.preceding.toLong)
      case DoubleType => Literal(windowFrame.preceding.toDouble)
      case FloatType => Literal(windowFrame.preceding.toFloat)
      case ShortType => Literal(windowFrame.preceding.toShort)
      case DecimalType => Literal(BigDecimal(windowFrame.preceding))
      case _=> throw new Exception(s"not support dataType ")
    }
    val following = sortExpression.child.dataType match {
      case IntegerType => Literal(windowFrame.following)
      case LongType => Literal(windowFrame.following.toLong)
      case DoubleType => Literal(windowFrame.following.toDouble)
      case FloatType => Literal(windowFrame.following.toFloat)
      case ShortType => Literal(windowFrame.following.toShort)
      case DecimalType => Literal(BigDecimal(windowFrame.following))
      case _=> throw new Exception(s"not support dataType ")
    }

    val rangeResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {
      val currentRow = rows(rowIndex)
      val precedingExpr =
        if (sortExpression.direction == Ascending) {
          Literal(sortExpression.child.eval(currentRow)) - sortExpression.child <= preceding
        } else {
          sortExpression.child - Literal(sortExpression.child.eval(currentRow)) <= preceding
        }


      val followingExpr =
        if (sortExpression.direction == Ascending) {
          sortExpression.child - Literal(sortExpression.child.eval(currentRow)) <= following
        } else {
          Literal(sortExpression.child.eval(currentRow)) - sortExpression.child <= following
        }

      var precedingIndex = 0
      var followingIndex = rows.size - 1
      if (sortExpression != null) {

        if (windowFrame.preceding != Int.MaxValue) precedingIndex = rowIndex
        while (precedingIndex > 0 &&
          precedingExpr.eval(rows(precedingIndex - 1)).asInstanceOf[Boolean]) {
          precedingIndex -= 1
        }

        if (windowFrame.following != Int.MaxValue) followingIndex = rowIndex
        while (followingIndex < rows.size - 1 &&
          followingExpr.eval(rows(followingIndex + 1)).asInstanceOf[Boolean]) {
          followingIndex += 1
        }
      }
      // new aggregate function
      val aggr = base.newInstance()
      (precedingIndex to followingIndex).foreach(i => aggr.update(rows(i)))
      rangeResults += aggr.eval(EmptyRow)
      rowIndex += 1
    }
    rangeResults
  }

  private[this] def getNextFunctionsRow(
    functionsResult: Array[Iterator[Any]]): GenericMutableRow = {
    val result = new GenericMutableRow(functionsResult.length)
    var i = 0
    while (i < functionsResult.length) {
      result(i) = functionsResult(i).next
      i += 1
    }
    result
  }


  override def execute() = attachTree(this, "execute") {
    if (partitionExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>

        val resultProjection = new InterpretedProjection(resultExpressions, resultAttributes)

        val otherProjection = new InterpretedMutableProjection(otherExpressions, childOutput)
        val joinedRow = new JoinedRow

        val rows = new CompactBuffer[Row]()
        while (iter.hasNext) {
          rows += iter.next().copy()
        }
        new Iterator[Row] {
          private[this] val functionsResult = computeFunctions(rows)
          private[this] var currentRowIndex: Int = 0

          override final def hasNext: Boolean = currentRowIndex < rows.size

          override final def next(): Row = {

            val otherResults = otherProjection(rows(currentRowIndex)).copy()
            currentRowIndex += 1
            resultProjection(joinedRow(otherResults,getNextFunctionsRow(functionsResult)))
          }
        }

      }
    } else {
      child.execute().mapPartitions { iter =>
        val partitionTable = new HashMap[Row, CompactBuffer[Row]]
        val partitionProjection =
          new InterpretedMutableProjection(partitionExpressions, childOutput)

        var currentRow: Row = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val partitionKey = partitionProjection(currentRow).copy()
          val existingMatchList = partitionTable.get(partitionKey)
          val matchList = if (existingMatchList == null) {
            val newMatchList = new CompactBuffer[Row]()
            partitionTable.put(partitionKey, newMatchList)
            newMatchList
          } else {
            existingMatchList
          }
          matchList += currentRow.copy()
        }

        new Iterator[Row] {
          private[this] val partitionTableIter = partitionTable.entrySet().iterator()
          private[this] var currentpartition: CompactBuffer[Row] = _
          private[this] var functionsResult: Array[Iterator[Any]] = _
          private[this] var currentRowIndex: Int = -1

          val resultProjection = new InterpretedProjection(resultExpressions, resultAttributes)
          val otherProjection = new InterpretedMutableProjection(otherExpressions, childOutput)
          val joinedRow = new JoinedRow

          override final def hasNext: Boolean =
            (currentRowIndex != -1 && currentRowIndex < currentpartition.size) ||
              (partitionTableIter.hasNext && fetchNext())

          override final def next(): Row = {

            val otherResults = otherProjection(currentpartition(currentRowIndex)).copy()
            currentRowIndex += 1
            resultProjection(joinedRow(otherResults,getNextFunctionsRow(functionsResult)))

          }

          private final def fetchNext(): Boolean = {

            currentRowIndex = 0
            if (partitionTableIter.hasNext) {
              currentpartition = partitionTableIter.next().getValue
              functionsResult = computeFunctions(currentpartition)
              true
            } else false
          }
        }

      }
    }
  }
}
