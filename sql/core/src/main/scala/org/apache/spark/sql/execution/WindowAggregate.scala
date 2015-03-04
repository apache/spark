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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, InterpretedMutableProjection, _}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution}
import org.apache.spark.util.collection.CompactBuffer
import org.apache.spark.sql.types._


/**
 * :: DeveloperApi ::
 * Groups input data by `partitionExpressions` and computes the `computeExpressions` for each
 * group.
 * @param partitionExpressions expressions that are evaluated to determine partition.
 * @param computeExpressions computeExpressions that are computed now for each partition.
 * @param otherExpressions otherExpressions that are expressions except computeExpressions.
 * @param child the input data source.
 */
@DeveloperApi
case class WindowAggregate(
    partitionExpressions: Seq[Expression],
    computeExpressions: Seq[WindowExpression],
    otherExpressions: Seq[NamedExpression],
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

  override def output = (computeExpressions ++ otherExpressions).map(_.toAttribute)

  private[this] val computeAttributes = computeExpressions.map(_.child).map { func =>
    func -> AttributeReference(s"funcResult:$func", func.dataType, func.nullable)()}

  private[this] val otherAttributes = otherExpressions.map(_.toAttribute)

  /** The schema of the result of all evaluations */
  private[this] val resultAttributes = otherAttributes ++ computeAttributes.map(_._2)

  private[this] val resultMap =
    (otherExpressions.map { other => other -> other.toAttribute } ++ computeAttributes).toMap

  private[this] val resultExpressions = (computeExpressions ++ otherExpressions).map { sel =>
    sel.transform {
      case e: Expression if resultMap.contains(e) => resultMap(e)
    }
  }

  private[this] val sortExpressions = child match {
    case Sort(sortOrder, _, _) => sortOrder
    case _ => null
  }

  private[this] def computeFunctions(rows: CompactBuffer[Row]): Seq[Iterator[Any]] =
    computeExpressions.map{ expr =>
      val baseExpr = BindReferences.bindReference(
        expr.child.asInstanceOf[AggregateExpression], childOutput)
      expr.windowSpec.windowFrame.map {frame =>
        frame.frameType match {
          case RowsFrame => rowsWindowFunction(baseExpr, frame, rows).iterator
          case ValueFrame => valueWindowFunction(baseExpr, frame, rows).iterator
        }
      }.getOrElse {
        val function = baseExpr.newInstance()
        if (sortExpressions != null) {
          function.dataType match {
            case _: ArrayType =>
              rows.foreach(function.update)
              function.eval(EmptyRow).asInstanceOf[Seq[Any]].iterator
            case _ =>
              rows.map { row =>
                function.update(row)
                function.eval(EmptyRow)
              }.iterator
          }
        } else {
          rows.foreach(function.update)
          function.eval(EmptyRow) match {
            case r: Seq[_] => r.iterator
            case other => (0 to rows.size - 1).map(r => other).iterator
          }
        }
      }
    }

  private[this] def rowsWindowFunction(base: AggregateExpression, frame: WindowFrame,
      rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    val rangeResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {
      var start =
        if (frame.preceding == Int.MaxValue) 0
        else rowIndex - frame.preceding
      if (start < 0) start = 0
      var end =
        if (frame.following == Int.MaxValue) {
          rows.size - 1
        } else {
          rowIndex + frame.following
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

  private[this] def valueWindowFunction(base: AggregateExpression, frame: WindowFrame,
      rows: CompactBuffer[Row]): CompactBuffer[Any] = {

    // range only support 1 order
    val sortExpression = BindReferences.bindReference(sortExpressions.head, childOutput)

    val preceding = sortExpression.child.dataType match {
      case IntegerType => Literal(frame.preceding)
      case LongType => Literal(frame.preceding.toLong)
      case DoubleType => Literal(frame.preceding.toDouble)
      case FloatType => Literal(frame.preceding.toFloat)
      case ShortType => Literal(frame.preceding.toShort)
      case DecimalType() => Literal(BigDecimal(frame.preceding))
      case _=> throw new Exception(s"not support dataType ")
    }
    val following = sortExpression.child.dataType match {
      case IntegerType => Literal(frame.following)
      case LongType => Literal(frame.following.toLong)
      case DoubleType => Literal(frame.following.toDouble)
      case FloatType => Literal(frame.following.toFloat)
      case ShortType => Literal(frame.following.toShort)
      case DecimalType() => Literal(BigDecimal(frame.following))
      case _=> throw new Exception(s"not support dataType ")
    }

    val rangeResults = new CompactBuffer[Any]()
    var rowIndex = 0
    while (rowIndex < rows.size) {
      val currentRow = rows(rowIndex)
      val eval = sortExpression.child.eval(currentRow)
      val precedingExpr =
        if (sortExpression.direction == Ascending) {
          Literal(eval) - sortExpression.child <= preceding
        } else {
          sortExpression.child - Literal(eval) <= preceding
        }

      val followingExpr =
        if (sortExpression.direction == Ascending) {
          sortExpression.child - Literal(eval) <= following
        } else {
          Literal(eval) - sortExpression.child <= following
        }

      var precedingIndex = 0
      var followingIndex = rows.size - 1
      if (sortExpression != null) {
        if (frame.preceding != Int.MaxValue) precedingIndex = rowIndex
        while (precedingIndex > 0 &&
          precedingExpr.eval(rows(precedingIndex - 1)).asInstanceOf[Boolean]) {
          precedingIndex -= 1
        }

        if (frame.following != Int.MaxValue) followingIndex = rowIndex
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
      functionsResult: Seq[Iterator[Any]]): GenericMutableRow = {
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

        val otherProjection = new InterpretedMutableProjection(otherAttributes, childOutput)
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
          private[this] var functionsResult: Seq[Iterator[Any]] = _
          private[this] var currentRowIndex: Int = -1

          val resultProjection = new InterpretedProjection(resultExpressions, resultAttributes)
          val otherProjection = new InterpretedMutableProjection(otherAttributes, childOutput)
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