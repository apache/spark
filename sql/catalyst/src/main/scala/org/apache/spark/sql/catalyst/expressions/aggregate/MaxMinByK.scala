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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.{GenericArrayData, TypeUtils}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * Returns top/bottom K values ordered by orderingExpr.
 * Uses a heap (min-heap for max_by, max-heap for min_by) to efficiently maintain K elements.
 */
@ExpressionDescription(
  usage = """
    _FUNC_(x, y, k) - Returns an array of the `k` values of `x` associated with the
    maximum/minimum values of `y`. Use max_by for maximum, min_by for minimum.
  """,
  examples = """
    Examples:
      > SELECT max_by(x, y, 2) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);
       ["b","c"]
      > SELECT min_by(x, y, 2) FROM VALUES ('a', 10), ('b', 50), ('c', 20) AS tab(x, y);
       ["a","c"]
  """,
  arguments = """
    Arguments:
      * x - the value expression to collect
      * y - the ordering expression (must be orderable)
      * k - the number of values to return (must be a positive integer literal)
  """,
  note = """
    The function is non-deterministic because the order of collected results depends
    on the order of the rows which may be non-deterministic after a shuffle when there
    are ties in the ordering expression.
  """,
  group = "agg_funcs",
  since = "4.2.0")
case class MaxMinByK(
    valueExpr: Expression,
    orderingExpr: Expression,
    kExpr: Expression,
    reverse: Boolean = false,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate
  with TernaryLike[Expression]
  with ImplicitCastInputTypes {

  def this(valueExpr: Expression, orderingExpr: Expression, kExpr: Expression) =
    this(valueExpr, orderingExpr, kExpr, false, 0, 0)

  def this(valueExpr: Expression, orderingExpr: Expression, kExpr: Expression, reverse: Boolean) =
    this(valueExpr, orderingExpr, kExpr, reverse, 0, 0)

  final val MAX_K = 100000
  lazy val k: Int = {
    if (!kExpr.foldable) {
      throw new IllegalArgumentException(
        s"The third argument of $prettyName must be a foldable expression, got: $kExpr")
    }
    val kValue = kExpr.eval() match {
      case i: Int if i > 0 => i
      case l: Long if l > 0 && l <= Int.MaxValue => l.toInt
      case s: Short if s > 0 => s.toInt
      case b: Byte if b > 0 => b.toInt
      case other =>
        throw new IllegalArgumentException(
          s"The third argument of $prettyName must be a positive integer, got: $other")
    }
    if (kValue > MAX_K) {
      throw new IllegalArgumentException(
        s"The third argument of $prettyName must be at most $MAX_K, got: $kValue")
    }
    kValue
  }

  override def first: Expression = valueExpr
  override def second: Expression = orderingExpr
  override def third: Expression = kExpr

  override def prettyName: String = if (reverse) "min_by" else "max_by"

  // The default aggregation result is an empty array, which is not nullable.
  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(valueExpr.dataType, containsNull = true)

  override def inputTypes: Seq[AbstractDataType] = Seq(
    AnyDataType,
    AnyDataType,
    IntegralType
  )

  private lazy val valuesAttr = AttributeReference(
    "values",
    ArrayType(valueExpr.dataType, containsNull = true),
    nullable = false
  )()
  private lazy val orderingsAttr = AttributeReference(
    "orderings",
    ArrayType(orderingExpr.dataType, containsNull = true),
    nullable = false
  )()
  private lazy val heapIndicesAttr = AttributeReference(
    "heapIndices",
    ArrayType(IntegerType, containsNull = false),
    nullable = false
  )()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq(valuesAttr, orderingsAttr, heapIndicesAttr)

  private val VALUES_OFFSET = 0
  private val ORDERINGS_OFFSET = 1
  private val HEAP_OFFSET = 2

  override lazy val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  override def aggBufferSchema: StructType =
    StructType(aggBufferAttributes.map(a => StructField(a.name, a.dataType, a.nullable)))
  override def defaultResult: Option[Literal] = Option(Literal.create(Array(), dataType))

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!kExpr.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> "k",
          "inputType" -> kExpr.dataType.catalogString,
          "inputExpr" -> kExpr.sql
        )
      )
    } else {
      val orderingCheck = TypeUtils.checkForOrderingExpr(orderingExpr.dataType, prettyName)
      if (orderingCheck.isSuccess) {
        try {
          val _ = k
          TypeCheckResult.TypeCheckSuccess
        } catch {
          case _: IllegalArgumentException =>
            DataTypeMismatch(
              errorSubClass = "VALUE_OUT_OF_RANGE",
              messageParameters = Map(
                "exprName" -> toSQLId("k"),
                "valueRange" -> s"[1, $MAX_K]",
                "currentValue" -> kExpr.sql
              )
            )
        }
      } else {
        orderingCheck
      }
    }
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(orderingExpr.dataType)

  // max_by uses min-heap (smaller at top), min_by uses max-heap (larger at top)
  private def heapCompare(ordA: Any, ordB: Any): Int =
    if (reverse) -ordering.compare(ordA, ordB) else ordering.compare(ordA, ordB)

  override def initialize(buffer: InternalRow): Unit = {
    val offset = mutableAggBufferOffset
    buffer.update(offset + VALUES_OFFSET, new GenericArrayData(new Array[Any](k)))
    buffer.update(offset + ORDERINGS_OFFSET, new GenericArrayData(new Array[Any](k)))
    // heapArr is Array[Any] with boxed Integers: [size, idx0, idx1, ..., idx(k-1)]
    val heapArr = new Array[Any](k + 1)
    heapArr(0) = 0
    buffer.update(offset + HEAP_OFFSET, new GenericArrayData(heapArr))
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val ord = orderingExpr.eval(inputRow)
    if (ord == null) return

    val value = valueExpr.eval(inputRow)
    val offset = mutableAggBufferOffset

    val valuesArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + VALUES_OFFSET, valueExpr.dataType)
    val orderingsArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + ORDERINGS_OFFSET, orderingExpr.dataType)
    val heapArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + HEAP_OFFSET, IntegerType)

    MaxMinByKHeap.insert(value, ord, k, valuesArr, orderingsArr, heapArr, heapCompare)
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    val offset = mutableAggBufferOffset
    val inOff = inputAggBufferOffset

    val valuesArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + VALUES_OFFSET, valueExpr.dataType)
    val orderingsArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + ORDERINGS_OFFSET, orderingExpr.dataType)
    val heapArr = MaxMinByKHeap.getMutableArray(
      mutableAggBuffer, offset + HEAP_OFFSET, IntegerType)

    val inputValues = MaxMinByKHeap.readArray(
      inputAggBuffer.getArray(inOff + VALUES_OFFSET), valueExpr.dataType)
    val inputOrderings = MaxMinByKHeap.readArray(
      inputAggBuffer.getArray(inOff + ORDERINGS_OFFSET), orderingExpr.dataType)
    val inputHeapData = inputAggBuffer.getArray(inOff + HEAP_OFFSET)
    val inputHeapSize = inputHeapData.getInt(0)

    for (i <- 0 until inputHeapSize) {
      val idx = inputHeapData.getInt(i + 1)
      val inputOrd = inputOrderings(idx)
      if (inputOrd != null) {
        MaxMinByKHeap.insert(
          inputValues(idx), inputOrd, k, valuesArr, orderingsArr, heapArr, heapCompare)
      }
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val offset = mutableAggBufferOffset

    val valuesArr = MaxMinByKHeap.getMutableArray(
      buffer, offset + VALUES_OFFSET, valueExpr.dataType)
    val orderingsArr = MaxMinByKHeap.getMutableArray(
      buffer, offset + ORDERINGS_OFFSET, orderingExpr.dataType)
    val heapArr = MaxMinByKHeap.getMutableArray(
      buffer, offset + HEAP_OFFSET, IntegerType)
    val heapSize = MaxMinByKHeap.getSize(heapArr)

    val elements = new Array[(Any, Any)](heapSize)
    for (i <- 0 until heapSize) {
      elements(i) = (InternalRow.copyValue(valuesArr(i)), orderingsArr(i))
    }

    // Sort result array (heap maintains K elements but not in sorted order).
    val sorted = if (reverse) {
      elements.sortWith { (a, b) => ordering.compare(a._2, b._2) < 0 }
    } else {
      elements.sortWith { (a, b) => ordering.compare(a._2, b._2) > 0 }
    }
    new GenericArrayData(sorted.map(_._1))
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newOffset)

  override protected def withNewChildrenInternal(
      newFirst: Expression,
      newSecond: Expression,
      newThird: Expression): MaxMinByK =
    copy(valueExpr = newFirst, orderingExpr = newSecond, kExpr = newThird)
}

object MaxByBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    expressions.length match {
      case 2 => MaxBy(expressions(0), expressions(1))
      case 3 => new MaxMinByK(expressions(0), expressions(1), expressions(2), reverse = false)
      case n =>
        throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2, 3), n)
    }
  }
}

object MinByBuilder extends ExpressionBuilder {
  override def build(funcName: String, expressions: Seq[Expression]): Expression = {
    expressions.length match {
      case 2 => MinBy(expressions(0), expressions(1))
      case 3 => new MaxMinByK(expressions(0), expressions(1), expressions(2), reverse = true)
      case n =>
        throw QueryCompilationErrors.wrongNumArgsError(funcName, Seq(2, 3), n)
    }
  }
}
