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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{ExpressionBuilder, TypeCheckResult}
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.DataTypeMismatch
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TernaryLike
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, TypeUtils}
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types._

/**
 * Returns the top/bottom K values of `valueExpr` ordered by `orderingExpr`.
 *
 * This is the k-element variant of max_by/min_by that returns an array of values
 * instead of a single value.
 *
 * Buffer layout: Struct{values: Array<V>, orderings: Array<O>, heapIndices: Binary}
 * - values/orderings: stored at indices 0..size-1
 * - heapIndices: serialized int[] where heapIndices[heapPos] = index into values/orderings
 *
 * When reverse=false (default, for max_by): Uses a min-heap to keep top K largest values.
 * When reverse=true (for min_by): Uses a max-heap to keep bottom K smallest values.
 * Heap operations only modify heapIndices, avoiding copying of potentially large values.
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

  lazy val k: Int = {
    if (!kExpr.foldable) {
      throw new IllegalArgumentException(
        s"The third argument of $prettyName must be a foldable expression, got: $kExpr")
    }
    kExpr.eval() match {
      case i: Int if i > 0 => i
      case l: Long if l > 0 && l <= Int.MaxValue => l.toInt
      case s: Short if s > 0 => s.toInt
      case b: Byte if b > 0 => b.toInt
      case other =>
        throw new IllegalArgumentException(
          s"The third argument of $prettyName must be a positive integer, got: $other")
    }
  }

  override def first: Expression = valueExpr
  override def second: Expression = orderingExpr
  override def third: Expression = kExpr

  override def prettyName: String = if (reverse) "min_by" else "max_by"

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
    BinaryType,
    nullable = false
  )()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    Seq(valuesAttr, orderingsAttr, heapIndicesAttr)

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
                "valueRange" -> "[1, Integer.MAX_VALUE]",
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

  // For heap operations: determines if a should be above b in the heap.
  // For max_by (min-heap): smaller at top. For min_by (max-heap): larger at top.
  private def heapCompare(ordA: Any, ordB: Any): Int =
    if (reverse) -ordering.compare(ordA, ordB) else ordering.compare(ordA, ordB)

  // New value replaces extremum if it would be below it in the heap (i.e., "better").
  private def shouldReplace(newOrd: Any, currentOrd: Any): Boolean =
    heapCompare(newOrd, currentOrd) > 0

  // Binary format: [size: Int][indices: Int*] - little endian, fixed buffer size.
  private def serializeHeapIndices(indices: Array[Int], size: Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(4 + k * 4).order(ByteOrder.LITTLE_ENDIAN)
    buf.putInt(size)
    var i = 0
    while (i < size) {
      buf.putInt(indices(i))
      i += 1
    }
    buf.array()
  }

  private def deserializeHeapIndices(bytes: Array[Byte]): (Array[Int], Int) = {
    val buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    val size = buf.getInt
    val indices = new Array[Int](k)
    var i = 0
    while (i < size) {
      indices(i) = buf.getInt
      i += 1
    }
    (indices, size)
  }

  override def initialize(buffer: InternalRow): Unit = {
    // Pre-allocate arrays of size K for in-place updates.
    buffer.update(mutableAggBufferOffset, new GenericArrayData(new Array[Any](k)))
    buffer.update(mutableAggBufferOffset + 1, new GenericArrayData(new Array[Any](k)))
    buffer.update(mutableAggBufferOffset + 2, serializeHeapIndices(new Array[Int](k), 0))
  }

  private def getArray(arr: ArrayData): Array[Any] =
    arr.asInstanceOf[GenericArrayData].array.asInstanceOf[Array[Any]]

  /** Insert a (value, ord) pair into the heap. Returns new size. */
  private def insertIntoHeap(
      value: Any,
      ord: Any,
      valuesArr: Array[Any],
      orderingsArr: Array[Any],
      heapIndices: Array[Int],
      size: Int): Int = {
    if (size < k) {
      valuesArr(size) = InternalRow.copyValue(value)
      orderingsArr(size) = InternalRow.copyValue(ord)
      heapIndices(size) = size
      siftUpInPlace(heapIndices, size, orderingsArr)
      size + 1
    } else if (shouldReplace(ord, orderingsArr(heapIndices(0)))) {
      val idx = heapIndices(0)
      valuesArr(idx) = InternalRow.copyValue(value)
      orderingsArr(idx) = InternalRow.copyValue(ord)
      siftDownInPlace(heapIndices, 0, size, orderingsArr)
      size
    } else {
      size
    }
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val ord = orderingExpr.eval(inputRow)
    if (ord == null) return

    val value = valueExpr.eval(inputRow)
    val valuesArr = getArray(mutableAggBuffer.getArray(mutableAggBufferOffset))
    val orderingsArr = getArray(mutableAggBuffer.getArray(mutableAggBufferOffset + 1))
    val (heapIndices, heapSize) = deserializeHeapIndices(
      mutableAggBuffer.getBinary(mutableAggBufferOffset + 2))

    val newSize = insertIntoHeap(value, ord, valuesArr, orderingsArr, heapIndices, heapSize)
    if (newSize != heapSize) {
      mutableAggBuffer.update(
        mutableAggBufferOffset + 2, serializeHeapIndices(heapIndices, newSize))
    }
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    val valuesArr = getArray(mutableAggBuffer.getArray(mutableAggBufferOffset))
    val orderingsArr = getArray(mutableAggBuffer.getArray(mutableAggBufferOffset + 1))
    val (heapIndices, heapSize) = deserializeHeapIndices(
      mutableAggBuffer.getBinary(mutableAggBufferOffset + 2))

    val inputValues = inputAggBuffer.getArray(inputAggBufferOffset)
    val inputOrderings = inputAggBuffer.getArray(inputAggBufferOffset + 1)
    val (inputHeapIndices, inputHeapSize) = deserializeHeapIndices(
      inputAggBuffer.getBinary(inputAggBufferOffset + 2))

    var size = heapSize
    for (i <- 0 until inputHeapSize) {
      val idx = inputHeapIndices(i)
      size = insertIntoHeap(
        inputValues.get(idx, valueExpr.dataType),
        inputOrderings.get(idx, orderingExpr.dataType),
        valuesArr, orderingsArr, heapIndices, size)
    }

    mutableAggBuffer.update(mutableAggBufferOffset + 2, serializeHeapIndices(heapIndices, size))
  }

  override def eval(buffer: InternalRow): Any = {
    val valuesArr = getArray(buffer.getArray(mutableAggBufferOffset))
    val orderingsArr = getArray(buffer.getArray(mutableAggBufferOffset + 1))
    val (_, heapSize) = deserializeHeapIndices(buffer.getBinary(mutableAggBufferOffset + 2))

    val elements = new Array[(Any, Any)](heapSize)
    for (i <- 0 until heapSize) {
      elements(i) = (InternalRow.copyValue(valuesArr(i)), orderingsArr(i))
    }

    // Sort in appropriate order:
    val sorted = if (reverse) {
      elements.sortWith { (a, b) => ordering.compare(a._2, b._2) < 0 }
    } else {
      elements.sortWith { (a, b) => ordering.compare(a._2, b._2) > 0 }
    }
    new GenericArrayData(sorted.map(_._1))
  }

  // Heap operations
  private def siftUpInPlace(
    heap: Array[Int],
    pos: Int,
    orderings: Array[Any]): Unit = {
    var current = pos
    while (current > 0) { // while has parent
      val parent = (current - 1) / 2
      if (heapCompare(orderings(heap(current)), orderings(heap(parent))) < 0) {
        swap(heap, current, parent)
        current = parent
      } else {
        return
      }
    }
  }

  private def siftDownInPlace(
    heap: Array[Int],
    pos: Int,
    size: Int,
    orderings: Array[Any]): Unit = {
    var current = pos
    while (2 * current + 1 < size) { // while has children.
      val left = 2 * current + 1
      val right = left + 1
      // Find the child that should be at top of heap.
      val preferred =
        if (right < size && heapCompare(orderings(heap(right)), orderings(heap(left))) < 0) {
          right
        } else {
          left
        }
      // Stop if current should stay above preferred child.
      if (heapCompare(orderings(heap(current)), orderings(heap(preferred))) <= 0) {
        return
      }
      swap(heap, current, preferred)
      current = preferred
    }
  }

  private def swap(heap: Array[Int], i: Int, j: Int): Unit = {
    val tmp = heap(i)
    heap(i) = heap(j)
    heap(j) = tmp
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

/**
 * Expression builder for max_by function.
 * Routes to MaxBy for 2 arguments, MaxMinByK for 3 arguments.
 */
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

/**
 * Expression builder for min_by function.
 * Routes to MinBy for 2 arguments, MaxMinByK (with reverse=true) for 3 arguments.
 */
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
