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
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.{DataTypeMismatch, TypeCheckSuccess}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.catalyst.trees.{BinaryLike, UnaryLike}
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLExpr
import org.apache.spark.sql.errors.DataTypeErrors.{toSQLId, toSQLType}
import org.apache.spark.sql.types.{AbstractDataType, AnyDataType, ArrayType, BooleanType, DataType}
import org.apache.spark.util.collection.OpenHashMap

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
    _FUNC_(col[, deterministic]) - Returns the most frequent value for the values within `col`. NULL values are ignored. If all the values are NULL, or there are 0 rows, returns NULL.
      When multiple values have the same greatest frequency then either any of values is returned if `deterministic` is false or is not defined, or the lowest value is returned if `deterministic` is true.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10) AS tab(col);
       10
      > SELECT _FUNC_(col) FROM VALUES (INTERVAL '0' MONTH), (INTERVAL '10' MONTH), (INTERVAL '10' MONTH) AS tab(col);
       0-10
      > SELECT _FUNC_(col) FROM VALUES (0), (10), (10), (null), (null), (null) AS tab(col);
       10
      > SELECT _FUNC_(col, false) FROM VALUES (-10), (0), (10) AS tab(col);
       0
      > SELECT _FUNC_(col, true) FROM VALUES (-10), (0), (10) AS tab(col);
       -10
  """,
  group = "agg_funcs",
  since = "3.4.0")
// scalastyle:on line.size.limit
case class Mode(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0,
    deterministicExpr: Expression = Literal.FalseLiteral)
  extends TypedAggregateWithHashMapAsBuffer with ImplicitCastInputTypes
    with BinaryLike[Expression] {

  def this(child: Expression) = this(child, 0, 0)

  def this(child: Expression, deterministicExpr: Expression) = {
    this(child, 0, 0, deterministicExpr)
  }

  @transient
  protected lazy val deterministicResult = deterministicExpr.eval().asInstanceOf[Boolean]

  override def left: Expression = child

  override def right: Expression = deterministicExpr

  // Returns null for empty inputs
  override def nullable: Boolean = true

  override def dataType: DataType = child.dataType

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      return defaultCheck
    }
    if (!deterministicExpr.foldable) {
      DataTypeMismatch(
        errorSubClass = "NON_FOLDABLE_INPUT",
        messageParameters = Map(
          "inputName" -> toSQLId("deterministic"),
          "inputType" -> toSQLType(deterministicExpr.dataType),
          "inputExpr" -> toSQLExpr(deterministicExpr)
        )
      )
    } else if (deterministicExpr.eval() == null) {
      DataTypeMismatch(
        errorSubClass = "UNEXPECTED_NULL",
        messageParameters = Map("exprName" -> toSQLId("deterministic")))
    } else {
      TypeCheckSuccess
    }
  }

  override def prettyName: String = "mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.isEmpty) {
      return null
    }

    (if (deterministicResult) {
      // When deterministic result is rquired but multiple keys have the same greatest frequency
      // then let's select the lowest.
      val defaultKeyOrdering =
        PhysicalDataType.ordering(child.dataType).asInstanceOf[Ordering[AnyRef]]
      val ordering = Ordering.Tuple2(Ordering.Long, defaultKeyOrdering.reverse)
      buffer.maxBy { case (key, count) => (count, key) }(ordering)
    } else {
      buffer.maxBy(_._2)
    })._1
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): Mode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): Mode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression =
    copy(child = newLeft, deterministicExpr = newRight)
}

/**
 * Mode in Pandas' fashion. This expression is dedicated only for Pandas API on Spark.
 * It has two main difference from `Mode`:
 * 1, it accepts NULLs when `ignoreNA` is False;
 * 2, it returns all the modes for a multimodal dataset;
 */
case class PandasMode(
    child: Expression,
    ignoreNA: Boolean = true,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0) extends TypedAggregateWithHashMapAsBuffer
  with ImplicitCastInputTypes with UnaryLike[Expression] {

  def this(child: Expression) = this(child, true, 0, 0)

  // Returns empty array for empty inputs
  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(child.dataType, containsNull = !ignoreNA)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def prettyName: String = "pandas_mode"

  override def update(
      buffer: OpenHashMap[AnyRef, Long],
      input: InternalRow): OpenHashMap[AnyRef, Long] = {
    val key = child.eval(input)

    if (key != null) {
      buffer.changeValue(InternalRow.copyValue(key).asInstanceOf[AnyRef], 1L, _ + 1L)
    } else if (!ignoreNA) {
      buffer.changeValue(null, 1L, _ + 1L)
    }
    buffer
  }

  override def merge(
      buffer: OpenHashMap[AnyRef, Long],
      other: OpenHashMap[AnyRef, Long]): OpenHashMap[AnyRef, Long] = {
    other.foreach { case (key, count) =>
      buffer.changeValue(key, count, _ + count)
    }
    buffer
  }

  override def eval(buffer: OpenHashMap[AnyRef, Long]): Any = {
    if (buffer.isEmpty) {
      return new GenericArrayData(Array.empty)
    }

    val modes = collection.mutable.ArrayBuffer.empty[AnyRef]
    var maxCount = -1L
    val iter = buffer.iterator
    while (iter.hasNext) {
      val (key, count) = iter.next()
      if (maxCount < count) {
        modes.clear()
        modes.append(key)
        maxCount = count
      } else if (maxCount == count) {
        modes.append(key)
      }
    }
    new GenericArrayData(modes)
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): PandasMode =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): PandasMode =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
