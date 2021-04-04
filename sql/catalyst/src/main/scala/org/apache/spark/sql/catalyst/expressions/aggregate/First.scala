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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult.TypeCheckSuccess
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.types._

/**
 * Returns the first value of `child` for a group of rows. If the first value of `child`
 * is `null`, it returns `null` (respecting nulls). Even if [[First]] is used on an already
 * sorted column, if we do partial aggregation and final aggregation (when mergeExpression
 * is used) its result will not be deterministic (unless the input table is sorted and has
 * a single partition, and we use a single reducer to do the aggregation.).
 */
@ExpressionDescription(
  usage = """
    _FUNC_(expr[, isIgnoreNull]) - Returns the first value of `expr` for a group of rows.
      If `isIgnoreNull` is true, returns only non-null values.""",
  examples = """
    Examples:
      > SELECT _FUNC_(col) FROM VALUES (10), (5), (20) AS tab(col);
       10
      > SELECT _FUNC_(col) FROM VALUES (NULL), (5), (20) AS tab(col);
       NULL
      > SELECT _FUNC_(col, true) FROM VALUES (NULL), (5), (20) AS tab(col);
       5
  """,
  note = """
    The function is non-deterministic because its results depends on the order of the rows
    which may be non-deterministic after a shuffle.
  """,
  group = "agg_funcs",
  since = "2.0.0")
case class First(
    child: Expression,
    ignoreNulls: Boolean,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[FirstLast.State] with ExpectsInputTypes
    with UnaryLike[Expression]{

  def this(child: Expression) = this(child, false)

  def this(child: Expression, ignoreNullsExpr: Expression) = {
    this(child, FirstLast.validateIgnoreNullExpr(ignoreNullsExpr, "first"))
  }

  override def nullable: Boolean = true

  // First is not a deterministic function.
  override lazy val deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType, BooleanType)

  override def checkInputDataTypes(): TypeCheckResult = {
    val defaultCheck = super.checkInputDataTypes()
    if (defaultCheck.isFailure) {
      defaultCheck
    } else {
      TypeCheckSuccess
    }
  }

  override def createAggregationBuffer(): FirstLast.State = {
    FirstLast.State(element = null, valueSet = false)
  }

  override def update(buffer: FirstLast.State, input: InternalRow): FirstLast.State = {
    lazy val value = child.eval(input.copy())
    if (ignoreNulls) {
      if (!buffer.valueSet && value != null) {
        buffer.element = value
        buffer.valueSet = true
      }
    } else {
      if (!buffer.valueSet) {
        buffer.element = value
        buffer.valueSet = true
      }
    }
    buffer
  }

  override def merge(buffer: FirstLast.State, input: FirstLast.State): FirstLast.State = {
    if (!buffer.valueSet) {
      input
    } else {
      buffer
    }
  }

  override def eval(buffer: FirstLast.State): Any = buffer.element

  override def toString: String = s"$prettyName($child)${if (ignoreNulls) " ignore nulls"}"

  override def serialize(buffer: FirstLast.State): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val dataStream = new ObjectOutputStream(byteStream)
    dataStream.writeObject(buffer)
    byteStream.toByteArray
  }

  override def deserialize(storageFormat: Array[Byte]): FirstLast.State = {
    val byteStream = new ByteArrayInputStream(storageFormat)
    val dataStream = new ObjectInputStream(byteStream)
    dataStream.readObject().asInstanceOf[FirstLast.State]
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)
}

object FirstLast {

  case class State(var element: Any, var valueSet: Boolean)

  def validateIgnoreNullExpr(exp: Expression, funcName: String): Boolean = exp match {
    case Literal(b: Boolean, BooleanType) => b
    case _ => throw new AnalysisException(
      s"The second argument in $funcName should be a boolean literal.")
  }
}
