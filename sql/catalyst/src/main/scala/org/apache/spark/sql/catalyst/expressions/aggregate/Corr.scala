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

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.dsl.expressions._
=======
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

/**
 * Compute Pearson correlation between two expressions.
 * When applied on empty data (i.e., count is zero), it returns NULL.
 *
 * Definition of Pearson correlation can be found at
 * http://en.wikipedia.org/wiki/Pearson_product-moment_correlation_coefficient
 */
<<<<<<< HEAD
case class Corr(x: Expression, y: Expression) extends DeclarativeAggregate {
=======
case class Corr(
    left: Expression,
    right: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends ImperativeAggregate {

  def this(left: Expression, right: Expression) =
    this(left, right, mutableAggBufferOffset = 0, inputAggBufferOffset = 0)

  override def children: Seq[Expression] = Seq(left, right)

  override def nullable: Boolean = false
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3

  override def children: Seq[Expression] = Seq(x, y)
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType, DoubleType)

<<<<<<< HEAD
  protected val n = AttributeReference("n", DoubleType, nullable = false)()
  protected val xAvg = AttributeReference("xAvg", DoubleType, nullable = false)()
  protected val yAvg = AttributeReference("yAvg", DoubleType, nullable = false)()
  protected val ck = AttributeReference("ck", DoubleType, nullable = false)()
  protected val xMk = AttributeReference("xMk", DoubleType, nullable = false)()
  protected val yMk = AttributeReference("yMk", DoubleType, nullable = false)()

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(n, xAvg, yAvg, ck, xMk, yMk)

  override val initialValues: Seq[Expression] = Array.fill(6)(Literal(0.0))

  override val updateExpressions: Seq[Expression] = {
    val newN = n + Literal(1.0)
    val dx = x - xAvg
    val dxN = dx / newN
    val dy = y - yAvg
    val dyN = dy / newN
    val newXAvg = xAvg + dxN
    val newYAvg = yAvg + dyN
    val newCk = ck + dx * (y - newYAvg)
    val newXMk = xMk + dx * (x - newXAvg)
    val newYMk = yMk + dy * (y - newYAvg)

    val isNull = IsNull(x) || IsNull(y)
    Seq(
      If(isNull, n, newN),
      If(isNull, xAvg, newXAvg),
      If(isNull, yAvg, newYAvg),
      If(isNull, ck, newCk),
      If(isNull, xMk, newXMk),
      If(isNull, yMk, newYMk)
    )
=======
  override def checkInputDataTypes(): TypeCheckResult = {
    if (left.dataType.isInstanceOf[DoubleType] && right.dataType.isInstanceOf[DoubleType]) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure(
        s"corr requires that both arguments are double type, " +
          s"not (${left.dataType}, ${right.dataType}).")
    }
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = {
    aggBufferAttributes.map(_.newInstance())
  }

  override val aggBufferAttributes: Seq[AttributeReference] = Seq(
    AttributeReference("xAvg", DoubleType)(),
    AttributeReference("yAvg", DoubleType)(),
    AttributeReference("Ck", DoubleType)(),
    AttributeReference("MkX", DoubleType)(),
    AttributeReference("MkY", DoubleType)(),
    AttributeReference("count", LongType)())

  // Local cache of mutableAggBufferOffset(s) that will be used in update and merge
  private[this] val mutableAggBufferOffsetPlus1 = mutableAggBufferOffset + 1
  private[this] val mutableAggBufferOffsetPlus2 = mutableAggBufferOffset + 2
  private[this] val mutableAggBufferOffsetPlus3 = mutableAggBufferOffset + 3
  private[this] val mutableAggBufferOffsetPlus4 = mutableAggBufferOffset + 4
  private[this] val mutableAggBufferOffsetPlus5 = mutableAggBufferOffset + 5

  // Local cache of inputAggBufferOffset(s) that will be used in update and merge
  private[this] val inputAggBufferOffsetPlus1 = inputAggBufferOffset + 1
  private[this] val inputAggBufferOffsetPlus2 = inputAggBufferOffset + 2
  private[this] val inputAggBufferOffsetPlus3 = inputAggBufferOffset + 3
  private[this] val inputAggBufferOffsetPlus4 = inputAggBufferOffset + 4
  private[this] val inputAggBufferOffsetPlus5 = inputAggBufferOffset + 5

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def initialize(buffer: MutableRow): Unit = {
    buffer.setDouble(mutableAggBufferOffset, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus1, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus2, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus3, 0.0)
    buffer.setDouble(mutableAggBufferOffsetPlus4, 0.0)
    buffer.setLong(mutableAggBufferOffsetPlus5, 0L)
>>>>>>> 022e06d18471bf54954846c815c8a3666aef9fc3
  }

  override val mergeExpressions: Seq[Expression] = {

    val n1 = n.left
    val n2 = n.right
    val newN = n1 + n2
    val dx = xAvg.right - xAvg.left
    val dxN = If(newN === Literal(0.0), Literal(0.0), dx / newN)
    val dy = yAvg.right - yAvg.left
    val dyN = If(newN === Literal(0.0), Literal(0.0), dy / newN)
    val newXAvg = xAvg.left + dxN * n2
    val newYAvg = yAvg.left + dyN * n2
    val newCk = ck.left + ck.right + dx * dyN * n1 * n2
    val newXMk = xMk.left + xMk.right + dx * dxN * n1 * n2
    val newYMk = yMk.left + yMk.right + dy * dyN * n1 * n2

    Seq(newN, newXAvg, newYAvg, newCk, newXMk, newYMk)
  }

  override val evaluateExpression: Expression = {
    If(n === Literal(0.0), Literal.create(null, DoubleType),
      If(n === Literal(1.0), Literal(Double.NaN),
        ck / Sqrt(xMk * yMk)))
  }

  override def prettyName: String = "corr"
}
