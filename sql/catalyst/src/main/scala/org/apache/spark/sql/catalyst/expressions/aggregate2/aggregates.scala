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

package org.apache.spark.sql.catalyst.expressions.aggregate2

import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.trees.{LeafNode, UnaryNode}
import org.apache.spark.sql.types._

private[sql] sealed trait AggregateMode

private[sql] case object Partial extends AggregateMode

private[sql] case object PartialMerge extends AggregateMode

private[sql] case object Final extends AggregateMode

private[sql] case object Complete extends AggregateMode

/**
 * A container of a Aggregate Function, Aggregate Mode, and a field (`isDistinct`) indicating
 * if DISTINCT keyword is specified for this function.
 * @param aggregateFunction
 * @param mode
 * @param isDistinct
 */
private[sql] case class AggregateExpression2(
    aggregateFunction: AggregateFunction2,
    mode: AggregateMode,
    isDistinct: Boolean) extends Expression {

  override def children: Seq[Expression] = aggregateFunction :: Nil

  override def dataType: DataType = aggregateFunction.dataType
  override def foldable: Boolean = aggregateFunction.foldable
  override def nullable: Boolean = aggregateFunction.nullable

  override def toString: String = s"(${aggregateFunction}2,mode=$mode,isDistinct=$isDistinct)"

  override def eval(input: InternalRow = null): Any =
    throw new TreeNodeException(this, s"No function to evaluate expression. type: ${this.nodeName}")
}

abstract class AggregateFunction2
  extends Expression {

  self: Product =>

  var bufferOffset: Int = 0

  def withBufferOffset(newBufferOffset: Int): AggregateFunction2 = {
    bufferOffset = newBufferOffset
    this
  }

  def bufferValueDataTypes: StructType

  def initialBufferValues: Array[Any]

  def initialize(buffer: MutableRow): Unit

  def updateBuffer(buffer: MutableRow, bufferValues: Array[Any]): Unit = {
    var i = 0
    println("bufferOffset in average2 " + bufferOffset)
    while (i < bufferValues.length) {
      buffer.update(bufferOffset + i, bufferValues(i))
      i += 1
    }
  }

  def update(buffer: MutableRow, input: InternalRow): Unit

  def merge(buffer1: MutableRow, buffer2: InternalRow): Unit

  override def eval(buffer: InternalRow = null): Any
}

case class Average(child: Expression)
  extends AggregateFunction2 with UnaryNode[Expression] {

  override def nullable: Boolean = child.nullable

  override def bufferValueDataTypes: StructType = child match {
    case e @ DecimalType() =>
      StructType(
        StructField("Sum", DecimalType.Unlimited) ::
        StructField("Count", LongType) :: Nil)
    case _ =>
      StructType(
        StructField("Sum", DoubleType) ::
        StructField("Count", LongType) :: Nil)
  }

  override def dataType: DataType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 4, scale + 4)
    case DecimalType.Unlimited => DecimalType.Unlimited
    case _ => DoubleType
  }

  override def initialBufferValues: Array[Any] = {
    Array(
      Cast(Literal(0), bufferValueDataTypes("Sum").dataType).eval(null), // Sum
      0L)                                                                // Count
  }

  override def initialize(buffer: MutableRow): Unit =
    updateBuffer(buffer, initialBufferValues)

  private val inputLiteral =
    MutableLiteral(null, child.dataType)
  private val bufferedSum =
    MutableLiteral(null, bufferValueDataTypes("Sum").dataType)
  private val bufferedCount = MutableLiteral(null, LongType)
  private val updateSum =
    Add(Cast(inputLiteral, bufferValueDataTypes("Sum").dataType), bufferedSum)
  private val inputBufferedSum =
    MutableLiteral(null, bufferValueDataTypes("Sum").dataType)
  private val mergeSum = Add(inputBufferedSum, bufferedSum)
  private val evaluateAvg =
    Cast(Divide(bufferedSum, Cast(bufferedCount, bufferValueDataTypes("Sum").dataType)), dataType)

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val newInput = child.eval(input)
    println("newInput " + newInput)
    if (newInput != null) {
      inputLiteral.value = newInput
      bufferedSum.value = buffer(bufferOffset)
      val newSum = updateSum.eval(null)
      val newCount = buffer.getLong(bufferOffset + 1) + 1
      buffer.update(bufferOffset, newSum)
      buffer.update(bufferOffset + 1, newCount)
    }
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    if (buffer2(bufferOffset + 1) != 0L) {
      inputBufferedSum.value = buffer2(bufferOffset)
      bufferedSum.value = buffer1(bufferOffset)
      val newSum = mergeSum.eval(null)
      val newCount =
        buffer1.getLong(bufferOffset + 1) + buffer2.getLong(bufferOffset + 1)
      buffer1.update(bufferOffset, newSum)
      buffer1.update(bufferOffset + 1, newCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    if (buffer(bufferOffset + 1) == 0L) {
      null
    } else {
      bufferedSum.value = buffer(bufferOffset)
      bufferedCount.value = buffer.getLong(bufferOffset + 1)
      evaluateAvg.eval(null)
    }
  }
}
