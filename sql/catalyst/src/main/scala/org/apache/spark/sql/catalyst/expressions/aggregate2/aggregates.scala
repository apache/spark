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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{expressions, InternalRow}
import org.apache.spark.sql.catalyst.trees.{LeafNode, UnaryNode}
import org.apache.spark.sql.types._

private[sql] sealed trait AggregateMode

private[sql] case object Partial extends AggregateMode

private[sql] case object PartialMerge extends AggregateMode

private[sql] case object Final extends AggregateMode

private[sql] case object Complete extends AggregateMode

case object NoOp extends Expression {
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = ???
  override def dataType: DataType = NullType
  override def children: Seq[Expression] = Nil
}

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

  def bufferSchema: StructType = aggregateFunction.bufferSchema
  def bufferAttributes: Seq[Attribute] = aggregateFunction.bufferAttributes
}

abstract class AggregateFunction2
  extends Expression {

  self: Product =>

  var bufferOffset: Int = 0

  def withBufferOffset(newBufferOffset: Int): AggregateFunction2 = {
    bufferOffset = newBufferOffset
    this
  }

  /** The schema of the aggregation buffer. */
  def bufferSchema: StructType

  /** Attributes of fields in bufferSchema. */
  def bufferAttributes: Seq[Attribute]

  def initialize(buffer: MutableRow): Unit

  def update(buffer: MutableRow, input: InternalRow): Unit

  def merge(buffer1: MutableRow, buffer2: InternalRow): Unit

  override def eval(buffer: InternalRow = null): Any
}

case class MyDoubleSum(child: Expression) extends AggregateFunction2 {
  override val bufferSchema: StructType =
    StructType(StructField("currentSum", DoubleType, true) :: Nil)

  override val bufferAttributes: Seq[Attribute] = bufferSchema.toAttributes

  override def initialize(buffer: MutableRow): Unit = {
    buffer.update(bufferOffset, null)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val inputValue = child.eval(input)
    if (inputValue != null) {
      if (buffer.isNullAt(bufferOffset) == null) {
        buffer.setDouble(bufferOffset, inputValue.asInstanceOf[Double])
      } else {
        val currentSum = buffer.getDouble(bufferOffset)
        buffer.setDouble(bufferOffset, currentSum + inputValue.asInstanceOf[Double])
      }
    }
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    if (!buffer2.isNullAt(bufferOffset)) {
      if (buffer1.isNullAt(bufferOffset)) {
        buffer1.setDouble(bufferOffset, buffer2.getDouble(bufferOffset))
      } else {
        val currentSum = buffer1.getDouble(bufferOffset)
        buffer1.setDouble(bufferOffset, currentSum + buffer2.getDouble(bufferOffset))
      }
    }
  }

  override def eval(buffer: InternalRow = null): Any = {
    if (buffer.isNullAt(bufferOffset)) {
      null
    } else {
      buffer.getDouble(bufferOffset)
    }
  }

  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def children: Seq[Expression] = child :: Nil
}

/**
 * A helper class for aggregate functions that can be implemented in terms of catalyst expressions.
 */
abstract class AlgebraicAggregate extends AggregateFunction2 with Serializable {
  self: Product =>

  val initialValues: Seq[Expression]
  val updateExpressions: Seq[Expression]
  val mergeExpressions: Seq[Expression]
  val evaluateExpression: Expression

  /** Must be filled in by the executors */
  var inputSchema: Seq[Attribute] = _

  override def withBufferOffset(newBufferOffset: Int): AlgebraicAggregate = {
    bufferOffset = newBufferOffset
    this
  }

  def offsetExpressions: Seq[Attribute] = Seq.fill(bufferOffset)(AttributeReference("offset", NullType)())

  lazy val rightBufferSchema = bufferAttributes.map(_.newInstance())
  implicit class RichAttribute(a: AttributeReference) {
    def left = a
    def right = rightBufferSchema(bufferAttributes.indexOf(a))
  }

  /** An AlgebraicAggregate's bufferSchema is derived from bufferAttributes. */
  override def bufferSchema: StructType = StructType.fromAttributes(bufferAttributes)

  override def initialize(buffer: MutableRow): Unit = {
    var i = 0
    while (i < bufferAttributes.size) {
      buffer(i + bufferOffset) = initialValues(i).eval()
      i += 1
    }
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's update should not be called directly")
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's merge should not be called directly")
  }

  override def eval(buffer: InternalRow): Any = {
    throw new UnsupportedOperationException(
      "AlgebraicAggregate's eval should not be called directly")
  }
}

case class Average(child: Expression) extends AlgebraicAggregate {
  val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType(precision + 4, scale + 4)
    case DecimalType.Unlimited => DecimalType.Unlimited
    case _ => DoubleType
  }

  val sumDataType = child.dataType match {
    case _ @ DecimalType() => DecimalType.Unlimited
    case _ => DoubleType
  }

  val currentSum = AttributeReference("currentSum", sumDataType)()
  val currentCount = AttributeReference("currentCount", LongType)()

  override val bufferAttributes = currentSum :: currentCount :: Nil

  val initialValues = Seq(
    /* currentSum = */ Cast(Literal(0), sumDataType),
    /* currentCount = */ Literal(0L)
  )

  val updateExpressions = Seq(
    /* currentSum = */
      Add(
        currentSum,
        Coalesce(Cast(child, sumDataType) :: Cast(Literal(0), sumDataType) :: Nil)),
    /* currentCount = */ If(IsNull(child), currentCount, currentCount + 1L)
  )

  val mergeExpressions = Seq(
    /* currentSum = */ currentSum.left + currentSum.right,
    /* currentCount = */ currentCount.left + currentCount.right
  )

  val evaluateExpression = Cast(currentSum, resultType) / Cast(currentCount, resultType)

  override def nullable: Boolean = true
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = child :: Nil
}