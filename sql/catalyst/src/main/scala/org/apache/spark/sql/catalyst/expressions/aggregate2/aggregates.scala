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
  override def eval(input: expressions.InternalRow): Any = ???
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

/**
 * A helper class for aggregate functions that can be implemented in terms of catalyst expressions.
 */
abstract class AlgebraicAggregate extends AggregateFunction2 with Serializable{
  self: Product =>

  val initialValues: Seq[Expression]
  val updateExpressions: Seq[Expression]
  val mergeExpressions: Seq[Expression]
  val evaluateExpression: Expression

  /** Must be filled in by the executors */
  var inputSchema: Seq[Attribute] = _

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

  lazy val boundUpdateExpressions = {
    val updateSchema = inputSchema ++ offsetExpressions ++ bufferAttributes
    val bound = updateExpressions.map(BindReferences.bindReference(_, updateSchema)).toArray
    println(s"update: ${updateExpressions.mkString(",")}")
    println(s"update: ${bound.mkString(",")}")
    bound
  }

  val joinedRow = new JoinedRow
  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    var i = 0
    while (i < bufferAttributes.size) {
      buffer(i + bufferOffset) = boundUpdateExpressions(i).eval(joinedRow(input, buffer))
      i += 1
    }
  }

  lazy val boundMergeExpressions = {
    val mergeSchema = offsetExpressions ++ bufferAttributes ++ offsetExpressions ++ rightBufferSchema
    mergeExpressions.map(BindReferences.bindReference(_, mergeSchema)).toArray
  }
  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    var i = 0
    println(s"Merging: $buffer1 $buffer2 with ${boundMergeExpressions.mkString(",")}")
    joinedRow(buffer1, buffer2)
    while (i < bufferAttributes.size) {
      println(s"$i + $bufferOffset: ${boundMergeExpressions(i).eval(joinedRow)}")
      buffer1(i + bufferOffset) = boundMergeExpressions(i).eval(joinedRow)
      i += 1
    }
  }

  lazy val boundEvaluateExpression =
    BindReferences.bindReference(evaluateExpression, offsetExpressions ++ bufferAttributes)
  override def eval(buffer: InternalRow): Any = {
    println(s"eval: $buffer")
    val res = boundEvaluateExpression.eval(buffer)
    println(s"eval: $buffer with $boundEvaluateExpression => $res")
    res
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

  override def nullable: Boolean = false
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = child :: Nil
}