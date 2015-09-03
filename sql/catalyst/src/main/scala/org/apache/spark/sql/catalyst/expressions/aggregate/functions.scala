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

import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

case class Average(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select avg(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(NumericType, NullType))

  private val resultType = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      DecimalType.bounded(p + 4, s + 4)
    case _ => DoubleType
  }

  private val sumDataType = child.dataType match {
    case _ @ DecimalType.Fixed(p, s) => DecimalType.bounded(p + 10, s)
    case _ => DoubleType
  }

  private val currentSum = AttributeReference("currentSum", sumDataType)()
  private val currentCount = AttributeReference("currentCount", LongType)()

  override val bufferAttributes = currentSum :: currentCount :: Nil

  override val initialValues = Seq(
    /* currentSum = */ Cast(Literal(0), sumDataType),
    /* currentCount = */ Literal(0L)
  )

  override val updateExpressions = Seq(
    /* currentSum = */
    Add(
      currentSum,
      Coalesce(Cast(child, sumDataType) :: Cast(Literal(0), sumDataType) :: Nil)),
    /* currentCount = */ If(IsNull(child), currentCount, currentCount + 1L)
  )

  override val mergeExpressions = Seq(
    /* currentSum = */ currentSum.left + currentSum.right,
    /* currentCount = */ currentCount.left + currentCount.right
  )

  // If all input are nulls, currentCount will be 0 and we will get null after the division.
  override val evaluateExpression = child.dataType match {
    case DecimalType.Fixed(p, s) =>
      // increase the precision and scale to prevent precision loss
      val dt = DecimalType.bounded(p + 14, s + 4)
      Cast(Cast(currentSum, dt) / Cast(currentCount, dt), resultType)
    case _ =>
      Cast(currentSum, resultType) / Cast(currentCount, resultType)
  }
}

case class Count(child: Expression) extends AlgebraicAggregate {
  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = false

  // Return data type.
  override def dataType: DataType = LongType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val currentCount = AttributeReference("currentCount", LongType)()

  override val bufferAttributes = currentCount :: Nil

  override val initialValues = Seq(
    /* currentCount = */ Literal(0L)
  )

  override val updateExpressions = Seq(
    /* currentCount = */ If(IsNull(child), currentCount, currentCount + 1L)
  )

  override val mergeExpressions = Seq(
    /* currentCount = */ currentCount.left + currentCount.right
  )

  override val evaluateExpression = Cast(currentCount, LongType)
}

case class First(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // First is not a deterministic function.
  override def deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val first = AttributeReference("first", child.dataType)()

  override val bufferAttributes = first :: Nil

  override val initialValues = Seq(
    /* first = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* first = */ If(IsNull(first), child, first)
  )

  override val mergeExpressions = Seq(
    /* first = */ If(IsNull(first.left), first.right, first.left)
  )

  override val evaluateExpression = first
}

case class Last(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Last is not a deterministic function.
  override def deterministic: Boolean = false

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val last = AttributeReference("last", child.dataType)()

  override val bufferAttributes = last :: Nil

  override val initialValues = Seq(
    /* last = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* last = */ If(IsNull(child), last, child)
  )

  override val mergeExpressions = Seq(
    /* last = */ If(IsNull(last.right), last.left, last.right)
  )

  override val evaluateExpression = last
}

case class Max(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val max = AttributeReference("max", child.dataType)()

  override val bufferAttributes = max :: Nil

  override val initialValues = Seq(
    /* max = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* max = */ If(IsNull(child), max, If(IsNull(max), child, Greatest(Seq(max, child))))
  )

  override val mergeExpressions = {
    val greatest = Greatest(Seq(max.left, max.right))
    Seq(
      /* max = */ If(IsNull(max.right), max.left, If(IsNull(max.left), max.right, greatest))
    )
  }

  override val evaluateExpression = max
}

case class Min(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = child.dataType

  // Expected input data type.
  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  private val min = AttributeReference("min", child.dataType)()

  override val bufferAttributes = min :: Nil

  override val initialValues = Seq(
    /* min = */ Literal.create(null, child.dataType)
  )

  override val updateExpressions = Seq(
    /* min = */ If(IsNull(child), min, If(IsNull(min), child, Least(Seq(min, child))))
  )

  override val mergeExpressions = {
    val least = Least(Seq(min.left, min.right))
    Seq(
      /* min = */ If(IsNull(min.right), min.left, If(IsNull(min.left), min.right, least))
    )
  }

  override val evaluateExpression = min
}

case class Sum(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select sum(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(LongType, DoubleType, DecimalType, NullType))

  private val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    // TODO: Remove this line once we remove the NullType from inputTypes.
    case NullType => IntegerType
    case _ => child.dataType
  }

  private val sumDataType = resultType

  private val currentSum = AttributeReference("currentSum", sumDataType)()

  private val zero = Cast(Literal(0), sumDataType)

  override val bufferAttributes = currentSum :: Nil

  override val initialValues = Seq(
    /* currentSum = */ Literal.create(null, sumDataType)
  )

  override val updateExpressions = Seq(
    /* currentSum = */
    Coalesce(Seq(Add(Coalesce(Seq(currentSum, zero)), Cast(child, sumDataType)), currentSum))
  )

  override val mergeExpressions = {
    val add = Add(Coalesce(Seq(currentSum.left, zero)), Cast(currentSum.right, sumDataType))
    Seq(
      /* currentSum = */
      Coalesce(Seq(add, currentSum.left))
    )
  }

  override val evaluateExpression = Cast(currentSum, resultType)
}

case class Corr(left: Expression, right: Expression) extends AggregateFunction2 {

  def children: Seq[Expression] = Seq(left, right)

  def nullable: Boolean = false

  def dataType: DataType = DoubleType

  def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  def bufferSchema: StructType = StructType.fromAttributes(bufferAttributes)

  def cloneBufferAttributes: Seq[Attribute] = bufferAttributes.map(_.newInstance())

  val bufferAttributes: Seq[AttributeReference] = Seq(
    AttributeReference("xAvg", DoubleType)(),
    AttributeReference("yAvg", DoubleType)(),
    AttributeReference("Ck", DoubleType)(),
    AttributeReference("MkX", DoubleType)(),
    AttributeReference("MkY", DoubleType)(),
    AttributeReference("count", LongType)())

  override def initialize(buffer: MutableRow): Unit = {
    (0 until 5).map(idx => buffer.setDouble(mutableBufferOffset + idx, 0.0))
    buffer.setLong(mutableBufferOffset + 5, 0L)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val x = left.eval(input).asInstanceOf[Double]
    val y = right.eval(input).asInstanceOf[Double]

    var xAvg = buffer.getDouble(mutableBufferOffset)
    var yAvg = buffer.getDouble(mutableBufferOffset + 1)
    var Ck = buffer.getDouble(mutableBufferOffset + 2)
    var MkX = buffer.getDouble(mutableBufferOffset + 3)
    var MkY = buffer.getDouble(mutableBufferOffset + 4)
    var count = buffer.getLong(mutableBufferOffset + 5)

    val deltaX = x - xAvg
    val deltaY = y - yAvg
    count += 1
    xAvg += deltaX / count
    yAvg += deltaY / count
    Ck += deltaX * (y - yAvg)
    MkX += deltaX * (x - xAvg)
    MkY += deltaY * (y - yAvg)

    buffer.setDouble(mutableBufferOffset, xAvg)
    buffer.setDouble(mutableBufferOffset + 1, yAvg)
    buffer.setDouble(mutableBufferOffset + 2, Ck)
    buffer.setDouble(mutableBufferOffset + 3, MkX)
    buffer.setDouble(mutableBufferOffset + 4, MkY)
    buffer.setLong(mutableBufferOffset + 5, count)
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val count2 = buffer2.getLong(inputBufferOffset + 5)

    if (count2 > 0) {
      var xAvg = buffer1.getDouble(mutableBufferOffset)
      var yAvg = buffer1.getDouble(mutableBufferOffset + 1)
      var Ck = buffer1.getDouble(mutableBufferOffset + 2)
      var MkX = buffer1.getDouble(mutableBufferOffset + 3)
      var MkY = buffer1.getDouble(mutableBufferOffset + 4)
      var count = buffer1.getLong(mutableBufferOffset + 5)

      val xAvg2 = buffer2.getDouble(inputBufferOffset)
      val yAvg2 = buffer2.getDouble(inputBufferOffset + 1)
      val Ck2 = buffer2.getDouble(inputBufferOffset + 2)
      val MkX2 = buffer2.getDouble(inputBufferOffset + 3)
      val MkY2 = buffer2.getDouble(inputBufferOffset + 4)

      val totalCount = count + count2
      val deltaX = xAvg - xAvg2
      val deltaY = yAvg - yAvg2
      Ck += Ck2 + deltaX * deltaY * count / totalCount * count2
      xAvg = (xAvg * count + xAvg2 * count2) / totalCount
      yAvg = (yAvg * count + yAvg2 * count2) / totalCount
      MkX += MkX2 + deltaX * deltaX * count / totalCount * count2
      MkY += MkY2 + deltaY * deltaY * count / totalCount * count2
      count = totalCount

      buffer1.setDouble(mutableBufferOffset, xAvg)
      buffer1.setDouble(mutableBufferOffset + 1, yAvg)
      buffer1.setDouble(mutableBufferOffset + 2, Ck)
      buffer1.setDouble(mutableBufferOffset + 3, MkX)
      buffer1.setDouble(mutableBufferOffset + 4, MkY)
      buffer1.setLong(mutableBufferOffset + 5, count)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val Ck = buffer.getDouble(mutableBufferOffset + 2)
    val MkX = buffer.getDouble(mutableBufferOffset + 3)
    val MkY = buffer.getDouble(mutableBufferOffset + 4)
    Ck / math.sqrt(MkX * MkY)
  }
}
