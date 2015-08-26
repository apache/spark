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

import scala.collection.mutable

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst._
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

/**
 * Calculate the approximate quantile.
 * @param child
 * @param quantile
 * @param epsilon
 *
 */
case class ApproxQuantile(
    child: Expression,
    quantile: Double,
    epsilon: Double = 0.05,
    compressThreshold: Int = 1000) extends AggregateFunction2 {

  def children: Seq[Expression] = Seq(child)

  def nullable: Boolean = false

  def dataType: DataType = DoubleType

  def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  def bufferSchema: StructType = StructType.fromAttributes(bufferAttributes)

  def cloneBufferAttributes: Seq[Attribute] = bufferAttributes.map(_.newInstance())

  private[this] val innerStruct =
    StructType(
      StructField("f1", DoubleType, false) ::
      StructField("f2", IntegerType, false) ::
      StructField("f3", IntegerType, false) :: Nil)

  val bufferAttributes: Seq[AttributeReference] = Seq(
    AttributeReference("sampled", ArrayType(innerStruct, false))(),
    AttributeReference("count", LongType)())

  private[this] def getConstant(count: Long): Double = 2 * epsilon * count

  private[this] def compress(sampled: mutable.Buffer[InternalRow], count: Long): Unit = {
    var i = 0
    while (i < sampled.size - 1) {
      val sample1 = sampled(i)
      val sample2 = sampled(i + 1)
      if (sample1.getInt(1) + sample2.getInt(1) +
        sample2.getInt(2) < math.floor(getConstant(count))) {
        sampled.update(i + 1,
          InternalRow(sample2.getDouble(0), sample1.getInt(1) + sample2.getInt(1),
            sample2.getInt(2)))
        sampled.remove(i)
      }
      i += 1
    }
  }

  private[this] def query(sampled: Array[InternalRow], count: Long): Double = {
    val rank = (quantile * count).toInt
    var minRank = 0
    var i = 1
    while (i < sampled.size) {
      val curSample = sampled(i)
      val prevSample = sampled(i - 1)
      minRank += prevSample.getInt(1)
      if (minRank + curSample.getInt(1) + curSample.getInt(1) > rank + getConstant(count)) {
        return prevSample.getDouble(0)
      }
      i += 1
    }
    return sampled.last.getDouble(0)
  }

  override def initialize(buffer: MutableRow): Unit = {
    buffer.update(mutableBufferOffset,
      new GenericArrayData(Array[InternalRow]().asInstanceOf[Array[Any]]))
    buffer.setLong(mutableBufferOffset + 1, 0L)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val v = child.eval(input)
    if (v != null) {
      val inputValue = v.asInstanceOf[Double]

      val sampled: mutable.Buffer[InternalRow] = new mutable.ArrayBuffer[InternalRow]()
      buffer.getArray(mutableBufferOffset).toArray[InternalRow](innerStruct).copyToBuffer(sampled)

      var count: Long = buffer.getLong(mutableBufferOffset + 1)

      var idx: Int = sampled.indexWhere(_.getDouble(0) > inputValue)
      if (idx == -1) {
        idx = sampled.size
      }

      val delta: Int = if (idx == 0 || idx == sampled.size) {
        0
      } else {
        math.floor(getConstant(count)).toInt
      }

      val tuple = InternalRow(inputValue, 1, delta)
      sampled.insert(idx, tuple)
      count += 1

      if (sampled.size > compressThreshold) {
        compress(sampled, count)
      }

      buffer.update(mutableBufferOffset, new GenericArrayData(sampled.toArray))
      buffer.setLong(mutableBufferOffset + 1, count)
    }
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val otherCount = buffer2.getLong(inputBufferOffset + 1)
    var count: Long = buffer1.getLong(mutableBufferOffset + 1)

    val otherSampled = new mutable.ArrayBuffer[InternalRow]()
    buffer2.getArray(inputBufferOffset).toArray[InternalRow](innerStruct).copyToBuffer(otherSampled)
    val sampled: mutable.Buffer[InternalRow] = new mutable.ArrayBuffer[InternalRow]()
    buffer1.getArray(mutableBufferOffset).toArray[InternalRow](innerStruct).copyToBuffer(sampled)

    if (otherCount > 0 && count > 0) {
      otherSampled.foreach { sample =>
        val idx = sampled.indexWhere(s => s.getDouble(0) > sample.getDouble(0))
        if (idx == 0) {
          val new_sampled =
            InternalRow(sampled(0).getDouble(0), sampled(0).getInt(1), sampled(1).getInt(2) / 2)
          sampled.update(0, new_sampled)
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1), 0)
          sampled.insert(0, new_sample)
        } else if (idx == -1) {
          val new_sampled = InternalRow(sampled(sampled.size - 1).getDouble(0),
            sampled(sampled.size - 1).getInt(1),
            (sampled(sampled.size - 2).getInt(2) * 2 * epsilon).toInt)
          sampled.update(sampled.size - 1, new_sampled)
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1), 0)
          sampled.insert(sampled.size, new_sample)
        } else {
          val new_sample = InternalRow(sample.getDouble(0), sample.getInt(1),
            (sampled(idx - 1).getInt(2) + sampled(idx).getInt(2)) / 2)
          sampled.insert(idx, new_sample)
        }
      }

      count = count + otherCount
      compress(sampled, count)
      buffer1.update(mutableBufferOffset, new GenericArrayData(sampled.toArray))
      buffer1.setLong(mutableBufferOffset + 1, count)
    } else if (otherCount > 0) {
      buffer1.update(mutableBufferOffset, new GenericArrayData(otherSampled.toArray))
      buffer1.setLong(mutableBufferOffset + 1, otherCount)
    }
  }

  override def eval(buffer: InternalRow): Any = {
    val count: Long = buffer.getLong(mutableBufferOffset + 1)
    val sampled: Array[InternalRow] =
      buffer.getArray(mutableBufferOffset).toArray[InternalRow](innerStruct)
    query(sampled, count)
  }
}
