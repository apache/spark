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

// Compute the sample standard deviation of a column
case class Stddev(child: Expression) extends StddevAgg(child) {

  override def isSample: Boolean = true
  override def prettyName: String = "stddev"
}

// Compute the population standard deviation of a column
case class StddevPop(child: Expression) extends StddevAgg(child) {

  override def isSample: Boolean = false
  override def prettyName: String = "stddev_pop"
}

// Compute the sample standard deviation of a column
case class StddevSamp(child: Expression) extends StddevAgg(child) {

  override def isSample: Boolean = true
  override def prettyName: String = "stddev_samp"
}

// Compute standard deviation based on online algorithm specified here:
// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
abstract class StddevAgg(child: Expression) extends AlgebraicAggregate {

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  def isSample: Boolean

  // Return data type.
  override def dataType: DataType = resultType

  // Expected input data type.
  // TODO: Right now, we replace old aggregate functions (based on AggregateExpression1) to the
  // new version at planning time (after analysis phase). For now, NullType is added at here
  // to make it resolved when we have cases like `select stddev(null)`.
  // We can use our analyzer to cast NullType to the default data type of the NumericType once
  // we remove the old aggregate functions. Then, we will not need NullType at here.
  override def inputTypes: Seq[AbstractDataType] = Seq(TypeCollection(NumericType, NullType))

  private val resultType = DoubleType

  private val preCount = AttributeReference("preCount", resultType)()
  private val currentCount = AttributeReference("currentCount", resultType)()
  private val preAvg = AttributeReference("preAvg", resultType)()
  private val currentAvg = AttributeReference("currentAvg", resultType)()
  private val currentMk = AttributeReference("currentMk", resultType)()

  override val bufferAttributes = preCount :: currentCount :: preAvg ::
                                  currentAvg :: currentMk :: Nil

  override val initialValues = Seq(
    /* preCount = */ Cast(Literal(0), resultType),
    /* currentCount = */ Cast(Literal(0), resultType),
    /* preAvg = */ Cast(Literal(0), resultType),
    /* currentAvg = */ Cast(Literal(0), resultType),
    /* currentMk = */ Cast(Literal(0), resultType)
  )

  override val updateExpressions = {

    // update average
    // avg = avg + (value - avg)/count
    def avgAdd: Expression = {
      currentAvg + ((Cast(child, resultType) - currentAvg) / currentCount)
    }

    // update sum of square of difference from mean
    // Mk = Mk + (value - preAvg) * (value - updatedAvg)
    def mkAdd: Expression = {
      val delta1 = Cast(child, resultType) - preAvg
      val delta2 = Cast(child, resultType) - currentAvg
      currentMk + (delta1 * delta2)
    }

    Seq(
      /* preCount = */ If(IsNull(child), preCount, currentCount),
      /* currentCount = */ If(IsNull(child), currentCount,
                           Add(currentCount, Cast(Literal(1), resultType))),
      /* preAvg = */ If(IsNull(child), preAvg, currentAvg),
      /* currentAvg = */ If(IsNull(child), currentAvg, avgAdd),
      /* currentMk = */ If(IsNull(child), currentMk, mkAdd)
    )
  }

  override val mergeExpressions = {

    // count merge
    def countMerge: Expression = {
      currentCount.left + currentCount.right
    }

    // average merge
    def avgMerge: Expression = {
      ((currentAvg.left * preCount) + (currentAvg.right * currentCount.right)) /
      (preCount + currentCount.right)
    }

    // update sum of square differences
    def mkMerge: Expression = {
      val avgDelta = currentAvg.right - preAvg
      val mkDelta = (avgDelta * avgDelta) * (preCount * currentCount.right) /
        (preCount + currentCount.right)

      currentMk.left + currentMk.right + mkDelta
    }

    Seq(
      /* preCount = */ If(IsNull(currentCount.left),
                         Cast(Literal(0), resultType), currentCount.left),
      /* currentCount = */ If(IsNull(currentCount.left), currentCount.right,
                             If(IsNull(currentCount.right), currentCount.left, countMerge)),
      /* preAvg = */ If(IsNull(currentAvg.left), Cast(Literal(0), resultType), currentAvg.left),
      /* currentAvg = */ If(IsNull(currentAvg.left), currentAvg.right,
                           If(IsNull(currentAvg.right), currentAvg.left, avgMerge)),
      /* currentMk = */ If(IsNull(currentMk.left), currentMk.right,
                          If(IsNull(currentMk.right), currentMk.left, mkMerge))
    )
  }

  override val evaluateExpression = {
    // when currentCount == 0, return null
    // when currentCount == 1, return 0
    // when currentCount >1
    // stddev_samp = sqrt (currentMk/(currentCount -1))
    // stddev_pop = sqrt (currentMk/currentCount)
    val varCol = {
      if (isSample) {
        currentMk / Cast((currentCount - Cast(Literal(1), resultType)), resultType)
      }
      else {
        currentMk / currentCount
      }
    }

    If(EqualTo(currentCount, Cast(Literal(0), resultType)), Cast(Literal(null), resultType),
      If(EqualTo(currentCount, Cast(Literal(1), resultType)), Cast(Literal(0), resultType),
        Cast(Sqrt(varCol), resultType)))
  }
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
