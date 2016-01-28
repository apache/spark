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

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A central moment is the expected value of a specified power of the deviation of a random
 * variable from the mean. Central moments are often used to characterize the properties of about
 * the shape of a distribution.
 *
 * This class implements online, one-pass algorithms for computing the central moments of a set of
 * points.
 *
 * Behavior:
 *  - null values are ignored
 *  - returns `Double.NaN` when the column contains `Double.NaN` values
 *
 * References:
 *  - Xiangrui Meng.  "Simpler Online Updates for Arbitrary-Order Central Moments."
 *      2015. http://arxiv.org/abs/1510.04923
 *
 * @see [[https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
 *     Algorithms for calculating variance (Wikipedia)]]
 *
 * @param child to compute central moments of.
 */
abstract class CentralMomentAgg(child: Expression) extends DeclarativeAggregate {

  /**
   * The central moment order to be computed.
   */
  protected val momentOrder: Int

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(DoubleType)

  protected val count = AttributeReference("count", DoubleType, nullable = false)()
  protected val avg = AttributeReference("avg", DoubleType, nullable = false)()
  protected val m2 = AttributeReference("m2", DoubleType, nullable = false)()
  protected val m3 = AttributeReference("m3", DoubleType, nullable = false)()
  protected val m4 = AttributeReference("m4", DoubleType, nullable = false)()

  override val aggBufferAttributes = count :: avg :: m2 :: m3 :: m4 :: Nil

  override val initialValues: Seq[Expression] = Seq(
    /* count = */ Literal(0.0),
    /* avg = */ Literal(0.0),
    /* m2 = */ Literal(0.0),
    /* m3 = */ Literal(0.0),
    /* m4 = */ Literal(0.0)
  )

  override lazy val updateExpressions: Seq[Expression] = {
    val n = count + Literal(1.0)
    val delta = child - avg
    val deltaN = delta / n
    val newAvg = avg + deltaN
    val newM2 = m2 + delta * (delta - deltaN)

    val delta2 = delta * delta
    val deltaN2 = deltaN * deltaN
    val newM3 = m3 - Literal(3.0) * deltaN * newM2 + delta * (delta2 - deltaN2)

    val newM4 = m4 - Literal(4.0) * deltaN * newM3 - Literal(6.0) * deltaN2 * newM2 +
      delta * (delta * delta2 - deltaN * deltaN2)

    Seq(
      /* count = */ If(IsNull(child), count, n),
      /* avg = */ If(IsNull(child), avg, newAvg),
      /* m2 = */ If(IsNull(child), m2, newM2),
      /* m3 = */ if (momentOrder >= 3) If(IsNull(child), m3, newM3) else Literal(0.0),
      /* m4 = */ if (momentOrder >= 4) If(IsNull(child), m4, newM4) else Literal(0.0)
    )
  }

  override lazy val mergeExpressions: Seq[Expression] = {

    val n1 = count.left
    val n2 = count.right
    val n = n1 + n2
    val delta = avg.right - avg.left
    val deltaN = If(EqualTo(n, Literal(0.0)), Literal(0.0), delta / n)
    val newAvg = avg.left + deltaN * n2

    // higher order moments computed according to:
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    val newM2 = m2.left + m2.right + delta * deltaN * n1 * n2
    val newM3 = m3.left + m3.right + deltaN * deltaN * delta * n1 * n2 * (n1 - n2) +
      Literal(3.0) * deltaN * (n1 * m2.right - n2 * m2.left)

    val newM4 = m4.left + m4.right +
      deltaN * deltaN * deltaN * delta * n1 * n2 * (n1 * n1 - n1 * n2 + n2 * n2) +
      Literal(6.0) * deltaN * deltaN * (n1 * n1 * m2.right + n2 * n2 * m2.left) +
      Literal(4.0) * deltaN * (n1 * m3.right - n2 * m3.left)

    Seq(
      /* count = */ n,
      /* avg = */ newAvg,
      /* m2 = */ newM2,
      /* m3 = */ if (momentOrder >= 3) newM3 else Literal(0.0),
      /* m4 = */ if (momentOrder >= 4) newM4 else Literal(0.0)
    )
  }
}
