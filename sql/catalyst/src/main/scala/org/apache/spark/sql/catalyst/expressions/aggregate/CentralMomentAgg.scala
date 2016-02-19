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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

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
abstract class CentralMomentAgg(child: Expression) extends ImperativeAggregate with Serializable {

  /**
   * The central moment order to be computed.
   */
  protected def momentOrder: Int

  override def children: Seq[Expression] = Seq(child)

  override def nullable: Boolean = false

  override def dataType: DataType = DoubleType

  override def inputTypes: Seq[AbstractDataType] = Seq(NumericType)

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, s"function $prettyName")

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  /**
   * Size of aggregation buffer.
   */
  private[this] val bufferSize = 5

  override val aggBufferAttributes: Seq[AttributeReference] = Seq.tabulate(bufferSize) { i =>
    AttributeReference(s"M$i", DoubleType)()
  }

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // buffer offsets
  private[this] val nOffset = mutableAggBufferOffset
  private[this] val meanOffset = mutableAggBufferOffset + 1
  private[this] val secondMomentOffset = mutableAggBufferOffset + 2
  private[this] val thirdMomentOffset = mutableAggBufferOffset + 3
  private[this] val fourthMomentOffset = mutableAggBufferOffset + 4

  // frequently used values for online updates
  private[this] var delta = 0.0
  private[this] var deltaN = 0.0
  private[this] var delta2 = 0.0
  private[this] var deltaN2 = 0.0
  private[this] var n = 0.0
  private[this] var mean = 0.0
  private[this] var m2 = 0.0
  private[this] var m3 = 0.0
  private[this] var m4 = 0.0

  /**
   * Initialize all moments to zero.
   */
  override def initialize(buffer: MutableRow): Unit = {
    for (aggIndex <- 0 until bufferSize) {
      buffer.setDouble(mutableAggBufferOffset + aggIndex, 0.0)
    }
  }

  /**
   * Update the central moments buffer.
   */
  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val v = Cast(child, DoubleType).eval(input)
    if (v != null) {
      val updateValue = v match {
        case d: Double => d
      }

      n = buffer.getDouble(nOffset)
      mean = buffer.getDouble(meanOffset)

      n += 1.0
      buffer.setDouble(nOffset, n)
      delta = updateValue - mean
      deltaN = delta / n
      mean += deltaN
      buffer.setDouble(meanOffset, mean)

      if (momentOrder >= 2) {
        m2 = buffer.getDouble(secondMomentOffset)
        m2 += delta * (delta - deltaN)
        buffer.setDouble(secondMomentOffset, m2)
      }

      if (momentOrder >= 3) {
        delta2 = delta * delta
        deltaN2 = deltaN * deltaN
        m3 = buffer.getDouble(thirdMomentOffset)
        m3 += -3.0 * deltaN * m2 + delta * (delta2 - deltaN2)
        buffer.setDouble(thirdMomentOffset, m3)
      }

      if (momentOrder >= 4) {
        m4 = buffer.getDouble(fourthMomentOffset)
        m4 += -4.0 * deltaN * m3 - 6.0 * deltaN2 * m2 +
          delta * (delta * delta2 - deltaN * deltaN2)
        buffer.setDouble(fourthMomentOffset, m4)
      }
    }
  }

  /**
   * Merge two central moment buffers.
   */
  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val n1 = buffer1.getDouble(nOffset)
    val n2 = buffer2.getDouble(inputAggBufferOffset)
    val mean1 = buffer1.getDouble(meanOffset)
    val mean2 = buffer2.getDouble(inputAggBufferOffset + 1)

    var secondMoment1 = 0.0
    var secondMoment2 = 0.0

    var thirdMoment1 = 0.0
    var thirdMoment2 = 0.0

    var fourthMoment1 = 0.0
    var fourthMoment2 = 0.0

    n = n1 + n2
    buffer1.setDouble(nOffset, n)
    delta = mean2 - mean1
    deltaN = if (n == 0.0) 0.0 else delta / n
    mean = mean1 + deltaN * n2
    buffer1.setDouble(mutableAggBufferOffset + 1, mean)

    // higher order moments computed according to:
    // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Higher-order_statistics
    if (momentOrder >= 2) {
      secondMoment1 = buffer1.getDouble(secondMomentOffset)
      secondMoment2 = buffer2.getDouble(inputAggBufferOffset + 2)
      m2 = secondMoment1 + secondMoment2 + delta * deltaN * n1 * n2
      buffer1.setDouble(secondMomentOffset, m2)
    }

    if (momentOrder >= 3) {
      thirdMoment1 = buffer1.getDouble(thirdMomentOffset)
      thirdMoment2 = buffer2.getDouble(inputAggBufferOffset + 3)
      m3 = thirdMoment1 + thirdMoment2 + deltaN * deltaN * delta * n1 * n2 *
        (n1 - n2) + 3.0 * deltaN * (n1 * secondMoment2 - n2 * secondMoment1)
      buffer1.setDouble(thirdMomentOffset, m3)
    }

    if (momentOrder >= 4) {
      fourthMoment1 = buffer1.getDouble(fourthMomentOffset)
      fourthMoment2 = buffer2.getDouble(inputAggBufferOffset + 4)
      m4 = fourthMoment1 + fourthMoment2 + deltaN * deltaN * deltaN * delta * n1 *
        n2 * (n1 * n1 - n1 * n2 + n2 * n2) + deltaN * deltaN * 6.0 *
        (n1 * n1 * secondMoment2 + n2 * n2 * secondMoment1) +
        4.0 * deltaN * (n1 * thirdMoment2 - n2 * thirdMoment1)
      buffer1.setDouble(fourthMomentOffset, m4)
    }
  }

  /**
   * Compute aggregate statistic from sufficient moments.
   * @param centralMoments Length `momentOrder + 1` array of central moments (un-normalized)
   *                       needed to compute the aggregate stat.
   */
  def getStatistic(n: Double, mean: Double, centralMoments: Array[Double]): Any

  override final def eval(buffer: InternalRow): Any = {
    val n = buffer.getDouble(nOffset)
    val mean = buffer.getDouble(meanOffset)
    val moments = Array.ofDim[Double](momentOrder + 1)
    moments(0) = 1.0
    moments(1) = 0.0
    if (momentOrder >= 2) {
      moments(2) = buffer.getDouble(secondMomentOffset)
    }
    if (momentOrder >= 3) {
      moments(3) = buffer.getDouble(thirdMomentOffset)
    }
    if (momentOrder >= 4) {
      moments(4) = buffer.getDouble(fourthMomentOffset)
    }

    getStatistic(n, mean, moments)
  }
}
