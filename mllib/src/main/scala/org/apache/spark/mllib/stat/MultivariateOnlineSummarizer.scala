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

package org.apache.spark.mllib.stat

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat

/**
 * :: DeveloperApi ::
 * MultivariateOnlineSummarizer implements [[MultivariateStatisticalSummary]] to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for instances in sparse or dense vector
 * format in an online fashion.
 *
 * Two MultivariateOnlineSummarizer can be merged together to have a statistical summary of
 * the corresponding joint dataset.
 *
 * A numerically stable algorithm is implemented to compute the mean and variance of instances:
 * Reference: [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance variance-wiki]]
 * Zero elements (including explicit zero values) are skipped when calling add(),
 * to have time complexity O(nnz) instead of O(n) for each column.
 *
 * For weighted instances, the unbiased estimation of variance is defined by the reliability
 * weights: [[https://en.wikipedia.org/wiki/Weighted_arithmetic_mean#Reliability_weights]].
 */
@Since("1.1.0")
@DeveloperApi
class MultivariateOnlineSummarizer(mask: Int)
  extends MultivariateStatisticalSummary with Serializable {

  import MultivariateOnlineSummarizer._
  def this() = {
    this(MultivariateOnlineSummarizer.allMask)
  }
  private def testMask(m: Int): Boolean = (mask & m) != 0

  private var n = 0
  private var currMean: Array[Double] = _
  private var currM2n: Array[Double] = _
  private var currM2: Array[Double] = _
  private var currL1: Array[Double] = _
  private var totalCnt: Long = 0
  private var totalWeightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private var weightSum: Array[Double] = _
  private var nnz: Array[Long] = _
  private var currMax: Array[Double] = _
  private var currMin: Array[Double] = _

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This MultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def add(sample: Vector): this.type = add(sample, 1.0)

  private[spark] def add(instance: Vector, weight: Double): this.type = {
    require(weight >= 0.0, s"sample weight, ${weight} has to be >= 0.0")
    if (weight == 0.0) return this

    if (n == 0) {
      require(instance.size > 0, s"Vector should have dimension larger than zero.")
      n = instance.size

      if(testMask(currMeanMask)) currMean = Array.ofDim[Double](n)
      if(testMask(currM2nMask)) currM2n = Array.ofDim[Double](n)
      if(testMask(currM2Mask)) currM2 = Array.ofDim[Double](n)
      if(testMask(currL1Mask)) currL1 = Array.ofDim[Double](n)
      if(testMask(weightSumMask)) weightSum = Array.ofDim[Double](n)
      if(testMask(nnzMask)) nnz = Array.ofDim[Long](n)
      if(testMask(currMaxMask)) currMax = Array.fill[Double](n)(Double.MinValue)
      if(testMask(currMinMask)) currMin = Array.fill[Double](n)(Double.MaxValue)
    }

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localWeightSum = weightSum
    val localNumNonzeros = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    instance.foreachActive { (index, value) =>
      if (value != 0.0) {
        if (testMask(currMaxMask) && localCurrMax(index) < value) {
          localCurrMax(index) = value
        }
        if (testMask(currMinMask) && localCurrMin(index) > value) {
          localCurrMin(index) = value
        }

        if (testMask(currMeanMask)) {
          val prevMean = localCurrMean(index)
          val diff = value - prevMean
          localCurrMean(index) = prevMean + weight * diff / (localWeightSum(index) + weight)
          if (testMask(currM2nMask)) {
            localCurrM2n(index) += weight * (value - localCurrMean(index)) * diff
          }
        }
        if(testMask(currM2Mask)) localCurrM2(index) += weight * value * value
        if(testMask(currL1Mask)) localCurrL1(index) += weight * math.abs(value)

        if(testMask(weightSumMask)) localWeightSum(index) += weight
        if(testMask(nnzMask)) localNumNonzeros(index) += 1
      }
    }

    totalWeightSum += weight
    weightSquareSum += weight * weight
    totalCnt += 1
    this
  }

  /**
   * Merge another MultivariateOnlineSummarizer, and update the statistical summary.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other MultivariateOnlineSummarizer to be merged.
   * @return This MultivariateOnlineSummarizer object.
   */
  @Since("1.1.0")
  def merge(other: MultivariateOnlineSummarizer): this.type = {
    if (this.totalWeightSum != 0.0 && other.totalWeightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      totalWeightSum += other.totalWeightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        if (testMask(weightSumMask)) {
          val thisWeightSum = weightSum(i)
          val otherWeightSum = other.weightSum(i)
          val totalWeightSum = thisWeightSum + otherWeightSum
          if (totalWeightSum != 0.0) {
            if (testMask(currMeanMask)) {
              val deltaMean = other.currMean(i) - currMean(i)
              // merge mean together
              currMean(i) += deltaMean * otherWeightSum / totalWeightSum
              // merge m2n together
              if (testMask(currM2nMask)) {
                currM2n(i) += other.currM2n(i) +
                  deltaMean * deltaMean * thisWeightSum * otherWeightSum / totalWeightSum
              }
            }
            // merge m2 together
            if(testMask(currM2Mask)) currM2(i) += other.currM2(i)
            // merge l1 together
            if(testMask(currL1Mask)) currL1(i) += other.currL1(i)
          }
          weightSum(i) = totalWeightSum
        }
        if (testMask(nnzMask)) {
          val totalNnz = nnz(i) + other.nnz(i)
          if (totalNnz != 0) {
            // merge max and min
            if (testMask(currMaxMask)) currMax(i) = math.max(currMax(i), other.currMax(i))
            if (testMask(currMinMask)) currMin(i) = math.min(currMin(i), other.currMin(i))
          }
          nnz(i) = totalNnz
        }
        i += 1
      }
    } else if (totalWeightSum == 0.0 && other.totalWeightSum != 0.0) {
      this.n = other.n
      if (testMask(currMeanMask)) this.currMean = other.currMean.clone()
      if (testMask(currM2nMask)) this.currM2n = other.currM2n.clone()
      if (testMask(currM2Mask)) this.currM2 = other.currM2.clone()
      if (testMask(currL1Mask)) this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.totalWeightSum = other.totalWeightSum
      this.weightSquareSum = other.weightSquareSum
      if (testMask(weightSumMask)) this.weightSum = other.weightSum.clone()
      if (testMask(nnzMask)) this.nnz = other.nnz.clone()
      if (testMask(currMaxMask)) this.currMax = other.currMax.clone()
      if (testMask(currMinMask)) this.currMin = other.currMin.clone()
    }
    this
  }

  /**
   * Sample mean of each dimension.
   *
   */
  @Since("1.1.0")
  override def mean: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = Array.ofDim[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (weightSum(i) / totalWeightSum)
      i += 1
    }
    Vectors.dense(realMean)
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  @Since("1.1.0")
  override def variance: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = Array.ofDim[Double](n)

    val denominator = totalWeightSum - (weightSquareSum / totalWeightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      val len = currM2n.length
      while (i < len) {
        realVariance(i) = (currM2n(i) + deltaMean(i) * deltaMean(i) * weightSum(i) *
          (totalWeightSum - weightSum(i)) / totalWeightSum) / denominator
        i += 1
      }
    }
    Vectors.dense(realVariance)
  }

  /**
   * Sample size.
   *
   */
  @Since("1.1.0")
  override def count: Long = totalCnt

  /**
   * Number of nonzero elements in each dimension.
   *
   */
  @Since("1.1.0")
  override def numNonzeros: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(nnz.map(_.toDouble))
  }

  /**
   * Maximum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def max: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.dense(currMax)
  }

  /**
   * Minimum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def min: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.dense(currMin)
  }

  /**
   * L2 (Euclidian) norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL2: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = Array.ofDim[Double](n)

    var i = 0
    val len = currM2.length
    while (i < len) {
      realMagnitude(i) = math.sqrt(currM2(i))
      i += 1
    }
    Vectors.dense(realMagnitude)
  }

  /**
   * L1 norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL1: Vector = {
    require(totalWeightSum > 0, s"Nothing has been added to this summarizer.")

    Vectors.dense(currL1)
  }
}

object MultivariateOnlineSummarizer {

  private val currMeanMask = 0x1
  private val currM2nMask = 0x2
  private val currM2Mask = 0x4
  private val currL1Mask = 0x8
  private val weightSumMask = 0x10
  private val nnzMask = 0x20
  private val currMaxMask = 0x40
  private val currMinMask = 0x80

  val meanMask = currMeanMask | weightSumMask
  val varianceMask = currMeanMask | currM2nMask | weightSumMask
  val numNonZerosMask = nnzMask
  val maxMask = nnzMask | currMaxMask
  val minMask = nnzMask | currMinMask

  val allMask = 0xFFFFFFFF
}
