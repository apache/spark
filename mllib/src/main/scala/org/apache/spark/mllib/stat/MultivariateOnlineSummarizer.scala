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
import org.apache.spark.mllib.linalg.{DenseVectorBuilder, SparseVectorBuilder, Vector, VectorBuilder, VectorBuilders, Vectors}

/**
 * :: DeveloperApi ::
 * MultivariateOnlineSummarizer implements [[MultivariateStatisticalSummary]] to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for instances in sparse or dense vector
 * format in a online fashion.
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
 *
 * @param sparse whether the features are sparse; if `true`, data structures are used which are optimized for sparse
 *               data (default `false`)
 */
@Since("1.1.0")
@DeveloperApi
class MultivariateOnlineSummarizer(sparse: Boolean = false) extends MultivariateStatisticalSummary with Serializable {

  private var n = 0
  private var currMean: VectorBuilder = _
  private var currM2n: VectorBuilder = _
  private var currM2: VectorBuilder = _
  private var currL1: VectorBuilder = _
  private var totalCnt: Long = 0
  private var weightSum: Double = 0.0
  private var weightSquareSum: Double = 0.0
  private var nnz: VectorBuilder = _
  private var currMax: VectorBuilder = _
  private var currMin: VectorBuilder = _

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

      currMean = VectorBuilders.create(n, sparse)
      currM2n = VectorBuilders.create(n, sparse)
      currM2 = VectorBuilders.create(n, sparse)
      currL1 = VectorBuilders.create(n, sparse)
      nnz = VectorBuilders.create(n, sparse)
      currMax = VectorBuilders.create(n, sparse, Double.MinValue)
      currMin = VectorBuilders.create(n, sparse, Double.MaxValue)
    }

    require(n == instance.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${instance.size}.")

    val localCurrMean = currMean
    val localCurrM2n = currM2n
    val localCurrM2 = currM2
    val localCurrL1 = currL1
    val localNnz = nnz
    val localCurrMax = currMax
    val localCurrMin = currMin
    instance.foreachActive { (index, value) =>
      if (value != 0.0) {
        if (localCurrMax(index) < value) {
          localCurrMax.set(index, value)
        }
        if (localCurrMin(index) > value) {
          localCurrMin.set(index, value)
        }

        val prevMean = localCurrMean(index)
        val diff = value - prevMean
        localCurrMean.set(index, prevMean + weight * diff / (localNnz(index) + weight))
        localCurrM2n.add(index, weight * (value - localCurrMean(index)) * diff)
        localCurrM2.add(index, weight * value * value)
        localCurrL1.add(index, weight * math.abs(value))

        localNnz.add(index, weight)
      }
    }

    weightSum += weight
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
    if (this.weightSum != 0.0 && other.weightSum != 0.0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      weightSum += other.weightSum
      weightSquareSum += other.weightSquareSum
      var i = 0
      while (i < n) {
        val thisNnz = nnz(i)
        val otherNnz = other.nnz(i)
        val totalNnz = thisNnz + otherNnz
        if (totalNnz != 0.0) {
          val deltaMean = other.currMean(i) - currMean(i)
          // merge mean together
          currMean.add(i, deltaMean * otherNnz / totalNnz)
          // merge m2n together
          currM2n.add(i, other.currM2n(i) + deltaMean * deltaMean * thisNnz * otherNnz / totalNnz)
          // merge m2 together
          currM2.add(i, other.currM2(i))
          // merge l1 together
          currL1.add(i, other.currL1(i))
          // merge max and min
          currMax.set(i, math.max(currMax(i), other.currMax(i)))
          currMin.set(i, math.min(currMin(i), other.currMin(i)))
        }
        if (nnz(i) != totalNnz) nnz.set(i, totalNnz)
        i += 1
      }
    } else if (weightSum == 0.0 && other.weightSum != 0.0) {
      this.n = other.n
      this.currMean = other.currMean.clone()
      this.currM2n = other.currM2n.clone()
      this.currM2 = other.currM2.clone()
      this.currL1 = other.currL1.clone()
      this.totalCnt = other.totalCnt
      this.weightSum = other.weightSum
      this.weightSquareSum = other.weightSquareSum
      this.nnz = other.nnz.clone()
      this.currMax = other.currMax.clone()
      this.currMin = other.currMin.clone()
    }
    this
  }

  /**
   * Sample mean of each dimension.
   *
   */
  @Since("1.1.0")
  override def mean: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    val realMean = VectorBuilders.create(n, sparse)
    currMean.foreachActive((i, m) => realMean.set(i, m * (nnz(i) / weightSum)))
    realMean.toVector
  }

  /**
   * Unbiased estimate of sample variance of each dimension.
   *
   */
  @Since("1.1.0")
  override def variance: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    val realVariance = VectorBuilders.create(n, sparse)

    val denominator = weightSum - (weightSquareSum / weightSum)

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      currM2n.foreachActive { (i, m2n) =>
        val varianceAtI = (m2n + deltaMean(i) * deltaMean(i) * nnz(i) *
          (weightSum - nnz(i)) / weightSum) / denominator
        if (varianceAtI != 0) realVariance.set(i, varianceAtI)
      }
    }
    realVariance.toVector
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
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    nnz.toVector
  }

  /**
   * Maximum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def max: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    currMax.foreachActive((i, v) => if ((nnz(i) < weightSum) && (v < 0.0)) currMax.set(i, 0.0))
    currMax.toVector
  }

  /**
   * Minimum value of each dimension.
   *
   */
  @Since("1.1.0")
  override def min: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    currMin.foreachActive((i, v) => if ((nnz(i) < weightSum) && (v > 0.0)) currMin.set(i, 0.0))
    currMin.toVector
  }

  /**
   * L2 (Euclidian) norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL2: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    val realMagnitude = VectorBuilders.create(n, sparse)

    currM2.foreachActive((i, v) => realMagnitude.set(i, math.sqrt(v)))
    realMagnitude.toVector
  }

  /**
   * L1 norm of each dimension.
   *
   */
  @Since("1.2.0")
  override def normL1: Vector = {
    require(weightSum > 0, s"Nothing has been added to this summarizer.")

    currL1.toVector
  }
}
