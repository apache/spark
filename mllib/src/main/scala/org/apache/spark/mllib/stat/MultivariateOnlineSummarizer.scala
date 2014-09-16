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

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * :: DeveloperApi ::
 * MultivariateOnlineSummarizer implements [[MultivariateStatisticalSummary]] to compute the mean,
 * variance, minimum, maximum, counts, and nonzero counts for samples in sparse or dense vector
 * format in a online fashion.
 *
 * Two MultivariateOnlineSummarizer can be merged together to have a statistical summary of
 * the corresponding joint dataset.
 *
 * A numerically stable algorithm is implemented to compute sample mean and variance:
 * Reference: [[http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance variance-wiki]]
 * Zero elements (including explicit zero values) are skipped when calling add(),
 * to have time complexity O(nnz) instead of O(n) for each column.
 */
@DeveloperApi
class MultivariateOnlineSummarizer extends MultivariateStatisticalSummary with Serializable {

  private var n = 0
  private var currMean: BDV[Double] = _
  private var currM2n: BDV[Double] = _
  private var totalCnt: Long = 0
  private var nnz: BDV[Double] = _
  private var currMax: BDV[Double] = _
  private var currMin: BDV[Double] = _

  /**
   * Add a new sample to this summarizer, and update the statistical summary.
   *
   * @param sample The sample in dense/sparse vector format to be added into this summarizer.
   * @return This MultivariateOnlineSummarizer object.
   */
  def add(sample: Vector): this.type = {
    if (n == 0) {
      require(sample.size > 0, s"Vector should have dimension larger than zero.")
      n = sample.size

      currMean = BDV.zeros[Double](n)
      currM2n = BDV.zeros[Double](n)
      nnz = BDV.zeros[Double](n)
      currMax = BDV.fill(n)(Double.MinValue)
      currMin = BDV.fill(n)(Double.MaxValue)
    }

    require(n == sample.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $n but got ${sample.size}.")

    sample.toBreeze.activeIterator.foreach {
      case (_, 0.0) => // Skip explicit zero elements.
      case (i, value) =>
        if (currMax(i) < value) {
          currMax(i) = value
        }
        if (currMin(i) > value) {
          currMin(i) = value
        }

        val tmpPrevMean = currMean(i)
        currMean(i) = (currMean(i) * nnz(i) + value) / (nnz(i) + 1.0)
        currM2n(i) += (value - currMean(i)) * (value - tmpPrevMean)

        nnz(i) += 1.0
    }

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
  def merge(other: MultivariateOnlineSummarizer): this.type = {
   if (this.totalCnt != 0 && other.totalCnt != 0) {
      require(n == other.n, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $n but got ${other.n}.")
      totalCnt += other.totalCnt
      val deltaMean: BDV[Double] = currMean - other.currMean
      var i = 0
      while (i < n) {
        // merge mean together
        if (other.currMean(i) != 0.0) {
          currMean(i) = (currMean(i) * nnz(i) + other.currMean(i) * other.nnz(i)) /
            (nnz(i) + other.nnz(i))
        }
        // merge m2n together
        if (nnz(i) + other.nnz(i) != 0.0) {
          currM2n(i) += other.currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * other.nnz(i) /
            (nnz(i) + other.nnz(i))
        }
        if (currMax(i) < other.currMax(i)) {
          currMax(i) = other.currMax(i)
        }
        if (currMin(i) > other.currMin(i)) {
          currMin(i) = other.currMin(i)
        }
        i += 1
      }
      nnz += other.nnz
    } else if (totalCnt == 0 && other.totalCnt != 0) {
      this.n = other.n
      this.currMean = other.currMean.copy
      this.currM2n = other.currM2n.copy
      this.totalCnt = other.totalCnt
      this.nnz = other.nnz.copy
      this.currMax = other.currMax.copy
      this.currMin = other.currMin.copy
    }
    this
  }

  override def mean: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    val realMean = BDV.zeros[Double](n)
    var i = 0
    while (i < n) {
      realMean(i) = currMean(i) * (nnz(i) / totalCnt)
      i += 1
    }
    Vectors.fromBreeze(realMean)
  }

  override def variance: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    val realVariance = BDV.zeros[Double](n)

    val denominator = totalCnt - 1.0

    // Sample variance is computed, if the denominator is less than 0, the variance is just 0.
    if (denominator > 0.0) {
      val deltaMean = currMean
      var i = 0
      while (i < currM2n.size) {
        realVariance(i) =
          currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * (totalCnt - nnz(i)) / totalCnt
        realVariance(i) /= denominator
        i += 1
      }
    }

    Vectors.fromBreeze(realVariance)
  }

  override def count: Long = totalCnt

  override def numNonzeros: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    Vectors.fromBreeze(nnz)
  }

  override def max: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMax(i) < 0.0)) currMax(i) = 0.0
      i += 1
    }
    Vectors.fromBreeze(currMax)
  }

  override def min: Vector = {
    require(totalCnt > 0, s"Nothing has been added to this summarizer.")

    var i = 0
    while (i < n) {
      if ((nnz(i) < totalCnt) && (currMin(i) > 0.0)) currMin(i) = 0.0
      i += 1
    }
    Vectors.fromBreeze(currMin)
  }
}
