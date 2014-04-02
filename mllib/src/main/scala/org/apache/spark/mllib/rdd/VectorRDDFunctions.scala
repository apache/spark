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
package org.apache.spark.mllib.rdd

import breeze.linalg.{axpy, Vector => BV}

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD

/**
 * Case class of the summary statistics, including mean, variance, count, max, min, and non-zero
 * elements count.
 */
trait VectorRDDStatisticalSummary {
  def mean(): Vector
  def variance(): Vector
  def totalCount(): Long
  def numNonZeros(): Vector
  def max(): Vector
  def min(): Vector
}

private class Aggregator(
    val currMean: BV[Double],
    val currM2n: BV[Double],
    var totalCnt: Double,
    val nnz: BV[Double],
    val currMax: BV[Double],
    val currMin: BV[Double]) extends VectorRDDStatisticalSummary {
  nnz.activeIterator.foreach {
    case (id, 0.0) =>
      currMax(id) = 0.0
      currMin(id) = 0.0
    case _ =>
  }
  override def mean(): Vector = Vectors.fromBreeze(currMean :* nnz :/ totalCnt)
  override def variance(): Vector = {
    val deltaMean = currMean
    val realM2n = currM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)
    realM2n :/= totalCnt
    Vectors.fromBreeze(realM2n)
  }

  override def totalCount(): Long = totalCnt.toLong

  override def numNonZeros(): Vector = Vectors.fromBreeze(nnz)
  override def max(): Vector = Vectors.fromBreeze(currMax)
  override def min(): Vector = Vectors.fromBreeze(currMin)
  /**
   * Aggregate function used for aggregating elements in a worker together.
   */
  def add(currData: BV[Double]): this.type = {
    currData.activeIterator.foreach {
      case (id, 0.0) =>
      case (id, value) =>
        if (currMax(id) < value) currMax(id) = value
        if (currMin(id) > value) currMin(id) = value

        val tmpPrevMean = currMean(id)
        currMean(id) = (currMean(id) * totalCnt + value) / (totalCnt + 1.0)
        currM2n(id) += (value - currMean(id)) * (value - tmpPrevMean)

        nnz(id) += 1.0
        totalCnt += 1.0
    }
    this
  }
  /**
   * Combine function used for combining intermediate results together from every worker.
   */
  def merge(other: this.type): this.type = {
    totalCnt += other.totalCnt
    val deltaMean = currMean - other.currMean

    other.currMean.activeIterator.foreach {
      case (id, 0.0) =>
      case (id, value) =>
        currMean(id) = (currMean(id) * nnz(id) + other.currMean(id) * other.nnz(id)) / (nnz(id) + other.nnz(id))
    }

    other.currM2n.activeIterator.foreach {
      case (id, 0.0) =>
      case (id, value) =>
        currM2n(id) +=
          value + deltaMean(id) * deltaMean(id) * nnz(id) * other.nnz(id) / (nnz(id)+other.nnz(id))
    }

    other.currMax.activeIterator.foreach {
      case (id, value) =>
        if (currMax(id) < value) currMax(id) = value
    }

    other.currMin.activeIterator.foreach {
      case (id, value) =>
        if (currMin(id) > value) currMin(id) = value
    }

    axpy(1.0, other.nnz, nnz)
    this
  }
}

case class VectorRDDStatisticalAggregator(
    mean: BV[Double],
    statCnt: BV[Double],
    totalCnt: Double,
    nnz: BV[Double],
    currMax: BV[Double],
    currMin: BV[Double])

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Aggregate function used for aggregating elements in a worker together.
   */
  private def seqOp(
      aggregator: VectorRDDStatisticalAggregator,
      currData: BV[Double]): VectorRDDStatisticalAggregator = {
    aggregator match {
      case VectorRDDStatisticalAggregator(prevMean, prevM2n, cnt, nnzVec, maxVec, minVec) =>
        currData.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value

            val tmpPrevMean = prevMean(id)
            prevMean(id) = (prevMean(id) * cnt + value) / (cnt + 1.0)
            prevM2n(id) += (value - prevMean(id)) * (value - tmpPrevMean)

            nnzVec(id) += 1.0
        }

        VectorRDDStatisticalAggregator(
          prevMean,
          prevM2n,
          cnt + 1.0,
          nnzVec,
          maxVec,
          minVec)
    }
  }

  /**
   * Combine function used for combining intermediate results together from every worker.
   */
  private def combOp(
      statistics1: VectorRDDStatisticalAggregator,
      statistics2: VectorRDDStatisticalAggregator): VectorRDDStatisticalAggregator = {
    (statistics1, statistics2) match {
      case (VectorRDDStatisticalAggregator(mean1, m2n1, cnt1, nnz1, max1, min1),
            VectorRDDStatisticalAggregator(mean2, m2n2, cnt2, nnz2, max2, min2)) =>
        val totalCnt = cnt1 + cnt2
        val deltaMean = mean2 - mean1

        mean2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            mean1(id) = (mean1(id) * nnz1(id) + mean2(id) * nnz2(id)) / (nnz1(id) + nnz2(id))
        }

        m2n2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) =>
            m2n1(id) +=
              value + deltaMean(id) * deltaMean(id) * nnz1(id) * nnz2(id) / (nnz1(id)+nnz2(id))
        }

        max2.activeIterator.foreach {
          case (id, value) =>
            if (max1(id) < value) max1(id) = value
        }

        min2.activeIterator.foreach {
          case (id, value) =>
            if (min1(id) > value) min1(id) = value
        }

        axpy(1.0, nnz2, nnz1)
        VectorRDDStatisticalAggregator(mean1, m2n1, totalCnt, nnz1, max1, min1)
    }
  }

  /**
   * Compute full column-wise statistics for the RDD with the size of Vector as input parameter.
   */
  def summarizeStatistics(): VectorRDDStatisticalAggregator = {
    val size = self.take(1).head.size
    val zeroValue = VectorRDDStatisticalAggregator(
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size)(Double.MinValue),
      BV.fill(size)(Double.MaxValue))

    val VectorRDDStatisticalAggregator(currMean, currM2n, totalCnt, nnz, currMax, currMin) =
      self.map(_.toBreeze).aggregate(zeroValue)(seqOp, combOp)

    // solve real mean
    val realMean = currMean :* nnz :/ totalCnt

    // solve real m2n
    val deltaMean = currMean
    val realM2n = currM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)

    // remove the initial value in max and min, i.e. the Double.MaxValue or Double.MinValue.
    nnz.activeIterator.foreach {
      case (id, 0.0) =>
        currMax(id) = 0.0
        currMin(id) = 0.0
      case _ =>
    }

    // get variance
    realM2n :/= totalCnt

    VectorRDDStatisticalAggregator(
      realMean,
      realM2n,
      totalCnt,
      nnz,
      currMax,
      currMin)
  }
}