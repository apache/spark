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

import breeze.linalg.{Vector => BV, axpy}

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

case class VectorRDDStatisticalSummary(
    mean: Vector,
    variance: Vector,
    count: Long,
    max: Vector,
    min: Vector,
    nonZeroCnt: Vector) extends Serializable

private case class VectorRDDStatisticalRing(
    fakeMean: BV[Double],
    fakeM2n: BV[Double],
    totalCnt: Double,
    nnz: BV[Double],
    fakeMax: BV[Double],
    fakeMin: BV[Double])

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Compute full column-wise statistics for the RDD, including
   * {{{
   *   Mean:              Vector,
   *   Variance:          Vector,
   *   Count:             Double,
   *   Non-zero count:    Vector,
   *   Maximum elements:  Vector,
   *   Minimum elements:  Vector.
   * }}},
   * with the size of Vector as input parameter.
   */

  private def seqOp(
      aggregator: VectorRDDStatisticalRing,
      currData: BV[Double]): VectorRDDStatisticalRing = {
    aggregator match {
      case VectorRDDStatisticalRing(prevMean, prevM2n, cnt, nnzVec, maxVec, minVec) =>
        currData.activeIterator.foreach {
          case (id, value) =>
            if (maxVec(id) < value) maxVec(id) = value
            if (minVec(id) > value) minVec(id) = value

            val tmpPrevMean = prevMean(id)
            prevMean(id) = (prevMean(id) * cnt + value) / (cnt + 1.0)
            prevM2n(id) += (value - prevMean(id)) * (value - tmpPrevMean)

            nnzVec(id) += 1.0
        }

        VectorRDDStatisticalRing(
          prevMean,
          prevM2n,
          cnt + 1.0,
          nnzVec,
          maxVec,
          minVec)
    }
  }

  private def combOp(
      statistics1: VectorRDDStatisticalRing,
      statistics2: VectorRDDStatisticalRing): VectorRDDStatisticalRing = {
    (statistics1, statistics2) match {
      case (VectorRDDStatisticalRing(mean1, m2n1, cnt1, nnz1, max1, min1),
            VectorRDDStatisticalRing(mean2, m2n2, cnt2, nnz2, max2, min2)) =>
        val totalCnt = cnt1 + cnt2
        val deltaMean = mean2 - mean1
        mean2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) => mean1(id) = (mean1(id) * nnz1(id) + mean2(id) * nnz2(id)) / (nnz1(id) + nnz2(id))
        }
        m2n2.activeIterator.foreach {
          case (id, 0.0) =>
          case (id, value) => m2n1(id) += value + deltaMean(id) * deltaMean(id) * nnz1(id) * nnz2(id) / (nnz1(id)+nnz2(id))
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
        VectorRDDStatisticalRing(mean1, m2n1, totalCnt, nnz1, max1, min1)
    }
  }

  def summarizeStatistics(size: Int): VectorRDDStatisticalSummary = {
    val zeroValue = VectorRDDStatisticalRing(
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size)(Double.MinValue),
      BV.fill(size)(Double.MaxValue))

    val breezeVectors = self.map(_.toBreeze)
    val VectorRDDStatisticalRing(fakeMean, fakeM2n, totalCnt, nnz, fakeMax, fakeMin) =
      breezeVectors.aggregate(zeroValue)(seqOp, combOp)

    // solve real mean
    val realMean = fakeMean :* nnz :/ totalCnt
    // solve real variance
    val deltaMean = fakeMean :- 0.0
    val realVar = fakeM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)
    // max, min
    val max = Vectors.sparse(size, fakeMax.activeIterator.map { case (id, value) =>
      if ((value == Double.MinValue) && (realMean(id) != Double.MinValue)) (id, 0.0)
      else (id, value)
    }.toSeq)
    val min = Vectors.sparse(size, fakeMin.activeIterator.map { case (id, value) =>
      if ((value == Double.MaxValue) && (realMean(id) != Double.MaxValue)) (id, 0.0)
      else (id, value)
    }.toSeq)

    // get variance
    realVar :/= totalCnt

    VectorRDDStatisticalSummary(
      Vectors.fromBreeze(realMean),
      Vectors.fromBreeze(realVar),
      totalCnt.toLong,
      Vectors.fromBreeze(nnz),
      max,
      min)
  }
}
