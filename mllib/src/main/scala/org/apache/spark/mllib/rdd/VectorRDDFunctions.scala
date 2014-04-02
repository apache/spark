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
 * Trait of the summary statistics, including mean, variance, count, max, min, and non-zero elements
 * count.
 */
trait VectorRDDStatisticalSummary {
  def mean: Vector
  def variance: Vector
  def totalCount: Long
  def numNonZeros: Vector
  def max: Vector
  def min: Vector
}

/**
 * Aggregates [[org.apache.spark.mllib.rdd.VectorRDDStatisticalSummary VectorRDDStatisticalSummary]]
 * together with add() and merge() function.
 */
private class Aggregator(
    val currMean: BV[Double],
    val currM2n: BV[Double],
    var totalCnt: Double,
    val nnz: BV[Double],
    val currMax: BV[Double],
    val currMin: BV[Double]) extends VectorRDDStatisticalSummary with Serializable {

  // lazy val is used for computing only once time. Same below.
  override lazy val mean = Vectors.fromBreeze(currMean :* nnz :/ totalCnt)

  // Online variance solution used in add() function, while parallel variance solution used in
  // merge() function. Reference here:
  // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
  // Solution here ignoring the zero elements when calling add() and merge(), for decreasing the
  // O(n) algorithm to O(nnz). Real variance is computed here after we get other statistics, simply
  // by another parallel combination process.
  override lazy val variance = {
    val deltaMean = currMean
    var i = 0
    while(i < currM2n.size) {
      currM2n(i) += deltaMean(i) * deltaMean(i) * nnz(i) * (totalCnt-nnz(i)) / totalCnt
      currM2n(i) /= totalCnt
      i += 1
    }
    Vectors.fromBreeze(currM2n)
  }

  override lazy val totalCount: Long = totalCnt.toLong

  override lazy val numNonZeros: Vector = Vectors.fromBreeze(nnz)

  override lazy val max: Vector = {
    nnz.iterator.foreach {
      case (id, count) =>
        if ((count == 0.0) || ((count < totalCnt) && (currMax(id) < 0.0)))  currMax(id) = 0.0
    }
    Vectors.fromBreeze(currMax)
  }

  override lazy val min: Vector = {
    nnz.iterator.foreach {
      case (id, count) =>
        if ((count == 0.0) || ((count < totalCnt) && (currMin(id) > 0.0))) currMin(id) = 0.0
    }
    Vectors.fromBreeze(currMin)
  }

  /**
   * Aggregate function used for aggregating elements in a worker together.
   */
  def add(currData: BV[Double]): this.type = {
    currData.activeIterator.foreach {
      // this case is used for filtering the zero elements if the vector is a dense one.
      case (id, 0.0) =>
      case (id, value) =>
        if (currMax(id) < value) currMax(id) = value
        if (currMin(id) > value) currMin(id) = value

        val tmpPrevMean = currMean(id)
        currMean(id) = (currMean(id) * nnz(id) + value) / (nnz(id) + 1.0)
        currM2n(id) += (value - currMean(id)) * (value - tmpPrevMean)

        nnz(id) += 1.0
    }

    totalCnt += 1.0
    this
  }

  /**
   * Combine function used for combining intermediate results together from every worker.
   */
  def merge(other: Aggregator): this.type = {

    totalCnt += other.totalCnt

    val deltaMean = currMean - other.currMean

    other.currMean.activeIterator.foreach {
      case (id, 0.0) =>
      case (id, value) =>
        currMean(id) =
          (currMean(id) * nnz(id) + other.currMean(id) * other.nnz(id)) / (nnz(id) + other.nnz(id))
    }

    var i = 0
    while(i < currM2n.size) {
      (nnz(i), other.nnz(i)) match {
        case (0.0, 0.0) =>
        case _ => currM2n(i) +=
          other.currM2n(i) + deltaMean(i) * deltaMean(i) * nnz(i) * other.nnz(i) / (nnz(i)+other.nnz(i))
      }
      i += 1
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

/**
 * Extra functions available on RDDs of [[org.apache.spark.mllib.linalg.Vector Vector]] through an
 * implicit conversion. Import `org.apache.spark.MLContext._` at the top of your program to use
 * these functions.
 */
class VectorRDDFunctions(self: RDD[Vector]) extends Serializable {

  /**
   * Compute full column-wise statistics for the RDD with the size of Vector as input parameter.
   */
  def summarizeStatistics(): VectorRDDStatisticalSummary = {
    val size = self.take(1).head.size

    val zeroValue = new Aggregator(
      BV.zeros[Double](size),
      BV.zeros[Double](size),
      0.0,
      BV.zeros[Double](size),
      BV.fill(size)(Double.MinValue),
      BV.fill(size)(Double.MaxValue))

    self.map(_.toBreeze).aggregate[Aggregator](zeroValue)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
    )
  }
}
