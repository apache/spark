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
    val currMin: BV[Double]) extends VectorRDDStatisticalSummary with Serializable {

  override def mean(): Vector = {
    Vectors.fromBreeze(currMean :* nnz :/ totalCnt)
  }

  override def variance(): Vector = {
    val deltaMean = currMean
    val realM2n = currM2n - ((deltaMean :* deltaMean) :* (nnz :* (nnz :- totalCnt)) :/ totalCnt)
    realM2n :/= totalCnt
    Vectors.fromBreeze(realM2n)
  }

  override def totalCount(): Long = totalCnt.toLong

  override def numNonZeros(): Vector = Vectors.fromBreeze(nnz)

  override def max(): Vector = {
    nnz.activeIterator.foreach {
      case (id, 0.0) => currMax(id) = 0.0
      case _ =>
    }
    Vectors.fromBreeze(currMax)
  }

  override def min(): Vector = {
    nnz.activeIterator.foreach {
      case (id, 0.0) => currMin(id) = 0.0
      case _ =>
    }
    Vectors.fromBreeze(currMin)
  }

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