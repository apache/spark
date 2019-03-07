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

package org.apache.spark.mllib.tree.impurity

import org.apache.spark.annotation.{DeveloperApi, Since}

/**
 * Class for calculating variance during regression
 */
@Since("1.0.0")
object Variance extends Impurity {

  /**
   * :: DeveloperApi ::
   * information calculation for multiclass classification
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @Since("1.1.0")
  @DeveloperApi
  override def calculate(counts: Array[Double], totalCount: Double): Double =
     throw new UnsupportedOperationException("Variance.calculate")

  /**
   * :: DeveloperApi ::
   * variance calculation
   * @param count number of instances
   * @param sum sum of labels
   * @param sumSquares summation of squares of the labels
   * @return information value, or 0 if count = 0
   */
  @Since("1.0.0")
  @DeveloperApi
  override def calculate(count: Double, sum: Double, sumSquares: Double): Double = {
    if (count == 0) {
      return 0
    }
    val squaredLoss = sumSquares - (sum * sum) / count
    squaredLoss / count
  }

  /**
   * Get this impurity instance.
   * This is useful for passing impurity parameters to a Strategy in Java.
   */
  @Since("1.0.0")
  def instance: this.type = this

}

/**
 * Class for updating views of a vector of sufficient statistics,
 * in order to compute impurity from a sample. For variance, we track:
 *   - sum(w_i)
 *   - sum(w_i * y_i)
 *   - sum(w_i * y_i * y_i)
 *   - count(y_i)
 * Note: Instances of this class do not hold the data; they operate on views of the data.
 */
private[spark] class VarianceAggregator()
  extends ImpurityAggregator(statsSize = 4) with Serializable {

  /**
   * Update stats for one (node, feature, bin) with the given label.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def update(
      allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double): Unit = {
    val instanceWeight = numSamples * sampleWeight
    allStats(offset) += instanceWeight
    allStats(offset + 1) += instanceWeight * label
    allStats(offset + 2) += instanceWeight * label * label
    allStats(offset + 3) += numSamples
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): VarianceCalculator = {
    new VarianceCalculator(allStats.view(offset, offset + statsSize - 1).toArray,
      allStats(offset + statsSize - 1).toLong)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[GiniAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class VarianceCalculator(stats: Array[Double], var rawCount: Long)
  extends ImpurityCalculator(stats) {

  require(stats.length == 3,
    s"VarianceCalculator requires sufficient statistics array stats to be of length 3," +
    s" but was given array of length ${stats.length}.")

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: VarianceCalculator = new VarianceCalculator(stats.clone(), rawCount)

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = Variance.calculate(stats(0), stats(1), stats(2))

  /**
   * Weighted number of data points accounted for in the sufficient statistics.
   */
  def count: Double = stats(0)

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double = if (count == 0) {
    0
  } else {
    stats(1) / count
  }

  override def toString: String = {
    s"VarianceAggregator(cnt = ${stats(0)}, sum = ${stats(1)}, sum2 = ${stats(2)})"
  }

}
