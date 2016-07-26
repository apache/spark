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

package org.apache.spark.ml.tree.impurity

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.tree.impurity._

/**
 * [[ApproxLaplaceImpurity]] currently uses variance as a (proxy) impurity measure
 * during tree construction. The main purpose of the class is to have an alternative
 * leaf prediction calculation.
 *
 * Only data with examples each of weight 1.0 is supported.
 *
 * Class for calculating variance during regression.
 */
@Since("2.1")
object ApproxLaplaceImpurity extends Impurity {

  /**
   * :: DeveloperApi ::
   * information calculation for multiclass classification
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @Since("2.1")
  @DeveloperApi
  override def calculate(counts: Array[Double], totalCount: Double): Double =
    throw new UnsupportedOperationException("ApproxLaplaceImpurity.calculate")

  /**
   * :: DeveloperApi ::
   * variance calculation
   * @param count number of instances
   * @param sum sum of labels
   * @param sumSquares summation of squares of the labels
   * @return information value, or 0 if count = 0
   */
  @Since("2.1")
  @DeveloperApi
  override def calculate(count: Double, sum: Double, sumSquares: Double): Double = {
    Variance.calculate(count, sum, sumSquares)
  }

  /**
   * Get this impurity instance.
   * This is useful for passing impurity parameters to a Strategy in Java.
   */
  @Since("2.1")
  def instance: this.type = this
}

/**
 * Class for updating views of a vector of sufficient statistics,
 * in order to compute impurity from a sample.
 * Note: Instances of this class do not hold the data; they operate on views of the data.
 */
private[spark] class ApproxLaplaceAggregator
  extends ImpurityAggregator(statsSize = 3) with Serializable {

  /**
   * Update stats for one (node, feature, bin) with the given label.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def update(allStats: Array[Double], offset: Int, label: Double, instanceWeight: Double): Unit = {
    assert(instanceWeight == 1.0)
    allStats(offset) += instanceWeight
    allStats(offset + 1) += instanceWeight * label
    allStats(offset + 2) += instanceWeight * label * label
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): ApproxLaplaceCalculator = {
    new ApproxLaplaceCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[ImpurityAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class ApproxLaplaceCalculator(stats: Array[Double])
  extends ImpurityCalculator(stats) {

  require(stats.length == 1,
    s"ApproxLaplaceCalculator requires sufficient statistics array stats to be of length 1," +
      s" but was given array of length ${stats.length}.")

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: ApproxLaplaceCalculator = new ApproxLaplaceCalculator(stats.clone())

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = ApproxLaplaceImpurity.calculate(stats(0), stats(1), stats(2))

  /**
   * Number of data points accounted for in the sufficient statistics.
   */
  def count: Long = stats(0).toLong

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double = 0.0
  // TODO: LaplaceImpurity is currently unsupported.
  // We should use QuantileSummaries to compute the median in an online fashion.

  override def toString: String = {
    s"ApproxLaplaceAggregator(cnt = ${stats(0)}, sum = ${stats(1)}," +
      s"sum2 = ${stats(2)}, sumAbs = ${stats(3)})"
  }

}