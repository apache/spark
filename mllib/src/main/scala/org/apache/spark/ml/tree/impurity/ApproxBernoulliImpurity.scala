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
 * [[ApproxBernoulliImpurity]] currently uses variance as a (proxy) impurity measure
 * during tree construction. The main purpose of the class is to have an alternative
 * leaf prediction calculation.
 *
 * Only data with examples each of weight 1.0 is supported.
 *
 * Class for calculating variance during regression.
 */
@Since("2.1")
object ApproxBernoulliImpurity extends Impurity {

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
    throw new UnsupportedOperationException("ApproxBernoulliImpurity.calculate")

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
private[spark] class ApproxBernoulliAggregator
  extends ImpurityAggregator(statsSize = 4) with Serializable {

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
    allStats(offset + 3) += instanceWeight * Math.abs(label)
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): ApproxBernoulliCalculator = {
    new ApproxBernoulliCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[ImpurityAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class ApproxBernoulliCalculator(stats: Array[Double])
  extends ImpurityCalculator(stats) {

  require(stats.length == 4,
    s"ApproxBernoulliCalculator requires sufficient statistics array stats to be of length 4," +
      s" but was given array of length ${stats.length}.")

  // Per Friedman 1999, we use a single Newton-Ralphson step to find the optimal leaf
  // prediction, the solution gamma to the minimization problem:
  // sum((p_i, y_i) in leaf) log(1 + exp(-2 y_i (p_i + gamma)))
  // Where p_i is the previous GBT model's prediction for point i. Even though our LogLoss
  // is twice the above, the optimization problem is unchanged.
  private val prediction: Double = stats(1) / (2 * stats(3) - stats(2))

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: ApproxBernoulliCalculator = new ApproxBernoulliCalculator(stats.clone())

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = ApproxBernoulliImpurity.calculate(stats(0), stats(1), stats(2))

  /**
   * Number of data points accounted for in the sufficient statistics.
   */
  def count: Long = stats(0).toLong

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double = prediction

  override def toString: String = {
    s"VarianceAggregator(cnt = ${stats(0)}, sum = ${stats(1)}," +
      s"sum2 = ${stats(2)}, sumAbs = ${stats(3)})"
  }

}