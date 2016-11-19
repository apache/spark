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
private[spark] object ApproxBernoulliImpurity extends Impurity {

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
  def predict: Double = if (count == 0) {
    0
  } else {
    // Per Friedman 1999, we use a single Newton-Raphson step from gamma = 0 to find the
    // optimal leaf prediction, the solution gamma to the minimization problem:
    // L = sum((p_i, y_i) in leaf) 2 log(1 + exp(-2 y_i (p_i + gamma)))
    // Where p_i is the previous GBT model's prediction for point i. The above with the factor of
    // 2 is equivalent to the LogLoss defined in Spark.
    //
    // We solve this problem by iterative root-finding for the gradients themselves (this turns
    // out to be equivalent to Newton's optimization method anyway).
    //
    // The single NR step from 0 yields the solution H^{-1} s, where H is the Hessian
    // and s is the gradient for L wrt gamma above at gamma = 0.
    //
    // The derivative of the i-th term wrt gamma is
    // - 4 y_i / (1 + E), where E = exp(2 y_i (p_i + gamma))
    // At gamma = 0 it's equivalent to the gradient of LogLoss for i, the sum of which is
    // stored in stats(1).
    //
    // The Hessian of the i-th term wrt to gamma is
    // 8 y_i^2 E / (1 + E)^2 = 8 y_i^2 / (1 + E) - 8 y_i^2 / (1 + E)^2
    // At gamma = 0, the latter term is the half of the square of the gradient of the LogLoss for i.
    // Since y_i is one of {-1, +1}, the first term is the absolute value of the gradient of the
    // LogLoss for i times 2. These statistics are stored in stats(2) and stats(3), respectively.
    stats(1) / (2 * stats(3) - stats(2) / 2)
  }

  override def toString: String = {
    s"ApproxBernoulliAggregator(cnt = ${stats(0)}, sum = ${stats(1)}," +
      s"sum2 = ${stats(2)}, sumAbs = ${stats(3)})"
  }
}
