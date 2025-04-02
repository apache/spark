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

import org.apache.spark.annotation.Since

/**
 * Class for calculating the Gini impurity
 * (http://en.wikipedia.org/wiki/Decision_tree_learning#Gini_impurity)
 * during multiclass classification.
 */
@Since("1.0.0")
object Gini extends Impurity {

  /**
   * information calculation for multiclass classification
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @Since("1.1.0")
  override def calculate(counts: Array[Double], totalCount: Double): Double = {
    if (totalCount == 0) {
      return 0
    }
    val numClasses = counts.length
    var impurity = 1.0
    var classIndex = 0
    while (classIndex < numClasses) {
      val freq = counts(classIndex) / totalCount
      impurity -= freq * freq
      classIndex += 1
    }
    impurity
  }

  /**
   * variance calculation
   * @param count number of instances
   * @param sum sum of labels
   * @param sumSquares summation of squares of the labels
   * @return information value, or 0 if count = 0
   */
  @Since("1.0.0")
  override def calculate(count: Double, sum: Double, sumSquares: Double): Double =
    throw new UnsupportedOperationException("Gini.calculate")

  /**
   * Get this impurity instance.
   * This is useful for passing impurity parameters to a Strategy in Java.
   */
  @Since("1.1.0")
  def instance: this.type = this

}

/**
 * Class for updating views of a vector of sufficient statistics,
 * in order to compute impurity from a sample.
 * Note: Instances of this class do not hold the data; they operate on views of the data.
 * @param numClasses  Number of classes for label.
 */
private[spark] class GiniAggregator(numClasses: Int)
  extends ImpurityAggregator(numClasses + 1) with Serializable {

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
    if (label >= numClasses) {
      throw new IllegalArgumentException(s"GiniAggregator given label $label" +
        s" but requires label < numClasses (= ${numClasses}).")
    }
    if (label < 0) {
      throw new IllegalArgumentException(s"GiniAggregator given label $label" +
        s"but requires label to be non-negative.")
    }
    allStats(offset + label.toInt) += numSamples * sampleWeight
    allStats(offset + statsSize - 1) += numSamples
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): GiniCalculator = {
    new GiniCalculator(allStats.slice(offset, offset + statsSize - 1),
      allStats(offset + statsSize - 1).toLong)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[GiniAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class GiniCalculator(stats: Array[Double], var rawCount: Long)
  extends ImpurityCalculator(stats) {

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: GiniCalculator = new GiniCalculator(stats.clone(), rawCount)

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = Gini.calculate(stats, stats.sum)

  /**
   * Weighted number of data points accounted for in the sufficient statistics.
   */
  def count: Double = stats.sum

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double = if (count == 0) {
    0
  } else {
    indexOfLargestArrayElement(stats)
  }

  /**
   * Probability of the label given by [[predict]].
   */
  override def prob(label: Double): Double = {
    val lbl = label.toInt
    require(lbl < stats.length,
      s"GiniCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "GiniImpurity does not support negative labels")
    val cnt = count
    if (cnt == 0) {
      0
    } else {
      stats(lbl) / cnt
    }
  }

  override def toString: String = s"GiniCalculator(stats = [${stats.mkString(", ")}])"

}
