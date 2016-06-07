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

import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}

/**
 * :: Experimental ::
 * Class for calculating the
 * [[http://en.wikipedia.org/wiki/Decision_tree_learning#Gini_impurity Gini impurity]]
 * during binary classification.
 */
@Since("1.0.0")
@Experimental
object WeightedGini extends Impurity {

  /**
   * :: DeveloperApi ::
   * information calculation for multiclass classification
   * @param weightedCounts Array[Double] with counts for each label
   * @param weightedTotalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @Since("1.1.0")
  @DeveloperApi
  override def calculate(weightedCounts: Array[Double], weightedTotalCount: Double): Double = {
    if (weightedTotalCount == 0) {
      return 0
    }
    val numClasses = weightedCounts.length
    var impurity = 1.0
    var classIndex = 0
    while (classIndex < numClasses) {
      val freq = weightedCounts(classIndex) / weightedTotalCount
      impurity -= freq * freq
      classIndex += 1
    }
    impurity
  }

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
  override def calculate(count: Double, sum: Double, sumSquares: Double): Double =
    throw new UnsupportedOperationException("WeightedGini.calculate")

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
 * @param classWeights Weights of classes
 */
private[spark] class WeightedGiniAggregator(numClasses: Int, classWeights: Array[Double])
  extends ImpurityAggregator(numClasses) with Serializable {

  /**
   * Update stats for one (node, feature, bin) with the given label.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def update(allStats: Array[Double], offset: Int, label: Double, instanceWeight: Double): Unit = {
    if (label >= statsSize) {
      throw new IllegalArgumentException(s"WeightedGiniAggregator given label $label" +
        s" but requires label < numClasses (= $statsSize).")
    }
    if (label < 0) {
      throw new IllegalArgumentException(s"WeightedGiniAggregator given label $label" +
        s"but requires label is non-negative.")
    }
    allStats(offset + label.toInt) += instanceWeight
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): WeightedGiniCalculator = {
    new WeightedGiniCalculator(allStats.view(offset, offset + statsSize).toArray, classWeights)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[WeightedGiniAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 * @param classWeights Weights of classes
 */
private[spark] class WeightedGiniCalculator(stats: Array[Double], classWeights: Array[Double])
  extends ImpurityCalculator(stats) {

  var weightedStats = stats.zip(classWeights).map(x => x._1 * x._2)
  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: WeightedGiniCalculator = new WeightedGiniCalculator(stats.clone(), classWeights.clone())

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = WeightedGini.calculate(weightedStats, weightedStats.sum)

  /**
   * Number of data points accounted for in the sufficient statistics.
   */
  def count: Long = stats.sum.toLong

  /**
   * Weighted summary statistics of data points
   */
  def weightedCount: Double = weightedStats.sum

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double = if (count == 0) {
    0
  } else {
    indexOfLargestArrayElement(weightedStats)
  }

  /**
   * Probability of the label given by [[predict]].
   */
  override def prob(label: Double): Double = {
    val lbl = label.toInt
    require(lbl < stats.length,
      s"WeightedGiniCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "WeightedGiniImpurity does not support negative labels")
    val cnt = weightedCount
    if (cnt == 0) {
      0
    } else {
      weightedStats(lbl) / cnt
    }
  }

  override def toString: String = s"WeightedGiniCalculator(stats = [${stats.mkString(", ")}])"

  /**
   * Add the stats from another calculator into this one, modifying and returning this calculator.
   * Update the weightedStats at the same time
   */
  override def add(other: ImpurityCalculator): ImpurityCalculator = {
    require(stats.length == other.stats.length,
      s"Two ImpurityCalculator instances cannot be added with different counts sizes." +
        s"  Sizes are ${stats.length} and ${other.stats.length}.")
    val otherCalculator = other.asInstanceOf[WeightedGiniCalculator]
    var i = 0
    val len = other.stats.length
    while (i < len) {
      stats(i) += other.stats(i)
      weightedStats(i) += otherCalculator.weightedStats(i)
      i += 1
    }
    this
  }

  /**
   * Subtract the stats from another calculator from this one, modifying and returning this
   * calculator. Update the weightedStats at the same time
   */
  override def subtract(other: ImpurityCalculator): ImpurityCalculator = {
    require(stats.length == other.stats.length,
      s"Two ImpurityCalculator instances cannot be subtracted with different counts sizes." +
        s"  Sizes are ${stats.length} and ${other.stats.length}.")
    val otherCalculator = other.asInstanceOf[WeightedGiniCalculator]
    var i = 0
    val len = other.stats.length
    while (i < len) {
      stats(i) -= other.stats(i)
      weightedStats(i) -= otherCalculator.weightedStats(i)
      i += 1
    }
    this
  }
}
