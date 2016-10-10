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
 * Class for calculating entropy during multiclass classification.
 */
@Since("1.0.0")
object Entropy extends Impurity {

  private[tree] def log2(x: Double): Double = {
    if (x == 0) {
      return 0.0
    } else {
      return scala.math.log(x) / scala.math.log(2)
    }
  }

  /**
   * :: DeveloperApi ::
   * information calculation for multiclass classification
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @Since("1.1.0")
  @DeveloperApi
  override def calculate(counts: Array[Double], totalCount: Double): Double = {
    if (totalCount == 0) {
      return 0
    }
    val numClasses = counts.length
    var impurity = 0.0
    var classIndex = 0
    while (classIndex < numClasses) {
      val classCount = counts(classIndex)
      val freq = classCount / totalCount
      impurity -= freq * log2(freq)
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
    throw new UnsupportedOperationException("Entropy.calculate")

  /**
   * Get this impurity instance.
   * This is useful for passing impurity parameters to a Strategy in Java.
   */
  @Since("1.1.0")
  def instance: this.type = this

  /**
   * Information gain calculation.
   * allStats(leftChildOffset: leftChildOffset + statsSize) contains the impurity
   * information of the leftChild.
   * parentsStats(parentOffset: parentOffset + statsSize) contains the impurity
   * information of the parent.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param leftChildOffset  Start index of stats for the left child.
   * @param parentStats  Flat stats array for impurity calculation of the parent.
   * @param parentOffset  Start index of stats for the parent.
   * @param statsSize  Size of the stats for the left child and the parent.
   * @param minInstancePerNode  minimum no. of instances in the child nodes for non-zero gain.
   * @param minInfoGain  return zero if gain < minInfoGain.
   * @return information gain.
   */
  override def calculateGain(
      allStats: Array[Double],
      leftChildOffset: Int,
      parentStats: Array[Double],
      parentOffset: Int,
      statsSize: Int,
      minInstancesPerNode: Int,
      minInfoGain: Double): Double = {
    var leftCount = 0.0
    var totalCount = 0.0
    var i = 0
    while (i < statsSize) {
      leftCount += allStats(leftChildOffset + i)
      totalCount += parentStats(parentOffset + i)
      i += 1
    }
    val rightCount = totalCount - leftCount

    if ((leftCount < minInstancesPerNode) ||
      (rightCount < minInstancesPerNode)) {
      return Double.MinValue
    }

    var leftImpurity = 0.0
    var rightImpurity = 0.0
    var parentImpurity = 0.0

    i = 0
    while (i < statsSize) {
      val leftStats = allStats(leftChildOffset + i)
      val totalStats = parentStats(parentOffset + i)

      val leftFreq = leftStats / leftCount
      val rightFreq = (totalStats - leftStats) / rightCount
      val parentFreq = totalStats / totalCount

      leftImpurity -= leftFreq * log2(leftFreq)
      rightImpurity -= rightFreq * log2(rightFreq)
      parentImpurity -= parentFreq * log2(parentFreq)

      i += 1
    }
    val leftWeighted = leftCount / totalCount * leftImpurity
    val rightWeighted = rightCount / totalCount * rightImpurity
    val gain = parentImpurity - leftWeighted - rightWeighted

    if (gain < minInfoGain) {
      return Double.MinValue
    }
    gain
  }
}

/**
 * Class for updating views of a vector of sufficient statistics,
 * in order to compute impurity from a sample.
 * Note: Instances of this class do not hold the data; they operate on views of the data.
 * @param numClasses  Number of classes for label.
 */
private[spark] class EntropyAggregator(numClasses: Int)
  extends ImpurityAggregator(numClasses) with Serializable {

  /**
   * Update stats for one (node, feature, bin) with the given label.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def update(allStats: Array[Double], offset: Int, label: Double, instanceWeight: Double): Unit = {
    if (label >= statsSize) {
      throw new IllegalArgumentException(s"EntropyAggregator given label $label" +
        s" but requires label < numClasses (= $statsSize).")
    }
    if (label < 0) {
      throw new IllegalArgumentException(s"EntropyAggregator given label $label" +
        s"but requires label is non-negative.")
    }
    allStats(offset + label.toInt) += instanceWeight
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): EntropyCalculator = {
    new EntropyCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * Unlike [[EntropyAggregator]], this class stores its own data and is for a specific
 * (node, feature, bin).
 * @param stats  Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class EntropyCalculator(stats: Array[Double]) extends ImpurityCalculator(stats) {

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: EntropyCalculator = new EntropyCalculator(stats.clone())

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = Entropy.calculate(stats, stats.sum)

  /**
   * Number of data points accounted for in the sufficient statistics.
   */
  def count: Long = stats.sum.toLong

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
      s"EntropyCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "Entropy does not support negative labels")
    val cnt = count
    if (cnt == 0) {
      0
    } else {
      stats(lbl) / cnt
    }
  }

  override def toString: String = s"EntropyCalculator(stats = [${stats.mkString(", ")}])"

}
