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
 * Class for calculating Chi Squared as a split quality metric during binary classification.
 */
@Since("2.2.0")
@Experimental
object ChiSquared extends Impurity {
  private object CSTest extends org.apache.commons.math3.stat.inference.ChiSquareTest()

  /**
   * Get this impurity instance.
   * This is useful for passing impurity parameters to a Strategy in Java.
   */
  @Since("1.1.0")
  def instance: this.type = this

  /**
   * :: DeveloperApi ::
   * Placeholding definition of classification-based purity.
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return This method will throw an exception for [[ChiSquared]]
   */
  @Since("1.1.0")
  @DeveloperApi
  override def calculate(counts: Array[Double], totalCount: Double): Double =
    throw new UnsupportedOperationException("ChiSquared.calculate")

  /**
   * :: DeveloperApi ::
   * Placeholding definition of regression-based purity.
   * @param count number of instances
   * @param sum sum of labels
   * @param sumSquares summation of squares of the labels
   * @return This method will throw an exception for [[ChiSquared]]
   */
  @Since("1.0.0")
  @DeveloperApi
  override def calculate(count: Double, sum: Double, sumSquares: Double): Double =
    throw new UnsupportedOperationException("ChiSquared.calculate")

  /**
   * :: DeveloperApi ::
   * Chi-squared p-values from [[ImpurityCalculator]] for left and right split populations
   * @param calcL impurity calculator for the left split population
   * @param calcR impurity calculator for the right split population
   * @return The p-value for the chi squared null hypothesis; that left and right split populations
   * represent the same distribution of categorical values
   */
  @Since("2.0.0")
  @DeveloperApi
  override def calculate(calcL: ImpurityCalculator, calcR: ImpurityCalculator): Double = {
    CSTest.chiSquareTest(
      Array(
        calcL.stats.map(_.toLong),
        calcR.stats.map(_.toLong)
      )
    )
  }

  /**
   * :: DeveloperApi ::
   * Determine if this impurity measure is a test-statistic measure (true for Chi-squared)
   * @return For [[ChiSquared]] will return true
   */
  @Since("2.0.0")
  @DeveloperApi
  override def isTestStatistic: Boolean = true
}

/**
 * Class for updating views of a vector of sufficient statistics,
 * in order to compute impurity from a sample.
 * Note: Instances of this class do not hold the data; they operate on views of the data.
 * @param numClasses Number of classes for label.
 */
private[spark] class ChiSquaredAggregator(numClasses: Int)
  extends ImpurityAggregator(numClasses) with Serializable {

  /**
   * Update stats for one (node, feature, bin) with the given label.
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def update(allStats: Array[Double], offset: Int, label: Double, instanceWeight: Double): Unit = {
    allStats(offset + label.toInt) += instanceWeight
  }

  /**
   * Get an [[ImpurityCalculator]] for a (node, feature, bin).
   * @param allStats  Flat stats array, with stats for this (node, feature, bin) contiguous.
   * @param offset    Start index of stats for this (node, feature, bin).
   */
  def getCalculator(allStats: Array[Double], offset: Int): ChiSquaredCalculator = {
    new ChiSquaredCalculator(allStats.view(offset, offset + statsSize).toArray)
  }
}

/**
 * Stores statistics for one (node, feature, bin) for calculating impurity.
 * This class stores its own data and is for a specific (node, feature, bin).
 * @param stats Array of sufficient statistics for a (node, feature, bin).
 */
private[spark] class ChiSquaredCalculator(stats: Array[Double]) extends ImpurityCalculator(stats) {

  /**
   * Make a deep copy of this [[ImpurityCalculator]].
   */
  def copy: ChiSquaredCalculator = new ChiSquaredCalculator(stats.clone())

  /**
   * Calculate the impurity from the stored sufficient statistics.
   */
  def calculate(): Double = 1.0

  /**
   * Number of data points accounted for in the sufficient statistics.
   */
  def count: Long = stats.sum.toLong

  /**
   * Prediction which should be made based on the sufficient statistics.
   */
  def predict: Double =
    if (count == 0) 0 else indexOfLargestArrayElement(stats)

  /**
   * Probability of the label given by [[predict]].
   */
  override def prob(label: Double): Double = {
    val lbl = label.toInt
    require(lbl < stats.length,
      s"ChiSquaredCalculator.prob given invalid label: $lbl (should be < ${stats.length}")
    require(lbl >= 0, "ChiSquaredImpurity does not support negative labels")
    val cnt = count
    if (cnt == 0) 0 else (stats(lbl) / cnt)
  }

  /** output in a string format */
  override def toString: String = s"ChiSquaredCalculator(stats = [${stats.mkString(", ")}])"
}
