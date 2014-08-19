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

import org.apache.spark.annotation.{DeveloperApi, Experimental}

/**
 * :: Experimental ::
 * Trait for calculating information gain.
 */
@Experimental
trait Impurity extends Serializable {

  /**
   * :: DeveloperApi ::
   * information calculation for multiclass classification
   * @param counts Array[Double] with counts for each label
   * @param totalCount sum of counts for all labels
   * @return information value, or 0 if totalCount = 0
   */
  @DeveloperApi
  def calculate(counts: Array[Double], totalCount: Double): Double

  /**
   * :: DeveloperApi ::
   * information calculation for regression
   * @param count number of instances
   * @param sum sum of labels
   * @param sumSquares summation of squares of the labels
   * @return information value, or 0 if count = 0
   */
  @DeveloperApi
  def calculate(count: Double, sum: Double, sumSquares: Double): Double
}

/**
 * This class holds a set of sufficient statistics for computing impurity from a sample.
 * @param statsSize  Length of the vector of sufficient statistics.
 */
private[tree] abstract class ImpurityAggregator(statsSize: Int) extends Serializable {

  /**
   * Sufficient statistics for calculating impurity.
   */
  var counts: Array[Double] = new Array[Double](statsSize)

  def copy: ImpurityAggregator

  /**
   * Add the given label to this aggregator.
   */
  def add(label: Double): Unit

  /**
   * Compute the impurity for the samples given so far.
   * If no samples have been collected, return 0.
   */
  def calculate(): Double

  /**
   * Merge another aggregator into this one, modifying this aggregator.
   * @param other  Aggregator of the same type.
   * @return  merged aggregator
   */
  def merge(other: ImpurityAggregator): ImpurityAggregator = {
    require(counts.size == other.counts.size,
      s"Two ImpurityAggregator instances cannot be merged with different counts sizes." +
      s"  Sizes are ${counts.size} and ${other.counts.size}.")
    var i = 0
    while (i < other.counts.size) {
      counts(i) += other.counts(i)
      i += 1
    }
    this
  }

  /**
   * Number of samples added to this aggregator.
   */
  def count: Long

  /**
   * Create a new (empty) aggregator of the same type as this one.
   */
  def newAggregator: ImpurityAggregator

  /**
   * Return the prediction corresponding to the set of labels given to this aggregator.
   */
  def predict: Double

  /**
   * Return the probability of the prediction returned by [[predict]],
   * or -1 if no probability is available.
   */
  def prob(label: Double): Double = -1

  /**
   * Return the index of the largest element in this array.
   * If there are ties, the first maximal element is chosen.
   */
  protected def indexOfLargestArrayElement(array: Array[Double]): Int = {
    val result = array.foldLeft(-1, Double.MinValue, 0) {
      case ((maxIndex, maxValue, currentIndex), currentValue) =>
        if (currentValue > maxValue) {
          (currentIndex, currentValue, currentIndex + 1)
        } else {
          (maxIndex, maxValue, currentIndex + 1)
        }
    }
    if (result._1 < 0) {
      throw new RuntimeException("ImpurityAggregator internal error:" +
        " indexOfLargestArrayElement failed")
    }
    result._1
  }

}
