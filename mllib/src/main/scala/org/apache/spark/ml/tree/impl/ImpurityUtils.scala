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

package org.apache.spark.ml.tree.impl

import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.ImpurityStats

/** Helper methods for impurity-related calculations during node split decisions. */
private[impl] object ImpurityUtils {

  /**
   * Get impurity calculator containing statistics for all labels for rows corresponding to
   * feature values in [from, to).
   * @param indices indices(i) = row index corresponding to ith feature value
   */
  private[impl] def getParentImpurityCalculator(
      metadata: DecisionTreeMetadata,
      indices: Array[Int],
      from: Int,
      to: Int,
      instanceWeights: Array[Double],
      labels: Array[Double]): ImpurityCalculator = {
    // Compute sufficient stats (e.g. label counts) for all data at the current node,
    // store result in currNodeStatsAgg.parentStats so that we can share it across
    // all features for the current node
    val currNodeStatsAgg = new DTStatsAggregator(metadata, featureSubset = None)
    AggUpdateUtils.updateParentImpurity(currNodeStatsAgg, indices, from, to,
      instanceWeights, labels)
    currNodeStatsAgg.getParentImpurityCalculator()
  }

  /**
   * Calculate the impurity statistics for a given (feature, split) based upon left/right
   * aggregates.
   *
   * @param parentImpurityCalculator An ImpurityCalculator containing the impurity stats
   *                                 of the node currently being split.
   * @param leftImpurityCalculator left node aggregates for this (feature, split)
   * @param rightImpurityCalculator right node aggregate for this (feature, split)
   * @param metadata learning and dataset metadata for DecisionTree
   * @return Impurity statistics for this (feature, split)
   */
  private[impl] def calculateImpurityStats(
      parentImpurityCalculator: ImpurityCalculator,
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator,
      metadata: DecisionTreeMetadata): ImpurityStats = {

    val impurity: Double = parentImpurityCalculator.calculate()

    val leftCount = leftImpurityCalculator.count
    val rightCount = rightImpurityCalculator.count

    val totalCount = leftCount + rightCount

    // If left child or right child doesn't satisfy minimum instances per node,
    // then this split is invalid, return invalid information gain stats.
    if ((leftCount < metadata.minInstancesPerNode) ||
      (rightCount < metadata.minInstancesPerNode)) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    val leftImpurity = leftImpurityCalculator.calculate() // Note: This equals 0 if count = 0
    val rightImpurity = rightImpurityCalculator.calculate()

    val leftWeight = leftCount / totalCount.toDouble
    val rightWeight = rightCount / totalCount.toDouble

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
    // If information gain doesn't satisfy minimum information gain,
    // then this split is invalid, return invalid information gain stats.
    if (gain < metadata.minInfoGain) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    // If information gain is non-positive but doesn't violate the minimum info gain constraint,
    // return a stats object with correct values but valid = false to indicate that we should not
    // split.
    if (gain <= 0) {
      return new ImpurityStats(gain, impurity, parentImpurityCalculator, leftImpurityCalculator,
        rightImpurityCalculator, valid = false)
    }


    new ImpurityStats(gain, impurity, parentImpurityCalculator,
      leftImpurityCalculator, rightImpurityCalculator)
  }

  /**
   * Given an impurity aggregator containing label statistics for a given (node, feature, bin),
   * returns the corresponding "centroid", used to order bins while computing best splits.
   *
   * @param metadata learning and dataset metadata for DecisionTree
   */
  private[impl] def getCentroid(
      metadata: DecisionTreeMetadata,
      binStats: ImpurityCalculator): Double = {

    if (binStats.count != 0) {
      if (metadata.isMulticlass) {
        // multiclass classification
        // For categorical features in multiclass classification,
        // the bins are ordered by the impurity of their corresponding labels.
        binStats.calculate()
      } else if (metadata.isClassification) {
        // binary classification
        // For categorical features in binary classification,
        // the bins are ordered by the count of class 1.
        binStats.stats(1)
      } else {
        // regression
        // For categorical features in regression and binary classification,
        // the bins are ordered by the prediction.
        binStats.predict
      }
    } else {
      Double.MaxValue
    }
  }
}
