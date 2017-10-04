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

import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats

/** Helper methods for impurity-related calculations during node split decisions. */
private[impl] object ImpurityUtils {

  /**
   * Calculate the impurity statistics for a given (feature, split) based upon left/right
   * aggregates.
   *
   * @param leftImpurityCalculator left node aggregates for this (feature, split)
   * @param rightImpurityCalculator right node aggregate for this (feature, split)
   * @param metadata learning and dataset metadata for DecisionTree
   * @return Impurity statistics for this (feature, split)
   */
  private[impl] def calculateImpurityStats(
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator,
      metadata: DecisionTreeMetadata): ImpurityStats = {

    val parentImpurityCalculator = leftImpurityCalculator.copy.add(rightImpurityCalculator)

    val impurity = parentImpurityCalculator.calculate()

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
    // if information gain doesn't satisfy minimum information gain,
    // then this split is invalid, return invalid information gain stats.
    // NOTE: We check gain < metadata.minInfoGain and gain <= 0 separately as this is what the
    // original tree training logic did.
    if (gain < metadata.minInfoGain || gain <= 0) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
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
