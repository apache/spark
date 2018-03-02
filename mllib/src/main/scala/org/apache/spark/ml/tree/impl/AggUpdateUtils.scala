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

import org.apache.spark.ml.tree.Split

/**
 * Helpers for updating DTStatsAggregators during collection of sufficient stats for tree training.
 */
private[impl] object AggUpdateUtils {

  /**
   * Updates the parent node stats of the passed-in impurity aggregator with the labels
   * corresponding to the feature values at indices [from, to).
   * @param indices Array of row indices for feature values; indices(i) = row index of the ith
   *                feature value
   */
  private[impl] def updateParentImpurity(
      statsAggregator: DTStatsAggregator,
      indices: Array[Int],
      from: Int,
      to: Int,
      instanceWeights: Array[Double],
      labels: Array[Double]): Unit = {
    from.until(to).foreach { idx =>
      val rowIndex = indices(idx)
      val label = labels(rowIndex)
      statsAggregator.updateParent(label, instanceWeights(rowIndex))
    }
  }

  /**
   * Update aggregator for an (unordered feature, label) pair
   * @param featureSplits Array of splits for the current feature
   */
  private[impl] def updateUnorderedFeature(
      agg: DTStatsAggregator,
      featureValue: Int,
      label: Double,
      featureIndex: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split],
      instanceWeight: Double): Unit = {
    val leftNodeFeatureOffset = agg.getFeatureOffset(featureIndexIdx)
    // Each unordered split has a corresponding bin for impurity stats of data points that fall
    // onto the left side of the split. For each unordered split, update left-side bin if applicable
    // for the current data point.
    val numSplits = agg.metadata.numSplits(featureIndex)
    var splitIndex = 0
    while (splitIndex < numSplits) {
      if (featureSplits(splitIndex).shouldGoLeft(featureValue, featureSplits)) {
        agg.featureUpdate(leftNodeFeatureOffset, splitIndex, label, instanceWeight)
      }
      splitIndex += 1
    }
  }

  /** Update aggregator for an (ordered feature, label) pair */
  private[impl] def updateOrderedFeature(
      agg: DTStatsAggregator,
      featureValue: Int,
      label: Double,
      featureIndexIdx: Int,
      instanceWeight: Double): Unit = {
    // The bin index of an ordered feature is just the feature value itself
    val binIndex = featureValue
    agg.update(featureIndexIdx, binIndex, label, instanceWeight)
  }

}
