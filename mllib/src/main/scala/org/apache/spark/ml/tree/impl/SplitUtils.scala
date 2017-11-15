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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.tree.{CategoricalSplit, Split}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats

/** Utility methods for choosing splits during local & distributed tree training. */
private[impl] object SplitUtils extends Logging {

  /** Sorts ordered feature categories by label centroid, returning an ordered list of categories */
  private def sortByCentroid(
      binAggregates: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int): List[Int] = {
    /* Each bin is one category (feature value).
     * The bins are ordered based on centroidForCategories, and this ordering determines which
     * splits are considered.  (With K categories, we consider K - 1 possible splits.)
     *
     * centroidForCategories is a list: (category, centroid)
     */
    val numCategories = binAggregates.metadata.numBins(featureIndex)
    val nodeFeatureOffset = binAggregates.getFeatureOffset(featureIndexIdx)

    val centroidForCategories = Range(0, numCategories).map { featureValue =>
      val categoryStats =
        binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
      val centroid = ImpurityUtils.getCentroid(binAggregates.metadata, categoryStats)
      (featureValue, centroid)
    }
    logDebug("Centroids for categorical variable: " + centroidForCategories.mkString(","))
    // bins sorted by centroids
    val categoriesSortedByCentroid = centroidForCategories.toList.sortBy(_._2).map(_._1)
     logDebug("Sorted centroids for categorical variable = " +
       categoriesSortedByCentroid.mkString(","))
    categoriesSortedByCentroid
  }

  /**
   * Find the best split for an unordered categorical feature at a single node.
   *
   * Algorithm:
   *  - Considers all possible subsets (exponentially many)
   *
   * @param featureIndex  Global index of feature being split.
   * @param featureIndexIdx Index of feature being split within subset of features for current node.
   * @param featureSplits Array of splits for the current feature
   * @param parentCalculator Optional: ImpurityCalculator containing impurity stats for current node
   * @return  (best split, statistics for split)  If no valid split was found, the returned
   *          ImpurityStats instance will be invalid (have member valid = false).
   */
  private[impl] def chooseUnorderedCategoricalSplit(
      binAggregates: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split],
      parentCalculator: Option[ImpurityCalculator] = None): (Split, ImpurityStats) = {
    // Unordered categorical feature
    val nodeFeatureOffset = binAggregates.getFeatureOffset(featureIndexIdx)
    val numSplits = binAggregates.metadata.numSplits(featureIndex)
    val parentCalc = parentCalculator.getOrElse(binAggregates.getParentImpurityCalculator())
    val (bestFeatureSplitIndex, bestFeatureGainStats) =
      Range(0, numSplits).map { splitIndex =>
        val leftChildStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, splitIndex)
        val rightChildStats = binAggregates.getParentImpurityCalculator()
          .subtract(leftChildStats)
        val gainAndImpurityStats = ImpurityUtils.calculateImpurityStats(parentCalc,
          leftChildStats, rightChildStats, binAggregates.metadata)
        (splitIndex, gainAndImpurityStats)
      }.maxBy(_._2.gain)
    (featureSplits(bestFeatureSplitIndex), bestFeatureGainStats)

  }

  /**
   * Choose splitting rule: feature value <= threshold
   *
   * @return  (best split, statistics for split)  If the best split actually puts all instances
   *          in one leaf node, then it will be set to None.  If no valid split was found, the
   *          returned ImpurityStats instance will be invalid (have member valid = false)
   */
  private[impl] def chooseContinuousSplit(
      binAggregates: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split],
      parentCalculator: Option[ImpurityCalculator] = None): (Split, ImpurityStats) = {
    // For a continuous feature, bins are already sorted for splitting
    // Number of "categories" = number of bins
    val sortedCategories = Range(0, binAggregates.metadata.numBins(featureIndex)).toList
    // Get & return best split info
    val (bestFeatureSplitIndex, bestFeatureGainStats) = orderedSplitHelper(binAggregates,
      featureIndex, featureIndexIdx, sortedCategories, parentCalculator)
    (featureSplits(bestFeatureSplitIndex), bestFeatureGainStats)
  }

  /**
   * Computes the index of the best split for an ordered feature.
   * @param parentCalculator Optional: ImpurityCalculator containing impurity stats for current node
   */
  private def orderedSplitHelper(
      binAggregates: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int,
      categoriesSortedByCentroid: List[Int],
      parentCalculator: Option[ImpurityCalculator]): (Int, ImpurityStats) = {
    // Cumulative sum (scanLeft) of bin statistics.
    // Afterwards, binAggregates for a bin is the sum of aggregates for
    // that bin + all preceding bins.
    val numSplits = binAggregates.metadata.numSplits(featureIndex)
    val nodeFeatureOffset = binAggregates.getFeatureOffset(featureIndexIdx)
    var splitIndex = 0
    while (splitIndex < numSplits) {
      val currentCategory = categoriesSortedByCentroid(splitIndex)
      val nextCategory = categoriesSortedByCentroid(splitIndex + 1)
      binAggregates.mergeForFeature(nodeFeatureOffset, nextCategory, currentCategory)
      splitIndex += 1
    }
    // lastCategory = index of bin with total aggregates for this (node, feature)
    val lastCategory = categoriesSortedByCentroid.last

    // Find best split.
    val parentCalc = parentCalculator.getOrElse(binAggregates.getParentImpurityCalculator())
    Range(0, numSplits).map { splitIndex =>
      val featureValue = categoriesSortedByCentroid(splitIndex)
      val leftChildStats =
        binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
      val rightChildStats =
        binAggregates.getImpurityCalculator(nodeFeatureOffset, lastCategory)
      rightChildStats.subtract(leftChildStats)
      val gainAndImpurityStats = ImpurityUtils.calculateImpurityStats(parentCalc,
        leftChildStats, rightChildStats, binAggregates.metadata)
      (splitIndex, gainAndImpurityStats)
    }.maxBy(_._2.gain)
  }

  /**
   * Choose the best split for an ordered categorical feature.
   * @param parentCalculator Optional: ImpurityCalculator containing impurity stats for current node
   */
  private[impl] def chooseOrderedCategoricalSplit(
      binAggregates: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int,
      parentCalculator: Option[ImpurityCalculator] = None): (Split, ImpurityStats) = {
    // Sort feature categories by label centroid
    val categoriesSortedByCentroid = sortByCentroid(binAggregates, featureIndex, featureIndexIdx)
    // Get index, stats of best split
    val (bestFeatureSplitIndex, bestFeatureGainStats) = orderedSplitHelper(binAggregates,
      featureIndex, featureIndexIdx, categoriesSortedByCentroid, parentCalculator)
    // Create result (CategoricalSplit instance)
    val categoriesForSplit =
      categoriesSortedByCentroid.map(_.toDouble).slice(0, bestFeatureSplitIndex + 1)
    val numCategories = binAggregates.metadata.featureArity(featureIndex)
    val bestFeatureSplit =
      new CategoricalSplit(featureIndex, categoriesForSplit.toArray, numCategories)
    (bestFeatureSplit, bestFeatureGainStats)
  }

  /**
   * Choose the best split for a feature at a node.
   *
   * @param parentCalculator Optional: ImpurityCalculator containing impurity stats for current node
   * @return  (best split, statistics for split)  If no valid split was found, the returned
   *          ImpurityStats will have member stats.valid = false.
   */
  private[impl] def chooseSplit(
      statsAggregator: DTStatsAggregator,
      featureIndex: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split],
      parentCalculator: Option[ImpurityCalculator] = None): (Split, ImpurityStats) = {
    val metadata = statsAggregator.metadata
    if (metadata.isCategorical(featureIndex)) {
      if (metadata.isUnordered(featureIndex)) {
        SplitUtils.chooseUnorderedCategoricalSplit(statsAggregator, featureIndex,
          featureIndexIdx, featureSplits, parentCalculator)
      } else {
        SplitUtils.chooseOrderedCategoricalSplit(statsAggregator, featureIndex,
          featureIndexIdx, parentCalculator)
      }
    } else {
      SplitUtils.chooseContinuousSplit(statsAggregator, featureIndex, featureIndexIdx,
        featureSplits, parentCalculator)
    }

  }

}
