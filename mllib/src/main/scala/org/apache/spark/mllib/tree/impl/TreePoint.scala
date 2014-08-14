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

package org.apache.spark.mllib.tree.impl

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.model.Bin
import org.apache.spark.rdd.RDD

/**
 *         Bins is an Array of [[org.apache.spark.mllib.tree.model.Bin]]
 *          of size (numFeatures, numBins).
 * TODO: ADD DOC
 */
private[tree] class TreePoint(val label: Double, val features: Array[Int]) extends Serializable {
}

private[tree] object TreePoint {

  def convertToTreeRDD(
      input: RDD[LabeledPoint],
      strategy: Strategy,
      bins: Array[Array[Bin]]): RDD[TreePoint] = {
    input.map { x =>
      TreePoint.labeledPointToTreePoint(x, strategy.isMulticlassClassification, bins,
        strategy.categoricalFeaturesInfo)
    }
  }

  def labeledPointToTreePoint(
      labeledPoint: LabeledPoint,
      isMulticlassClassification: Boolean,
      bins: Array[Array[Bin]],
      categoricalFeaturesInfo: Map[Int, Int]): TreePoint = {

    val numFeatures = labeledPoint.features.size
    val numBins = bins(0).size
    val arr = new Array[Int](numFeatures)
    var featureIndex = 0 // offset by 1 for label
    while (featureIndex < numFeatures) {
      val featureInfo = categoricalFeaturesInfo.get(featureIndex)
      val isFeatureContinuous = featureInfo.isEmpty
      if (isFeatureContinuous) {
        arr(featureIndex) = findBin(featureIndex, labeledPoint, isFeatureContinuous, false,
          bins, categoricalFeaturesInfo)
      } else {
        val featureCategories = featureInfo.get
        val isSpaceSufficientForAllCategoricalSplits
          = numBins > math.pow(2, featureCategories.toInt - 1) - 1
        val isUnorderedFeature =
          isMulticlassClassification && isSpaceSufficientForAllCategoricalSplits
        arr(featureIndex) = findBin(featureIndex, labeledPoint, isFeatureContinuous,
          isUnorderedFeature, bins, categoricalFeaturesInfo)
      }
      featureIndex += 1
    }

    new TreePoint(labeledPoint.label, arr)
  }


  /**
   * Find bin for one (labeledPoint, feature).
   *
   * @param featureIndex
   * @param labeledPoint
   * @param isFeatureContinuous
   * @param isUnorderedFeature  (only applies if feature is categorical)
   * @param bins  Bins is an Array of [[org.apache.spark.mllib.tree.model.Bin]]
   *              of size (numFeatures, numBins).
   * @param categoricalFeaturesInfo
   * @return
   */
  def findBin(
      featureIndex: Int,
      labeledPoint: LabeledPoint,
      isFeatureContinuous: Boolean,
      isUnorderedFeature: Boolean,
      bins: Array[Array[Bin]],
      categoricalFeaturesInfo: Map[Int, Int]): Int = {

    /**
     * Binary search helper method for continuous feature.
     */
    def binarySearchForBins(): Int = {
      val binForFeatures = bins(featureIndex)
      val feature = labeledPoint.features(featureIndex)
      var left = 0
      var right = binForFeatures.length - 1
      while (left <= right) {
        val mid = left + (right - left) / 2
        val bin = binForFeatures(mid)
        val lowThreshold = bin.lowSplit.threshold
        val highThreshold = bin.highSplit.threshold
        if ((lowThreshold < feature) && (highThreshold >= feature)) {
          return mid
        }
        else if (lowThreshold >= feature) {
          right = mid - 1
        }
        else {
          left = mid + 1
        }
      }
      -1
    }

    /**
     * Sequential search helper method to find bin for categorical feature in multiclass
     * classification. The category is returned since each category can belong to multiple
     * splits. The actual left/right child allocation per split is performed in the
     * sequential phase of the bin aggregate operation.
     */
    def sequentialBinSearchForUnorderedCategoricalFeatureInClassification(): Int = {
      labeledPoint.features(featureIndex).toInt
    }

    /**
     * Sequential search helper method to find bin for categorical feature
     * (for classification and regression).
     */
    def sequentialBinSearchForOrderedCategoricalFeature(): Int = {
      val featureCategories = categoricalFeaturesInfo(featureIndex)
      val featureValue = labeledPoint.features(featureIndex)
      var binIndex = 0
      while (binIndex < featureCategories) {
        val bin = bins(featureIndex)(binIndex)
        val categories = bin.highSplit.categories
        if (categories.contains(featureValue)) {
          return binIndex
        }
        binIndex += 1
      }
      if (featureValue < 0 || featureValue >= featureCategories) {
        throw new IllegalArgumentException(
          s"DecisionTree given invalid data:" +
            s" Feature $featureIndex is categorical with values in" +
            s" {0,...,${featureCategories - 1}," +
            s" but a data point gives it value $featureValue.\n" +
            "  Bad data point: " + labeledPoint.toString)
      }
      -1
    }

    if (isFeatureContinuous) {
      // Perform binary search for finding bin for continuous features.
      val binIndex = binarySearchForBins()
      if (binIndex == -1) {
        throw new UnknownError("no bin was found for continuous variable.")
      }
      binIndex
    } else {
      // Perform sequential search to find bin for categorical features.
      val binIndex = if (isUnorderedFeature) {
          sequentialBinSearchForUnorderedCategoricalFeatureInClassification()
        } else {
          sequentialBinSearchForOrderedCategoricalFeature()
        }
      if (binIndex == -1) {
        throw new UnknownError("no bin was found for categorical variable.")
      }
      binIndex
    }
  }
}
