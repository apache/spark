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
import org.apache.spark.mllib.tree.model.Bin
import org.apache.spark.rdd.RDD


/**
 * Internal representation of LabeledPoint for DecisionTree.
 * This bins feature values based on a subsampled of data as follows:
 *  (a) Continuous features are binned into ranges.
 *  (b) Unordered categorical features are binned based on subsets of feature values.
 *      "Unordered categorical features" are categorical features with low arity used in
 *      multiclass classification.
 *  (c) Ordered categorical features are binned based on feature values.
 *      "Ordered categorical features" are categorical features with high arity,
 *      or any categorical feature used in regression or binary classification.
 *
 * @param label  Label from LabeledPoint
 * @param binnedFeatures  Binned feature values.
 *                        Same length as LabeledPoint.features, but values are bin indices.
 */
private[tree] class TreePoint(val label: Double, val binnedFeatures: Array[Int])
  extends Serializable {
}

private[tree] object TreePoint {

  /**
   * Convert an input dataset into its TreePoint representation,
   * binning feature values in preparation for DecisionTree training.
   * @param input     Input dataset.
   * @param bins      Bins for features, of size (numFeatures, numBins).
   * @param metadata  Learning and dataset metadata
   * @return  TreePoint dataset representation
   */
  def convertToTreeRDD(
      input: RDD[LabeledPoint],
      bins: Array[Array[Bin]],
      metadata: DecisionTreeMetadata): RDD[TreePoint] = {
    // Construct arrays for featureArity for efficiency in the inner loop.
    val featureArity: Array[Int] = new Array[Int](metadata.numFeatures)
    var featureIndex = 0
    while (featureIndex < metadata.numFeatures) {
      featureArity(featureIndex) = metadata.featureArity.getOrElse(featureIndex, 0)
      featureIndex += 1
    }
    input.map { x =>
      TreePoint.labeledPointToTreePoint(x, bins, featureArity)
    }
  }

  /**
   * Convert one LabeledPoint into its TreePoint representation.
   * @param bins      Bins for features, of size (numFeatures, numBins).
   * @param featureArity  Array indexed by feature, with value 0 for continuous and numCategories
   *                      for categorical features.
   */
  private def labeledPointToTreePoint(
      labeledPoint: LabeledPoint,
      bins: Array[Array[Bin]],
      featureArity: Array[Int]): TreePoint = {
    val numFeatures = labeledPoint.features.size
    val arr = new Array[Int](numFeatures)
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      arr(featureIndex) = findBin(featureIndex, labeledPoint, featureArity(featureIndex),
        bins)
      featureIndex += 1
    }
    new TreePoint(labeledPoint.label, arr)
  }

  /**
   * Find bin for one (labeledPoint, feature).
   *
   * @param featureArity  0 for continuous features; number of categories for categorical features.
   * @param bins   Bins for features, of size (numFeatures, numBins).
   */
  private def findBin(
      featureIndex: Int,
      labeledPoint: LabeledPoint,
      featureArity: Int,
      bins: Array[Array[Bin]]): Int = {

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
        } else if (lowThreshold >= feature) {
          right = mid - 1
        } else {
          left = mid + 1
        }
      }
      -1
    }

    if (featureArity == 0) {
      // Perform binary search for finding bin for continuous features.
      val binIndex = binarySearchForBins()
      if (binIndex == -1) {
        throw new RuntimeException("No bin was found for continuous feature." +
          " This error can occur when given invalid data values (such as NaN)." +
          s" Feature index: $featureIndex.  Feature value: ${labeledPoint.features(featureIndex)}")
      }
      binIndex
    } else {
      // Categorical feature bins are indexed by feature values.
      val featureValue = labeledPoint.features(featureIndex)
      if (featureValue < 0 || featureValue >= featureArity) {
        throw new IllegalArgumentException(
          s"DecisionTree given invalid data:" +
            s" Feature $featureIndex is categorical with values in" +
            s" {0,...,${featureArity - 1}," +
            s" but a data point gives it value $featureValue.\n" +
            "  Bad data point: " + labeledPoint.toString)
      }
      featureValue.toInt
    }
  }
}
