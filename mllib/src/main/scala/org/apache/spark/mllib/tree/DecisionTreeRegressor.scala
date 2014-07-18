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

package org.apache.spark.mllib.tree

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.rdd.DatasetMetadata
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.DTRegressorParams
import org.apache.spark.mllib.tree.impurity.{RegressionImpurity, Variance}
import org.apache.spark.mllib.tree.model.{InformationGainStats, Bin, DecisionTreeRegressorModel}
import org.apache.spark.rdd.RDD


/**
 * :: Experimental ::
 * A class that implements a decision tree algorithm for regression.
 * It supports both continuous and categorical features.
 * @param params The configuration parameters for the tree algorithm.
 */
@Experimental
class DecisionTreeRegressor (params: DTRegressorParams)
  extends DecisionTree[DecisionTreeRegressorModel](params) {

  private val impurityFunctor = params.impurity match {
    case "variance" => Variance
    case _ => throw new IllegalArgumentException(s"Bad impurity parameter for regression: ${params.impurity}")
  }

  /**
   * Method to train a decision tree model over an RDD
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
   *                                number of discrete values they take. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It's important to note that features are
   *                                zero-indexed.
   * @return a DecisionTreeRegressorModel that can be used for prediction
   */
  def train(
    input: RDD[LabeledPoint],
    categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int]()): DecisionTreeRegressorModel = {

    logDebug("algo = Regression")

    val topNode = super.trainSub(input, 0, categoricalFeaturesInfo)

    new DecisionTreeRegressorModel(topNode)
  }

  //===========================================================================
  //  Protected methods (abstract from DecisionTree)
  //===========================================================================

  protected def computeCentroidForCategories(
      featureIndex: Int,
      sampledInput: Array[LabeledPoint],
      dsMeta: DatasetMetadata): Map[Double,Double] = {
    // For categorical variables in regression, each bin is a category.
    // The bins are sorted and are ordered by calculating the centroid of their corresponding labels.
    sampledInput.map(lp => (lp.features(featureIndex), lp.label))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.map(_._1).length)
  }

  /**
   * Extracts left and right split aggregates.
   * @param binData Array[Double] of size 2*numFeatures*numSplits
   * @return (leftNodeAgg, rightNodeAgg) tuple of type (Array[Array[Array[Double\]\]\],
   *         Array[Array[Array[Double\]\]\]) where each array is of size(numFeature,
   *         (numBins - 1), numClasses)
   */
  protected def extractLeftRightNodeAggregates(
      binData: Array[Double],
      dsMeta: DatasetMetadata,
      numBins: Int): (Array[Array[Array[Double]]], Array[Array[Array[Double]]]) = {

    // Initialize left and right split aggregates.
    val leftNodeAgg = Array.ofDim[Double](dsMeta.numFeatures, numBins - 1, 3)
    val rightNodeAgg = Array.ofDim[Double](dsMeta.numFeatures, numBins - 1, 3)
    // Iterate over all features.
    var featureIndex = 0
    while (featureIndex < dsMeta.numFeatures) {
      // shift for this featureIndex
      val shift = 3 * featureIndex * numBins
      // left node aggregate for the lowest split
      leftNodeAgg(featureIndex)(0)(0) = binData(shift + 0)
      leftNodeAgg(featureIndex)(0)(1) = binData(shift + 1)
      leftNodeAgg(featureIndex)(0)(2) = binData(shift + 2)

      // right node aggregate for the highest split
      rightNodeAgg(featureIndex)(numBins - 2)(0) =
        binData(shift + (3 * (numBins - 1)))
      rightNodeAgg(featureIndex)(numBins - 2)(1) =
        binData(shift + (3 * (numBins - 1)) + 1)
      rightNodeAgg(featureIndex)(numBins - 2)(2) =
        binData(shift + (3 * (numBins - 1)) + 2)

      // Iterate over all splits.
      var splitIndex = 1
      while (splitIndex < numBins - 1) {
        var i = 0 // index for regression histograms
        while (i < 3) { // count, sum, sum^2
          // calculating left node aggregate for a split as a sum of left node aggregate of a
          // lower split and the left bin aggregate of a bin where the split is a high split
          leftNodeAgg(featureIndex)(splitIndex)(i) = binData(shift + 3 * splitIndex + i) +
            leftNodeAgg(featureIndex)(splitIndex - 1)(i)
          // calculating right node aggregate for a split as a sum of right node aggregate of a
          // higher split and the right bin aggregate of a bin where the split is a low split
          rightNodeAgg(featureIndex)(numBins - 2 - splitIndex)(i) =
            binData(shift + (3 * (numBins - 1 - splitIndex) + i)) +
              rightNodeAgg(featureIndex)(numBins - 1 - splitIndex)(i)
          i += 1
        }
        splitIndex += 1
      }
      featureIndex += 1
    }
    (leftNodeAgg, rightNodeAgg)
  }

  protected def getElementsPerNode(
      dsMeta: DatasetMetadata,
      numBins: Int): Int = {
    3 * numBins * dsMeta.numFeatures
  }

  /**
   * Performs a sequential aggregation of bins stats over a partition for regression.
   * For l nodes, k features,
   * the count, sum, sum of squares of one of the p bins is incremented.
   *
   * @param agg Array[Double] storing aggregate calculation of size
   *            3 * numSplits * numFeatures * numNodes for classification
   * @param arr Array[Double] of size 1 + (numFeatures * numNodes)
   * @return Array[Double] storing aggregate calculation of size
   *         3 * numSplits * numFeatures * numNodes for regression
   */
  protected def binSeqOpSub(
      agg: Array[Double],
      arr: Array[Double],
      dsMeta: DatasetMetadata,
      numNodes: Int,
      bins: Array[Array[Bin]]): Array[Double] = {
    val numBins = bins(0).length
    // Iterate over all nodes.
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      // Check whether the instance was valid for this nodeIndex.
      val validSignalIndex = 1 + dsMeta.numFeatures * nodeIndex
      val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
      if (isSampleValidForNode) {
        // actual class label
        val label = arr(0)
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < dsMeta.numFeatures) {
          // Find the bin index for this feature.
          val arrShift = 1 + dsMeta.numFeatures * nodeIndex
          val arrIndex = arrShift + featureIndex
          // Update count, sum, and sum^2 for one bin.
          val aggShift = 3 * numBins * dsMeta.numFeatures * nodeIndex
          val aggIndex = aggShift + 3 * featureIndex * numBins + arr(arrIndex).toInt * 3
          agg(aggIndex) = agg(aggIndex) + 1
          agg(aggIndex + 1) = agg(aggIndex + 1) + label
          agg(aggIndex + 2) = agg(aggIndex + 2) + label*label
          featureIndex += 1
        }
      }
      nodeIndex += 1
    }
    agg
  }

  /**
   * Calculates the information gain for all splits based upon left/right split aggregates.
   * @param leftNodeAgg left node aggregates
   * @param featureIndex feature index
   * @param splitIndex split index
   * @param rightNodeAgg right node aggregate
   * @param topImpurity impurity of the parent node
   * @return information gain and statistics for all splits
   */
  protected def calculateGainForSplit(
      leftNodeAgg: Array[Array[Array[Double]]],
      featureIndex: Int,
      splitIndex: Int,
      rightNodeAgg: Array[Array[Array[Double]]],
      topImpurity: Double,
      numClasses: Int,
      level: Int): InformationGainStats = {

    val leftCount = leftNodeAgg(featureIndex)(splitIndex)(0)
    val leftSum = leftNodeAgg(featureIndex)(splitIndex)(1)
    val leftSumSquares = leftNodeAgg(featureIndex)(splitIndex)(2)

    val rightCount = rightNodeAgg(featureIndex)(splitIndex)(0)
    val rightSum = rightNodeAgg(featureIndex)(splitIndex)(1)
    val rightSumSquares = rightNodeAgg(featureIndex)(splitIndex)(2)

    val impurity = {
      if (level > 0) {
        topImpurity
      } else {
        // Calculate impurity for root node.
        val count = leftCount + rightCount
        val sum = leftSum + rightSum
        val sumSquares = leftSumSquares + rightSumSquares
        impurityFunctor.calculate(count, sum, sumSquares)
      }
    }

    if (leftCount == 0) {
      return new InformationGainStats(0, topImpurity, Double.MinValue, topImpurity,
        rightSum / rightCount)
    }
    if (rightCount == 0) {
      return new InformationGainStats(0, topImpurity ,topImpurity,
        Double.MinValue, leftSum / leftCount)
    }

    val leftImpurity = impurityFunctor.calculate(leftCount, leftSum, leftSumSquares)
    val rightImpurity = impurityFunctor.calculate(rightCount, rightSum, rightSumSquares)

    val leftWeight = leftCount.toDouble / (leftCount + rightCount)
    val rightWeight = rightCount.toDouble / (leftCount + rightCount)

    val gain = {
      if (level > 0) {
        impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
      } else {
        impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
      }
    }

    val predict = (leftSum + rightSum) / (leftCount + rightCount)
    new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict)
  }

  /**
   * Get bin data for one node.
   */
  protected def getBinDataForNode(
      node: Int,
      binAggregates: Array[Double],
      dsMeta: DatasetMetadata,
      numNodes: Int,
      numBins: Int): Array[Double] = {
    val shift = 3 * node * numBins * dsMeta.numFeatures
    val binsForNode = binAggregates.slice(shift, shift + 3 * numBins * dsMeta.numFeatures)
    binsForNode
  }

  //===========================================================================
  //  Protected methods
  //===========================================================================

  /**
   * Performs a sequential aggregation over a partition for regression.
   */
  def regressionBinSeqOp(arr: Array[Double], agg: Array[Double]) = {
  }

}
