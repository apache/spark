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
import org.apache.spark.Logging
import org.apache.spark.mllib.rdd.DatasetInfo
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{QuantileStrategies, DTRegressorParams}
import org.apache.spark.mllib.tree.impurity.{RegressionImpurities, RegressionImpurity}
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

  private val impurityFunctor = RegressionImpurities.impurity(params.impurity)

  /**
   * Method to train a decision tree model over an RDD
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @param datasetInfo  Dataset metadata specifying number of classes, features, etc.
   * @return a DecisionTreeRegressorModel that can be used for prediction
   */
  def run(
    input: RDD[LabeledPoint],
    datasetInfo: DatasetInfo): DecisionTreeRegressorModel = {

    require(datasetInfo.isRegression)
    logDebug("algo = Regression")

    val topNode = super.trainSub(input, datasetInfo)

    new DecisionTreeRegressorModel(topNode)
  }

  //===========================================================================
  //  Protected methods (abstract from DecisionTree)
  //===========================================================================

  protected def computeCentroidForCategories(
      featureIndex: Int,
      sampledInput: Array[LabeledPoint],
      datasetInfo: DatasetInfo): Map[Double,Double] = {
    // For categorical variables in regression, each bin is a category.
    // The bins are sorted and are ordered by calculating the centroid of their corresponding labels.
    sampledInput.map(lp => (lp.features(featureIndex), lp.label))
      .groupBy(_._1)
      .mapValues(x => x.map(_._2).sum / x.map(_._1).length)
  }

  /**
   * Extracts left and right split aggregates.
   * @param binData Array[Double] of size 2 * numFeatures * numBins
   * @return (leftNodeAgg, rightNodeAgg) tuple of type (Array[Array[Array[Double\]\]\],
   *         Array[Array[Array[Double\]\]\]) where each array is of size(numFeature,
   *         (numBins - 1), 3)
   */
  protected def extractLeftRightNodeAggregates(
      binData: Array[Double],
      datasetInfo: DatasetInfo,
      numBins: Int): (Array[Array[Array[Double]]], Array[Array[Array[Double]]]) = {

    // Initialize left and right split aggregates.
    val leftNodeAgg = Array.ofDim[Double](datasetInfo.numFeatures, numBins - 1, 3)
    val rightNodeAgg = Array.ofDim[Double](datasetInfo.numFeatures, numBins - 1, 3)
    // Iterate over all features.
    var featureIndex = 0
    while (featureIndex < datasetInfo.numFeatures) {
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
      datasetInfo: DatasetInfo,
      numBins: Int): Int = {
    3 * numBins * datasetInfo.numFeatures
  }

  /**
   * Performs a sequential aggregation of bins stats over a partition for regression.
   * For l nodes, k features,
   * the count, sum, sum of squares of one of the p bins is incremented.
   *
   * @param agg Array[Double] storing aggregate calculation of size
   *            3 * numBins * numFeatures * numNodes for classification
   * @param arr Array[Double] of size 1 + (numFeatures * numNodes)
   * @return Array[Double] storing aggregate calculation of size
   *         3 * numBins * numFeatures * numNodes for regression
   */
  protected def binSeqOpSub(
      agg: Array[Double],
      arr: Array[Double],
      datasetInfo: DatasetInfo,
      numNodes: Int,
      bins: Array[Array[Bin]]): Array[Double] = {
    val numBins = bins(0).length
    // Iterate over all nodes.
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      // Check whether the instance was valid for this nodeIndex.
      val validSignalIndex = 1 + datasetInfo.numFeatures * nodeIndex
      val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
      if (isSampleValidForNode) {
        // actual class label
        val label = arr(0)
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < datasetInfo.numFeatures) {
          // Find the bin index for this feature.
          val arrShift = 1 + datasetInfo.numFeatures * nodeIndex
          val arrIndex = arrShift + featureIndex
          // Update count, sum, and sum^2 for one bin.
          val aggShift = 3 * numBins * datasetInfo.numFeatures * nodeIndex
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
      datasetInfo: DatasetInfo,
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
      datasetInfo: DatasetInfo,
      numNodes: Int,
      numBins: Int): Array[Double] = {
    val shift = 3 * node * numBins * datasetInfo.numFeatures
    val binsForNode = binAggregates.slice(shift, shift + 3 * numBins * datasetInfo.numFeatures)
    binsForNode
  }

}


object DecisionTreeRegressor extends Serializable with Logging {

  /**
   * Get a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTreeRegressor]].
   */
  def defaultParams(): DTRegressorParams = {
    new DTRegressorParams()
  }

  /**
   * Train a decision tree model for regression.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should be real values.
   * @param datasetInfo Dataset metadata (number of features, number of classes, etc.)
   * @return DecisionTreeRegressorModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      datasetInfo: DatasetInfo): DecisionTreeRegressorModel = {
    new DecisionTreeRegressor(new DTRegressorParams()).run(input, datasetInfo)
  }

  /**
   * Train a decision tree model for regression.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should be real values.
   * @param datasetInfo Dataset metadata (number of features, number of classes, etc.)
   * @param params The configuration parameters for the tree learning algorithm
   *               (tree depth, quantile calculation strategy, etc.)
   * @return DecisionTreeRegressorModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      datasetInfo: DatasetInfo,
      params: DTRegressorParams = new DTRegressorParams()): DecisionTreeRegressorModel = {
    new DecisionTreeRegressor(params).run(input, datasetInfo)
  }

}
