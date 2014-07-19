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
import org.apache.spark.mllib.rdd.DatasetMetadata
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.DTClassifierParams
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.impurity.{ClassificationImpurity, Entropy, Gini}
import org.apache.spark.mllib.tree.model.{InformationGainStats, Bin, DecisionTreeClassifierModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom


/**
 * :: Experimental ::
 * A class that implements a decision tree algorithm for classification.
 * It supports both continuous and categorical features.
 * @param params The configuration parameters for the tree algorithm.
 */
@Experimental
class DecisionTreeClassifier (params: DTClassifierParams)
  extends DecisionTree[DecisionTreeClassifierModel](params) {

  private val impurityFunctor = params.impurity

  /**
   * Method to train a decision tree model over an RDD
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   * @param dsMeta  Dataset metadata specifying number of classes, features, etc.
   * @return a DecisionTreeClassifierModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      dsMeta: DatasetMetadata): DecisionTreeClassifierModel = {

    require(dsMeta.isClassification)
    logDebug("algo = Classification")

    val topNode = super.trainSub(input, dsMeta)
    new DecisionTreeClassifierModel(topNode)
  }

  //===========================================================================
  //  Protected methods (abstract from DecisionTree)
  //===========================================================================

  protected def computeCentroidForCategories(
      featureIndex: Int,
      sampledInput: Array[LabeledPoint],
      dsMeta: DatasetMetadata): Map[Double,Double] = {
    if (dsMeta.isMulticlass) {
      // For categorical variables in multiclass classification,
      // each bin is a category. The bins are sorted and they
      // are ordered by calculating the impurity of their corresponding labels.
      sampledInput.map(lp => (lp.features(featureIndex), lp.label))
        .groupBy(_._1)
        .mapValues(x => x.groupBy(_._2).mapValues(x => x.size.toDouble))
        .map(x => (x._1, x._2.values.toArray))
        .map(x => (x._1, impurityFunctor.calculate(x._2,x._2.sum)))
    } else { // binary classification
      // For categorical variables in binary classification,
      // each bin is a category. The bins are sorted and they
      // are ordered by calculating the centroid of their corresponding labels.
      sampledInput.map(lp => (lp.features(featureIndex), lp.label))
        .groupBy(_._1)
        .mapValues(x => x.map(_._2).sum / x.map(_._1).length)
    }
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

    def findAggForOrderedFeatureClassification(
        leftNodeAgg: Array[Array[Array[Double]]],
        rightNodeAgg: Array[Array[Array[Double]]],
        featureIndex: Int) {

      // shift for this featureIndex
      val shift = dsMeta.numClasses * featureIndex * numBins

      var classIndex = 0
      while (classIndex < dsMeta.numClasses) {
        // left node aggregate for the lowest split
        leftNodeAgg(featureIndex)(0)(classIndex) = binData(shift + classIndex)
        // right node aggregate for the highest split
        rightNodeAgg(featureIndex)(numBins - 2)(classIndex)
          = binData(shift + (dsMeta.numClasses * (numBins - 1)) + classIndex)
        classIndex += 1
      }

      // Iterate over all splits.
      var splitIndex = 1
      while (splitIndex < numBins - 1) {
        // calculating left node aggregate for a split as a sum of left node aggregate of a
        // lower split and the left bin aggregate of a bin where the split is a high split
        var innerClassIndex = 0
        while (innerClassIndex < dsMeta.numClasses) {
          leftNodeAgg(featureIndex)(splitIndex)(innerClassIndex)
            = binData(shift + dsMeta.numClasses * splitIndex + innerClassIndex) +
            leftNodeAgg(featureIndex)(splitIndex - 1)(innerClassIndex)
          rightNodeAgg(featureIndex)(numBins - 2 - splitIndex)(innerClassIndex) =
            binData(shift + (dsMeta.numClasses * (numBins - 1 - splitIndex) + innerClassIndex)) +
              rightNodeAgg(featureIndex)(numBins - 1 - splitIndex)(innerClassIndex)
          innerClassIndex += 1
        }
        splitIndex += 1
      }
    }

    def findAggForUnorderedFeatureClassification(
        leftNodeAgg: Array[Array[Array[Double]]],
        rightNodeAgg: Array[Array[Array[Double]]],
        featureIndex: Int) {

      val rightChildShift = dsMeta.numClasses * numBins * dsMeta.numFeatures
      var splitIndex = 0
      while (splitIndex < numBins - 1) {
        var classIndex = 0
        while (classIndex < dsMeta.numClasses) {
          // shift for this featureIndex
          val shift =
            dsMeta.numClasses * featureIndex * numBins + splitIndex * dsMeta.numClasses
          val leftBinValue = binData(shift + classIndex)
          val rightBinValue = binData(rightChildShift + shift + classIndex)
          leftNodeAgg(featureIndex)(splitIndex)(classIndex) = leftBinValue
          rightNodeAgg(featureIndex)(splitIndex)(classIndex) = rightBinValue
          classIndex += 1
        }
        splitIndex += 1
      }
    }

    // Initialize left and right split aggregates.
    val leftNodeAgg =
      Array.ofDim[Double](dsMeta.numFeatures, numBins - 1, dsMeta.numClasses)
    val rightNodeAgg =
      Array.ofDim[Double](dsMeta.numFeatures, numBins - 1, dsMeta.numClasses)
    var featureIndex = 0
    while (featureIndex < dsMeta.numFeatures) {
      if (dsMeta.isMulticlassWithCategoricalFeatures){
        val isFeatureContinuous = dsMeta.categoricalFeaturesInfo.get(featureIndex).isEmpty
        if (isFeatureContinuous) {
          findAggForOrderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
        } else {
          val featureCategories = dsMeta.categoricalFeaturesInfo(featureIndex)
          val isSpaceSufficientForAllCategoricalSplits
          = numBins > math.pow(2, featureCategories.toInt - 1) - 1
          if (isSpaceSufficientForAllCategoricalSplits) {
            findAggForUnorderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
          } else {
            findAggForOrderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
          }
        }
      } else {
        findAggForOrderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
      }
      featureIndex += 1
    }

    (leftNodeAgg, rightNodeAgg)
  }

  protected def getElementsPerNode(
      dsMeta: DatasetMetadata,
      numBins: Int): Int = {
    if (dsMeta.isMulticlassWithCategoricalFeatures) {
      2 * dsMeta.numClasses * numBins * dsMeta.numFeatures
    } else {
      dsMeta.numClasses * numBins * dsMeta.numFeatures
    }
  }

  /**
   * Performs a sequential aggregation over a partition for classification.
   * For l nodes, k features,
   * either the left count or the right count of one of the p bins is
   * incremented based upon whether the feature is classified as 0 or 1.
   * @param agg Array[Double] storing aggregate calculation of size
   *            numClasses * numSplits * numFeatures*numNodes for classification
   * @param arr Array[Double] of size 1 + (numFeatures * numNodes)
   * @return Array[Double] storing aggregate calculation, of size:
   *         2 * numSplits * numFeatures * numNodes for ordered features, or
   *         2 * numClasses * numSplits * numFeatures * numNodes for unordered features
   */
  protected def binSeqOpSub(
      agg: Array[Double],
      arr: Array[Double],
      dsMeta: DatasetMetadata,
      numNodes: Int,
      bins: Array[Array[Bin]]): Array[Double] = {
    val numBins = bins(0).length
    if(dsMeta.isMulticlassWithCategoricalFeatures) {
      unorderedClassificationBinSeqOp(arr, agg, dsMeta, numNodes, bins)
    } else {
      orderedClassificationBinSeqOp(arr, agg, dsMeta, numNodes, numBins)
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

    var classIndex = 0
    val leftCounts: Array[Double] = new Array[Double](numClasses)
    val rightCounts: Array[Double] = new Array[Double](numClasses)
    var leftTotalCount = 0.0
    var rightTotalCount = 0.0
    while (classIndex < numClasses) {
      val leftClassCount = leftNodeAgg(featureIndex)(splitIndex)(classIndex)
      val rightClassCount = rightNodeAgg(featureIndex)(splitIndex)(classIndex)
      leftCounts(classIndex) = leftClassCount
      leftTotalCount += leftClassCount
      rightCounts(classIndex) = rightClassCount
      rightTotalCount += rightClassCount
      classIndex += 1
    }

    val impurity = {
      if (level > 0) {
        topImpurity
      } else {
        // Calculate impurity for root node.
        val rootNodeCounts = new Array[Double](numClasses)
        var classIndex = 0
        while (classIndex < numClasses) {
          rootNodeCounts(classIndex) = leftCounts(classIndex) + rightCounts(classIndex)
          classIndex += 1
        }
        impurityFunctor.calculate(rootNodeCounts, leftTotalCount + rightTotalCount)
      }
    }

    if (leftTotalCount == 0) {
      return new InformationGainStats(0, topImpurity, topImpurity, Double.MinValue, 1)
    }
    if (rightTotalCount == 0) {
      return new InformationGainStats(0, topImpurity, Double.MinValue, topImpurity, 1)
    }

    val leftImpurity = impurityFunctor.calculate(leftCounts, leftTotalCount)
    val rightImpurity = impurityFunctor.calculate(rightCounts, rightTotalCount)

    val leftWeight = leftTotalCount / (leftTotalCount + rightTotalCount)
    val rightWeight = rightTotalCount / (leftTotalCount + rightTotalCount)

    val gain = {
      if (level > 0) {
        impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
      } else {
        impurity - leftWeight * leftImpurity - rightWeight * rightImpurity
      }
    }

    val totalCount = leftTotalCount + rightTotalCount

    // Sum of count for each label
    val leftRightCounts: Array[Double]
    = leftCounts.zip(rightCounts)
      .map{case (leftCount, rightCount) => leftCount + rightCount}

    def indexOfLargestArrayElement(array: Array[Double]): Int = {
      val result = array.foldLeft(-1, Double.MinValue, 0) {
        case ((maxIndex, maxValue, currentIndex), currentValue) =>
          if(currentValue > maxValue) (currentIndex, currentValue, currentIndex + 1)
          else (maxIndex, maxValue, currentIndex + 1)
      }
      if (result._1 < 0) 0 else result._1
    }

    val predict = indexOfLargestArrayElement(leftRightCounts)
    val prob = leftRightCounts(predict) / totalCount

    new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict, prob)
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
    if (dsMeta.isMulticlassWithCategoricalFeatures) {
      val shift = dsMeta.numClasses * node * numBins * dsMeta.numFeatures
      val rightChildShift = dsMeta.numClasses * numBins * dsMeta.numFeatures * numNodes
      val binsForNode = {
        val leftChildData
        = binAggregates.slice(shift, shift + dsMeta.numClasses * numBins * dsMeta.numFeatures)
        val rightChildData
        = binAggregates.slice(rightChildShift + shift,
          rightChildShift + shift + dsMeta.numClasses * numBins * dsMeta.numFeatures)
        leftChildData ++ rightChildData
      }
      binsForNode
    } else {
      val shift = dsMeta.numClasses * node * numBins * dsMeta.numFeatures
      val binsForNode = binAggregates.slice(shift, shift + dsMeta.numClasses * numBins * dsMeta.numFeatures)
      binsForNode
    }
  }

  //===========================================================================
  //  Private methods
  //===========================================================================

  private def updateBinForOrderedFeature(
      arr: Array[Double],
      agg: Array[Double],
      nodeIndex: Int,
      label: Double,
      featureIndex: Int,
      dsMeta: DatasetMetadata,
      numBins: Int) = {

    // Find the bin index for this feature.
    val arrShift = 1 + dsMeta.numFeatures * nodeIndex
    val arrIndex = arrShift + featureIndex
    // Update the left or right count for one bin.
    val aggShift = dsMeta.numClasses * numBins * dsMeta.numFeatures * nodeIndex
    val aggIndex
      = aggShift + dsMeta.numClasses * featureIndex * numBins
        + arr(arrIndex).toInt * dsMeta.numClasses
    val labelInt = label.toInt
    agg(aggIndex + labelInt) = agg(aggIndex + labelInt) + 1
  }

  private def updateBinForUnorderedFeature(
      nodeIndex: Int,
      featureIndex: Int,
      arr: Array[Double],
      label: Double,
      agg: Array[Double],
      rightChildShift: Int,
      dsMeta: DatasetMetadata,
      numBins: Int,
      bins: Array[Array[Bin]]) = {

    // Find the bin index for this feature.
    val arrShift = 1 + dsMeta.numFeatures * nodeIndex
    val arrIndex = arrShift + featureIndex
    // Update the left or right count for one bin.
    val aggShift = dsMeta.numClasses * numBins * dsMeta.numFeatures * nodeIndex
    val aggIndex
    = aggShift + dsMeta.numClasses * featureIndex * numBins + arr(arrIndex).toInt * dsMeta.numClasses
    // Find all matching bins and increment their values
    val featureCategories = dsMeta.categoricalFeaturesInfo(featureIndex)
    val numCategoricalBins = math.pow(2.0, featureCategories - 1).toInt - 1
    var binIndex = 0
    while (binIndex < numCategoricalBins) {
      val labelInt = label.toInt
      if (bins(featureIndex)(binIndex).highSplit.categories.contains(labelInt)) {
        agg(aggIndex + binIndex)
          = agg(aggIndex + binIndex) + 1
      } else {
        agg(rightChildShift + aggIndex + binIndex)
          = agg(rightChildShift + aggIndex + binIndex) + 1
      }
      binIndex += 1
    }
  }

  /**
   * Helper for binSeqOp
   */
  private def orderedClassificationBinSeqOp(
      arr: Array[Double],
      agg: Array[Double],
      dsMeta: DatasetMetadata,
      numNodes: Int,
      numBins: Int) = {
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
          updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, dsMeta, numBins)
          featureIndex += 1
        }
      }
      nodeIndex += 1
    }
  }

  /**
   * Helper for binSeqOp
   */
  private def unorderedClassificationBinSeqOp(
      arr: Array[Double],
      agg: Array[Double],
      dsMeta: DatasetMetadata,
      numNodes: Int,
      bins: Array[Array[Bin]]) = {
    val numBins = bins(0).length
    // Iterate over all nodes.
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      // Check whether the instance was valid for this nodeIndex.
      val validSignalIndex = 1 + dsMeta.numFeatures * nodeIndex
      val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
      if (isSampleValidForNode) {
        val rightChildShift = dsMeta.numClasses * numBins * dsMeta.numFeatures * numNodes
        // actual class label
        val label = arr(0)
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < dsMeta.numFeatures) {
          val isFeatureContinuous = dsMeta.categoricalFeaturesInfo.get(featureIndex).isEmpty
          if (isFeatureContinuous) {
            updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, dsMeta, numBins)
          } else {
            val featureCategories = dsMeta.categoricalFeaturesInfo(featureIndex)
            val isSpaceSufficientForAllCategoricalSplits =
              numBins > math.pow(2, featureCategories.toInt - 1) - 1
            if (isSpaceSufficientForAllCategoricalSplits) {
              updateBinForUnorderedFeature(nodeIndex, featureIndex, arr, label, agg,
                rightChildShift, dsMeta, numBins, bins)
            } else {
              updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, dsMeta, numBins)
            }
          }
          featureIndex += 1
        }
      }
      nodeIndex += 1
    }
  }

}

object DecisionTreeClassifier extends Serializable with Logging {

  /**
   * Train a decision tree model for binary or multiclass classification.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should take values {0, 1, ..., numClasses-1}.
   * @param dsMeta Dataset metadata (number of features, number of classes, etc.)
   * @param params The configuration parameters for the tree learning algorithm
   *               (tree depth, quantile calculation strategy, etc.)
   * @return DecisionTreeClassifierModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      dsMeta: DatasetMetadata,
      params: DTClassifierParams = new DTClassifierParams()): DecisionTreeClassifierModel = {
    require(dsMeta.numClasses >= 2)
    new DecisionTreeClassifier(params).train(input, dsMeta)
  }

  /**
   * Train a decision tree model for binary or multiclass classification.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClasses Number of classes (label types) for classification.
   *                   Default = 2 (binary classification).
   * @param categoricalFeaturesInfo A map from each categorical variable to the
   *                                number of discrete values it takes. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It is important to note that features are
   *                                zero-indexed.
   *                                Default = treat all features as continuous.
   * @param params The configuration parameters for the tree learning algorithm
   *               (tree depth, quantile calculation strategy, etc.)
   * @return DecisionTreeClassifierModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      numClasses: Int = 2,
      categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
      params: DTClassifierParams = new DTClassifierParams()): DecisionTreeClassifierModel = {

    // Find the number of features by looking at the first sample.
    val numFeatures = input.first().features.size
    val dsMeta = new DatasetMetadata(numClasses, numFeatures, categoricalFeaturesInfo)

    train(input, dsMeta, params)
  }

  // TODO: Move elsewhere!
  protected def getImpurity(impurityName: String): ClassificationImpurity = {
    impurityName match {
      case "gini" => Gini
      case "entropy" => Entropy
      case _ => throw new IllegalArgumentException(
          s"Bad impurity parameter for classification: $impurityName")
    }
  }

  // TODO: Add various versions of train() function below.

  /**
   * Train a decision tree model for binary or multiclass classification.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClasses Number of classes (label types) for classification.
   * @param categoricalFeaturesInfo A map from each categorical variable to the
   *                                number of discrete values it takes. For example, an entry (n ->
   *                                k) implies the feature n is categorical with k categories 0,
   *                                1, 2, ... , k-1. It is important to note that features are
   *                                zero-indexed.
   * @param impurityName Criterion used for information gain calculation
   * @param maxDepth  Maximum depth of the tree
   * @param maxBins  Maximum number of bins used for splitting features
   * @param quantileStrategyName  Algorithm for calculating quantiles
   * @return DecisionTreeClassifierModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      impurityName: String,
      maxDepth: Int,
      maxBins: Int,
      quantileStrategyName: String,
      maxMemoryInMB: Int): DecisionTreeClassifierModel = {

    val impurity = getImpurity(impurityName)
    val quantileStrategy = getQuantileStrategy(quantileStrategyName)
    val params =
      new DTClassifierParams(impurity, maxDepth, maxBins, quantileStrategy, maxMemoryInMB)
    train(input, numClasses, categoricalFeaturesInfo, params)
  }

}
