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
import org.apache.spark.mllib.tree.configuration.DTClassifierParams
import org.apache.spark.mllib.tree.impurity.ClassificationImpurities
import org.apache.spark.mllib.tree.model.{InformationGainStats, Bin, DecisionTreeClassifierModel}
import org.apache.spark.rdd.RDD


/**
 * :: Experimental ::
 * A class that implements a decision tree algorithm for classification.
 * It supports both continuous and categorical features.
 * @param params The configuration parameters for the tree algorithm.
 */
@Experimental
class DecisionTreeClassifier (params: DTClassifierParams) extends DecisionTree(params) {

  private val impurityFunctor = ClassificationImpurities.impurity(params.impurity)

  /**
   * Method to train a decision tree model over an RDD
   * @param input RDD of [[org.apache.spark.mllib.regression.LabeledPoint]] used as training data
   * @param datasetInfo  Dataset metadata specifying number of classes, features, etc.
   * @return a DecisionTreeClassifierModel that can be used for prediction
   */
  def run(
      input: RDD[LabeledPoint],
      datasetInfo: DatasetInfo): DecisionTreeClassifierModel = {

    require(datasetInfo.isClassification)
    logDebug("algo = Classification")

    val topNode = super.runSub(input, datasetInfo)
    new DecisionTreeClassifierModel(topNode)
  }

  protected def computeCentroidForCategories(
      featureIndex: Int,
      sampledInput: Array[LabeledPoint],
      datasetInfo: DatasetInfo): Map[Double,Double] = {
    if (datasetInfo.isMulticlass) {
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
   * @param binData Aggregate array slice from getBinDataForNode.
   *                For unordered features, this is leftChildData ++ rightChildData,
   *                 each of which is indexed by (feature, split/bin, class),
   *                 with class being the least significant bit.
   *                For ordered features, this is of size numClasses * numBins * numFeatures.
   * @return (leftNodeAgg, rightNodeAgg) tuple of type (Array[Array[Array[Double\]\]\],
   *         Array[Array[Array[Double\]\]\]) where each array is of size(numFeature,
   *         (numBins - 1), numClasses)
   */
  protected def extractLeftRightNodeAggregates(
      binData: Array[Double],
      datasetInfo: DatasetInfo,
      numBins: Int): (Array[Array[Array[Double]]], Array[Array[Array[Double]]]) = {

    def findAggForOrderedFeatureClassification(
        leftNodeAgg: Array[Array[Array[Double]]],
        rightNodeAgg: Array[Array[Array[Double]]],
        featureIndex: Int) {

      // shift for this featureIndex
      val numClasses = datasetInfo.numClasses
      val shift = numClasses * featureIndex * numBins

      var classIndex = 0
      while (classIndex < numClasses) {
        // left node aggregate for the lowest split
        leftNodeAgg(featureIndex)(0)(classIndex) = binData(shift + classIndex)
        // right node aggregate for the highest split
        rightNodeAgg(featureIndex)(numBins - 2)(classIndex)
          = binData(shift + (numClasses * (numBins - 1)) + classIndex)
        classIndex += 1
      }

      // Iterate over all splits.
      var splitIndex = 1
      while (splitIndex < numBins - 1) {
        // calculating left node aggregate for a split as a sum of left node aggregate of a
        // lower split and the left bin aggregate of a bin where the split is a high split
        var innerClassIndex = 0
        while (innerClassIndex < numClasses) {
          leftNodeAgg(featureIndex)(splitIndex)(innerClassIndex)
            = binData(shift + numClasses * splitIndex + innerClassIndex) +
            leftNodeAgg(featureIndex)(splitIndex - 1)(innerClassIndex)
          rightNodeAgg(featureIndex)(numBins - 2 - splitIndex)(innerClassIndex) =
            binData(shift + (numClasses * (numBins - 1 - splitIndex) + innerClassIndex)) +
              rightNodeAgg(featureIndex)(numBins - 1 - splitIndex)(innerClassIndex)
          innerClassIndex += 1
        }
        splitIndex += 1
      }
    }

    /**
     * Reshape binData for this feature.
     * Indexes binData as (feature, split, class) with class as the least significant bit.
     * @param leftNodeAgg   leftNodeAgg(featureIndex)(splitIndex)(classIndex) = aggregate value
     */
    def findAggForUnorderedFeatureClassification(
        leftNodeAgg: Array[Array[Array[Double]]],
        rightNodeAgg: Array[Array[Array[Double]]],
        featureIndex: Int) {

      val rightChildShift = datasetInfo.numClasses * numBins * datasetInfo.numFeatures
      var splitIndex = 0
      while (splitIndex < numBins - 1) {
        var classIndex = 0
        while (classIndex < datasetInfo.numClasses) {
          // shift for this featureIndex
          val shift =
            datasetInfo.numClasses * featureIndex * numBins + splitIndex * datasetInfo.numClasses
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
      Array.ofDim[Double](datasetInfo.numFeatures, numBins - 1, datasetInfo.numClasses)
    val rightNodeAgg =
      Array.ofDim[Double](datasetInfo.numFeatures, numBins - 1, datasetInfo.numClasses)
    var featureIndex = 0
    while (featureIndex < datasetInfo.numFeatures) {
      if (datasetInfo.isMulticlassWithCategoricalFeatures){
        val isFeatureContinuous = datasetInfo.categoricalFeaturesInfo.get(featureIndex).isEmpty
        if (isFeatureContinuous) {
          findAggForOrderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
        } else {
          val featureCategories = datasetInfo.categoricalFeaturesInfo(featureIndex)
          val isSpaceSufficientForAllCategoricalSplits =
            numBins > math.pow(2, featureCategories.toInt - 1) - 1
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

  /**
   * Get number of values to be stored per node in the bin aggregate counts.
   * @param datasetInfo  Dataset metadata
   * @param numBins      Number of bins = 1 + number of possible splits.
   * @return
   */
  protected def getElementsPerNode(
      datasetInfo: DatasetInfo,
      numBins: Int): Int = {
    if (datasetInfo.isMulticlassWithCategoricalFeatures) {
      2 * datasetInfo.numClasses * numBins * datasetInfo.numFeatures
    } else {
      datasetInfo.numClasses * numBins * datasetInfo.numFeatures
    }
  }

  /**
   * Performs a sequential aggregation over a partition for classification.
   * For l nodes, k features,
   * either the left count or the right count of one of the p bins is
   * incremented based upon whether the feature is classified as 0 or 1.
   * @param agg Array storing aggregate calculation, of size:
   *            numClasses * numBins * numFeatures * numNodes for ordered features, or
   *            2 * numClasses * numBins * numFeatures * numNodes for unordered features
   * @param arr  Bin mapping from findBinsForLevel.
   *             Array of size 1 + (numFeatures * numNodes).
   * @return Array storing aggregate calculation, of size:
   *
   */
  protected def binSeqOpSub(
      agg: Array[Double],
      arr: Array[Double],
      datasetInfo: DatasetInfo,
      numNodes: Int,
      bins: Array[Array[Bin]]): Array[Double] = {
    val numBins = bins(0).length
    if(datasetInfo.isMulticlassWithCategoricalFeatures) {
      multiclassWithCategoricalBinSeqOp(arr, agg, datasetInfo, numNodes, bins)
    } else {
      binaryOrNoCategoricalBinSeqOp(arr, agg, datasetInfo, numNodes, numBins)
    }
    agg
  }

  /**
   * Calculates the information gain for all splits based upon left/right split aggregates.
   * @param leftNodeAgg Left node aggregates:
   *                    leftNodeAgg(feature)(split)(class) = weight of examples
   * @param featureIndex feature index
   * @param splitIndex split index
   * @param rightNodeAgg Right node aggregates:
   *                     rightNodeAgg(feature)(split)(class) = weight of examples
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

    val numClasses = datasetInfo.numClasses

    val leftCounts: Array[Double] = leftNodeAgg(featureIndex)(splitIndex)
    val rightCounts: Array[Double] = rightNodeAgg(featureIndex)(splitIndex)
    var leftTotalCount = leftCounts.sum
    var rightTotalCount = rightCounts.sum

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

    val totalCount = leftTotalCount + rightTotalCount
    if (totalCount == 0) {
      // Return arbitrary prediction.
      return new InformationGainStats(0, topImpurity, topImpurity, topImpurity, 0)
    }

    // Sum of count for each label
    val leftRightCounts: Array[Double] =
      leftCounts.zip(rightCounts).map{ case (leftCount, rightCount) => leftCount + rightCount }

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

    val leftImpurity = if (leftTotalCount == 0) {
      topImpurity
    } else {
      impurityFunctor.calculate(leftCounts, leftTotalCount)
    }
    val rightImpurity = if (rightTotalCount == 0) {
      topImpurity
    } else {
      impurityFunctor.calculate(rightCounts, rightTotalCount)
    }

    val leftWeight = leftTotalCount / totalCount
    val rightWeight = rightTotalCount / totalCount

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

    new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict, prob)
  }

  /**
   * Get bin data for one node.
   *
   * @param node  Node index in this (level, group).
   * @param binAggregates  For unordered features,
   *                        the first half of binAggregates contains leftChildData,
   *                        and the second half contains rightChildData.
   *                        Each half is of size numNodes * numFeatures * numBins * numClasses.
   *                       For ordered features,
   *                        this is of size numNodes * numFeatures * numBins * numClasses.
   *                       Indexing uses node as the most significant bit.
   * @return  For unordered features, returns leftChildData ++ rightChildData,
   *           each of which is indexed by (feature, bin/split, class),
   *           with class being the least significant bit.
   *          For ordered features, returns data of size numClasses * numBins * numFeatures.
   */
  protected def getBinDataForNode(
      node: Int,
      binAggregates: Array[Double],
      datasetInfo: DatasetInfo,
      numNodes: Int,
      numBins: Int): Array[Double] = {
    if (datasetInfo.isMulticlassWithCategoricalFeatures) {
      val shift = datasetInfo.numClasses * node * numBins * datasetInfo.numFeatures
      val rightChildShift = datasetInfo.numClasses * numBins * datasetInfo.numFeatures * numNodes
      val binsForNode = {
        val leftChildData = binAggregates.slice(
            shift,
            shift + datasetInfo.numClasses * numBins * datasetInfo.numFeatures)
        val rightChildData = binAggregates.slice(
            rightChildShift + shift,
            rightChildShift + shift + datasetInfo.numClasses * numBins * datasetInfo.numFeatures)
        leftChildData ++ rightChildData
      }
      binsForNode
    } else {
      val shift = datasetInfo.numClasses * node * numBins * datasetInfo.numFeatures
      val binsForNode = binAggregates.slice(
        shift,
        shift + datasetInfo.numClasses * numBins * datasetInfo.numFeatures)
      binsForNode
    }
  }

  /**
   * Increment aggregate in location for (node, feature, bin, label)
   * to indicate that, for this (example,
   * @param arr  Bin mapping from findBinsForLevel.  arr(0) stores the class label.
   *             Array of size 1 + (numFeatures * numNodes).
   * @param agg  Array storing aggregate calculation, of size:
   *             numClasses * numBins * numFeatures * numNodes.
   *             Indexed by (node, feature, bin, label) where label is the least significant bit.
   */
  private def updateBinForOrderedFeature(
      arr: Array[Double],
      agg: Array[Double],
      nodeIndex: Int,
      label: Double,
      featureIndex: Int,
      datasetInfo: DatasetInfo,
      numBins: Int) = {

    // Find the bin index for this feature.
    val arrIndex = 1 + datasetInfo.numFeatures * nodeIndex + featureIndex
    // Update the left or right count for one bin.
    val aggShift = datasetInfo.numClasses * numBins * datasetInfo.numFeatures * nodeIndex
    val aggIndex = aggShift + datasetInfo.numClasses * featureIndex * numBins +
      arr(arrIndex).toInt * datasetInfo.numClasses
    agg(aggIndex + label.toInt) += 1
  }

  /**
   *
   * @param arr  arr(0) = label.
   *             arr(1 + featureIndex + nodeIndex * numFeatures) = feature value (category)
   * @param agg  Indexed by (node, feature, bin, label) where label is the least significant bit.
   * @param rightChildShift
   * @param bins
   */
  private def updateBinForUnorderedFeature(
      arr: Array[Double],
      agg: Array[Double],
      nodeIndex: Int,
      featureIndex: Int,
      label: Double,
      rightChildShift: Int,
      datasetInfo: DatasetInfo,
      numBins: Int,
      bins: Array[Array[Bin]]) = {

    // Find the bin index for this feature.
    val arrIndex = 1 + datasetInfo.numFeatures * nodeIndex + featureIndex
    val featureValue = arr(arrIndex)
    // Update the left or right count for one bin.
    val aggShift =
      nodeIndex * datasetInfo.numFeatures * numBins * datasetInfo.numClasses +
      featureIndex * numBins * datasetInfo.numClasses +
      label.toInt
    // Find all matching bins and increment their values
    val featureCategories = datasetInfo.categoricalFeaturesInfo(featureIndex)
    val numCategoricalBins = math.pow(2.0, featureCategories - 1).toInt - 1
    var binIndex = 0
    while (binIndex < numCategoricalBins) {
      val aggIndex = aggShift + binIndex * datasetInfo.numClasses
      if (bins(featureIndex)(binIndex).highSplit.categories.contains(featureValue)) {
        agg(aggIndex) += 1
      } else {
        agg(rightChildShift + aggIndex) += 1
      }
      binIndex += 1
    }
  }

  /**
   * Helper for binSeqOp
   * @param arr  Bin mapping from findBinsForLevel. arr(0) stores the class label.
   *             Array of size 1 + (numFeatures * numNodes).
   * @param agg  Array storing aggregate calculation, of size:
   *             numClasses * numBins * numFeatures * numNodes
   * @param datasetInfo
   * @param numNodes
   * @param numBins
   */
  private def binaryOrNoCategoricalBinSeqOp(
      arr: Array[Double],
      agg: Array[Double],
      datasetInfo: DatasetInfo,
      numNodes: Int,
      numBins: Int) = {
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
          updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, datasetInfo, numBins)
          featureIndex += 1
        }
      }
      nodeIndex += 1
    }
  }

  /**
   * Helper for binSeqOp.
   *
   * @param arr  Bin mapping from findBinsForLevel. arr(0) stores the class label.
   *             Array of size 1 + (numFeatures * numNodes).
   * @param agg Array storing aggregate calculation of size
   *            numClasses * numBins * numFeatures * numNodes
   *            // Size set by getElementsPerNode():
   *            //   2 * numClasses * numBins * numFeatures * numNodes
   *           SHOULD BE indexed by (node, feature, bin, class),
   *            with class being the least significant bit. (based on future use)
   * @param datasetInfo  Dataset metadata.
   * @param numNodes     Number of nodes in this (level, group).
   * @param bins
   */
  private def multiclassWithCategoricalBinSeqOp(
      arr: Array[Double],
      agg: Array[Double],
      datasetInfo: DatasetInfo,
      numNodes: Int,
      bins: Array[Array[Bin]]) = {
    val numBins = bins(0).length
    // Iterate over all nodes.
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      // Check whether the instance was valid for this nodeIndex.
      val validSignalIndex = 1 + datasetInfo.numFeatures * nodeIndex
      val isSampleValidForNode = arr(validSignalIndex) != InvalidBinIndex
      if (isSampleValidForNode) {
        val rightChildShift = datasetInfo.numClasses * numBins * datasetInfo.numFeatures * numNodes
        // actual class label
        val label = arr(0)
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < datasetInfo.numFeatures) {
          val isFeatureContinuous = datasetInfo.categoricalFeaturesInfo.get(featureIndex).isEmpty
          if (isFeatureContinuous) {
            updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, datasetInfo,
              numBins)
          } else {
            val featureCategories = datasetInfo.categoricalFeaturesInfo(featureIndex)
            val isSpaceSufficientForAllCategoricalSplits =
              numBins > math.pow(2, featureCategories.toInt - 1) - 1
            if (isSpaceSufficientForAllCategoricalSplits) {
              updateBinForUnorderedFeature(arr, agg, nodeIndex, featureIndex, label,
                rightChildShift, datasetInfo, numBins, bins)
            } else {
              updateBinForOrderedFeature(arr, agg, nodeIndex, label, featureIndex, datasetInfo,
                numBins)
            }
          }
          featureIndex += 1
        }
      }
      nodeIndex += 1
    }
  }

}


@Experimental
object DecisionTreeClassifier extends Serializable with Logging {

  /**
   * Get a default set of parameters for [[org.apache.spark.mllib.tree.DecisionTreeClassifier]].
   */
  def defaultParams(): DTClassifierParams = {
    new DTClassifierParams()
  }

  /**
   * Train a decision tree model for binary or multiclass classification,
   * using the default set of learning parameters.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should take values {0, 1, ..., numClasses-1}.
   * @param datasetInfo Dataset metadata (number of features, number of classes, etc.)
   * @return DecisionTreeClassifierModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      datasetInfo: DatasetInfo): DecisionTreeClassifierModel = {
    require(datasetInfo.numClasses >= 2)
    new DecisionTreeClassifier(new DTClassifierParams()).run(input, datasetInfo)
  }

  /**
   * Train a decision tree model for binary or multiclass classification.
   *
   * @param input  Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *               Labels should take values {0, 1, ..., numClasses-1}.
   * @param datasetInfo Dataset metadata (number of features, number of classes, etc.)
   * @param params The configuration parameters for the tree learning algorithm
   *               (tree depth, quantile calculation strategy, etc.)
   * @return DecisionTreeClassifierModel which can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      datasetInfo: DatasetInfo,
      params: DTClassifierParams): DecisionTreeClassifierModel = {
    require(datasetInfo.numClasses >= 2)
    new DecisionTreeClassifier(params).run(input, datasetInfo)
  }

}
