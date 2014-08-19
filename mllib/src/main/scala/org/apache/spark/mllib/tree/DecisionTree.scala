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

import scala.collection.JavaConverters._

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.Logging
import org.apache.spark.mllib.rdd.RDDFunctions._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impl.{DecisionTreeMetadata, TimeTracker, TreePoint}
import org.apache.spark.mllib.tree.impurity.{Impurities, Impurity}
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.random.XORShiftRandom


/**
 * :: Experimental ::
 * A class which implements a decision tree learning algorithm for classification and regression.
 * It supports both continuous and categorical features.
 * @param strategy The configuration parameters for the tree algorithm which specify the type
 *                 of algorithm (classification, regression, etc.), feature type (continuous,
 *                 categorical), depth of the tree, quantile calculation strategy, etc.
 */
@Experimental
class DecisionTree (private val strategy: Strategy) extends Serializable with Logging {

  strategy.assertValid()

  /**
   * Method to train a decision tree model over an RDD
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(input: RDD[LabeledPoint]): DecisionTreeModel = {

    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    val retaggedInput = input.retag(classOf[LabeledPoint])
    val metadata = DecisionTreeMetadata.buildMetadata(retaggedInput, strategy)
    logDebug("algo = " + strategy.algo)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val (splits, bins) = DecisionTree.findSplitsBins(retaggedInput, metadata)
    val numBins = bins(0).length
    timer.stop("findSplitsBins")
    logDebug("numBins = " + numBins)

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, bins, metadata)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val numFeatures = metadata.numFeatures
    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    // the max number of nodes possible given the depth of the tree
    val maxNumNodes = (2 << maxDepth) - 1
    // Initialize an array to hold parent impurity calculations for each node.
    val parentImpurities = new Array[Double](maxNumNodes)
    // dummy value for top node (updated during first split calculation)
    val nodes = new Array[Node](maxNumNodes)

    // Calculate level for single group construction

    // Max memory usage for aggregates
    val maxMemoryUsage = strategy.maxMemoryInMB * 1024 * 1024
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    val numElementsPerNode = DecisionTree.getElementsPerNode(metadata, numBins)

    logDebug("numElementsPerNode = " + numElementsPerNode)
    val arraySizePerNode = 8 * numElementsPerNode // approx. memory usage for bin aggregate array
    val maxNumberOfNodesPerGroup = math.max(maxMemoryUsage / arraySizePerNode, 1)
    logDebug("maxNumberOfNodesPerGroup = " + maxNumberOfNodesPerGroup)
    // nodes at a level is 2^level. level is zero indexed.
    val maxLevelForSingleGroup = math.max(
      (math.log(maxNumberOfNodesPerGroup) / math.log(2)).floor.toInt, 0)
    logDebug("max level for single group = " + maxLevelForSingleGroup)

    timer.stop("init")

    /*
     * The main idea here is to perform level-wise training of the decision tree nodes thus
     * reducing the passes over the data from l to log2(l) where l is the total number of nodes.
     * Each data sample is handled by a particular node at that level (or it reaches a leaf
     * beforehand and is not used in later levels.
     */

    var level = 0
    var break = false
    while (level <= maxDepth && !break) {

      logDebug("#####################################")
      logDebug("level = " + level)
      logDebug("#####################################")

      // Find best split for all nodes at a level.
      timer.start("findBestSplits")
      val splitsStatsForLevel = DecisionTree.findBestSplits(treeInput, parentImpurities,
        metadata, level, nodes, splits, bins, maxLevelForSingleGroup, timer)
      timer.stop("findBestSplits")

      val levelNodeIndexOffset = (1 << level) - 1
      for ((nodeSplitStats, index) <- splitsStatsForLevel.view.zipWithIndex) {
        val nodeIndex = levelNodeIndexOffset + index
        val isLeftChild = level != 0 && nodeIndex % 2 == 1
        val parentNodeIndex = if (isLeftChild) { // -1 for root node
            (nodeIndex - 1) / 2
          } else {
            (nodeIndex - 2) / 2
          }
        // Extract info for this node (index) at the current level.
        timer.start("extractNodeInfo")
        extractNodeInfo(nodeSplitStats, level, index, nodes)
        timer.stop("extractNodeInfo")
        if (level != 0) {
          // Set parent.
          if (isLeftChild) {
            nodes(parentNodeIndex).leftNode = Some(nodes(nodeIndex))
          } else {
            nodes(parentNodeIndex).rightNode = Some(nodes(nodeIndex))
          }
        }
        // Extract info for nodes at the next lower level.
        timer.start("extractInfoForLowerLevels")
        extractInfoForLowerLevels(level, index, maxDepth, nodeSplitStats, parentImpurities)
        timer.stop("extractInfoForLowerLevels")
        logDebug("final best split = " + nodeSplitStats._1)
      }
      require((1 << level) == splitsStatsForLevel.length)
      // Check whether all the nodes at the current level at leaves.
      val allLeaf = splitsStatsForLevel.forall(_._2.gain <= 0)
      logDebug("all leaf = " + allLeaf)
      if (allLeaf) {
        break = true // no more tree construction
      } else {
        level += 1
      }
    }

    logDebug("#####################################")
    logDebug("Extracting tree model")
    logDebug("#####################################")

    // Initialize the top or root node of the tree.
    val topNode = nodes(0)
    // Build the full tree using the node info calculated in the level-wise best split calculations.
    topNode.build(nodes)

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    new DecisionTreeModel(topNode, strategy.algo)
  }

  /**
   * Extract the decision tree node information for the given tree level and node index
   */
  private def extractNodeInfo(
      nodeSplitStats: (Split, InformationGainStats),
      level: Int,
      index: Int,
      nodes: Array[Node]): Unit = {
    val split = nodeSplitStats._1
    val stats = nodeSplitStats._2
    val nodeIndex = (1 << level) - 1 + index
    val isLeaf = (stats.gain <= 0) || (level == strategy.maxDepth)
    val node = new Node(nodeIndex, stats.predict, isLeaf, Some(split), None, None, Some(stats))
    logDebug("Node = " + node)
    nodes(nodeIndex) = node
  }

  /**
   *  Extract the decision tree node information for the children of the node
   */
  private def extractInfoForLowerLevels(
      level: Int,
      index: Int,
      maxDepth: Int,
      nodeSplitStats: (Split, InformationGainStats),
      parentImpurities: Array[Double]): Unit = {

    if (level >= maxDepth) {
      return
    }

    val leftNodeIndex = (2 << level) - 1 + 2 * index
    val leftImpurity = nodeSplitStats._2.leftImpurity
    logDebug("leftNodeIndex = " + leftNodeIndex + ", impurity = " + leftImpurity)
    parentImpurities(leftNodeIndex) = leftImpurity

    val rightNodeIndex = leftNodeIndex + 1
    val rightImpurity = nodeSplitStats._2.rightImpurity
    logDebug("rightNodeIndex = " + rightNodeIndex + ", impurity = " + rightImpurity)
    parentImpurities(rightNodeIndex) = rightImpurity
  }
}

object DecisionTree extends Serializable with Logging {

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param strategy The configuration parameters for the tree algorithm which specify the type
   *                 of algorithm (classification, regression, etc.), feature type (continuous,
   *                 categorical), depth of the tree, quantile calculation strategy, etc.
   * @return DecisionTreeModel that can be used for prediction
  */
  def train(input: RDD[LabeledPoint], strategy: Strategy): DecisionTreeModel = {
    new DecisionTree(strategy).train(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo algorithm, classification or regression
   * @param impurity impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth)
    new DecisionTree(strategy).train(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo algorithm, classification or regression
   * @param impurity impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param numClassesForClassification number of classes for classification. Default value of 2.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClassesForClassification: Int): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClassesForClassification)
    new DecisionTree(strategy).train(input)
  }

  /**
   * Method to train a decision tree model.
   * The method supports binary and multiclass classification and regression.
   *
   * Note: Using [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   *       and [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   *       is recommended to clearly separate classification and regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              For classification, labels should take values {0, 1, ..., numClasses-1}.
   *              For regression, labels are real numbers.
   * @param algo classification or regression
   * @param impurity criterion used for information gain calculation
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * @param numClassesForClassification number of classes for classification. Default value of 2.
   * @param maxBins maximum number of bins used for splitting features
   * @param quantileCalculationStrategy  algorithm for calculating quantiles
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @return DecisionTreeModel that can be used for prediction
   */
  def train(
      input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClassesForClassification: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int,Int]): DecisionTreeModel = {
    val strategy = new Strategy(algo, impurity, maxDepth, numClassesForClassification, maxBins,
      quantileCalculationStrategy, categoricalFeaturesInfo)
    new DecisionTree(strategy).train(input)
  }

  /**
   * Method to train a decision tree model for binary or multiclass classification.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels should take values {0, 1, ..., numClasses-1}.
   * @param numClassesForClassification number of classes for classification.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "gini" (recommended) or "entropy".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @return DecisionTreeModel that can be used for prediction
   */
  def trainClassifier(
      input: RDD[LabeledPoint],
      numClassesForClassification: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    train(input, Classification, impurityType, maxDepth, numClassesForClassification, maxBins, Sort,
      categoricalFeaturesInfo)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.DecisionTree$#trainClassifier]]
   */
  def trainClassifier(
      input: JavaRDD[LabeledPoint],
      numClassesForClassification: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    trainClassifier(input.rdd, numClassesForClassification,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      impurity, maxDepth, maxBins)
  }

  /**
   * Method to train a decision tree model for regression.
   *
   * @param input Training dataset: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]].
   *              Labels are real numbers.
   * @param categoricalFeaturesInfo Map storing arity of categorical features.
   *                                E.g., an entry (n -> k) indicates that feature n is categorical
   *                                with k categories indexed from 0: {0, 1, ..., k-1}.
   * @param impurity Criterion used for information gain calculation.
   *                 Supported values: "variance".
   * @param maxDepth Maximum depth of the tree.
   *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   *                  (suggested value: 4)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 100)
   * @return DecisionTreeModel that can be used for prediction
   */
  def trainRegressor(
      input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    val impurityType = Impurities.fromString(impurity)
    train(input, Regression, impurityType, maxDepth, 0, maxBins, Sort, categoricalFeaturesInfo)
  }

  /**
   * Java-friendly API for [[org.apache.spark.mllib.tree.DecisionTree$#trainRegressor]]
   */
  def trainRegressor(
      input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int): DecisionTreeModel = {
    trainRegressor(input.rdd,
      categoricalFeaturesInfo.asInstanceOf[java.util.Map[Int, Int]].asScala.toMap,
      impurity, maxDepth, maxBins)
  }


  private val InvalidBinIndex = -1

  /**
   * Returns an array of optimal splits for all nodes at a given level. Splits the task into
   * multiple groups if the level-wise training task could lead to memory overflow.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.tree.impl.TreePoint]]
   * @param parentImpurities Impurities for all parent nodes for the current level
   * @param metadata Learning and dataset metadata
   * @param level Level of the tree
   * @param splits possible splits for all features
   * @param bins possible bins for all features
   * @param maxLevelForSingleGroup the deepest level for single-group level-wise computation.
   * @return array (over nodes) of splits with best split for each node at a given level.
   */
  protected[tree] def findBestSplits(
      input: RDD[TreePoint],
      parentImpurities: Array[Double],
      metadata: DecisionTreeMetadata,
      level: Int,
      nodes: Array[Node],
      splits: Array[Array[Split]],
      bins: Array[Array[Bin]],
      maxLevelForSingleGroup: Int,
      timer: TimeTracker = new TimeTracker): Array[(Split, InformationGainStats)] = {
    // split into groups to avoid memory overflow during aggregation
    if (level > maxLevelForSingleGroup) {
      // When information for all nodes at a given level cannot be stored in memory,
      // the nodes are divided into multiple groups at each level with the number of groups
      // increasing exponentially per level. For example, if maxLevelForSingleGroup is 10,
      // numGroups is equal to 2 at level 11 and 4 at level 12, respectively.
      val numGroups = 1 << level - maxLevelForSingleGroup
      logDebug("numGroups = " + numGroups)
      var bestSplits = new Array[(Split, InformationGainStats)](0)
      // Iterate over each group of nodes at a level.
      var groupIndex = 0
      while (groupIndex < numGroups) {
        val bestSplitsForGroup = findBestSplitsPerGroup(input, parentImpurities, metadata, level,
          nodes, splits, bins, timer, numGroups, groupIndex)
        bestSplits = Array.concat(bestSplits, bestSplitsForGroup)
        groupIndex += 1
      }
      bestSplits
    } else {
      findBestSplitsPerGroup(input, parentImpurities, metadata, level, nodes, splits, bins, timer)
    }
  }

  /**
   * Returns an array of optimal splits for a group of nodes at a given level
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.tree.impl.TreePoint]]
   * @param parentImpurities Impurities for all parent nodes for the current level
   * @param metadata Learning and dataset metadata
   * @param level Level of the tree
   * @param splits possible splits for all features
   * @param bins possible bins for all features, indexed as (numFeatures)(numBins)
   * @param numGroups total number of node groups at the current level. Default value is set to 1.
   * @param groupIndex index of the node group being processed. Default value is set to 0.
   * @return array of splits with best splits for all nodes at a given level.
   */
  private def findBestSplitsPerGroup(
      input: RDD[TreePoint],
      parentImpurities: Array[Double],
      metadata: DecisionTreeMetadata,
      level: Int,
      nodes: Array[Node],
      splits: Array[Array[Split]],
      bins: Array[Array[Bin]],
      timer: TimeTracker,
      numGroups: Int = 1,
      groupIndex: Int = 0): Array[(Split, InformationGainStats)] = {

    /*
     * The high-level descriptions of the best split optimizations are noted here.
     *
     * *Level-wise training*
     * We perform bin calculations for all nodes at the given level to avoid making multiple
     * passes over the data. Thus, for a slightly increased computation and storage cost we save
     * several iterations over the data especially at higher levels of the decision tree.
     *
     * *Bin-wise computation*
     * We use a bin-wise best split computation strategy instead of a straightforward best split
     * computation strategy. Instead of analyzing each sample for contribution to the left/right
     * child node impurity of every split, we first categorize each feature of a sample into a
     * bin. Each bin is an interval between a low and high split. Since each split, and thus bin,
     * is ordered (read ordering for categorical variables in the findSplitsBins method),
     * we exploit this structure to calculate aggregates for bins and then use these aggregates
     * to calculate information gain for each split.
     *
     * *Aggregation over partitions*
     * Instead of performing a flatMap/reduceByKey operation, we exploit the fact that we know
     * the number of splits in advance. Thus, we store the aggregates (at the appropriate
     * indices) in a single array for all bins and rely upon the RDD aggregate method to
     * drastically reduce the communication overhead.
     */

    // Common calculations for multiple nested methods:

    // numNodes:  Number of nodes in this (level of tree, group),
    //            where nodes at deeper (larger) levels may be divided into groups.
    val numNodes = (1 << level) / numGroups
    logDebug("numNodes = " + numNodes)

    // Find the number of features by looking at the first sample.
    val numFeatures = metadata.numFeatures
    logDebug("numFeatures = " + numFeatures)

    // numBins:  Number of bins = 1 + number of possible splits
    val numBins = bins(0).length
    logDebug("numBins = " + numBins)

    val numClasses = metadata.numClasses
    logDebug("numClasses = " + numClasses)

    val isMulticlass = metadata.isMulticlass
    logDebug("isMulticlass = " + isMulticlass)

    val isMulticlassWithCategoricalFeatures = metadata.isMulticlassWithCategoricalFeatures
    logDebug("isMultiClassWithCategoricalFeatures = " + isMulticlassWithCategoricalFeatures)

    // shift when more than one group is used at deep tree level
    val groupShift = numNodes * groupIndex

    /**
     * Get the node index corresponding to this data point.
     * This function mimics prediction, passing an example from the root node down to a node
     * at the current level being trained; that node's index is returned.
     *
     * @return  Leaf index if the data point reaches a leaf.
     *          Otherwise, last node reachable in tree matching this example.
     */
    def predictNodeIndex(node: Node, binnedFeatures: Array[Int]): Int = {
      if (node.isLeaf) {
        node.id
      } else {
        val featureIndex = node.split.get.feature
        val splitLeft = node.split.get.featureType match {
          case Continuous => {
            val binIndex = binnedFeatures(featureIndex)
            val featureValueUpperBound = bins(featureIndex)(binIndex).highSplit.threshold
            // bin binIndex has range (bin.lowSplit.threshold, bin.highSplit.threshold]
            // We do not need to check lowSplit since bins are separated by splits.
            featureValueUpperBound <= node.split.get.threshold
          }
          case Categorical => {
            val featureValue = if (metadata.isUnordered(featureIndex)) {
                binnedFeatures(featureIndex)
              } else {
                val binIndex = binnedFeatures(featureIndex)
                bins(featureIndex)(binIndex).category
              }
            node.split.get.categories.contains(featureValue)
          }
          case _ => throw new RuntimeException(s"predictNodeIndex failed for unknown reason.")
        }
        if (node.leftNode.isEmpty || node.rightNode.isEmpty) {
          // Return index from next layer of nodes to train
          if (splitLeft) {
            node.id * 2 + 1 // left
          } else {
            node.id * 2 + 2 // right
          }
        } else {
          if (splitLeft) {
            predictNodeIndex(node.leftNode.get, binnedFeatures)
          } else {
            predictNodeIndex(node.rightNode.get, binnedFeatures)
          }
        }
      }
    }

    def nodeIndexToLevel(idx: Int): Int = {
      if (idx == 0) {
        0
      } else {
        math.floor(math.log(idx) / math.log(2)).toInt
      }
    }

    // Used for treePointToNodeIndex
    val levelOffset = (1 << level) - 1

    /**
     * Find the node index for the given example.
     * Nodes are indexed from 0 at the start of this (level, group).
     * If the example does not reach this level, returns a value < 0.
     */
    def treePointToNodeIndex(treePoint: TreePoint): Int = {
      if (level == 0) {
        0
      } else {
        val globalNodeIndex = predictNodeIndex(nodes(0), treePoint.binnedFeatures)
        // Get index for this (level, group).
        globalNodeIndex - levelOffset - groupShift
      }
    }

    /**
     * Increment aggregate in location for (node, feature, bin, label).
     *
     * @param treePoint  Data point being aggregated.
     * @param agg  Array storing aggregate calculation, of size:
     *             numClasses * numBins * numFeatures * numNodes.
     *             Indexed by (node, feature, bin, label) where label is the least significant bit.
     * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
     */
    def updateBinForOrderedFeature(
        treePoint: TreePoint,
        agg: Array[Double],
        nodeIndex: Int,
        featureIndex: Int): Unit = {
      // Update the left or right count for one bin.
      val aggIndex =
        numClasses * numBins * numFeatures * nodeIndex +
        numClasses * numBins * featureIndex +
        numClasses * treePoint.binnedFeatures(featureIndex) +
        treePoint.label.toInt
      agg(aggIndex) += 1
    }

    /**
     * Increment aggregate in location for (nodeIndex, featureIndex, [bins], label),
     * where [bins] ranges over all bins.
     * Updates left or right side of aggregate depending on split.
     *
     * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
     * @param treePoint  Data point being aggregated.
     * @param agg  Indexed by (left/right, node, feature, bin, label)
     *             where label is the least significant bit.
     *             The left/right specifier is a 0/1 index indicating left/right child info.
     * @param rightChildShift Offset for right side of agg.
     */
    def updateBinForUnorderedFeature(
        nodeIndex: Int,
        featureIndex: Int,
        treePoint: TreePoint,
        agg: Array[Double],
        rightChildShift: Int): Unit = {
      val featureValue = treePoint.binnedFeatures(featureIndex)
      // Update the left or right count for one bin.
      val aggShift =
        numClasses * numBins * numFeatures * nodeIndex +
        numClasses * numBins * featureIndex +
        treePoint.label.toInt
      // Find all matching bins and increment their values
      val featureCategories = metadata.featureArity(featureIndex)
      val numCategoricalBins = (1 << featureCategories - 1) - 1
      var binIndex = 0
      while (binIndex < numCategoricalBins) {
        val aggIndex = aggShift + binIndex * numClasses
        if (bins(featureIndex)(binIndex).highSplit.categories.contains(featureValue)) {
          agg(aggIndex) += 1
        } else {
          agg(rightChildShift + aggIndex) += 1
        }
        binIndex += 1
      }
    }

    /**
     * Helper for binSeqOp.
     *
     * @param agg  Array storing aggregate calculation, of size:
     *             numClasses * numBins * numFeatures * numNodes.
     *             Indexed by (node, feature, bin, label) where label is the least significant bit.
     * @param treePoint  Data point being aggregated.
     * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
     */
    def binaryOrNotCategoricalBinSeqOp(
        agg: Array[Double],
        treePoint: TreePoint,
        nodeIndex: Int): Unit = {
      // Iterate over all features.
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        updateBinForOrderedFeature(treePoint, agg, nodeIndex, featureIndex)
        featureIndex += 1
      }
    }

    val rightChildShift = numClasses * numBins * numFeatures * numNodes

    /**
     * Helper for binSeqOp.
     *
     * @param agg  Array storing aggregate calculation.
     *             For ordered features, this is of size:
     *               numClasses * numBins * numFeatures * numNodes.
     *             For unordered features, this is of size:
     *               2 * numClasses * numBins * numFeatures * numNodes.
     * @param treePoint   Data point being aggregated.
     * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
     */
    def multiclassWithCategoricalBinSeqOp(
        agg: Array[Double],
        treePoint: TreePoint,
        nodeIndex: Int): Unit = {
      val label = treePoint.label
      // Iterate over all features.
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        if (metadata.isUnordered(featureIndex)) {
          updateBinForUnorderedFeature(nodeIndex, featureIndex, treePoint, agg, rightChildShift)
        } else {
          updateBinForOrderedFeature(treePoint, agg, nodeIndex, featureIndex)
        }
        featureIndex += 1
      }
    }

    /**
     * Performs a sequential aggregation over a partition for regression.
     * For l nodes, k features,
     * the count, sum, sum of squares of one of the p bins is incremented.
     *
     * @param agg Array storing aggregate calculation, updated by this function.
     *            Size: 3 * numBins * numFeatures * numNodes
     * @param treePoint   Data point being aggregated.
     * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
     * @return agg
     */
    def regressionBinSeqOp(agg: Array[Double], treePoint: TreePoint, nodeIndex: Int): Unit = {
      val label = treePoint.label
      // Iterate over all features.
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        // Update count, sum, and sum^2 for one bin.
        val binIndex = treePoint.binnedFeatures(featureIndex)
        val aggIndex =
          3 * numBins * numFeatures * nodeIndex +
          3 * numBins * featureIndex +
          3 * binIndex
        agg(aggIndex) += 1
        agg(aggIndex + 1) += label
        agg(aggIndex + 2) += label * label
        featureIndex += 1
      }
    }

    /**
     * Performs a sequential aggregation over a partition.
     * For l nodes, k features,
     *   For classification:
     *     Either the left count or the right count of one of the bins is
     *     incremented based upon whether the feature is classified as 0 or 1.
     *   For regression:
     *     The count, sum, sum of squares of one of the bins is incremented.
     *
     * @param agg Array storing aggregate calculation, updated by this function.
     *            Size for classification:
     *              numClasses * numBins * numFeatures * numNodes for ordered features, or
     *              2 * numClasses * numBins * numFeatures * numNodes for unordered features.
     *            Size for regression:
     *              3 * numBins * numFeatures * numNodes.
     * @param treePoint   Data point being aggregated.
     * @return  agg
     */
    def binSeqOp(agg: Array[Double], treePoint: TreePoint): Array[Double] = {
      val nodeIndex = treePointToNodeIndex(treePoint)
      // If the example does not reach this level, then nodeIndex < 0.
      // If the example reaches this level but is handled in a different group,
      //  then either nodeIndex < 0 (previous group) or nodeIndex >= numNodes (later group).
      if (nodeIndex >= 0 && nodeIndex < numNodes) {
        if (metadata.isClassification) {
          if (isMulticlassWithCategoricalFeatures) {
            multiclassWithCategoricalBinSeqOp(agg, treePoint, nodeIndex)
          } else {
            binaryOrNotCategoricalBinSeqOp(agg, treePoint, nodeIndex)
          }
        } else {
          regressionBinSeqOp(agg, treePoint, nodeIndex)
        }
      }
      agg
    }

    // Calculate bin aggregate length for classification or regression.
    val binAggregateLength = numNodes * getElementsPerNode(metadata, numBins)
    logDebug("binAggregateLength = " + binAggregateLength)

    /**
     * Combines the aggregates from partitions.
     * @param agg1 Array containing aggregates from one or more partitions
     * @param agg2 Array containing aggregates from one or more partitions
     * @return Combined aggregate from agg1 and agg2
     */
    def binCombOp(agg1: Array[Double], agg2: Array[Double]): Array[Double] = {
      var index = 0
      val combinedAggregate = new Array[Double](binAggregateLength)
      while (index < binAggregateLength) {
        combinedAggregate(index) = agg1(index) + agg2(index)
        index += 1
      }
      combinedAggregate
    }

    // Calculate bin aggregates.
    timer.start("aggregation")
    val binAggregates = {
      input.treeAggregate(Array.fill[Double](binAggregateLength)(0))(binSeqOp, binCombOp)
    }
    timer.stop("aggregation")
    logDebug("binAggregates.length = " + binAggregates.length)

    /**
     * Calculate the information gain for a given (feature, split) based upon left/right aggregates.
     * @param leftNodeAgg left node aggregates for this (feature, split)
     * @param rightNodeAgg right node aggregate for this (feature, split)
     * @param topImpurity impurity of the parent node
     * @return information gain and statistics for all splits
     */
    def calculateGainForSplit(
        leftNodeAgg: Array[Double],
        rightNodeAgg: Array[Double],
        topImpurity: Double): InformationGainStats = {
      if (metadata.isClassification) {
        val leftTotalCount = leftNodeAgg.sum
        val rightTotalCount = rightNodeAgg.sum

        val impurity = {
          if (level > 0) {
            topImpurity
          } else {
            // Calculate impurity for root node.
            val rootNodeCounts = new Array[Double](numClasses)
            var classIndex = 0
            while (classIndex < numClasses) {
              rootNodeCounts(classIndex) = leftNodeAgg(classIndex) + rightNodeAgg(classIndex)
              classIndex += 1
            }
            metadata.impurity.calculate(rootNodeCounts, leftTotalCount + rightTotalCount)
          }
        }

        val totalCount = leftTotalCount + rightTotalCount
        if (totalCount == 0) {
          // Return arbitrary prediction.
          return new InformationGainStats(0, topImpurity, topImpurity, topImpurity, 0)
        }

        // Sum of count for each label
        val leftrightNodeAgg: Array[Double] =
          leftNodeAgg.zip(rightNodeAgg).map { case (leftCount, rightCount) =>
            leftCount + rightCount
          }

        def indexOfLargestArrayElement(array: Array[Double]): Int = {
          val result = array.foldLeft(-1, Double.MinValue, 0) {
            case ((maxIndex, maxValue, currentIndex), currentValue) =>
              if (currentValue > maxValue) {
                (currentIndex, currentValue, currentIndex + 1)
              } else {
                (maxIndex, maxValue, currentIndex + 1)
              }
          }
          if (result._1 < 0) {
            throw new RuntimeException("DecisionTree internal error:" +
              " calculateGainForSplit failed in indexOfLargestArrayElement")
          }
          result._1
        }

        val predict = indexOfLargestArrayElement(leftrightNodeAgg)
        val prob = leftrightNodeAgg(predict) / totalCount

        val leftImpurity = if (leftTotalCount == 0) {
          topImpurity
        } else {
          metadata.impurity.calculate(leftNodeAgg, leftTotalCount)
        }
        val rightImpurity = if (rightTotalCount == 0) {
          topImpurity
        } else {
          metadata.impurity.calculate(rightNodeAgg, rightTotalCount)
        }

        val leftWeight = leftTotalCount / totalCount
        val rightWeight = rightTotalCount / totalCount

        val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

        new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict, prob)

      } else {
        // Regression

        val leftCount = leftNodeAgg(0)
        val leftSum = leftNodeAgg(1)
        val leftSumSquares = leftNodeAgg(2)

        val rightCount = rightNodeAgg(0)
        val rightSum = rightNodeAgg(1)
        val rightSumSquares = rightNodeAgg(2)

        val impurity = {
          if (level > 0) {
            topImpurity
          } else {
            // Calculate impurity for root node.
            val count = leftCount + rightCount
            val sum = leftSum + rightSum
            val sumSquares = leftSumSquares + rightSumSquares
            metadata.impurity.calculate(count, sum, sumSquares)
          }
        }

        if (leftCount == 0) {
          return new InformationGainStats(0, topImpurity, Double.MinValue, topImpurity,
            rightSum / rightCount)
        }
        if (rightCount == 0) {
          return new InformationGainStats(0, topImpurity, topImpurity,
            Double.MinValue, leftSum / leftCount)
        }

        val leftImpurity = metadata.impurity.calculate(leftCount, leftSum, leftSumSquares)
        val rightImpurity = metadata.impurity.calculate(rightCount, rightSum, rightSumSquares)

        val leftWeight = leftCount.toDouble / (leftCount + rightCount)
        val rightWeight = rightCount.toDouble / (leftCount + rightCount)

        val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

        val predict = (leftSum + rightSum) / (leftCount + rightCount)
        new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict)
      }
    }

    /**
     * Extracts left and right split aggregates.
     * @param binData Aggregate array slice from getBinDataForNode.
     *                For classification:
     *                  For unordered features, this is leftChildData ++ rightChildData,
     *                    each of which is indexed by (feature, split/bin, class),
     *                    with class being the least significant bit.
     *                  For ordered features, this is of size numClasses * numBins * numFeatures.
     *                For regression:
     *                  This is of size 2 * numFeatures * numBins.
     * @return (leftNodeAgg, rightNodeAgg) pair of arrays.
     *         For classification, each array is of size (numFeatures, (numBins - 1), numClasses).
     *         For regression, each array is of size (numFeatures, (numBins - 1), 3).
     *
     */
    def extractLeftRightNodeAggregates(
        binData: Array[Double]): (Array[Array[Array[Double]]], Array[Array[Array[Double]]]) = {


      /**
       * The input binData is indexed as (feature, bin, class).
       * This computes cumulative sums over splits.
       * Each (feature, class) pair is handled separately.
       * Note: numSplits = numBins - 1.
       * @param leftNodeAgg  Each (feature, class) slice is an array over splits.
       *                     Element i (i = 0, ..., numSplits - 2) is set to be
       *                     the cumulative sum (from left) over binData for bins 0, ..., i.
       * @param rightNodeAgg Each (feature, class) slice is an array over splits.
       *                     Element i (i = 1, ..., numSplits - 1) is set to be
       *                     the cumulative sum (from right) over binData for bins
       *                     numBins - 1, ..., numBins - 1 - i.
       */
      def findAggForOrderedFeatureClassification(
          leftNodeAgg: Array[Array[Array[Double]]],
          rightNodeAgg: Array[Array[Array[Double]]],
          featureIndex: Int) {

        // shift for this featureIndex
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

        val rightChildShift = numClasses * numBins * numFeatures
        var splitIndex = 0
        while (splitIndex < numBins - 1) {
          var classIndex = 0
          while (classIndex < numClasses) {
            // shift for this featureIndex
            val shift = numClasses * featureIndex * numBins + splitIndex * numClasses
            val leftBinValue = binData(shift + classIndex)
            val rightBinValue = binData(rightChildShift + shift + classIndex)
            leftNodeAgg(featureIndex)(splitIndex)(classIndex) = leftBinValue
            rightNodeAgg(featureIndex)(splitIndex)(classIndex) = rightBinValue
            classIndex += 1
          }
          splitIndex += 1
        }
      }

      def findAggForRegression(
          leftNodeAgg: Array[Array[Array[Double]]],
          rightNodeAgg: Array[Array[Array[Double]]],
          featureIndex: Int) {

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
      }

      if (metadata.isClassification) {
        // Initialize left and right split aggregates.
        val leftNodeAgg = Array.ofDim[Double](numFeatures, numBins - 1, numClasses)
        val rightNodeAgg = Array.ofDim[Double](numFeatures, numBins - 1, numClasses)
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          if (metadata.isUnordered(featureIndex)) {
            findAggForUnorderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
          } else {
            findAggForOrderedFeatureClassification(leftNodeAgg, rightNodeAgg, featureIndex)
          }
          featureIndex += 1
        }
        (leftNodeAgg, rightNodeAgg)
      } else {
        // Regression
        // Initialize left and right split aggregates.
        val leftNodeAgg = Array.ofDim[Double](numFeatures, numBins - 1, 3)
        val rightNodeAgg = Array.ofDim[Double](numFeatures, numBins - 1, 3)
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          findAggForRegression(leftNodeAgg, rightNodeAgg, featureIndex)
          featureIndex += 1
        }
        (leftNodeAgg, rightNodeAgg)
      }
    }

    /**
     * Calculates information gain for all nodes splits.
     */
    def calculateGainsForAllNodeSplits(
        leftNodeAgg: Array[Array[Array[Double]]],
        rightNodeAgg: Array[Array[Array[Double]]],
        nodeImpurity: Double): Array[Array[InformationGainStats]] = {
      val gains = Array.ofDim[InformationGainStats](numFeatures, numBins - 1)

      var featureIndex = 0
      while (featureIndex < numFeatures) {
        val numSplitsForFeature = getNumSplitsForFeature(featureIndex)
        var splitIndex = 0
        while (splitIndex < numSplitsForFeature) {
          gains(featureIndex)(splitIndex) =
            calculateGainForSplit(leftNodeAgg(featureIndex)(splitIndex),
              rightNodeAgg(featureIndex)(splitIndex), nodeImpurity)
          splitIndex += 1
        }
        featureIndex += 1
      }
      gains
    }

    /**
     * Get the number of splits for a feature.
     */
    def getNumSplitsForFeature(featureIndex: Int): Int = {
      if (metadata.isContinuous(featureIndex)) {
        numBins - 1
      } else {
        // Categorical feature
        val featureCategories = metadata.featureArity(featureIndex)
        if (metadata.isUnordered(featureIndex)) {
          (1 << featureCategories - 1) - 1
        } else {
          featureCategories
        }
      }
    }

    /**
     * Find the best split for a node.
     * @param binData Bin data slice for this node, given by getBinDataForNode.
     * @param nodeImpurity impurity of the top node
     * @return tuple of split and information gain
     */
    def binsToBestSplit(
        binData: Array[Double],
        nodeImpurity: Double): (Split, InformationGainStats) = {

      logDebug("node impurity = " + nodeImpurity)

      // Extract left right node aggregates.
      val (leftNodeAgg, rightNodeAgg) = extractLeftRightNodeAggregates(binData)

      // Calculate gains for all splits.
      val gains = calculateGainsForAllNodeSplits(leftNodeAgg, rightNodeAgg, nodeImpurity)

      val (bestFeatureIndex, bestSplitIndex, gainStats) = {
        // Initialize with infeasible values.
        var bestFeatureIndex = Int.MinValue
        var bestSplitIndex = Int.MinValue
        var bestGainStats = new InformationGainStats(Double.MinValue, -1.0, -1.0, -1.0, -1.0)
        // Iterate over features.
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          // Iterate over all splits.
          var splitIndex = 0
          val numSplitsForFeature = getNumSplitsForFeature(featureIndex)
          while (splitIndex < numSplitsForFeature) {
            val gainStats = gains(featureIndex)(splitIndex)
            if (gainStats.gain > bestGainStats.gain) {
              bestGainStats = gainStats
              bestFeatureIndex = featureIndex
              bestSplitIndex = splitIndex
            }
            splitIndex += 1
          }
          featureIndex += 1
        }
        (bestFeatureIndex, bestSplitIndex, bestGainStats)
      }

      logDebug("best split = " + splits(bestFeatureIndex)(bestSplitIndex))
      logDebug("best split bin = " + bins(bestFeatureIndex)(bestSplitIndex))

      (splits(bestFeatureIndex)(bestSplitIndex), gainStats)
    }

    /**
     * Get bin data for one node.
     */
    def getBinDataForNode(node: Int): Array[Double] = {
      if (metadata.isClassification) {
        if (isMulticlassWithCategoricalFeatures) {
          val shift = numClasses * node * numBins * numFeatures
          val rightChildShift = numClasses * numBins * numFeatures * numNodes
          val binsForNode = {
            val leftChildData
            = binAggregates.slice(shift, shift + numClasses * numBins * numFeatures)
            val rightChildData
            = binAggregates.slice(rightChildShift + shift,
              rightChildShift + shift + numClasses * numBins * numFeatures)
            leftChildData ++ rightChildData
          }
          binsForNode
        } else {
          val shift = numClasses * node * numBins * numFeatures
          val binsForNode = binAggregates.slice(shift, shift + numClasses * numBins * numFeatures)
          binsForNode
        }
      } else {
        // Regression
        val shift = 3 * node * numBins * numFeatures
        val binsForNode = binAggregates.slice(shift, shift + 3 * numBins * numFeatures)
        binsForNode
      }
    }

    // Calculate best splits for all nodes at a given level
    timer.start("chooseSplits")
    val bestSplits = new Array[(Split, InformationGainStats)](numNodes)
    // Iterating over all nodes at this level
    var node = 0
    while (node < numNodes) {
      val nodeImpurityIndex = (1 << level) - 1 + node + groupShift
      val binsForNode: Array[Double] = getBinDataForNode(node)
      logDebug("nodeImpurityIndex = " + nodeImpurityIndex)
      val parentNodeImpurity = parentImpurities(nodeImpurityIndex)
      logDebug("parent node impurity = " + parentNodeImpurity)
      bestSplits(node) = binsToBestSplit(binsForNode, parentNodeImpurity)
      node += 1
    }
    timer.stop("chooseSplits")

    bestSplits
  }

  /**
   * Get the number of values to be stored per node in the bin aggregates.
   *
   * @param numBins  Number of bins = 1 + number of possible splits.
   */
  private def getElementsPerNode(metadata: DecisionTreeMetadata, numBins: Int): Int = {
    if (metadata.isClassification) {
      if (metadata.isMulticlassWithCategoricalFeatures) {
        2 * metadata.numClasses * numBins * metadata.numFeatures
      } else {
        metadata.numClasses * numBins * metadata.numFeatures
      }
    } else {
      3 * numBins * metadata.numFeatures
    }
  }

  /**
   * Returns splits and bins for decision tree calculation.
   * Continuous and categorical features are handled differently.
   *
   * Continuous features:
   *   For each feature, there are numBins - 1 possible splits representing the possible binary
   *   decisions at each node in the tree.
   *
   * Categorical features:
   *   For each feature, there is 1 bin per split.
   *   Splits and bins are handled in 2 ways:
   *   (a) "unordered features"
   *       For multiclass classification with a low-arity feature
   *       (i.e., if isMulticlass && isSpaceSufficientForAllCategoricalSplits),
   *       the feature is split based on subsets of categories.
   *       There are (1 << maxFeatureValue - 1) - 1 splits.
   *   (b) "ordered features"
   *       For regression and binary classification,
   *       and for multiclass classification with a high-arity feature,
   *       there is one bin per category.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @param metadata Learning and dataset metadata
   * @return A tuple of (splits, bins).
   *         Splits is an Array of [[org.apache.spark.mllib.tree.model.Split]]
   *          of size (numFeatures, numBins - 1).
   *         Bins is an Array of [[org.apache.spark.mllib.tree.model.Bin]]
   *          of size (numFeatures, numBins).
   */
  protected[tree] def findSplitsBins(
      input: RDD[LabeledPoint],
      metadata: DecisionTreeMetadata): (Array[Array[Split]], Array[Array[Bin]]) = {

    val count = input.count()

    // Find the number of features by looking at the first sample
    val numFeatures = input.take(1)(0).features.size

    val maxBins = metadata.maxBins
    val numBins = if (maxBins <= count) maxBins else count.toInt
    logDebug("numBins = " + numBins)
    val isMulticlass = metadata.isMulticlass
    logDebug("isMulticlass = " + isMulticlass)

    /*
     * Ensure numBins is always greater than the categories. For multiclass classification,
     * numBins should be greater than 2^(maxCategories - 1) - 1.
     * It's a limitation of the current implementation but a reasonable trade-off since features
     * with large number of categories get favored over continuous features.
     *
     * This needs to be checked here instead of in Strategy since numBins can be determined
     * by the number of training examples.
     * TODO: Allow this case, where we simply will know nothing about some categories.
     */
    if (metadata.featureArity.size > 0) {
      val maxCategoriesForFeatures = metadata.featureArity.maxBy(_._2)._2
      require(numBins > maxCategoriesForFeatures, "numBins should be greater than max categories " +
        "in categorical features")
    }

    // Calculate the number of sample for approximate quantile calculation.
    val requiredSamples = numBins*numBins
    val fraction = if (requiredSamples < count) requiredSamples.toDouble / count else 1.0
    logDebug("fraction of data used for calculating quantiles = " + fraction)

    // sampled input for RDD calculation
    val sampledInput =
      input.sample(withReplacement = false, fraction, new XORShiftRandom().nextInt()).collect()
    val numSamples = sampledInput.length

    val stride: Double = numSamples.toDouble / numBins
    logDebug("stride = " + stride)

    metadata.quantileStrategy match {
      case Sort =>
        val splits = Array.ofDim[Split](numFeatures, numBins - 1)
        val bins = Array.ofDim[Bin](numFeatures, numBins)

        // Find all splits.

        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          // Check whether the feature is continuous.
          val isFeatureContinuous = metadata.isContinuous(featureIndex)
          if (isFeatureContinuous) {
            val featureSamples = sampledInput.map(lp => lp.features(featureIndex)).sorted
            val stride: Double = numSamples.toDouble / numBins
            logDebug("stride = " + stride)
            for (index <- 0 until numBins - 1) {
              val sampleIndex = index * stride.toInt
              // Set threshold halfway in between 2 samples.
              val threshold = (featureSamples(sampleIndex) + featureSamples(sampleIndex + 1)) / 2.0
              val split = new Split(featureIndex, threshold, Continuous, List())
              splits(featureIndex)(index) = split
            }
          } else { // Categorical feature
            val featureCategories = metadata.featureArity(featureIndex)

            // Use different bin/split calculation strategy for categorical features in multiclass
            // classification that satisfy the space constraint.
            if (metadata.isUnordered(featureIndex)) {
              // 2^(maxFeatureValue- 1) - 1 combinations
              var index = 0
              while (index < (1 << featureCategories - 1) - 1) {
                val categories: List[Double]
                  = extractMultiClassCategories(index + 1, featureCategories)
                splits(featureIndex)(index)
                  = new Split(featureIndex, Double.MinValue, Categorical, categories)
                bins(featureIndex)(index) = {
                  if (index == 0) {
                    new Bin(
                      new DummyCategoricalSplit(featureIndex, Categorical),
                      splits(featureIndex)(0),
                      Categorical,
                      Double.MinValue)
                  } else {
                    new Bin(
                      splits(featureIndex)(index - 1),
                      splits(featureIndex)(index),
                      Categorical,
                      Double.MinValue)
                  }
                }
                index += 1
              }
            } else { // ordered feature
              /* For a given categorical feature, use a subsample of the data
               * to choose how to arrange possible splits.
               * This examines each category and computes a centroid.
               * These centroids are later used to sort the possible splits.
               * centroidForCategories is a mapping: category (for the given feature) --> centroid
               */
              val centroidForCategories = {
                if (isMulticlass) {
                  // For categorical variables in multiclass classification,
                  // each bin is a category. The bins are sorted and they
                  // are ordered by calculating the impurity of their corresponding labels.
                  sampledInput.map(lp => (lp.features(featureIndex), lp.label))
                   .groupBy(_._1)
                   .mapValues(x => x.groupBy(_._2).mapValues(x => x.size.toDouble))
                   .map(x => (x._1, x._2.values.toArray))
                   .map(x => (x._1, metadata.impurity.calculate(x._2, x._2.sum)))
                } else { // regression or binary classification
                  // For categorical variables in regression and binary classification,
                  // each bin is a category. The bins are sorted and they
                  // are ordered by calculating the centroid of their corresponding labels.
                  sampledInput.map(lp => (lp.features(featureIndex), lp.label))
                    .groupBy(_._1)
                    .mapValues(x => x.map(_._2).sum / x.map(_._1).length)
                }
              }

              logDebug("centroid for categories = " + centroidForCategories.mkString(","))

              // Check for missing categorical variables and putting them last in the sorted list.
              val fullCentroidForCategories = scala.collection.mutable.Map[Double,Double]()
              for (i <- 0 until featureCategories) {
                if (centroidForCategories.contains(i)) {
                  fullCentroidForCategories(i) = centroidForCategories(i)
                } else {
                  fullCentroidForCategories(i) = Double.MaxValue
                }
              }

              // bins sorted by centroids
              val categoriesSortedByCentroid = fullCentroidForCategories.toList.sortBy(_._2)

              logDebug("centroid for categorical variable = " + categoriesSortedByCentroid)

              var categoriesForSplit = List[Double]()
              categoriesSortedByCentroid.iterator.zipWithIndex.foreach {
                case ((key, value), index) =>
                  categoriesForSplit = key :: categoriesForSplit
                  splits(featureIndex)(index) = new Split(featureIndex, Double.MinValue,
                    Categorical, categoriesForSplit)
                  bins(featureIndex)(index) = {
                    if (index == 0) {
                      new Bin(new DummyCategoricalSplit(featureIndex, Categorical),
                        splits(featureIndex)(0), Categorical, key)
                    } else {
                      new Bin(splits(featureIndex)(index-1), splits(featureIndex)(index),
                        Categorical, key)
                    }
                  }
              }
            }
          }
          featureIndex += 1
        }

        // Find all bins.
        featureIndex = 0
        while (featureIndex < numFeatures) {
          val isFeatureContinuous = metadata.isContinuous(featureIndex)
          if (isFeatureContinuous) { // Bins for categorical variables are already assigned.
            bins(featureIndex)(0) = new Bin(new DummyLowSplit(featureIndex, Continuous),
              splits(featureIndex)(0), Continuous, Double.MinValue)
            for (index <- 1 until numBins - 1) {
              val bin = new Bin(splits(featureIndex)(index-1), splits(featureIndex)(index),
                Continuous, Double.MinValue)
              bins(featureIndex)(index) = bin
            }
            bins(featureIndex)(numBins-1) = new Bin(splits(featureIndex)(numBins-2),
              new DummyHighSplit(featureIndex, Continuous), Continuous, Double.MinValue)
          }
          featureIndex += 1
        }
        (splits, bins)
      case MinMax =>
        throw new UnsupportedOperationException("minmax not supported yet.")
      case ApproxHist =>
        throw new UnsupportedOperationException("approximate histogram not supported yet.")
    }
  }

  /**
   * Nested method to extract list of eligible categories given an index. It extracts the
   * position of ones in a binary representation of the input. If binary
   * representation of an number is 01101 (13), the output list should (3.0, 2.0,
   * 0.0). The maxFeatureValue depict the number of rightmost digits that will be tested for ones.
   */
  private[tree] def extractMultiClassCategories(
      input: Int,
      maxFeatureValue: Int): List[Double] = {
    var categories = List[Double]()
    var j = 0
    var bitShiftedInput = input
    while (j < maxFeatureValue) {
      if (bitShiftedInput % 2 != 0) {
        // updating the list of categories.
        categories = j.toDouble :: categories
      }
      // Right shift by one
      bitShiftedInput = bitShiftedInput >> 1
      j += 1
    }
    categories
  }

}
