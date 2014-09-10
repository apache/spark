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
import org.apache.spark.mllib.tree.impl._
import org.apache.spark.mllib.tree.impurity.{Impurities, Impurity}
import org.apache.spark.mllib.tree.impurity._
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
    logDebug("maxBins = " + metadata.maxBins)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val (splits, bins) = DecisionTree.findSplitsBins(retaggedInput, metadata)
    timer.stop("findSplitsBins")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
        s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
      }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, bins, metadata)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")
    // Number of nodes to allocate: max number of nodes possible given the depth of the tree, plus 1
    val maxNumNodesPlus1 = Node.startIndexInLevel(maxDepth + 1)
    // Initialize an array to hold parent impurity calculations for each node.
    val parentImpurities = new Array[Double](maxNumNodesPlus1)
    // dummy value for top node (updated during first split calculation)
    val nodes = new Array[Node](maxNumNodesPlus1)

    // Calculate level for single group construction

    // Max memory usage for aggregates
    val maxMemoryUsage = strategy.maxMemoryInMB * 1024 * 1024
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    // TODO: Calculate memory usage more precisely.
    val numElementsPerNode = DecisionTree.getElementsPerNode(metadata)

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
      val splitsStatsForLevel: Array[(Split, InformationGainStats, Predict)] =
        DecisionTree.findBestSplits(treeInput, parentImpurities,
          metadata, level, nodes, splits, bins, maxLevelForSingleGroup, timer)
      timer.stop("findBestSplits")

      val levelNodeIndexOffset = Node.startIndexInLevel(level)
      for ((nodeSplitStats, index) <- splitsStatsForLevel.view.zipWithIndex) {
        val nodeIndex = levelNodeIndexOffset + index

        // Extract info for this node (index) at the current level.
        timer.start("extractNodeInfo")
        val split = nodeSplitStats._1
        val stats = nodeSplitStats._2
        val predict = nodeSplitStats._3.predict
        val isLeaf = (stats.gain <= 0) || (level == strategy.maxDepth)
        val node = new Node(nodeIndex, predict, isLeaf, Some(split), None, None, Some(stats))
        logDebug("Node = " + node)
        nodes(nodeIndex) = node
        timer.stop("extractNodeInfo")

        if (level != 0) {
          // Set parent.
          val parentNodeIndex = Node.parentIndex(nodeIndex)
          if (Node.isLeftChild(nodeIndex)) {
            nodes(parentNodeIndex).leftNode = Some(nodes(nodeIndex))
          } else {
            nodes(parentNodeIndex).rightNode = Some(nodes(nodeIndex))
          }
        }
        // Extract info for nodes at the next lower level.
        timer.start("extractInfoForLowerLevels")
        if (level < maxDepth) {
          val leftChildIndex = Node.leftChildIndex(nodeIndex)
          val leftImpurity = stats.leftImpurity
          logDebug("leftChildIndex = " + leftChildIndex + ", impurity = " + leftImpurity)
          parentImpurities(leftChildIndex) = leftImpurity

          val rightChildIndex = Node.rightChildIndex(nodeIndex)
          val rightImpurity = stats.rightImpurity
          logDebug("rightChildIndex = " + rightChildIndex + ", impurity = " + rightImpurity)
          parentImpurities(rightChildIndex) = rightImpurity
        }
        timer.stop("extractInfoForLowerLevels")
        logDebug("final best split = " + split)
      }
      require(Node.maxNodesInLevel(level) == splitsStatsForLevel.length)
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
    val topNode = nodes(1)
    // Build the full tree using the node info calculated in the level-wise best split calculations.
    topNode.build(nodes)

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    new DecisionTreeModel(topNode, strategy.algo)
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
   *                  (suggested value: 5)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 32)
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
   *                  (suggested value: 5)
   * @param maxBins maximum number of bins used for splitting features
   *                 (suggested value: 32)
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

  /**
   * Returns an array of optimal splits for all nodes at a given level. Splits the task into
   * multiple groups if the level-wise training task could lead to memory overflow.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.tree.impl.TreePoint]]
   * @param parentImpurities Impurities for all parent nodes for the current level
   * @param metadata Learning and dataset metadata
   * @param level Level of the tree
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
   * @param maxLevelForSingleGroup the deepest level for single-group level-wise computation.
   * @return array (over nodes) of splits with best split for each node at a given level.
   */
  private[tree] def findBestSplits(
      input: RDD[TreePoint],
      parentImpurities: Array[Double],
      metadata: DecisionTreeMetadata,
      level: Int,
      nodes: Array[Node],
      splits: Array[Array[Split]],
      bins: Array[Array[Bin]],
      maxLevelForSingleGroup: Int,
      timer: TimeTracker = new TimeTracker): Array[(Split, InformationGainStats, Predict)] = {
    // split into groups to avoid memory overflow during aggregation
    if (level > maxLevelForSingleGroup) {
      // When information for all nodes at a given level cannot be stored in memory,
      // the nodes are divided into multiple groups at each level with the number of groups
      // increasing exponentially per level. For example, if maxLevelForSingleGroup is 10,
      // numGroups is equal to 2 at level 11 and 4 at level 12, respectively.
      val numGroups = 1 << level - maxLevelForSingleGroup
      logDebug("numGroups = " + numGroups)
      var bestSplits = new Array[(Split, InformationGainStats, Predict)](0)
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
   * Get the node index corresponding to this data point.
   * This function mimics prediction, passing an example from the root node down to a node
   * at the current level being trained; that node's index is returned.
   *
   * @param node  Node in tree from which to classify the given data point.
   * @param binnedFeatures  Binned feature vector for data point.
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
   * @param unorderedFeatures  Set of indices of unordered features.
   * @return  Leaf index if the data point reaches a leaf.
   *          Otherwise, last node reachable in tree matching this example.
   *          Note: This is the global node index, i.e., the index used in the tree.
   *                This index is different from the index used during training a particular
   *                set of nodes in a (level, group).
   */
  private def predictNodeIndex(
      node: Node,
      binnedFeatures: Array[Int],
      bins: Array[Array[Bin]],
      unorderedFeatures: Set[Int]): Int = {
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
          val featureValue = binnedFeatures(featureIndex)
          node.split.get.categories.contains(featureValue)
        }
        case _ => throw new RuntimeException(s"predictNodeIndex failed for unknown reason.")
      }
      if (node.leftNode.isEmpty || node.rightNode.isEmpty) {
        // Return index from next layer of nodes to train
        if (splitLeft) {
          Node.leftChildIndex(node.id)
        } else {
          Node.rightChildIndex(node.id)
        }
      } else {
        if (splitLeft) {
          predictNodeIndex(node.leftNode.get, binnedFeatures, bins, unorderedFeatures)
        } else {
          predictNodeIndex(node.rightNode.get, binnedFeatures, bins, unorderedFeatures)
        }
      }
    }
  }

  /**
   * Helper for binSeqOp, for data which can contain a mix of ordered and unordered features.
   *
   * For ordered features, a single bin is updated.
   * For unordered features, bins correspond to subsets of categories; either the left or right bin
   * for each subset is updated.
   *
   * @param agg  Array storing aggregate calculation, with a set of sufficient statistics for
   *             each (node, feature, bin).
   * @param treePoint  Data point being aggregated.
   * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
   * @param unorderedFeatures  Set of indices of unordered features.
   */
  private def mixedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      nodeIndex: Int,
      bins: Array[Array[Bin]],
      unorderedFeatures: Set[Int]): Unit = {
    // Iterate over all features.
    val numFeatures = treePoint.binnedFeatures.size
    val nodeOffset = agg.getNodeOffset(nodeIndex)
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      if (unorderedFeatures.contains(featureIndex)) {
        // Unordered feature
        val featureValue = treePoint.binnedFeatures(featureIndex)
        val (leftNodeFeatureOffset, rightNodeFeatureOffset) =
          agg.getLeftRightNodeFeatureOffsets(nodeIndex, featureIndex)
        // Update the left or right bin for each split.
        val numSplits = agg.numSplits(featureIndex)
        var splitIndex = 0
        while (splitIndex < numSplits) {
          if (bins(featureIndex)(splitIndex).highSplit.categories.contains(featureValue)) {
            agg.nodeFeatureUpdate(leftNodeFeatureOffset, splitIndex, treePoint.label)
          } else {
            agg.nodeFeatureUpdate(rightNodeFeatureOffset, splitIndex, treePoint.label)
          }
          splitIndex += 1
        }
      } else {
        // Ordered feature
        val binIndex = treePoint.binnedFeatures(featureIndex)
        agg.nodeUpdate(nodeOffset, featureIndex, binIndex, treePoint.label)
      }
      featureIndex += 1
    }
  }

  /**
   * Helper for binSeqOp, for regression and for classification with only ordered features.
   *
   * For each feature, the sufficient statistics of one bin are updated.
   *
   * @param agg  Array storing aggregate calculation, with a set of sufficient statistics for
   *             each (node, feature, bin).
   * @param treePoint  Data point being aggregated.
   * @param nodeIndex  Node corresponding to treePoint. Indexed from 0 at start of (level, group).
   * @return agg
   */
  private def orderedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      nodeIndex: Int): Unit = {
    val label = treePoint.label
    val nodeOffset = agg.getNodeOffset(nodeIndex)
    // Iterate over all features.
    val numFeatures = agg.numFeatures
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      val binIndex = treePoint.binnedFeatures(featureIndex)
      agg.nodeUpdate(nodeOffset, featureIndex, binIndex, label)
      featureIndex += 1
    }
  }

  /**
   * Returns an array of optimal splits for a group of nodes at a given level
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.tree.impl.TreePoint]]
   * @param parentImpurities Impurities for all parent nodes for the current level
   * @param metadata Learning and dataset metadata
   * @param level Level of the tree
   * @param nodes Array of all nodes in the tree.  Used for matching data points to nodes.
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
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
      groupIndex: Int = 0): Array[(Split, InformationGainStats, Predict)] = {

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
    val numNodes = Node.maxNodesInLevel(level) / numGroups
    logDebug("numNodes = " + numNodes)

    logDebug("numFeatures = " + metadata.numFeatures)
    logDebug("numClasses = " + metadata.numClasses)
    logDebug("isMulticlass = " + metadata.isMulticlass)
    logDebug("isMulticlassWithCategoricalFeatures = " +
      metadata.isMulticlassWithCategoricalFeatures)

    // shift when more than one group is used at deep tree level
    val groupShift = numNodes * groupIndex

    // Used for treePointToNodeIndex to get an index for this (level, group).
    // - Node.startIndexInLevel(level) gives the global index offset for nodes at this level.
    // - groupShift corrects for groups in this level before the current group.
    val globalNodeIndexOffset = Node.startIndexInLevel(level) + groupShift

    /**
     * Find the node index for the given example.
     * Nodes are indexed from 0 at the start of this (level, group).
     * If the example does not reach this level, returns a value < 0.
     */
    def treePointToNodeIndex(treePoint: TreePoint): Int = {
      if (level == 0) {
        0
      } else {
        val globalNodeIndex =
          predictNodeIndex(nodes(1), treePoint.binnedFeatures, bins, metadata.unorderedFeatures)
        globalNodeIndex - globalNodeIndexOffset
      }
    }

    /**
     * Performs a sequential aggregation over a partition.
     *
     * Each data point contributes to one node. For each feature,
     * the aggregate sufficient statistics are updated for the relevant bins.
     *
     * @param agg  Array storing aggregate calculation, with a set of sufficient statistics for
     *             each (node, feature, bin).
     * @param treePoint   Data point being aggregated.
     * @return  agg
     */
    def binSeqOp(
        agg: DTStatsAggregator,
        treePoint: TreePoint): DTStatsAggregator = {
      val nodeIndex = treePointToNodeIndex(treePoint)
      // If the example does not reach this level, then nodeIndex < 0.
      // If the example reaches this level but is handled in a different group,
      //  then either nodeIndex < 0 (previous group) or nodeIndex >= numNodes (later group).
      if (nodeIndex >= 0 && nodeIndex < numNodes) {
        if (metadata.unorderedFeatures.isEmpty) {
          orderedBinSeqOp(agg, treePoint, nodeIndex)
        } else {
          mixedBinSeqOp(agg, treePoint, nodeIndex, bins, metadata.unorderedFeatures)
        }
      }
      agg
    }

    // Calculate bin aggregates.
    timer.start("aggregation")
    val binAggregates: DTStatsAggregator = {
      val initAgg = new DTStatsAggregator(metadata, numNodes)
      input.treeAggregate(initAgg)(binSeqOp, DTStatsAggregator.binCombOp)
    }
    timer.stop("aggregation")

    // Calculate best splits for all nodes at a given level
    timer.start("chooseSplits")
    val bestSplits = new Array[(Split, InformationGainStats, Predict)](numNodes)
    // Iterating over all nodes at this level
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      val nodeImpurity = parentImpurities(globalNodeIndexOffset + nodeIndex)
      logDebug("node impurity = " + nodeImpurity)
      bestSplits(nodeIndex) =
        binsToBestSplit(binAggregates, nodeIndex, nodeImpurity, level, metadata, splits)
      logDebug("best split = " + bestSplits(nodeIndex)._1)
      nodeIndex += 1
    }
    timer.stop("chooseSplits")

    bestSplits
  }

  /**
   * Calculate the information gain for a given (feature, split) based upon left/right aggregates.
   * @param leftImpurityCalculator left node aggregates for this (feature, split)
   * @param rightImpurityCalculator right node aggregate for this (feature, split)
   * @param topImpurity impurity of the parent node
   * @return information gain and statistics for all splits
   */
  private def calculateGainForSplit(
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator,
      topImpurity: Double,
      level: Int,
      metadata: DecisionTreeMetadata): InformationGainStats = {
    val leftCount = leftImpurityCalculator.count
    val rightCount = rightImpurityCalculator.count

    // If left child or right child doesn't satisfy minimum instances per node,
    // then this split is invalid, return invalid information gain stats.
    if ((leftCount < metadata.minInstancesPerNode) ||
        (rightCount < metadata.minInstancesPerNode)) {
      return InformationGainStats.invalidInformationGainStats
    }

    val totalCount = leftCount + rightCount

    // impurity of parent node
    val impurity = if (level > 0) {
      topImpurity
    } else {
      val parentNodeAgg = leftImpurityCalculator.copy
      parentNodeAgg.add(rightImpurityCalculator)
      parentNodeAgg.calculate()
    }

    val leftImpurity = leftImpurityCalculator.calculate() // Note: This equals 0 if count = 0
    val rightImpurity = rightImpurityCalculator.calculate()

    val leftWeight = leftCount / totalCount.toDouble
    val rightWeight = rightCount / totalCount.toDouble

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

    // if information gain doesn't satisfy minimum information gain,
    // then this split is invalid, return invalid information gain stats.
    if (gain < metadata.minInfoGain) {
      return InformationGainStats.invalidInformationGainStats
    }

    new InformationGainStats(gain, impurity, leftImpurity, rightImpurity)
  }

  /**
   * Calculate predict value for current node, given stats of any split.
   * Note that this function is called only once for each node.
   * @param leftImpurityCalculator left node aggregates for a split
   * @param rightImpurityCalculator right node aggregates for a node
   * @return predict value for current node
   */
  private def calculatePredict(
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator): Predict =  {
    val parentNodeAgg = leftImpurityCalculator.copy
    parentNodeAgg.add(rightImpurityCalculator)
    val predict = parentNodeAgg.predict
    val prob = parentNodeAgg.prob(predict)

    new Predict(predict, prob)
  }

  /**
   * Find the best split for a node.
   * @param binAggregates Bin statistics.
   * @param nodeIndex Index for node to split in this (level, group).
   * @param nodeImpurity Impurity of the node (nodeIndex).
   * @return tuple for best split: (Split, information gain)
   */
  private def binsToBestSplit(
      binAggregates: DTStatsAggregator,
      nodeIndex: Int,
      nodeImpurity: Double,
      level: Int,
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]]): (Split, InformationGainStats, Predict) = {

    logDebug("node impurity = " + nodeImpurity)

    // calculate predict only once
    var predict: Option[Predict] = None

    // For each (feature, split), calculate the gain, and select the best (feature, split).
    val (bestSplit, bestSplitStats) = Range(0, metadata.numFeatures).map { featureIndex =>
      val numSplits = metadata.numSplits(featureIndex)
      if (metadata.isContinuous(featureIndex)) {
        // Cumulative sum (scanLeft) of bin statistics.
        // Afterwards, binAggregates for a bin is the sum of aggregates for
        // that bin + all preceding bins.
        val nodeFeatureOffset = binAggregates.getNodeFeatureOffset(nodeIndex, featureIndex)
        var splitIndex = 0
        while (splitIndex < numSplits) {
          binAggregates.mergeForNodeFeature(nodeFeatureOffset, splitIndex + 1, splitIndex)
          splitIndex += 1
        }
        // Find best split.
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { case splitIdx =>
            val leftChildStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, splitIdx)
            val rightChildStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, numSplits)
            rightChildStats.subtract(leftChildStats)
            predict = Some(predict.getOrElse(calculatePredict(leftChildStats, rightChildStats)))
            val gainStats =
              calculateGainForSplit(leftChildStats, rightChildStats, nodeImpurity, level, metadata)
            (splitIdx, gainStats)
          }.maxBy(_._2.gain)
        (splits(featureIndex)(bestFeatureSplitIndex), bestFeatureGainStats)
      } else if (metadata.isUnordered(featureIndex)) {
        // Unordered categorical feature
        val (leftChildOffset, rightChildOffset) =
          binAggregates.getLeftRightNodeFeatureOffsets(nodeIndex, featureIndex)
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { splitIndex =>
            val leftChildStats = binAggregates.getImpurityCalculator(leftChildOffset, splitIndex)
            val rightChildStats = binAggregates.getImpurityCalculator(rightChildOffset, splitIndex)
            predict = Some(predict.getOrElse(calculatePredict(leftChildStats, rightChildStats)))
            val gainStats =
              calculateGainForSplit(leftChildStats, rightChildStats, nodeImpurity, level, metadata)
            (splitIndex, gainStats)
          }.maxBy(_._2.gain)
        (splits(featureIndex)(bestFeatureSplitIndex), bestFeatureGainStats)
      } else {
        // Ordered categorical feature
        val nodeFeatureOffset = binAggregates.getNodeFeatureOffset(nodeIndex, featureIndex)
        val numBins = metadata.numBins(featureIndex)

        /* Each bin is one category (feature value).
         * The bins are ordered based on centroidForCategories, and this ordering determines which
         * splits are considered.  (With K categories, we consider K - 1 possible splits.)
         *
         * centroidForCategories is a list: (category, centroid)
         */
        val centroidForCategories = if (metadata.isMulticlass) {
          // For categorical variables in multiclass classification,
          // the bins are ordered by the impurity of their corresponding labels.
          Range(0, numBins).map { case featureValue =>
            val categoryStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
            val centroid = if (categoryStats.count != 0) {
              categoryStats.calculate()
            } else {
              Double.MaxValue
            }
            (featureValue, centroid)
          }
        } else { // regression or binary classification
          // For categorical variables in regression and binary classification,
          // the bins are ordered by the centroid of their corresponding labels.
          Range(0, numBins).map { case featureValue =>
            val categoryStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
            val centroid = if (categoryStats.count != 0) {
              categoryStats.predict
            } else {
              Double.MaxValue
            }
            (featureValue, centroid)
          }
        }

        logDebug("Centroids for categorical variable: " + centroidForCategories.mkString(","))

        // bins sorted by centroids
        val categoriesSortedByCentroid = centroidForCategories.toList.sortBy(_._2)

        logDebug("Sorted centroids for categorical variable = " +
          categoriesSortedByCentroid.mkString(","))

        // Cumulative sum (scanLeft) of bin statistics.
        // Afterwards, binAggregates for a bin is the sum of aggregates for
        // that bin + all preceding bins.
        var splitIndex = 0
        while (splitIndex < numSplits) {
          val currentCategory = categoriesSortedByCentroid(splitIndex)._1
          val nextCategory = categoriesSortedByCentroid(splitIndex + 1)._1
          binAggregates.mergeForNodeFeature(nodeFeatureOffset, nextCategory, currentCategory)
          splitIndex += 1
        }
        // lastCategory = index of bin with total aggregates for this (node, feature)
        val lastCategory = categoriesSortedByCentroid.last._1
        // Find best split.
        val (bestFeatureSplitIndex, bestFeatureGainStats) =
          Range(0, numSplits).map { splitIndex =>
            val featureValue = categoriesSortedByCentroid(splitIndex)._1
            val leftChildStats =
              binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
            val rightChildStats =
              binAggregates.getImpurityCalculator(nodeFeatureOffset, lastCategory)
            rightChildStats.subtract(leftChildStats)
            predict = Some(predict.getOrElse(calculatePredict(leftChildStats, rightChildStats)))
            val gainStats =
              calculateGainForSplit(leftChildStats, rightChildStats, nodeImpurity, level, metadata)
            (splitIndex, gainStats)
          }.maxBy(_._2.gain)
        val categoriesForSplit =
          categoriesSortedByCentroid.map(_._1.toDouble).slice(0, bestFeatureSplitIndex + 1)
        val bestFeatureSplit =
          new Split(featureIndex, Double.MinValue, Categorical, categoriesForSplit)
        (bestFeatureSplit, bestFeatureGainStats)
      }
    }.maxBy(_._2.gain)

    require(predict.isDefined, "must calculate predict for each node")

    (bestSplit, bestSplitStats, predict.get)
  }

  /**
   * Get the number of values to be stored per node in the bin aggregates.
   */
  private def getElementsPerNode(metadata: DecisionTreeMetadata): Int = {
    val totalBins = metadata.numBins.sum
    if (metadata.isClassification) {
      metadata.numClasses * totalBins
    } else {
      3 * totalBins
    }
  }

  /**
   * Returns splits and bins for decision tree calculation.
   * Continuous and categorical features are handled differently.
   *
   * Continuous features:
   *   For each feature, there are numBins - 1 possible splits representing the possible binary
   *   decisions at each node in the tree.
   *   This finds locations (feature values) for splits using a subsample of the data.
   *
   * Categorical features:
   *   For each feature, there is 1 bin per split.
   *   Splits and bins are handled in 2 ways:
   *   (a) "unordered features"
   *       For multiclass classification with a low-arity feature
   *       (i.e., if isMulticlass && isSpaceSufficientForAllCategoricalSplits),
   *       the feature is split based on subsets of categories.
   *   (b) "ordered features"
   *       For regression and binary classification,
   *       and for multiclass classification with a high-arity feature,
   *       there is one bin per category.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @param metadata Learning and dataset metadata
   * @return A tuple of (splits, bins).
   *         Splits is an Array of [[org.apache.spark.mllib.tree.model.Split]]
   *          of size (numFeatures, numSplits).
   *         Bins is an Array of [[org.apache.spark.mllib.tree.model.Bin]]
   *          of size (numFeatures, numBins).
   */
  protected[tree] def findSplitsBins(
      input: RDD[LabeledPoint],
      metadata: DecisionTreeMetadata): (Array[Array[Split]], Array[Array[Bin]]) = {

    logDebug("isMulticlass = " + metadata.isMulticlass)

    val numFeatures = metadata.numFeatures

    // Sample the input only if there are continuous features.
    val hasContinuousFeatures = Range(0, numFeatures).exists(metadata.isContinuous)
    val sampledInput = if (hasContinuousFeatures) {
      // Calculate the number of samples for approximate quantile calculation.
      val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
      val fraction = if (requiredSamples < metadata.numExamples) {
        requiredSamples.toDouble / metadata.numExamples
      } else {
        1.0
      }
      logDebug("fraction of data used for calculating quantiles = " + fraction)
      input.sample(withReplacement = false, fraction, new XORShiftRandom().nextInt()).collect()
    } else {
      new Array[LabeledPoint](0)
    }

    metadata.quantileStrategy match {
      case Sort =>
        val splits = new Array[Array[Split]](numFeatures)
        val bins = new Array[Array[Bin]](numFeatures)

        // Find all splits.
        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          val numSplits = metadata.numSplits(featureIndex)
          val numBins = metadata.numBins(featureIndex)
          if (metadata.isContinuous(featureIndex)) {
            val numSamples = sampledInput.length
            splits(featureIndex) = new Array[Split](numSplits)
            bins(featureIndex) = new Array[Bin](numBins)
            val featureSamples = sampledInput.map(lp => lp.features(featureIndex)).sorted
            val stride: Double = numSamples.toDouble / metadata.numBins(featureIndex)
            logDebug("stride = " + stride)
            for (splitIndex <- 0 until numSplits) {
              val sampleIndex = splitIndex * stride.toInt
              // Set threshold halfway in between 2 samples.
              val threshold = (featureSamples(sampleIndex) + featureSamples(sampleIndex + 1)) / 2.0
              splits(featureIndex)(splitIndex) =
                new Split(featureIndex, threshold, Continuous, List())
            }
            bins(featureIndex)(0) = new Bin(new DummyLowSplit(featureIndex, Continuous),
              splits(featureIndex)(0), Continuous, Double.MinValue)
            for (splitIndex <- 1 until numSplits) {
              bins(featureIndex)(splitIndex) =
                new Bin(splits(featureIndex)(splitIndex - 1), splits(featureIndex)(splitIndex),
                  Continuous, Double.MinValue)
            }
            bins(featureIndex)(numSplits) = new Bin(splits(featureIndex)(numSplits - 1),
              new DummyHighSplit(featureIndex, Continuous), Continuous, Double.MinValue)
          } else {
            // Categorical feature
            val featureArity = metadata.featureArity(featureIndex)
            if (metadata.isUnordered(featureIndex)) {
              // TODO: The second half of the bins are unused.  Actually, we could just use
              //       splits and not build bins for unordered features.  That should be part of
              //       a later PR since it will require changing other code (using splits instead
              //       of bins in a few places).
              // Unordered features
              //   2^(maxFeatureValue - 1) - 1 combinations
              splits(featureIndex) = new Array[Split](numSplits)
              bins(featureIndex) = new Array[Bin](numBins)
              var splitIndex = 0
              while (splitIndex < numSplits) {
                val categories: List[Double] =
                  extractMultiClassCategories(splitIndex + 1, featureArity)
                splits(featureIndex)(splitIndex) =
                  new Split(featureIndex, Double.MinValue, Categorical, categories)
                bins(featureIndex)(splitIndex) = {
                  if (splitIndex == 0) {
                    new Bin(
                      new DummyCategoricalSplit(featureIndex, Categorical),
                      splits(featureIndex)(0),
                      Categorical,
                      Double.MinValue)
                  } else {
                    new Bin(
                      splits(featureIndex)(splitIndex - 1),
                      splits(featureIndex)(splitIndex),
                      Categorical,
                      Double.MinValue)
                  }
                }
                splitIndex += 1
              }
            } else {
              // Ordered features
              //   Bins correspond to feature values, so we do not need to compute splits or bins
              //   beforehand.  Splits are constructed as needed during training.
              splits(featureIndex) = new Array[Split](0)
              bins(featureIndex) = new Array[Bin](0)
            }
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
