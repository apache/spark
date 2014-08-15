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
import scala.collection.mutable

import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.Logging
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impl.{TimeTracker, TreePoint}
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.random.XORShiftRandom


/**
 * Categorical feature metadata.
 *
 * TODO: Add doc about ordered vs. unordered features.
 * Ensure numBins is always greater than the categories. For multiclass classification,
 * numBins should be greater than math.pow(2, maxCategories - 1) - 1.
 * It's a limitation of the current implementation but a reasonable trade-off since features
 * with large number of categories get favored over continuous features.
 *
 * This needs to be checked here instead of in Strategy since numBins can be determined
 * by the number of training examples.
 * TODO: Allow this case, where we simply will know nothing about some categories.
 *
 * @param featureArity  Map: categorical feature index --> arity.
 *                      I.e., the feature takes values in {0, ..., arity - 1}.
 */
private[tree] class LearningMetadata(
    val numFeatures: Int,
    val numExamples: Long,
    val numClasses: Int,
    val maxBins: Int,
    val featureArity: Map[Int, Int],
    val unorderedFeatures: Set[Int],
    val numBins: Array[Int],
    val impurity: Impurity,
    val quantileStrategy: QuantileStrategy) extends Serializable {

  def isUnordered(featureIndex: Int): Boolean = unorderedFeatures.contains(featureIndex)

  def isClassification: Boolean = numClasses >= 2

  def isMulticlass: Boolean = numClasses > 2

  def isMulticlassWithCategoricalFeatures: Boolean = isMulticlass && (featureArity.size > 0)

  def isCategorical(featureIndex: Int): Boolean = featureArity.contains(featureIndex)

  def isContinuous(featureIndex: Int): Boolean = !featureArity.contains(featureIndex)

  def numSplits(featureIndex: Int): Int = if (isUnordered(featureIndex)) {
    numBins(featureIndex)
  } else {
    numBins(featureIndex) - 1
  }

}

private[tree] object LearningMetadata {

  def buildMetadata(input: RDD[LabeledPoint], strategy: Strategy): LearningMetadata = {

    val numFeatures = input.take(1)(0).features.size
    val numExamples = input.count()
    val numClasses = strategy.algo match {
      case Classification => strategy.numClassesForClassification
      case Regression => 0
    }

    val maxPossibleBins = math.min(strategy.maxBins, numExamples).toInt

    val unorderedFeatures = new mutable.HashSet[Int]()
    // numBins[featureIndex] = number of bins for feature
    val numBins = Array.fill[Int](numFeatures)(maxPossibleBins)
    if (numClasses >= 2) {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        val numUnorderedBins = DecisionTree.numUnorderedBins(k)
        if (numUnorderedBins < maxPossibleBins) {
          numBins(f) = numUnorderedBins
          unorderedFeatures.add(f)
        } else {
          // TODO: Check the below k <= maxBins.
          //       This used to be k < maxPossibleBins, but <= should work.
          //       However, there may have been a 1-off error later on allocating 1 extra
          //       (unused) bin.
          require(k <= maxPossibleBins, "numBins should be greater than max categories " +
            "in categorical features")
          numBins(f) = k
        }
      }
    } else {
      strategy.categoricalFeaturesInfo.foreach { case (f, k) =>
        require(k <= maxPossibleBins, "numBins should be greater than max categories " +
          "in categorical features")
        numBins(f) = k
      }
    }

    new LearningMetadata(numFeatures, numExamples, numClasses, numBins.max,
      strategy.categoricalFeaturesInfo, unorderedFeatures.toSet, numBins,
      strategy.impurity, strategy.quantileCalculationStrategy)
  }
}


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
    val metadata = LearningMetadata.buildMetadata(retaggedInput, strategy)
    timer.stop("init")

    logDebug("algo = " + strategy.algo)
    logDebug("maxBins = " + metadata.maxBins)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val (splits, bins) = DecisionTree.findSplitsBins(retaggedInput, metadata)
    val numBins = bins(0).length
    timer.stop("findSplitsBins")
    logDebug("numBins = " + numBins)

    /*
    println(s"splits:")
    for (f <- Range(0, splits.size)) {
      for (s <- Range(0, splits(f).size)) {
        println(s"  splits($f)($s): ${splits(f)(s)}")
      }
    }
    println(s"bins:")
    for (f <- Range(0, bins.size)) {
      for (s <- Range(0, bins(f).size)) {
        println(s"  bins($f)($s): ${bins(f)(s)}")
      }
    }
    */

    timer.start("init")
    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, bins, metadata).cache()
    timer.stop("init")

    val numFeatures = metadata.numFeatures
    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    // the max number of nodes possible given the depth of the tree
    val maxNumNodes = DecisionTree.maxNodesInLevel(maxDepth + 1) - 1
    // Initialize an array to hold parent impurity calculations for each node.
    val parentImpurities = new Array[Double](maxNumNodes)
    // dummy value for top node (updated during first split calculation)
    val nodes = new Array[Node](maxNumNodes)
    val nodesInTree = Array.fill[Boolean](maxNumNodes)(false) // put into nodes array later?
    nodesInTree(0) = true

    // Calculate level for single group construction

    // Max memory usage for aggregates
    val maxMemoryUsage = strategy.maxMemoryInMB * 1024 * 1024
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    // TODO: Calculate numElementsPerNode in metadata (more precisely)
    val numElementsPerNode = DecisionTree.getElementsPerNode(numFeatures, numBins,
      strategy.numClassesForClassification, strategy.isMulticlassWithCategoricalFeatures,
      strategy.algo)

    logDebug("numElementsPerNode = " + numElementsPerNode)
    val arraySizePerNode = 8 * numElementsPerNode // approx. memory usage for bin aggregate array
    val maxNumberOfNodesPerGroup = math.max(maxMemoryUsage / arraySizePerNode, 1)
    logDebug("maxNumberOfNodesPerGroup = " + maxNumberOfNodesPerGroup)
    // nodes at a level is 2^level. level is zero indexed.
    val maxLevelForSingleGroup = math.max(
      (math.log(maxNumberOfNodesPerGroup) / math.log(2)).floor.toInt, 0)
    logDebug("max level for single group = " + maxLevelForSingleGroup)

    /*
     * The main idea here is to perform level-wise training of the decision tree nodes thus
     * reducing the passes over the data from l to log2(l) where l is the total number of nodes.
     * Each data sample is handled by a particular node at that level (or it reaches a leaf
     * beforehand and is not used in later levels.
     */

    var level = 0
    var break = false
    while (level <= maxDepth && !break) {

      //println(s"LEVEL $level")
      logDebug("#####################################")
      logDebug("level = " + level)
      logDebug("#####################################")


      // Find best split for all nodes at a level.
      timer.start("findBestSplits")
      val splitsStatsForLevel: Array[(Split, InformationGainStats)] =
        DecisionTree.findBestSplits(treeInput, parentImpurities,
          metadata, level, nodes, splits, bins, maxLevelForSingleGroup, timer)
      timer.stop("findBestSplits")

      val levelNodeIndexOffset = DecisionTree.maxNodesInLevel(level) - 1
      for ((nodeSplitStats, index) <- splitsStatsForLevel.view.zipWithIndex) {
        /*println(s"splitsStatsForLevel: index=$index")
        println(s"\t split: ${nodeSplitStats._1}")
        println(s"\t gain stats: ${nodeSplitStats._2}")*/
        val nodeIndex = levelNodeIndexOffset + index
        val isLeftChild = level != 0 && nodeIndex % 2 == 1
        val parentNodeIndex = if (isLeftChild) { // -1 for root node
            (nodeIndex - 1) / 2
          } else {
            (nodeIndex - 2) / 2
          }
        // if (level == 0 || (nodesInTree(parentNodeIndex) && !nodes(parentNodeIndex).isLeaf))
        // TODO: Use above check to skip unused branch of tree
        // Extract info for this node (index) at the current level.
        timer.start("extractNodeInfo")
        val split = nodeSplitStats._1
        val stats = nodeSplitStats._2
        val isLeaf = (stats.gain <= 0) || (level == strategy.maxDepth)
        val node = new Node(nodeIndex, stats.predict, isLeaf, Some(split), None, None, Some(stats))
        logDebug("Node = " + node)
        nodes(nodeIndex) = node
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
      require(DecisionTree.maxNodesInLevel(level) == splitsStatsForLevel.length)
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

    logDebug("Internal timing for DecisionTree:")
    logDebug(s"$timer")

    new DecisionTreeModel(topNode, strategy.algo)
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
    if (level >= maxDepth)
      return
    // TODO: Move nodeIndexOffset calc out of function?
    val nodeIndexOffset = DecisionTree.maxNodesInLevel(level + 1) - 1
    // 0 corresponds to the left child node and 1 corresponds to the right child node.
    var i = 0
    while (i <= 1) {
      // Calculate the index of the node from the node level and the index at the current level.
      val nodeIndex = nodeIndexOffset + 2 * index + i
      val impurity = if (i == 0) {
        nodeSplitStats._2.leftImpurity
      } else {
        nodeSplitStats._2.rightImpurity
      }
      logDebug("nodeIndex = " + nodeIndex + ", impurity = " + impurity)
      // noting the parent impurities
      parentImpurities(nodeIndex) = impurity
      i += 1
    }
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
   * @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing
   *                 parameters for constructing the DecisionTree
   * @param level Level of the tree
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
   * @param maxLevelForSingleGroup the deepest level for single-group level-wise computation.
   * @param unorderedFeatures  Set of unordered (categorical) features.
   * @return array (over nodes) of splits with best split for each node at a given level.
   */
  protected[tree] def findBestSplits(
      input: RDD[TreePoint],
      parentImpurities: Array[Double],
      metadata: LearningMetadata,
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
      val numGroups = math.pow(2, level - maxLevelForSingleGroup).toInt
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
   * @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing
   *                 parameters for constructing the DecisionTree
   * @param level Level of the tree
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param bins possible bins for all features, indexed (numFeatures)(numBins)
   * @param unorderedFeatures  Set of unordered (categorical) features.
   * @param numGroups total number of node groups at the current level. Default value is set to 1.
   * @param groupIndex index of the node group being processed. Default value is set to 0.
   * @return array of splits with best splits for all nodes at a given level.
   */
  private def findBestSplitsPerGroup(
      input: RDD[TreePoint],
      parentImpurities: Array[Double],
      metadata: LearningMetadata,
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
    val numNodes = DecisionTree.maxNodesInLevel(level) / numGroups
    logDebug("numNodes = " + numNodes)

    // Find the number of features by looking at the first sample.
    val numFeatures = input.first().features.size
    logDebug("numFeatures = " + numFeatures)

    // numBins:  Number of bins = 1 + number of possible splits
    val numBins = bins(0).length
    logDebug("numBins = " + numBins)

    val numClasses = metadata.numClasses
    logDebug("numClasses = " + numClasses)

    val isMulticlass = metadata.isMulticlass
    logDebug("isMulticlass = " + isMulticlass)

    val isMulticlassWithCategoricalFeatures = metadata.isMulticlassWithCategoricalFeatures
    logDebug("isMulticlassWithCategoricalFeatures = " + isMulticlassWithCategoricalFeatures)

    // shift when more than one group is used at deep tree level
    val groupShift = numNodes * groupIndex

    /**
     * Get the node index corresponding to this data point.
     * This is used during training, mimicking prediction.
     * @return  Leaf index if the data point reaches a leaf.
     *          Otherwise, last node reachable in tree matching this example.
     */
    def predictNodeIndex(node: Node, features: Array[Int]): Int = {
      if (node.isLeaf) {
        node.id
      } else {
        val featureIndex = node.split.get.feature
        val splitLeft = node.split.get.featureType match {
          case Continuous => {
            val binIndex = features(featureIndex)
            val featureValueUpperBound = bins(featureIndex)(binIndex).highSplit.threshold
            // bin binIndex has range (bin.lowSplit.threshold, bin.highSplit.threshold]
            // We do not need to check lowSplit since bins are separated by splits.
            featureValueUpperBound <= node.split.get.threshold
          }
          case Categorical => {
            val featureValue = if (metadata.isUnordered(featureIndex)) {
                features(featureIndex)
              } else {
                val binIndex = features(featureIndex)
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
            predictNodeIndex(node.leftNode.get, features)
          } else {
            predictNodeIndex(node.rightNode.get, features)
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
    val levelOffset = DecisionTree.maxNodesInLevel(level) - 1

    /**
     * Find the node (indexed from 0 at the start of this level) for the given example.
     * If the example does not reach this level, returns a value < 0.
     */
    def treePointToNodeIndex(treePoint: TreePoint): Int = {
      if (level == 0) {
        0
      } else {
        val globalNodeIndex = predictNodeIndex(nodes(0), treePoint.features)
        // Get index for this level.
        globalNodeIndex - levelOffset
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
    def someUnorderedBinSeqOp(
        agg: Array[Array[Array[ImpurityAggregator]]],
        treePoint: TreePoint,
        nodeIndex: Int): Unit = {
      val label = treePoint.label
      // Iterate over all features.
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        if (metadata.isUnordered(featureIndex)) {
          // Unordered feature
          val featureValue = treePoint.features(featureIndex)
          // Update the left or right count for one bin.
          // Find all matching bins and increment their values.
          val numCategoricalBins = metadata.numBins(featureIndex)
          var binIndex = 0
          while (binIndex < numCategoricalBins) {
            if (bins(featureIndex)(binIndex).highSplit.categories.contains(featureValue)) {
              agg(nodeIndex)(featureIndex)(binIndex).add(treePoint.label)
            } else {
              agg(nodeIndex)(featureIndex)(numCategoricalBins + binIndex).add(treePoint.label)
            }
            binIndex += 1
          }
        } else {
          // Ordered feature
          val binIndex = treePoint.features(featureIndex)
          agg(nodeIndex)(featureIndex)(binIndex).add(treePoint.label)
        }
        featureIndex += 1
      }
    }

    /**
     * Helper for binSeqOp: for regression and for classification with only ordered features.
     *
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
    def orderedBinSeqOp(
        agg: Array[Array[Array[ImpurityAggregator]]],
        treePoint: TreePoint,
        nodeIndex: Int): Unit = {
      val label = treePoint.label
      // Iterate over all features.
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        // Update count, sum, and sum^2 for one bin.
        val binIndex = treePoint.features(featureIndex)
        if (binIndex >= agg(nodeIndex)(featureIndex).size) {
          throw new RuntimeException(
            s"binIndex: $binIndex, agg(nodeIndex)(featureIndex).size = ${agg(nodeIndex)(featureIndex).size}")
        }
        agg(nodeIndex)(featureIndex)(binIndex).add(label)
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
     *              Ordered features: numNodes * numFeatures * numBins.
     *              Unordered features: (2 * numNodes) * numFeatures * numBins.
     *            Size for regression:
     *              numNodes * numFeatures * numBins.
     * @param treePoint   Data point being aggregated.
     * @return  agg
     */
    def binSeqOp(
        agg: Array[Array[Array[ImpurityAggregator]]],
        treePoint: TreePoint): Array[Array[Array[ImpurityAggregator]]] = {
      val nodeIndex = treePointToNodeIndex(treePoint)
      if (nodeIndex >= 0) { // Otherwise, example does not reach this level.
        if (metadata.unorderedFeatures.isEmpty) {
          orderedBinSeqOp(agg, treePoint, nodeIndex)
        } else {
          someUnorderedBinSeqOp(agg, treePoint, nodeIndex)
        }
      }
      agg
    }

    /**
     * Combines the aggregates from partitions.
     * @param agg1 Array containing aggregates from one or more partitions
     * @param agg2 Array containing aggregates from one or more partitions
     * @return Combined aggregate from agg1 and agg2
     */
    def binCombOp(
        agg1: Array[Array[Array[ImpurityAggregator]]],
        agg2: Array[Array[Array[ImpurityAggregator]]]): Array[Array[Array[ImpurityAggregator]]] = {
      var n = 0
      while (n < agg2.size) {
        var f = 0
        while (f < agg2(n).size) {
          var b = 0
          while (b < agg2(n)(f).size) {
            agg1(n)(f)(b).merge(agg2(n)(f)(b))
            b += 1
          }
          f += 1
        }
        n += 1
      }
      agg1
    }

    // Calculate bin aggregates.
    timer.start("binAggregates")
    val binAggregates = {
      val initAgg = getEmptyBinAggregates(metadata, numNodes)
      input.aggregate(initAgg)(binSeqOp, binCombOp)
    }
    timer.stop("binAggregates")

    logDebug("binAggregates.length = " + binAggregates.length)

    /*
    println("binAggregates:")
    for (n <- Range(0, binAggregates.size)) {
      for (f <- Range(0, binAggregates(n).size)) {
        for (b <- Range(0, binAggregates(n)(f).size)) {
          println(s" ($n, $f, $b): ${binAggregates(n)(f)(b)}")
        }
      }
    }
    */

    // Calculate best splits for all nodes at a given level
    val bestSplits = new Array[(Split, InformationGainStats)](numNodes)
    val nodeIndexOffset = DecisionTree.maxNodesInLevel(level) - 1
    // Iterating over all nodes at this level
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      //println(s" HANDLING node $nodeIndex")
      val nodeImpurityIndex = nodeIndexOffset + nodeIndex + groupShift
      //val binsForNode: Array[Double] = getBinDataForNode(node)
      //logDebug("nodeImpurityIndex = " + nodeImpurityIndex)
      val parentNodeImpurity = parentImpurities(nodeImpurityIndex)
      logDebug("parent node impurity = " + parentNodeImpurity)

      val (bestFeatureIndex, bestSplitIndex, bestGain) =
        binsToBestSplit(binAggregates(nodeIndex), parentNodeImpurity, level, metadata)
      bestSplits(nodeIndex) = (splits(bestFeatureIndex)(bestSplitIndex), bestGain)
      logDebug("best split = " + splits(bestFeatureIndex)(bestSplitIndex))
      logDebug("best split bin = " + bins(bestFeatureIndex)(bestSplitIndex))
      //println(s"bestSplits(node:$node): ${bestSplits(node)}")

      nodeIndex += 1
    }

    bestSplits
  }

  /**
   * Calculate the information gain for a given (feature, split) based upon left/right aggregates.
   * @param leftNodeAgg left node aggregates for this (feature, split)
   * @param rightNodeAgg right node aggregate for this (feature, split)
   * @param topImpurity impurity of the parent node
   * @return information gain and statistics for all splits
   */
  def calculateGainForSplit(
      leftNodeAgg: ImpurityAggregator,
      rightNodeAgg: ImpurityAggregator,
      topImpurity: Double,
      level: Int,
      metadata: LearningMetadata): InformationGainStats = {

    val leftCount = leftNodeAgg.count
    val rightCount = rightNodeAgg.count

    val totalCount = leftCount + rightCount
    if (totalCount == 0) {
      // Return arbitrary prediction.
      //println(s"BLAH: feature $featureIndex, split $splitIndex. totalCount == 0")
      return new InformationGainStats(0, topImpurity, topImpurity, topImpurity, 0)
    }

    val parentNodeAgg = leftNodeAgg.copy
    parentNodeAgg.merge(rightNodeAgg)
    // impurity of parent node
    val impurity = if (level > 0) {
      topImpurity
    } else {
      parentNodeAgg.calculate()
    }

    val predict = parentNodeAgg.predict
    val prob = parentNodeAgg.prob(predict)

    val leftImpurity = leftNodeAgg.calculate() // Note: 0 if count = 0
    val rightImpurity = rightNodeAgg.calculate()

    /*
    println(s"calculateGainForSplit")
    println(s"\t leftImpurity = $leftImpurity, leftNodeAgg: $leftNodeAgg")
    println(s"\t rightImpurity = $rightImpurity, rightNodeAgg: $rightNodeAgg")
    */

    val leftWeight = leftCount / totalCount.toDouble
    val rightWeight = rightCount / totalCount.toDouble

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

    new InformationGainStats(gain, impurity, leftImpurity, rightImpurity, predict, prob)
  }

  /**
   * Calculates information gain for all nodes splits.
   * @param leftNodeAgg  Aggregate stats, of dimensions (numFeatures, numSplits(feature))
   * @param rightNodeAgg  Aggregate stats, of dimensions (numFeatures, numSplits(feature))
   * @param nodeImpurity  Impurity for node being split.
   * @return  Info gain, of dimensions (numFeatures, numSplits(feature))
   */
  def calculateGainsForAllNodeSplits(
      leftNodeAgg: Array[Array[ImpurityAggregator]],
      rightNodeAgg: Array[Array[ImpurityAggregator]],
      nodeImpurity: Double,
      level: Int,
      metadata: LearningMetadata): Array[Array[InformationGainStats]] = {
    val gains = new Array[Array[InformationGainStats]](metadata.numFeatures)

    for (featureIndex <- 0 until metadata.numFeatures) {
      val numSplitsForFeature = metadata.numSplits(featureIndex)
      gains(featureIndex) = new Array[InformationGainStats](numSplitsForFeature)
      for (splitIndex <- 0 until numSplitsForFeature) {
        gains(featureIndex)(splitIndex) =
          calculateGainForSplit(leftNodeAgg(featureIndex)(splitIndex),
            rightNodeAgg(featureIndex)(splitIndex), nodeImpurity, level, metadata)
      }
    }
    gains
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
   *         Each array is of size (numFeatures, numSplits(feature)).
   * TODO: Extract in-place.
   */
  def extractLeftRightNodeAggregates(
      nodeAggregates: Array[Array[ImpurityAggregator]],
      metadata: LearningMetadata): (Array[Array[ImpurityAggregator]], Array[Array[ImpurityAggregator]]) = {

    val numClasses = metadata.numClasses
    val numFeatures = metadata.numFeatures

    /**
     * Reshape binData for this feature.
     * Indexes binData as (feature, split, class) with class as the least significant bit.
     * @param leftNodeAgg   leftNodeAgg(featureIndex)(splitIndex)(classIndex) = aggregate value
     */
    def findAggForUnorderedFeature(
        binData: Array[Array[ImpurityAggregator]],
        leftNodeAgg: Array[Array[ImpurityAggregator]],
        rightNodeAgg: Array[Array[ImpurityAggregator]],
        featureIndex: Int) {
      // TODO: Don't pass in featureIndex; use index before call.
      // Note: numBins = numSplits for unordered features.
      val numBins = metadata.numBins(featureIndex)
      leftNodeAgg(featureIndex) = binData(featureIndex).slice(0, numBins)
      rightNodeAgg(featureIndex) = binData(featureIndex).slice(numBins, 2 * numBins)
    }

    /**
     * For ordered features (regression and classification with ordered features).
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
     * TODO: We could avoid doing one of these cumulative sums.
     */
    def findAggForOrderedFeature(
        binData: Array[Array[ImpurityAggregator]],
        leftNodeAgg: Array[Array[ImpurityAggregator]],
        rightNodeAgg: Array[Array[ImpurityAggregator]],
        featureIndex: Int) {

      // TODO: Don't pass in featureIndex; use index before call.

      val numSplits = metadata.numSplits(featureIndex)
      leftNodeAgg(featureIndex) = new Array[ImpurityAggregator](numSplits)
      rightNodeAgg(featureIndex) = new Array[ImpurityAggregator](numSplits)

      if (metadata.isContinuous(featureIndex)) {
        // left node aggregate for the lowest split
        leftNodeAgg(featureIndex)(0) = binData(featureIndex)(0).copy
        // right node aggregate for the highest split
        rightNodeAgg(featureIndex)(numSplits - 1) = binData(featureIndex)(numSplits).copy

        // Iterate over all splits.
        var splitIndex = 1
        while (splitIndex < numSplits) {
          // calculating left node aggregate for a split as a sum of left node aggregate of a
          // lower split and the left bin aggregate of a bin where the split is a high split
          leftNodeAgg(featureIndex)(splitIndex) = leftNodeAgg(featureIndex)(splitIndex - 1).copy
          leftNodeAgg(featureIndex)(splitIndex).merge(binData(featureIndex)(splitIndex))
          rightNodeAgg(featureIndex)(numSplits - 1 - splitIndex) =
            rightNodeAgg(featureIndex)(numSplits - splitIndex).copy
          rightNodeAgg(featureIndex)(numSplits - 1 - splitIndex).merge(
            binData(featureIndex)(numSplits - splitIndex))
          splitIndex += 1
        }
      } else { // ordered categorical feature
        /* TODO: This is a temp fix.
         *       Eventually, for ordered categorical features, change splits and bins to be
         *       for individual categories instead of running totals over a pre-defined category
         *       ordering.  Then, we could choose the ordering in this function, tailoring it
         *       to this particular node.
         */
        var splitIndex = 0
        while (splitIndex < numSplits) {
          // no need to clone since no cumulative sum is needed
          leftNodeAgg(featureIndex)(splitIndex) = binData(featureIndex)(splitIndex)
          rightNodeAgg(featureIndex)(splitIndex) = binData(featureIndex)(splitIndex + 1)
          splitIndex += 1
        }
      }
    }

    val leftNodeAgg = new Array[Array[ImpurityAggregator]](numFeatures)
    val rightNodeAgg = new Array[Array[ImpurityAggregator]](numFeatures)
    if (metadata.isClassification) {
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        if (metadata.isUnordered(featureIndex)) {
          findAggForUnorderedFeature(nodeAggregates, leftNodeAgg, rightNodeAgg, featureIndex)
        } else {
          findAggForOrderedFeature(nodeAggregates, leftNodeAgg, rightNodeAgg, featureIndex)
        }
        featureIndex += 1
      }
    } else { // Regression
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        findAggForOrderedFeature(nodeAggregates, leftNodeAgg, rightNodeAgg, featureIndex)
        featureIndex += 1
      }
    }
    (leftNodeAgg, rightNodeAgg)
  }

  /**
   * Find the best split for a node.
   * @param binData Bin data slice for this node, given by getBinDataForNode.
   * @param nodeImpurity impurity of the top node
   * @return tuple (best feature index, best split index, information gain)
   */
  def binsToBestSplit(
      nodeAggregates: Array[Array[ImpurityAggregator]],
      nodeImpurity: Double,
      level: Int,
      metadata: LearningMetadata): (Int, Int, InformationGainStats) = {

    logDebug("node impurity = " + nodeImpurity)
    /*
    println("nodeAggregates")
    for (f <- Range(0, nodeAggregates.size)) {
      for (b <- Range(0, nodeAggregates(f).size)) {
        println(s"nodeAggregates($f)($b): ${nodeAggregates(f)(b)}")
      }
    }
    */
    // Extract left right node aggregates.
    val (leftNodeAgg, rightNodeAgg) = extractLeftRightNodeAggregates(nodeAggregates, metadata)

    // Calculate gains for all splits.
    val gains =
      calculateGainsForAllNodeSplits(leftNodeAgg, rightNodeAgg, nodeImpurity, level, metadata)

    val (bestFeatureIndex, bestSplitIndex, gainStats) = {
      // Initialize with infeasible values.
      var bestFeatureIndex = Int.MinValue
      var bestSplitIndex = Int.MinValue
      var bestGainStats = new InformationGainStats(Double.MinValue, -1.0, -1.0, -1.0, -1.0)
      // Iterate over features.
      var featureIndex = 0
      while (featureIndex < metadata.numFeatures) {
        // Iterate over all splits.
        var splitIndex = 0
        val numSplitsForFeature = metadata.numSplits(featureIndex)
        while (splitIndex < numSplitsForFeature) {
          val gainStats = gains(featureIndex)(splitIndex)
          if (gainStats.gain > bestGainStats.gain) {
            bestGainStats = gainStats
            bestFeatureIndex = featureIndex
            bestSplitIndex = splitIndex
            //println(s" feature $featureIndex UPGRADED split $splitIndex: ${splits(featureIndex)(splitIndex)}: gainstats: $gainStats")
          }
          splitIndex += 1
        }
        featureIndex += 1
      }
      (bestFeatureIndex, bestSplitIndex, bestGainStats)
    }

    (bestFeatureIndex, bestSplitIndex, gainStats)
  }

  /**
   * Get the number of values to be stored per node in the bin aggregates.
   *
   * @param numBins  Number of bins = 1 + number of possible splits.
   */
  private def getElementsPerNode(
      numFeatures: Int,
      numBins: Int,
      numClasses: Int,
      isMulticlassClassificationWithCategoricalFeatures: Boolean,
      algo: Algo): Int = {
    algo match {
      case Classification =>
        if (isMulticlassClassificationWithCategoricalFeatures) {
          2 * numClasses * numBins * numFeatures
        } else {
          numClasses * numBins * numFeatures
        }
      case Regression => 3 * numBins * numFeatures
    }
  }

  /**
   * Get an empty instance of bin aggregates.
   * For ordered features, aggregate is indexed by: (nodeIndex)(featureIndex)(binIndex).
   * For unordered features, aggregate is indexed by: (nodeIndex)(featureIndex)(2 * binIndex),
   *  where the bins are ordered as (numBins left bins, numBins right bins).
   */
  private def getEmptyBinAggregates(
      metadata: LearningMetadata,
      numNodes: Int): Array[Array[Array[ImpurityAggregator]]] = {
    val impurityAggregator: ImpurityAggregator = metadata.impurity match {
      case Gini => new GiniAggregator(metadata.numClasses)
      case Entropy => new EntropyAggregator(metadata.numClasses)
      case Variance => new VarianceAggregator()
      case _ => throw new IllegalArgumentException(s"Bad impurity parameter: ${metadata.impurity}")
    }

    val agg = Array.fill[Array[ImpurityAggregator]](numNodes, metadata.numFeatures)(
      new Array[ImpurityAggregator](0))
    var nodeIndex = 0
    while (nodeIndex < numNodes) {
      var featureIndex = 0
      while (featureIndex < metadata.numFeatures) {
        val binMultiplier = if (metadata.isUnordered(featureIndex)) 2 else 1
        val effNumBins = metadata.numBins(featureIndex) * binMultiplier
        agg(nodeIndex)(featureIndex) = new Array[ImpurityAggregator](effNumBins)
        var binIndex = 0
        while (binIndex < effNumBins) {
          agg(nodeIndex)(featureIndex)(binIndex) = impurityAggregator.newAggregator
          binIndex += 1
        }
        featureIndex += 1
      }
      nodeIndex += 1
    }
    agg
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
   *       There are math.pow(2, maxFeatureValue - 1) - 1 splits.
   *   (b) "ordered features"
   *       For regression and binary classification,
   *       and for multiclass classification with a high-arity feature,
   *       there is one bin per category.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @param strategy [[org.apache.spark.mllib.tree.configuration.Strategy]] instance containing
   *                 parameters for construction the DecisionTree
   * @return A tuple of (splits, bins).
   *         Splits is an Array of [[org.apache.spark.mllib.tree.model.Split]]
   *          of size (numFeatures, numSplits).
   *         Bins is an Array of [[org.apache.spark.mllib.tree.model.Bin]]
   *          of size (numFeatures, numBins).
   */
  protected[tree] def findSplitsBins(
      input: RDD[LabeledPoint],
      metadata: LearningMetadata): (Array[Array[Split]], Array[Array[Bin]]) = {

    val isMulticlassClassification = metadata.isMulticlass
    logDebug("isMulticlass = " + isMulticlassClassification)

    val numFeatures = metadata.numFeatures

    // Calculate the number of sample for approximate quantile calculation.
    val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
    val fraction = if (requiredSamples < metadata.numExamples) {
      requiredSamples.toDouble / metadata.numExamples
    } else {
      1.0
    }
    logDebug("fraction of data used for calculating quantiles = " + fraction)

    // sampled input for RDD calculation
    val sampledInput =
      input.sample(withReplacement = false, fraction, new XORShiftRandom().nextInt()).collect()
    val numSamples = sampledInput.length

    metadata.quantileStrategy match {
      case Sort =>
        val splits = new Array[Array[Split]](numFeatures)
        val bins = new Array[Array[Bin]](numFeatures)
        var i = 0
        while (i < numFeatures) {
          splits(i) = new Array[Split](metadata.numSplits(i))
          bins(i) = new Array[Bin](metadata.numBins(i))
          i += 1
        }

        // Find all splits.

        // Iterate over all features.
        var featureIndex = 0
        while (featureIndex < numFeatures) {
          val numSplits = metadata.numSplits(featureIndex)
          if (metadata.isContinuous(featureIndex)) {
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
              // Unordered features: low-arity features in multiclass classification
              // 2^(maxFeatureValue- 1) - 1 combinations
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
              //  Ordered features: high-arity features, or not multiclass classification
              /* For a given categorical feature, use a subsample of the data
               * to choose how to arrange possible splits.
               * This examines each category and computes a centroid.
               * These centroids are later used to sort the possible splits.
               * centroidForCategories is a mapping: category (for the given feature) --> centroid
               */
              val centroidForCategories = {
                if (isMulticlassClassification) {
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
              for (i <- 0 until featureArity) {
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
                case ((category, value), binIndex) =>
                  categoriesForSplit = category :: categoriesForSplit
                  if (binIndex < numSplits) {
                    splits(featureIndex)(binIndex) =
                      new Split(featureIndex, Double.MinValue, Categorical, categoriesForSplit)
                  }
                  bins(featureIndex)(binIndex) = {
                    if (binIndex == 0) {
                      new Bin(new DummyCategoricalSplit(featureIndex, Categorical),
                        splits(featureIndex)(0), Categorical, category)
                    } else if (binIndex == numSplits) {
                      new Bin(splits(featureIndex)(binIndex - 1),
                        new Split(featureIndex, Double.MinValue, Categorical, categoriesForSplit),
                        Categorical, category)
                    } else {
                      new Bin(splits(featureIndex)(binIndex - 1), splits(featureIndex)(binIndex),
                        Categorical, category)
                    }
                  }
              }
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

  private[tree] def maxNodesInLevel(level: Int): Int = {
    math.pow(2, level).toInt
  }

  private[tree] def numUnorderedBins(arity: Int): Int = {
    (math.pow(2, arity - 1) - 1).toInt
  }

}
