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

import org.apache.spark.ml.tree._
import org.apache.spark.mllib.tree.model.ImpurityStats

/** Object exposing methods for local training of decision trees */
private[ml] object LocalDecisionTree {

  /**
   * Fully splits the passed-in node on the provided local dataset, returning
   * an InternalNode/LeafNode corresponding to the root of the resulting tree.
   *
   * @param node LearningNode to use as the root of the subtree fit on the passed-in dataset
   * @param metadata learning and dataset metadata for DecisionTree
   * @param splits splits(i) = array of splits for feature i
   */
  private[ml] def fitNode(
      input: Array[TreePoint],
      instanceWeights: Array[Double],
      node: LearningNode,
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]]): Node = {

    // The case with 1 node (depth = 0) is handled separately.
    // This allows all iterations in the depth > 0 case to use the same code.
    // TODO: Check that learning works when maxDepth > 0 but learning stops at 1 node (because of
    //       other parameters).
    if (metadata.maxDepth == 0) {
      return node.toNode
    }

    // Prepare column store.
    //   Note: rowToColumnStoreDense checks to make sure numRows < Int.MaxValue.
    val colStoreInit: Array[Array[Int]]
    = LocalDecisionTreeUtils.rowToColumnStoreDense(input.map(_.binnedFeatures))
    val labels = input.map(_.label)

    // Fit a regression model on the dataset, throwing an error if metadata indicates that
    // we should train a classifier.
    // TODO: Add support for training classifiers
    if (metadata.numClasses > 1 && metadata.numClasses <= 32) {
      throw new UnsupportedOperationException("Local training of a decision tree classifier is " +
        "unsupported; currently, only regression is supported")
    } else {
      trainRegressor(node, colStoreInit, instanceWeights, labels, metadata, splits)
    }
  }

  /**
   * Locally fits a decision tree regressor.
   * TODO(smurching): Logic for fitting a classifier & regressor is the same; only difference
   * is impurity metric. Use the same logic for fitting a classifier.
   *
   * @param rootNode Node to use as root of the tree fit on the passed-in dataset
   * @param colStoreInit Array of columns of training data
   * @param instanceWeights Array of weights for each training example
   * @param metadata learning and dataset metadata for DecisionTree
   * @param splits splits(i) = Array of possible splits for feature i
   * @return LeafNode or InternalNode representation of rootNode
   */
  private[ml] def trainRegressor(
      rootNode: LearningNode,
      colStoreInit: Array[Array[Int]],
      instanceWeights: Array[Double],
      labels: Array[Double],
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]]): Node = {

    // Sort each column by decision tree node.
    val colStore: Array[FeatureColumn] = colStoreInit.zipWithIndex.map { case (col, featureIndex) =>
      val featureArity: Int = metadata.featureArity.getOrElse(featureIndex, 0)
      FeatureColumn(featureIndex, col)
    }

    val numRows = colStore.headOption match {
      case None => 0
      case Some(column) => column.values.length
    }

    // Create a new TrainingInfo describing the status of our partially-trained subtree
    // at each iteration of training
    var trainingInfo: TrainingInfo = TrainingInfo(colStore,
      nodeOffsets = Array[(Int, Int)]((0, numRows)), currentLevelActiveNodes = Array(rootNode))

    // Iteratively learn, one level of the tree at a time.
    // Note: We do not use node IDs.
    var currentLevel = 0
    var doneLearning = false

    while (currentLevel < metadata.maxDepth && !doneLearning) {
      // Splits each active node if possible, returning an array of new active nodes
      val nextLevelNodes: Array[LearningNode] =
        computeBestSplits(trainingInfo, instanceWeights, labels, metadata, splits)
      // Count number of non-leaf nodes in the next level
      val estimatedRemainingActive = nextLevelNodes.count(!_.isLeaf)
      // TODO: Check to make sure we split something, and stop otherwise.
      doneLearning = currentLevel + 1 >= metadata.maxDepth || estimatedRemainingActive == 0
      if (!doneLearning) {
        // Obtain a new trainingInfo instance describing our current training status
        trainingInfo = trainingInfo.update(splits, nextLevelNodes)
      }
      currentLevel += 1
    }

    // Done with learning
    rootNode.toNode
  }

  /**
   * Iterate over feature values and labels for a specific (node, feature), updating stats
   * aggregator for the current node.
   */
  private[impl] def updateAggregator(
      statsAggregator: DTStatsAggregator,
      col: FeatureColumn,
      indices: Array[Int],
      instanceWeights: Array[Double],
      labels: Array[Double],
      from: Int,
      to: Int,
      featureIndexIdx: Int,
      featureSplits: Array[Split]): Unit = {
    val metadata = statsAggregator.metadata
    if (metadata.isUnordered(col.featureIndex)) {
      from.until(to).foreach { idx =>
        val rowIndex = indices(idx)
        AggUpdateUtils.updateUnorderedFeature(statsAggregator, col.values(idx), labels(rowIndex),
          featureIndex = col.featureIndex, featureIndexIdx, featureSplits,
          instanceWeight = instanceWeights(rowIndex))
      }
    } else {
      from.until(to).foreach { idx =>
        val rowIndex = indices(idx)
        AggUpdateUtils.updateOrderedFeature(statsAggregator, col.values(idx), labels(rowIndex),
          featureIndexIdx, instanceWeight = instanceWeights(rowIndex))
      }
    }
  }

  /**
   * Find the best splits for all active nodes
   *
   * @param trainingInfo Contains node offset info for current set of active nodes
   * @return  Array of new active nodes formed by splitting the current set of active nodes.
   */
  private def computeBestSplits(
      trainingInfo: TrainingInfo,
      instanceWeights: Array[Double],
      labels: Array[Double],
      metadata: DecisionTreeMetadata,
      splits: Array[Array[Split]]): Array[LearningNode] = {
    // For each node, select the best split across all features
    trainingInfo match {
      case TrainingInfo(columns: Array[FeatureColumn], nodeOffsets: Array[(Int, Int)],
        currentLevelActiveNodes: Array[LearningNode], _) => {
        // Filter out leaf nodes from the previous iteration
        val activeNonLeafs = currentLevelActiveNodes.zipWithIndex.filterNot(_._1.isLeaf)
        // Iterate over the active nodes in the current level.
        activeNonLeafs.flatMap { case (node: LearningNode, nodeIndex: Int) =>
          // Features for the current node start at fromOffset and end at toOffset
          val (from, to) = nodeOffsets(nodeIndex)
          // Get impurityCalculator containing label stats for all data points at the current node
          val parentImpurityCalc = ImpurityUtils.getParentImpurityCalculator(metadata,
            trainingInfo.indices, from, to, instanceWeights, labels)
          val validFeatureSplits = RandomForest.getFeaturesWithSplits(metadata,
            featuresForNode = None)
          // Find the best split for each feature for the current node
          val splitsAndImpurityInfo = validFeatureSplits.map { case (_, featureIndex) =>
            val col = columns(featureIndex)
            // Create a DTStatsAggregator to hold label statistics for each bin of the current
            // feature & compute said label statistics
            val statsAggregator = new DTStatsAggregator(metadata, Some(Array(featureIndex)))
            updateAggregator(statsAggregator, col, trainingInfo.indices, instanceWeights,
              labels, from, to, featureIndexIdx = 0, splits(col.featureIndex))
            // Choose best split for current feature based on label statistics
            SplitUtils.chooseSplit(statsAggregator, featureIndex, featureIndexIdx = 0,
              splits(featureIndex), Some(parentImpurityCalc))
          }
          // Find the best split overall (across all features) for the current node
          val (bestSplit, bestStats) = RandomForest.getBestSplitByGain(parentImpurityCalc, metadata,
            featuresForNode = None, splitsAndImpurityInfo)
          // Split current node, get an iterator over its children
          splitIfPossible(node, metadata, bestStats, bestSplit)
        }
      }
    }
  }

  /**
   * Splits the passed-in node if permitted by the parameters of the learning algorithm,
   * returning an iterator over its children. Returns an empty array if node could not be split.
   *
   * @param metadata learning and dataset metadata for DecisionTree
   * @param stats Label impurity stats associated with the current node
   */
  private[impl] def splitIfPossible(
      node: LearningNode,
      metadata: DecisionTreeMetadata,
      stats: ImpurityStats,
      split: Split): Iterator[LearningNode] = {
    if (stats.valid) {
      // Split node and return an iterator over its children; we filter out leaf nodes later
      doSplit(node, split, stats)
      Iterator(node.leftChild.get, node.rightChild.get)
    } else {
      node.stats = stats
      node.isLeaf = true
      Iterator()
    }
  }

  /**
   * Splits the passed-in node. This method returns nothing, but modifies the passed-in node
   * by updating its split and stats members.
   *
   * @param split Split to associate with the passed-in node
   * @param stats Label impurity statistics to associate with the passed-in node
   */
  private[impl] def doSplit(
      node: LearningNode,
      split: Split,
      stats: ImpurityStats): Unit = {
    val leftChildIsLeaf = stats.leftImpurity == 0
    node.leftChild = Some(LearningNode(id = LearningNode.leftChildIndex(node.id),
      isLeaf = leftChildIsLeaf,
      ImpurityStats.getEmptyImpurityStats(stats.leftImpurityCalculator)))
    val rightChildIsLeaf = stats.rightImpurity == 0
    node.rightChild = Some(LearningNode(id = LearningNode.rightChildIndex(node.id),
      isLeaf = rightChildIsLeaf,
      ImpurityStats.getEmptyImpurityStats(stats.rightImpurityCalculator)
    ))
    node.split = Some(split)
    node.isLeaf = false
    node.stats = stats
  }

}
