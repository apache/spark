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

import java.io.IOException

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impl.{BaggedPoint, DTStatsAggregator, DecisionTreeMetadata,
  TimeTracker}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{SamplingUtils, XORShiftRandom}


private[ml] object RandomForest extends Logging {

  /**
   * Train a random forest.
   * @param input Training data: RDD of [[org.apache.spark.mllib.regression.LabeledPoint]]
   * @return an unweighted set of trees
   */
  def run(
      input: RDD[LabeledPoint],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long,
      parentUID: Option[String] = None): Array[DecisionTreeModel] = {

    val timer = new TimeTracker()

    timer.start("total")

    timer.start("init")

    val retaggedInput = input.retag(classOf[LabeledPoint])
    val metadata =
      DecisionTreeMetadata.buildMetadata(retaggedInput, strategy, numTrees, featureSubsetStrategy)
    logDebug("algo = " + strategy.algo)
    logDebug("numTrees = " + numTrees)
    logDebug("seed = " + seed)
    logDebug("maxBins = " + metadata.maxBins)
    logDebug("featureSubsetStrategy = " + featureSubsetStrategy)
    logDebug("numFeaturesPerNode = " + metadata.numFeaturesPerNode)
    logDebug("subsamplingRate = " + strategy.subsamplingRate)

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplitsBins")
    val splits = findSplits(retaggedInput, metadata)
    timer.stop("findSplitsBins")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
      s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
    }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, splits, metadata)

    val withReplacement = numTrees > 1

    val baggedInput = BaggedPoint
      .convertToBaggedRDD(treeInput, strategy.subsamplingRate, numTrees, withReplacement, seed)
      .persist(StorageLevel.MEMORY_AND_DISK)

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")

    // Max memory usage for aggregates
    // TODO: Calculate memory usage more precisely.
    val maxMemoryUsage: Long = strategy.maxMemoryInMB * 1024L * 1024L
    logDebug("max memory usage for aggregates = " + maxMemoryUsage + " bytes.")
    val maxMemoryPerNode = {
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        // Find numFeaturesPerNode largest bins to get an upper bound on memory usage.
        Some(metadata.numBins.zipWithIndex.sortBy(- _._1)
          .take(metadata.numFeaturesPerNode).map(_._2))
      } else {
        None
      }
      RandomForest.aggregateSizeForNode(metadata, featureSubset) * 8L
    }
    require(maxMemoryPerNode <= maxMemoryUsage,
      s"RandomForest/DecisionTree given maxMemoryInMB = ${strategy.maxMemoryInMB}," +
        " which is too small for the given features." +
        s"  Minimum value = ${maxMemoryPerNode / (1024L * 1024L)}")

    timer.stop("init")

    /*
     * The main idea here is to perform group-wise training of the decision tree nodes thus
     * reducing the passes over the data from (# nodes) to (# nodes / maxNumberOfNodesPerGroup).
     * Each data sample is handled by a particular node (or it reaches a leaf and is not used
     * in lower levels).
     */

    // Create an RDD of node Id cache.
    // At first, all the rows belong to the root nodes (node Id == 1).
    val nodeIdCache = if (strategy.useNodeIdCache) {
      Some(NodeIdCache.init(
        data = baggedInput,
        numTrees = numTrees,
        checkpointInterval = strategy.checkpointInterval,
        initVal = 1))
    } else {
      None
    }

    // FIFO queue of nodes to train: (treeIndex, node)
    val nodeQueue = new mutable.Queue[(Int, LearningNode)]()

    val rng = new Random()
    rng.setSeed(seed)

    // Allocate and queue root nodes.
    val topNodes = Array.fill[LearningNode](numTrees)(LearningNode.emptyNode(nodeIndex = 1))
    Range(0, numTrees).foreach(treeIndex => nodeQueue.enqueue((treeIndex, topNodes(treeIndex))))

    while (nodeQueue.nonEmpty) {
      // Collect some nodes to split, and choose features for each node (if subsampling).
      // Each group of nodes may come from one or multiple trees, and at multiple levels.
      val (nodesForGroup, treeToNodeToIndexInfo) =
        RandomForest.selectNodesToSplit(nodeQueue, maxMemoryUsage, metadata, rng)
      // Sanity check (should never occur):
      assert(nodesForGroup.nonEmpty,
        s"RandomForest selected empty nodesForGroup.  Error for unknown reason.")

      // Choose node splits, and enqueue new nodes as needed.
      timer.start("findBestSplits")
      RandomForest.findBestSplits(baggedInput, metadata, topNodes, nodesForGroup,
        treeToNodeToIndexInfo, splits, nodeQueue, timer, nodeIdCache)
      timer.stop("findBestSplits")
    }

    baggedInput.unpersist()

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(s"$timer")

    // Delete any remaining checkpoints used for node Id cache.
    if (nodeIdCache.nonEmpty) {
      try {
        nodeIdCache.get.deleteAllCheckpoints()
      } catch {
        case e: IOException =>
          logWarning(s"delete all checkpoints failed. Error reason: ${e.getMessage}")
      }
    }

    parentUID match {
      case Some(uid) =>
        if (strategy.algo == OldAlgo.Classification) {
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(uid, rootNode.toNode, strategy.getNumClasses)
          }
        } else {
          topNodes.map(rootNode => new DecisionTreeRegressionModel(uid, rootNode.toNode))
        }
      case None =>
        if (strategy.algo == OldAlgo.Classification) {
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(rootNode.toNode, strategy.getNumClasses)
          }
        } else {
          topNodes.map(rootNode => new DecisionTreeRegressionModel(rootNode.toNode))
        }
    }
  }

  /**
   * Get the node index corresponding to this data point.
   * This function mimics prediction, passing an example from the root node down to a leaf
   * or unsplit node; that node's index is returned.
   *
   * @param node  Node in tree from which to classify the given data point.
   * @param binnedFeatures  Binned feature vector for data point.
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @return  Leaf index if the data point reaches a leaf.
   *          Otherwise, last node reachable in tree matching this example.
   *          Note: This is the global node index, i.e., the index used in the tree.
   *                This index is different from the index used during training a particular
   *                group of nodes on one call to [[findBestSplits()]].
   */
  private def predictNodeIndex(
      node: LearningNode,
      binnedFeatures: Array[Int],
      splits: Array[Array[Split]]): Int = {
    if (node.isLeaf || node.split.isEmpty) {
      node.id
    } else {
      val split = node.split.get
      val featureIndex = split.featureIndex
      val splitLeft = split.shouldGoLeft(binnedFeatures(featureIndex), splits(featureIndex))
      if (node.leftChild.isEmpty) {
        // Not yet split. Return index from next layer of nodes to train
        if (splitLeft) {
          LearningNode.leftChildIndex(node.id)
        } else {
          LearningNode.rightChildIndex(node.id)
        }
      } else {
        if (splitLeft) {
          predictNodeIndex(node.leftChild.get, binnedFeatures, splits)
        } else {
          predictNodeIndex(node.rightChild.get, binnedFeatures, splits)
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
   *             each (feature, bin).
   * @param treePoint  Data point being aggregated.
   * @param splits possible splits indexed (numFeatures)(numSplits)
   * @param unorderedFeatures  Set of indices of unordered features.
   * @param instanceWeight  Weight (importance) of instance in dataset.
   */
  private def mixedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      splits: Array[Array[Split]],
      unorderedFeatures: Set[Int],
      instanceWeight: Double,
      featuresForNode: Option[Array[Int]]): Unit = {
    val numFeaturesPerNode = if (featuresForNode.nonEmpty) {
      // Use subsampled features
      featuresForNode.get.length
    } else {
      // Use all features
      agg.metadata.numFeatures
    }
    // Iterate over features.
    var featureIndexIdx = 0
    while (featureIndexIdx < numFeaturesPerNode) {
      val featureIndex = if (featuresForNode.nonEmpty) {
        featuresForNode.get.apply(featureIndexIdx)
      } else {
        featureIndexIdx
      }
      if (unorderedFeatures.contains(featureIndex)) {
        // Unordered feature
        val featureValue = treePoint.binnedFeatures(featureIndex)
        val (leftNodeFeatureOffset, rightNodeFeatureOffset) =
          agg.getLeftRightFeatureOffsets(featureIndexIdx)
        // Update the left or right bin for each split.
        val numSplits = agg.metadata.numSplits(featureIndex)
        val featureSplits = splits(featureIndex)
        var splitIndex = 0
        while (splitIndex < numSplits) {
          if (featureSplits(splitIndex).shouldGoLeft(featureValue, featureSplits)) {
            agg.featureUpdate(leftNodeFeatureOffset, splitIndex, treePoint.label, instanceWeight)
          } else {
            agg.featureUpdate(rightNodeFeatureOffset, splitIndex, treePoint.label, instanceWeight)
          }
          splitIndex += 1
        }
      } else {
        // Ordered feature
        val binIndex = treePoint.binnedFeatures(featureIndex)
        agg.update(featureIndexIdx, binIndex, treePoint.label, instanceWeight)
      }
      featureIndexIdx += 1
    }
  }

  /**
   * Helper for binSeqOp, for regression and for classification with only ordered features.
   *
   * For each feature, the sufficient statistics of one bin are updated.
   *
   * @param agg  Array storing aggregate calculation, with a set of sufficient statistics for
   *             each (feature, bin).
   * @param treePoint  Data point being aggregated.
   * @param instanceWeight  Weight (importance) of instance in dataset.
   */
  private def orderedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      instanceWeight: Double,
      featuresForNode: Option[Array[Int]]): Unit = {
    val label = treePoint.label

    // Iterate over features.
    if (featuresForNode.nonEmpty) {
      // Use subsampled features
      var featureIndexIdx = 0
      while (featureIndexIdx < featuresForNode.get.length) {
        val binIndex = treePoint.binnedFeatures(featuresForNode.get.apply(featureIndexIdx))
        agg.update(featureIndexIdx, binIndex, label, instanceWeight)
        featureIndexIdx += 1
      }
    } else {
      // Use all features
      val numFeatures = agg.metadata.numFeatures
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        val binIndex = treePoint.binnedFeatures(featureIndex)
        agg.update(featureIndex, binIndex, label, instanceWeight)
        featureIndex += 1
      }
    }
  }

  /**
   * Given a group of nodes, this finds the best split for each node.
   *
   * @param input Training data: RDD of [[org.apache.spark.mllib.tree.impl.TreePoint]]
   * @param metadata Learning and dataset metadata
   * @param topNodes Root node for each tree.  Used for matching instances with nodes.
   * @param nodesForGroup Mapping: treeIndex --> nodes to be split in tree
   * @param treeToNodeToIndexInfo Mapping: treeIndex --> nodeIndex --> nodeIndexInfo,
   *                              where nodeIndexInfo stores the index in the group and the
   *                              feature subsets (if using feature subsets).
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param nodeQueue  Queue of nodes to split, with values (treeIndex, node).
   *                   Updated with new non-leaf nodes which are created.
   * @param nodeIdCache Node Id cache containing an RDD of Array[Int] where
   *                    each value in the array is the data point's node Id
   *                    for a corresponding tree. This is used to prevent the need
   *                    to pass the entire tree to the executors during
   *                    the node stat aggregation phase.
   */
  private[tree] def findBestSplits(
      input: RDD[BaggedPoint[TreePoint]],
      metadata: DecisionTreeMetadata,
      topNodes: Array[LearningNode],
      nodesForGroup: Map[Int, Array[LearningNode]],
      treeToNodeToIndexInfo: Map[Int, Map[Int, NodeIndexInfo]],
      splits: Array[Array[Split]],
      nodeQueue: mutable.Queue[(Int, LearningNode)],
      timer: TimeTracker = new TimeTracker,
      nodeIdCache: Option[NodeIdCache] = None): Unit = {

    /*
     * The high-level descriptions of the best split optimizations are noted here.
     *
     * *Group-wise training*
     * We perform bin calculations for groups of nodes to reduce the number of
     * passes over the data.  Each iteration requires more computation and storage,
     * but saves several iterations over the data.
     *
     * *Bin-wise computation*
     * We use a bin-wise best split computation strategy instead of a straightforward best split
     * computation strategy. Instead of analyzing each sample for contribution to the left/right
     * child node impurity of every split, we first categorize each feature of a sample into a
     * bin. We exploit this structure to calculate aggregates for bins and then use these aggregates
     * to calculate information gain for each split.
     *
     * *Aggregation over partitions*
     * Instead of performing a flatMap/reduceByKey operation, we exploit the fact that we know
     * the number of splits in advance. Thus, we store the aggregates (at the appropriate
     * indices) in a single array for all bins and rely upon the RDD aggregate method to
     * drastically reduce the communication overhead.
     */

    // numNodes:  Number of nodes in this group
    val numNodes = nodesForGroup.values.map(_.length).sum
    logDebug("numNodes = " + numNodes)
    logDebug("numFeatures = " + metadata.numFeatures)
    logDebug("numClasses = " + metadata.numClasses)
    logDebug("isMulticlass = " + metadata.isMulticlass)
    logDebug("isMulticlassWithCategoricalFeatures = " +
      metadata.isMulticlassWithCategoricalFeatures)
    logDebug("using nodeIdCache = " + nodeIdCache.nonEmpty.toString)

    /**
     * Performs a sequential aggregation over a partition for a particular tree and node.
     *
     * For each feature, the aggregate sufficient statistics are updated for the relevant
     * bins.
     *
     * @param treeIndex Index of the tree that we want to perform aggregation for.
     * @param nodeInfo The node info for the tree node.
     * @param agg Array storing aggregate calculation, with a set of sufficient statistics
     *            for each (node, feature, bin).
     * @param baggedPoint Data point being aggregated.
     */
    def nodeBinSeqOp(
        treeIndex: Int,
        nodeInfo: NodeIndexInfo,
        agg: Array[DTStatsAggregator],
        baggedPoint: BaggedPoint[TreePoint]): Unit = {
      if (nodeInfo != null) {
        val aggNodeIndex = nodeInfo.nodeIndexInGroup
        val featuresForNode = nodeInfo.featureSubset
        val instanceWeight = baggedPoint.subsampleWeights(treeIndex)
        if (metadata.unorderedFeatures.isEmpty) {
          orderedBinSeqOp(agg(aggNodeIndex), baggedPoint.datum, instanceWeight, featuresForNode)
        } else {
          mixedBinSeqOp(agg(aggNodeIndex), baggedPoint.datum, splits,
            metadata.unorderedFeatures, instanceWeight, featuresForNode)
        }
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
     * @param baggedPoint   Data point being aggregated.
     * @return  agg
     */
    def binSeqOp(
        agg: Array[DTStatsAggregator],
        baggedPoint: BaggedPoint[TreePoint]): Array[DTStatsAggregator] = {
      treeToNodeToIndexInfo.foreach { case (treeIndex, nodeIndexToInfo) =>
        val nodeIndex =
          predictNodeIndex(topNodes(treeIndex), baggedPoint.datum.binnedFeatures, splits)
        nodeBinSeqOp(treeIndex, nodeIndexToInfo.getOrElse(nodeIndex, null), agg, baggedPoint)
      }
      agg
    }

    /**
     * Do the same thing as binSeqOp, but with nodeIdCache.
     */
    def binSeqOpWithNodeIdCache(
        agg: Array[DTStatsAggregator],
        dataPoint: (BaggedPoint[TreePoint], Array[Int])): Array[DTStatsAggregator] = {
      treeToNodeToIndexInfo.foreach { case (treeIndex, nodeIndexToInfo) =>
        val baggedPoint = dataPoint._1
        val nodeIdCache = dataPoint._2
        val nodeIndex = nodeIdCache(treeIndex)
        nodeBinSeqOp(treeIndex, nodeIndexToInfo.getOrElse(nodeIndex, null), agg, baggedPoint)
      }

      agg
    }

    /**
     * Get node index in group --> features indices map,
     * which is a short cut to find feature indices for a node given node index in group.
     */
    def getNodeToFeatures(
        treeToNodeToIndexInfo: Map[Int, Map[Int, NodeIndexInfo]]): Option[Map[Int, Array[Int]]] = {
      if (!metadata.subsamplingFeatures) {
        None
      } else {
        val mutableNodeToFeatures = new mutable.HashMap[Int, Array[Int]]()
        treeToNodeToIndexInfo.values.foreach { nodeIdToNodeInfo =>
          nodeIdToNodeInfo.values.foreach { nodeIndexInfo =>
            assert(nodeIndexInfo.featureSubset.isDefined)
            mutableNodeToFeatures(nodeIndexInfo.nodeIndexInGroup) = nodeIndexInfo.featureSubset.get
          }
        }
        Some(mutableNodeToFeatures.toMap)
      }
    }

    // array of nodes to train indexed by node index in group
    val nodes = new Array[LearningNode](numNodes)
    nodesForGroup.foreach { case (treeIndex, nodesForTree) =>
      nodesForTree.foreach { node =>
        nodes(treeToNodeToIndexInfo(treeIndex)(node.id).nodeIndexInGroup) = node
      }
    }

    // Calculate best splits for all nodes in the group
    timer.start("chooseSplits")

    // In each partition, iterate all instances and compute aggregate stats for each node,
    // yield an (nodeIndex, nodeAggregateStats) pair for each node.
    // After a `reduceByKey` operation,
    // stats of a node will be shuffled to a particular partition and be combined together,
    // then best splits for nodes are found there.
    // Finally, only best Splits for nodes are collected to driver to construct decision tree.
    val nodeToFeatures = getNodeToFeatures(treeToNodeToIndexInfo)
    val nodeToFeaturesBc = input.sparkContext.broadcast(nodeToFeatures)

    val partitionAggregates : RDD[(Int, DTStatsAggregator)] = if (nodeIdCache.nonEmpty) {
      input.zip(nodeIdCache.get.nodeIdsForInstances).mapPartitions { points =>
        // Construct a nodeStatsAggregators array to hold node aggregate stats,
        // each node will have a nodeStatsAggregator
        val nodeStatsAggregators = Array.tabulate(numNodes) { nodeIndex =>
          val featuresForNode = nodeToFeaturesBc.value.flatMap { nodeToFeatures =>
            Some(nodeToFeatures(nodeIndex))
          }
          new DTStatsAggregator(metadata, featuresForNode)
        }

        // iterator all instances in current partition and update aggregate stats
        points.foreach(binSeqOpWithNodeIdCache(nodeStatsAggregators, _))

        // transform nodeStatsAggregators array to (nodeIndex, nodeAggregateStats) pairs,
        // which can be combined with other partition using `reduceByKey`
        nodeStatsAggregators.view.zipWithIndex.map(_.swap).iterator
      }
    } else {
      input.mapPartitions { points =>
        // Construct a nodeStatsAggregators array to hold node aggregate stats,
        // each node will have a nodeStatsAggregator
        val nodeStatsAggregators = Array.tabulate(numNodes) { nodeIndex =>
          val featuresForNode = nodeToFeaturesBc.value.flatMap { nodeToFeatures =>
            Some(nodeToFeatures(nodeIndex))
          }
          new DTStatsAggregator(metadata, featuresForNode)
        }

        // iterator all instances in current partition and update aggregate stats
        points.foreach(binSeqOp(nodeStatsAggregators, _))

        // transform nodeStatsAggregators array to (nodeIndex, nodeAggregateStats) pairs,
        // which can be combined with other partition using `reduceByKey`
        nodeStatsAggregators.view.zipWithIndex.map(_.swap).iterator
      }
    }

    val nodeToBestSplits = partitionAggregates.reduceByKey((a, b) => a.merge(b)).map {
      case (nodeIndex, aggStats) =>
        val featuresForNode = nodeToFeaturesBc.value.flatMap { nodeToFeatures =>
          Some(nodeToFeatures(nodeIndex))
        }

        // find best split for each node
        val (split: Split, stats: ImpurityStats) =
          binsToBestSplit(aggStats, splits, featuresForNode, nodes(nodeIndex))
        (nodeIndex, (split, stats))
    }.collectAsMap()

    timer.stop("chooseSplits")

    val nodeIdUpdaters = if (nodeIdCache.nonEmpty) {
      Array.fill[mutable.Map[Int, NodeIndexUpdater]](
        metadata.numTrees)(mutable.Map[Int, NodeIndexUpdater]())
    } else {
      null
    }
    // Iterate over all nodes in this group.
    nodesForGroup.foreach { case (treeIndex, nodesForTree) =>
      nodesForTree.foreach { node =>
        val nodeIndex = node.id
        val nodeInfo = treeToNodeToIndexInfo(treeIndex)(nodeIndex)
        val aggNodeIndex = nodeInfo.nodeIndexInGroup
        val (split: Split, stats: ImpurityStats) =
          nodeToBestSplits(aggNodeIndex)
        logDebug("best split = " + split)

        // Extract info for this node.  Create children if not leaf.
        val isLeaf =
          (stats.gain <= 0) || (LearningNode.indexToLevel(nodeIndex) == metadata.maxDepth)
        node.isLeaf = isLeaf
        node.stats = stats
        logDebug("Node = " + node)

        if (!isLeaf) {
          node.split = Some(split)
          val childIsLeaf = (LearningNode.indexToLevel(nodeIndex) + 1) == metadata.maxDepth
          val leftChildIsLeaf = childIsLeaf || (stats.leftImpurity == 0.0)
          val rightChildIsLeaf = childIsLeaf || (stats.rightImpurity == 0.0)
          node.leftChild = Some(LearningNode(LearningNode.leftChildIndex(nodeIndex),
            leftChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.leftImpurityCalculator)))
          node.rightChild = Some(LearningNode(LearningNode.rightChildIndex(nodeIndex),
            rightChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.rightImpurityCalculator)))

          if (nodeIdCache.nonEmpty) {
            val nodeIndexUpdater = NodeIndexUpdater(
              split = split,
              nodeIndex = nodeIndex)
            nodeIdUpdaters(treeIndex).put(nodeIndex, nodeIndexUpdater)
          }

          // enqueue left child and right child if they are not leaves
          if (!leftChildIsLeaf) {
            nodeQueue.enqueue((treeIndex, node.leftChild.get))
          }
          if (!rightChildIsLeaf) {
            nodeQueue.enqueue((treeIndex, node.rightChild.get))
          }

          logDebug("leftChildIndex = " + node.leftChild.get.id +
            ", impurity = " + stats.leftImpurity)
          logDebug("rightChildIndex = " + node.rightChild.get.id +
            ", impurity = " + stats.rightImpurity)
        }
      }
    }

    if (nodeIdCache.nonEmpty) {
      // Update the cache if needed.
      nodeIdCache.get.updateNodeIndices(input, nodeIdUpdaters, splits)
    }
  }

  /**
   * Calculate the impurity statistics for a give (feature, split) based upon left/right aggregates.
   * @param stats the recycle impurity statistics for this feature's all splits,
   *              only 'impurity' and 'impurityCalculator' are valid between each iteration
   * @param leftImpurityCalculator left node aggregates for this (feature, split)
   * @param rightImpurityCalculator right node aggregate for this (feature, split)
   * @param metadata learning and dataset metadata for DecisionTree
   * @return Impurity statistics for this (feature, split)
   */
  private def calculateImpurityStats(
      stats: ImpurityStats,
      leftImpurityCalculator: ImpurityCalculator,
      rightImpurityCalculator: ImpurityCalculator,
      metadata: DecisionTreeMetadata): ImpurityStats = {

    val parentImpurityCalculator: ImpurityCalculator = if (stats == null) {
      leftImpurityCalculator.copy.add(rightImpurityCalculator)
    } else {
      stats.impurityCalculator
    }

    val impurity: Double = if (stats == null) {
      parentImpurityCalculator.calculate()
    } else {
      stats.impurity
    }

    val leftCount = leftImpurityCalculator.count
    val rightCount = rightImpurityCalculator.count

    val totalCount = leftCount + rightCount

    // If left child or right child doesn't satisfy minimum instances per node,
    // then this split is invalid, return invalid information gain stats.
    if ((leftCount < metadata.minInstancesPerNode) ||
      (rightCount < metadata.minInstancesPerNode)) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    val leftImpurity = leftImpurityCalculator.calculate() // Note: This equals 0 if count = 0
    val rightImpurity = rightImpurityCalculator.calculate()

    val leftWeight = leftCount / totalCount.toDouble
    val rightWeight = rightCount / totalCount.toDouble

    val gain = impurity - leftWeight * leftImpurity - rightWeight * rightImpurity

    // if information gain doesn't satisfy minimum information gain,
    // then this split is invalid, return invalid information gain stats.
    if (gain < metadata.minInfoGain) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    new ImpurityStats(gain, impurity, parentImpurityCalculator,
      leftImpurityCalculator, rightImpurityCalculator)
  }

  /**
   * Find the best split for a node.
   * @param binAggregates Bin statistics.
   * @return tuple for best split: (Split, information gain, prediction at node)
   */
  private def binsToBestSplit(
      binAggregates: DTStatsAggregator,
      splits: Array[Array[Split]],
      featuresForNode: Option[Array[Int]],
      node: LearningNode): (Split, ImpurityStats) = {

    // Calculate InformationGain and ImpurityStats if current node is top node
    val level = LearningNode.indexToLevel(node.id)
    var gainAndImpurityStats: ImpurityStats = if (level ==0) {
      null
    } else {
      node.stats
    }

    // For each (feature, split), calculate the gain, and select the best (feature, split).
    val (bestSplit, bestSplitStats) =
      Range(0, binAggregates.metadata.numFeaturesPerNode).map { featureIndexIdx =>
        val featureIndex = if (featuresForNode.nonEmpty) {
          featuresForNode.get.apply(featureIndexIdx)
        } else {
          featureIndexIdx
        }
        val numSplits = binAggregates.metadata.numSplits(featureIndex)
        if (binAggregates.metadata.isContinuous(featureIndex)) {
          // Cumulative sum (scanLeft) of bin statistics.
          // Afterwards, binAggregates for a bin is the sum of aggregates for
          // that bin + all preceding bins.
          val nodeFeatureOffset = binAggregates.getFeatureOffset(featureIndexIdx)
          var splitIndex = 0
          while (splitIndex < numSplits) {
            binAggregates.mergeForFeature(nodeFeatureOffset, splitIndex + 1, splitIndex)
            splitIndex += 1
          }
          // Find best split.
          val (bestFeatureSplitIndex, bestFeatureGainStats) =
            Range(0, numSplits).map { case splitIdx =>
              val leftChildStats = binAggregates.getImpurityCalculator(nodeFeatureOffset, splitIdx)
              val rightChildStats =
                binAggregates.getImpurityCalculator(nodeFeatureOffset, numSplits)
              rightChildStats.subtract(leftChildStats)
              gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
                leftChildStats, rightChildStats, binAggregates.metadata)
              (splitIdx, gainAndImpurityStats)
            }.maxBy(_._2.gain)
          (splits(featureIndex)(bestFeatureSplitIndex), bestFeatureGainStats)
        } else if (binAggregates.metadata.isUnordered(featureIndex)) {
          // Unordered categorical feature
          val (leftChildOffset, rightChildOffset) =
            binAggregates.getLeftRightFeatureOffsets(featureIndexIdx)
          val (bestFeatureSplitIndex, bestFeatureGainStats) =
            Range(0, numSplits).map { splitIndex =>
              val leftChildStats = binAggregates.getImpurityCalculator(leftChildOffset, splitIndex)
              val rightChildStats =
                binAggregates.getImpurityCalculator(rightChildOffset, splitIndex)
              gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
                leftChildStats, rightChildStats, binAggregates.metadata)
              (splitIndex, gainAndImpurityStats)
            }.maxBy(_._2.gain)
          (splits(featureIndex)(bestFeatureSplitIndex), bestFeatureGainStats)
        } else {
          // Ordered categorical feature
          val nodeFeatureOffset = binAggregates.getFeatureOffset(featureIndexIdx)
          val numCategories = binAggregates.metadata.numBins(featureIndex)

          /* Each bin is one category (feature value).
           * The bins are ordered based on centroidForCategories, and this ordering determines which
           * splits are considered.  (With K categories, we consider K - 1 possible splits.)
           *
           * centroidForCategories is a list: (category, centroid)
           */
          val centroidForCategories = if (binAggregates.metadata.isMulticlass) {
            // For categorical variables in multiclass classification,
            // the bins are ordered by the impurity of their corresponding labels.
            Range(0, numCategories).map { case featureValue =>
              val categoryStats =
                binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
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
            Range(0, numCategories).map { case featureValue =>
              val categoryStats =
                binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
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
            binAggregates.mergeForFeature(nodeFeatureOffset, nextCategory, currentCategory)
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
              gainAndImpurityStats = calculateImpurityStats(gainAndImpurityStats,
                leftChildStats, rightChildStats, binAggregates.metadata)
              (splitIndex, gainAndImpurityStats)
            }.maxBy(_._2.gain)
          val categoriesForSplit =
            categoriesSortedByCentroid.map(_._1.toDouble).slice(0, bestFeatureSplitIndex + 1)
          val bestFeatureSplit =
            new CategoricalSplit(featureIndex, categoriesForSplit.toArray, numCategories)
          (bestFeatureSplit, bestFeatureGainStats)
        }
      }.maxBy(_._2.gain)

    (bestSplit, bestSplitStats)
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
  protected[tree] def findSplits(
      input: RDD[LabeledPoint],
      metadata: DecisionTreeMetadata): Array[Array[Split]] = {

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
      input.sample(withReplacement = false, fraction, new XORShiftRandom(1).nextInt()).collect()
    } else {
      new Array[LabeledPoint](0)
    }

    val splits = new Array[Array[Split]](numFeatures)

    // Find all splits.
    // Iterate over all features.
    var featureIndex = 0
    while (featureIndex < numFeatures) {
      if (metadata.isContinuous(featureIndex)) {
        val featureSamples = sampledInput.map(_.features(featureIndex))
        val featureSplits = findSplitsForContinuousFeature(featureSamples, metadata, featureIndex)

        val numSplits = featureSplits.length
        logDebug(s"featureIndex = $featureIndex, numSplits = $numSplits")
        splits(featureIndex) = new Array[Split](numSplits)

        var splitIndex = 0
        while (splitIndex < numSplits) {
          val threshold = featureSplits(splitIndex)
          splits(featureIndex)(splitIndex) = new ContinuousSplit(featureIndex, threshold)
          splitIndex += 1
        }
      } else {
        // Categorical feature
        if (metadata.isUnordered(featureIndex)) {
          val numSplits = metadata.numSplits(featureIndex)
          val featureArity = metadata.featureArity(featureIndex)
          // TODO: Use an implicit representation mapping each category to a subset of indices.
          //       I.e., track indices such that we can calculate the set of bins for which
          //       feature value x splits to the left.
          // Unordered features
          // 2^(maxFeatureValue - 1) - 1 combinations
          splits(featureIndex) = new Array[Split](numSplits)
          var splitIndex = 0
          while (splitIndex < numSplits) {
            val categories: List[Double] =
              extractMultiClassCategories(splitIndex + 1, featureArity)
            splits(featureIndex)(splitIndex) =
              new CategoricalSplit(featureIndex, categories.toArray, featureArity)
            splitIndex += 1
          }
        } else {
          // Ordered features
          //   Bins correspond to feature values, so we do not need to compute splits or bins
          //   beforehand.  Splits are constructed as needed during training.
          splits(featureIndex) = new Array[Split](0)
        }
      }
      featureIndex += 1
    }
    splits
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

  /**
   * Find splits for a continuous feature
   * NOTE: Returned number of splits is set based on `featureSamples` and
   *       could be different from the specified `numSplits`.
   *       The `numSplits` attribute in the `DecisionTreeMetadata` class will be set accordingly.
   * @param featureSamples feature values of each sample
   * @param metadata decision tree metadata
   *                 NOTE: `metadata.numbins` will be changed accordingly
   *                       if there are not enough splits to be found
   * @param featureIndex feature index to find splits
   * @return array of splits
   */
  private[tree] def findSplitsForContinuousFeature(
      featureSamples: Array[Double],
      metadata: DecisionTreeMetadata,
      featureIndex: Int): Array[Double] = {
    require(metadata.isContinuous(featureIndex),
      "findSplitsForContinuousFeature can only be used to find splits for a continuous feature.")

    val splits = {
      val numSplits = metadata.numSplits(featureIndex)

      // get count for each distinct value
      val valueCountMap = featureSamples.foldLeft(Map.empty[Double, Int]) { (m, x) =>
        m + ((x, m.getOrElse(x, 0) + 1))
      }
      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray

      // if possible splits is not enough or just enough, just return all possible splits
      val possibleSplits = valueCounts.length
      if (possibleSplits <= numSplits) {
        valueCounts.map(_._1)
      } else {
        // stride between splits
        val stride: Double = featureSamples.length.toDouble / (numSplits + 1)
        logDebug("stride = " + stride)

        // iterate `valueCount` to find splits
        val splitsBuilder = mutable.ArrayBuilder.make[Double]
        var index = 1
        // currentCount: sum of counts of values that have been visited
        var currentCount = valueCounts(0)._2
        // targetCount: target value for `currentCount`.
        // If `currentCount` is closest value to `targetCount`,
        // then current value is a split threshold.
        // After finding a split threshold, `targetCount` is added by stride.
        var targetCount = stride
        while (index < valueCounts.length) {
          val previousCount = currentCount
          currentCount += valueCounts(index)._2
          val previousGap = math.abs(previousCount - targetCount)
          val currentGap = math.abs(currentCount - targetCount)
          // If adding count of current value to currentCount
          // makes the gap between currentCount and targetCount smaller,
          // previous value is a split threshold.
          if (previousGap < currentGap) {
            splitsBuilder += valueCounts(index - 1)._1
            targetCount += stride
          }
          index += 1
        }

        splitsBuilder.result()
      }
    }

    // TODO: Do not fail; just ignore the useless feature.
    assert(splits.length > 0,
      s"DecisionTree could not handle feature $featureIndex since it had only 1 unique value." +
        "  Please remove this feature and then try again.")
    // set number of splits accordingly
    metadata.setNumSplits(featureIndex, splits.length)

    splits
  }

  private[tree] class NodeIndexInfo(
      val nodeIndexInGroup: Int,
      val featureSubset: Option[Array[Int]]) extends Serializable

  /**
   * Pull nodes off of the queue, and collect a group of nodes to be split on this iteration.
   * This tracks the memory usage for aggregates and stops adding nodes when too much memory
   * will be needed; this allows an adaptive number of nodes since different nodes may require
   * different amounts of memory (if featureSubsetStrategy is not "all").
   *
   * @param nodeQueue  Queue of nodes to split.
   * @param maxMemoryUsage  Bound on size of aggregate statistics.
   * @return  (nodesForGroup, treeToNodeToIndexInfo).
   *          nodesForGroup holds the nodes to split: treeIndex --> nodes in tree.
   *
   *          treeToNodeToIndexInfo holds indices selected features for each node:
   *            treeIndex --> (global) node index --> (node index in group, feature indices).
   *          The (global) node index is the index in the tree; the node index in group is the
   *           index in [0, numNodesInGroup) of the node in this group.
   *          The feature indices are None if not subsampling features.
   */
  private[tree] def selectNodesToSplit(
      nodeQueue: mutable.Queue[(Int, LearningNode)],
      maxMemoryUsage: Long,
      metadata: DecisionTreeMetadata,
      rng: Random): (Map[Int, Array[LearningNode]], Map[Int, Map[Int, NodeIndexInfo]]) = {
    // Collect some nodes to split:
    //  nodesForGroup(treeIndex) = nodes to split
    val mutableNodesForGroup = new mutable.HashMap[Int, mutable.ArrayBuffer[LearningNode]]()
    val mutableTreeToNodeToIndexInfo =
      new mutable.HashMap[Int, mutable.HashMap[Int, NodeIndexInfo]]()
    var memUsage: Long = 0L
    var numNodesInGroup = 0
    while (nodeQueue.nonEmpty && memUsage < maxMemoryUsage) {
      val (treeIndex, node) = nodeQueue.head
      // Choose subset of features for node (if subsampling).
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        Some(SamplingUtils.reservoirSampleAndCount(Range(0,
          metadata.numFeatures).iterator, metadata.numFeaturesPerNode, rng.nextLong())._1)
      } else {
        None
      }
      // Check if enough memory remains to add this node to the group.
      val nodeMemUsage = RandomForest.aggregateSizeForNode(metadata, featureSubset) * 8L
      if (memUsage + nodeMemUsage <= maxMemoryUsage) {
        nodeQueue.dequeue()
        mutableNodesForGroup.getOrElseUpdate(treeIndex, new mutable.ArrayBuffer[LearningNode]()) +=
          node
        mutableTreeToNodeToIndexInfo
          .getOrElseUpdate(treeIndex, new mutable.HashMap[Int, NodeIndexInfo]())(node.id)
          = new NodeIndexInfo(numNodesInGroup, featureSubset)
      }
      numNodesInGroup += 1
      memUsage += nodeMemUsage
    }
    // Convert mutable maps to immutable ones.
    val nodesForGroup: Map[Int, Array[LearningNode]] =
      mutableNodesForGroup.mapValues(_.toArray).toMap
    val treeToNodeToIndexInfo = mutableTreeToNodeToIndexInfo.mapValues(_.toMap).toMap
    (nodesForGroup, treeToNodeToIndexInfo)
  }

  /**
   * Get the number of values to be stored for this node in the bin aggregates.
   * @param featureSubset  Indices of features which may be split at this node.
   *                       If None, then use all features.
   */
  private def aggregateSizeForNode(
      metadata: DecisionTreeMetadata,
      featureSubset: Option[Array[Int]]): Long = {
    val totalBins = if (featureSubset.nonEmpty) {
      featureSubset.get.map(featureIndex => metadata.numBins(featureIndex).toLong).sum
    } else {
      metadata.numBins.map(_.toLong).sum
    }
    if (metadata.isClassification) {
      metadata.numClasses * totalBins
    } else {
      3 * totalBins
    }
  }

  /**
   * Given a Random Forest model, compute the importance of each feature.
   * This generalizes the idea of "Gini" importance to other losses,
   * following the explanation of Gini importance from "Random Forests" documentation
   * by Leo Breiman and Adele Cutler, and following the implementation from scikit-learn.
   *
   * This feature importance is calculated as follows:
   *  - Average over trees:
   *     - importance(feature j) = sum (over nodes which split on feature j) of the gain,
   *       where gain is scaled by the number of instances passing through node
   *     - Normalize importances for tree based on total number of training instances used
   *       to build tree.
   *  - Normalize feature importance vector to sum to 1.
   *
   * Note: This should not be used with Gradient-Boosted Trees.  It only makes sense for
   *       independently trained trees.
   * @param trees  Unweighted forest of trees
   * @param numFeatures  Number of features in model (even if not all are explicitly used by
   *                     the model).
   *                     If -1, then numFeatures is set based on the max feature index in all trees.
   * @return  Feature importance values, of length numFeatures.
   */
  private[ml] def featureImportances(trees: Array[DecisionTreeModel], numFeatures: Int): Vector = {
    val totalImportances = new OpenHashMap[Int, Double]()
    trees.foreach { tree =>
      // Aggregate feature importance vector for this tree
      val importances = new OpenHashMap[Int, Double]()
      computeFeatureImportance(tree.rootNode, importances)
      // Normalize importance vector for this tree, and add it to total.
      // TODO: In the future, also support normalizing by tree.rootNode.impurityStats.count?
      val treeNorm = importances.map(_._2).sum
      if (treeNorm != 0) {
        importances.foreach { case (idx, impt) =>
          val normImpt = impt / treeNorm
          totalImportances.changeValue(idx, normImpt, _ + normImpt)
        }
      }
    }
    // Normalize importances
    normalizeMapValues(totalImportances)
    // Construct vector
    val d = if (numFeatures != -1) {
      numFeatures
    } else {
      // Find max feature index used in trees
      val maxFeatureIndex = trees.map(_.maxSplitFeatureIndex()).max
      maxFeatureIndex + 1
    }
    if (d == 0) {
      assert(totalImportances.size == 0, s"Unknown error in computing RandomForest feature" +
        s" importance: No splits in forest, but some non-zero importances.")
    }
    val (indices, values) = totalImportances.iterator.toSeq.sortBy(_._1).unzip
    Vectors.sparse(d, indices.toArray, values.toArray)
  }

  /**
   * Recursive method for computing feature importances for one tree.
   * This walks down the tree, adding to the importance of 1 feature at each node.
   * @param node  Current node in recursion
   * @param importances  Aggregate feature importances, modified by this method
   */
  private[impl] def computeFeatureImportance(
      node: Node,
      importances: OpenHashMap[Int, Double]): Unit = {
    node match {
      case n: InternalNode =>
        val feature = n.split.featureIndex
        val scaledGain = n.gain * n.impurityStats.count
        importances.changeValue(feature, scaledGain, _ + scaledGain)
        computeFeatureImportance(n.leftChild, importances)
        computeFeatureImportance(n.rightChild, importances)
      case n: LeafNode =>
        // do nothing
    }
  }

  /**
   * Normalize the values of this map to sum to 1, in place.
   * If all values are 0, this method does nothing.
   * @param map  Map with non-negative values.
   */
  private[impl] def normalizeMapValues(map: OpenHashMap[Int, Double]): Unit = {
    val total = map.map(_._2).sum
    if (total != 0) {
      val keys = map.iterator.map(_._1).toArray
      keys.foreach { key => map.changeValue(key, 0.0, _ / total) }
    }
  }

}
