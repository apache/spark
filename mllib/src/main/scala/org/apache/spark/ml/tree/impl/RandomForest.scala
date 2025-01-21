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

import scala.collection.mutable
import scala.util.Random

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{MAX_MEMORY_SIZE, MEMORY_SIZE, NUM_CLASSES, NUM_EXAMPLES, NUM_FEATURES, NUM_NODES, NUM_WEIGHTED_EXAMPLES, TIMER}
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.impl.Utils
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.util.Instrumentation
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.ImpurityStats
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.util.PeriodicRDDCheckpointer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.collection.OpenHashMap
import org.apache.spark.util.random.{SamplingUtils, XORShiftRandom}


/**
 * ALGORITHM
 *
 * This is a sketch of the algorithm to help new developers.
 *
 * The algorithm partitions data by instances (rows).
 * On each iteration, the algorithm splits a set of nodes.  In order to choose the best split
 * for a given node, sufficient statistics are collected from the distributed data.
 * For each node, the statistics are collected to some worker node, and that worker selects
 * the best split.
 *
 * This setup requires discretization of continuous features.  This binning is done in the
 * findSplits() method during initialization, after which each continuous feature becomes
 * an ordered discretized feature with at most maxBins possible values.
 *
 * The main loop in the algorithm operates on a queue of nodes (nodeStack).  These nodes
 * lie at the periphery of the tree being trained.  If multiple trees are being trained at once,
 * then this queue contains nodes from all of them.  Each iteration works roughly as follows:
 *   On the master node:
 *     - Some number of nodes are pulled off of the queue (based on the amount of memory
 *       required for their sufficient statistics).
 *     - For random forests, if featureSubsetStrategy is not "all," then a subset of candidate
 *       features are chosen for each node.  See method selectNodesToSplit().
 *   On worker nodes, via method findBestSplits():
 *     - The worker makes one pass over its subset of instances.
 *     - For each (tree, node, feature, split) tuple, the worker collects statistics about
 *       splitting.  Note that the set of (tree, node) pairs is limited to the nodes selected
 *       from the queue for this iteration.  The set of features considered can also be limited
 *       based on featureSubsetStrategy.
 *     - For each node, the statistics for that node are aggregated to a particular worker
 *       via reduceByKey().  The designated worker chooses the best (feature, split) pair,
 *       or chooses to stop splitting if the stopping criteria are met.
 *   On the master node:
 *     - The master collects all decisions about splitting nodes and updates the model.
 *     - The updated model is passed to the workers on the next iteration.
 * This process continues until the node queue is empty.
 *
 * Most of the methods in this implementation support the statistics aggregation, which is
 * the heaviest part of the computation.  In general, this implementation is bound by either
 * the cost of statistics computation on workers or by communicating the sufficient statistics.
 */
private[spark] object RandomForest extends Logging with Serializable {

  /**
   * Train a random forest.
   *
   * @param input Training data: RDD of `LabeledPoint`
   * @return an unweighted set of trees
   */
  def run(
      input: RDD[LabeledPoint],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long): Array[DecisionTreeModel] = {
    val instances = input.map { case LabeledPoint(label, features) =>
      Instance(label, 1.0, features.asML)
    }
    run(instances, strategy, numTrees, featureSubsetStrategy, seed, None)
  }

  /**
   * Train a random forest with metadata and splits. This method is mainly for GBT,
   * in which bagged input can be reused among trees.
   *
   * @param baggedInput bagged training data: RDD of `BaggedPoint`
   * @param metadata Learning and dataset metadata for DecisionTree.
   * @return an unweighted set of trees
   */
  def runBagged(
      baggedInput: RDD[BaggedPoint[TreePoint]],
      metadata: DecisionTreeMetadata,
      bcSplits: Broadcast[Array[Array[Split]]],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long,
      instr: Option[Instrumentation],
      prune: Boolean = true, // exposed for testing only, real trees are always pruned
      parentUID: Option[String] = None): Array[DecisionTreeModel] = {
    val timer = new TimeTracker()
    timer.start("total")

    val sc = baggedInput.sparkContext

    instr match {
      case Some(instrumentation) =>
        instrumentation.logNumFeatures(metadata.numFeatures)
        instrumentation.logNumClasses(metadata.numClasses)
        instrumentation.logNumExamples(metadata.numExamples)
        instrumentation.logSumOfWeights(metadata.weightedNumExamples)
      case None =>
        logInfo(log"numFeatures: ${MDC(NUM_FEATURES, metadata.numFeatures)}")
        logInfo(log"numClasses: ${MDC(NUM_CLASSES, metadata.numClasses)}")
        logInfo(log"numExamples: ${MDC(NUM_EXAMPLES, metadata.numExamples)}")
        logInfo(log"weightedNumExamples: " +
          log"${MDC(NUM_WEIGHTED_EXAMPLES, metadata.weightedNumExamples)}")
    }

    timer.start("init")

    // depth of the decision tree
    val maxDepth = strategy.maxDepth
    require(maxDepth <= 30,
      s"DecisionTree currently only supports maxDepth <= 30, but was given maxDepth = $maxDepth.")

    // Max memory usage for aggregates
    // TODO: Calculate memory usage more precisely.
    val maxMemoryUsage: Long = strategy.maxMemoryInMB * 1024L * 1024L
    logDebug(s"max memory usage for aggregates = $maxMemoryUsage bytes.")

    /*
     * The main idea here is to perform group-wise training of the decision tree nodes thus
     * reducing the passes over the data from (# nodes) to (# nodes / maxNumberOfNodesPerGroup).
     * Each data sample is handled by a particular node (or it reaches a leaf and is not used
     * in lower levels).
     */

    var nodeIds: RDD[Array[Int]] = null
    var nodeIdCheckpointer: PeriodicRDDCheckpointer[Array[Int]] = null
    if (strategy.useNodeIdCache) {
      // Create an RDD of node Id cache.
      // At first, all the rows belong to the root nodes (node Id == 1).
      nodeIds = baggedInput.map { _ => Array.fill(numTrees)(1) }
      nodeIdCheckpointer = new PeriodicRDDCheckpointer[Array[Int]](
        strategy.getCheckpointInterval(), sc, StorageLevel.MEMORY_AND_DISK)
      nodeIdCheckpointer.update(nodeIds)
    }

    /*
      Stack of nodes to train: (treeIndex, node)
      The reason this is a stack is that we train many trees at once, but we want to focus on
      completing trees, rather than training all simultaneously.  If we are splitting nodes from
      1 tree, then the new nodes to split will be put at the top of this stack, so we will continue
      training the same tree in the next iteration.  This focus allows us to send fewer trees to
      workers on each iteration; see topNodesForGroup below.
     */
    val nodeStack = new mutable.ListBuffer[(Int, LearningNode)]

    val rng = new Random()
    rng.setSeed(seed)

    // Allocate and queue root nodes.
    val topNodes = Array.fill[LearningNode](numTrees)(LearningNode.emptyNode(nodeIndex = 1))
    for (treeIndex <- 0 until numTrees) {
      nodeStack.prepend((treeIndex, topNodes(treeIndex)))
    }

    timer.stop("init")

    while (nodeStack.nonEmpty) {
      // Collect some nodes to split, and choose features for each node (if subsampling).
      // Each group of nodes may come from one or multiple trees, and at multiple levels.
      val (nodesForGroup, treeToNodeToIndexInfo) =
      RandomForest.selectNodesToSplit(nodeStack, maxMemoryUsage, metadata, rng)
      // Sanity check (should never occur):
      assert(nodesForGroup.nonEmpty,
        s"RandomForest selected empty nodesForGroup.  Error for unknown reason.")

      // Only send trees to worker if they contain nodes being split this iteration.
      val topNodesForGroup: Map[Int, LearningNode] =
        nodesForGroup.keys.map(treeIdx => treeIdx -> topNodes(treeIdx)).toMap

      // Choose node splits, and enqueue new nodes as needed.
      timer.start("findBestSplits")
      val bestSplit = RandomForest.findBestSplits(baggedInput, metadata, topNodesForGroup,
        nodesForGroup, treeToNodeToIndexInfo, bcSplits, nodeStack, timer, nodeIds,
        outputBestSplits = strategy.useNodeIdCache)
      if (strategy.useNodeIdCache) {
        nodeIds = updateNodeIds(baggedInput, nodeIds, bcSplits, bestSplit)
        nodeIdCheckpointer.update(nodeIds)
      }

      timer.stop("findBestSplits")
    }

    timer.stop("total")

    logInfo("Internal timing for DecisionTree:")
    logInfo(log"${MDC(TIMER, timer)}")

    if (strategy.useNodeIdCache) {
      // Delete any remaining checkpoints used for node Id cache.
      nodeIdCheckpointer.unpersistDataSet()
      nodeIdCheckpointer.deleteAllCheckpoints()
    }

    val numFeatures = metadata.numFeatures

    parentUID match {
      case Some(uid) =>
        if (strategy.algo == OldAlgo.Classification) {
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(uid, rootNode.toNode(prune), numFeatures,
              strategy.getNumClasses())
          }
        } else {
          topNodes.map { rootNode =>
            new DecisionTreeRegressionModel(uid, rootNode.toNode(prune), numFeatures)
          }
        }
      case None =>
        if (strategy.algo == OldAlgo.Classification) {
          topNodes.map { rootNode =>
            new DecisionTreeClassificationModel(rootNode.toNode(prune), numFeatures,
              strategy.getNumClasses())
          }
        } else {
          topNodes.map(rootNode =>
            new DecisionTreeRegressionModel(rootNode.toNode(prune), numFeatures))
        }
    }
  }

  /**
   * Train a random forest.
   *
   * @param input Training data: RDD of `Instance`
   * @return an unweighted set of trees
   */
  def run(
      input: RDD[Instance],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long,
      instr: Option[Instrumentation],
      prune: Boolean = true, // exposed for testing only, real trees are always pruned
      parentUID: Option[String] = None): Array[DecisionTreeModel] = {
    val timer = new TimeTracker()

    timer.start("build metadata")
    val metadata = DecisionTreeMetadata
      .buildMetadata(input.retag(classOf[Instance]), strategy, numTrees, featureSubsetStrategy)
    timer.stop("build metadata")

    val retaggedInput = input.retag(classOf[Instance])

    // Find the splits and the corresponding bins (interval between the splits) using a sample
    // of the input data.
    timer.start("findSplits")
    val splits = findSplits(retaggedInput, metadata, seed)
    timer.stop("findSplits")
    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
      s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
    }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(retaggedInput, splits, metadata)

    val bcSplits = input.sparkContext.broadcast(splits)
    val baggedInput = BaggedPoint
      .convertToBaggedRDD(treeInput, strategy.subsamplingRate, numTrees, strategy.bootstrap,
        (tp: TreePoint) => tp.weight, seed = seed)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName("bagged tree points")

    val trees = runBagged(baggedInput = baggedInput, metadata = metadata, bcSplits = bcSplits,
      strategy = strategy, numTrees = numTrees, featureSubsetStrategy = featureSubsetStrategy,
      seed = seed, instr = instr, prune = prune, parentUID = parentUID)

    baggedInput.unpersist()
    bcSplits.destroy()

    trees
  }

  /**
   * Update node indices by newly found splits.
   */
  private def updateNodeIds(
      input: RDD[BaggedPoint[TreePoint]],
      nodeIds: RDD[Array[Int]],
      bcSplits: Broadcast[Array[Array[Split]]],
      bestSplits: Array[Map[Int, Split]]): RDD[Array[Int]] = {
    require(nodeIds != null && bestSplits != null)
    input.zip(nodeIds).map { case (point, ids) =>
      var treeId = 0
      while (treeId < bestSplits.length) {
        val bestSplitsInTree = bestSplits(treeId)
        if (bestSplitsInTree != null) {
          val nodeId = ids(treeId)
          bestSplitsInTree.get(nodeId).foreach { bestSplit =>
            val featureId = bestSplit.featureIndex
            val bin = point.datum.binnedFeatures(featureId)
            val newNodeId = if (bestSplit.shouldGoLeft(bin, bcSplits.value(featureId))) {
              LearningNode.leftChildIndex(nodeId)
            } else {
              LearningNode.rightChildIndex(nodeId)
            }
            ids(treeId) = newNodeId
          }
        }
        treeId += 1
      }
      ids
    }
  }

  /**
   * Helper for binSeqOp, for data which can contain a mix of ordered and unordered features.
   *
   * For ordered features, a single bin is updated.
   * For unordered features, bins correspond to subsets of categories; either the left or right bin
   * for each subset is updated.
   *
   * @param agg Array storing aggregate calculation, with a set of sufficient statistics for
   *            each (feature, bin).
   * @param treePoint Data point being aggregated.
   * @param splits Possible splits indexed (numFeatures)(numSplits)
   * @param unorderedFeatures Set of indices of unordered features.
   * @param numSamples Number of times this instance occurs in the sample.
   * @param sampleWeight Weight (importance) of instance in dataset.
   */
  private def mixedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      splits: Array[Array[Split]],
      unorderedFeatures: Set[Int],
      numSamples: Int,
      sampleWeight: Double,
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
        val leftNodeFeatureOffset = agg.getFeatureOffset(featureIndexIdx)
        // Update the left or right bin for each split.
        val numSplits = agg.metadata.numSplits(featureIndex)
        val featureSplits = splits(featureIndex)
        var splitIndex = 0
        while (splitIndex < numSplits) {
          if (featureSplits(splitIndex).shouldGoLeft(featureValue, featureSplits)) {
            agg.featureUpdate(leftNodeFeatureOffset, splitIndex, treePoint.label, numSamples,
              sampleWeight)
          }
          splitIndex += 1
        }
      } else {
        // Ordered feature
        val binIndex = treePoint.binnedFeatures(featureIndex)
        agg.update(featureIndexIdx, binIndex, treePoint.label, numSamples, sampleWeight)
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
   * @param numSamples Number of times this instance occurs in the sample.
   * @param sampleWeight  Weight (importance) of instance in dataset.
   */
  private def orderedBinSeqOp(
      agg: DTStatsAggregator,
      treePoint: TreePoint,
      numSamples: Int,
      sampleWeight: Double,
      featuresForNode: Option[Array[Int]]): Unit = {
    val label = treePoint.label

    // Iterate over features.
    if (featuresForNode.nonEmpty) {
      // Use subsampled features
      var featureIndexIdx = 0
      while (featureIndexIdx < featuresForNode.get.length) {
        val binIndex = treePoint.binnedFeatures(featuresForNode.get.apply(featureIndexIdx))
        agg.update(featureIndexIdx, binIndex, label, numSamples, sampleWeight)
        featureIndexIdx += 1
      }
    } else {
      // Use all features
      val numFeatures = agg.metadata.numFeatures
      var featureIndex = 0
      while (featureIndex < numFeatures) {
        val binIndex = treePoint.binnedFeatures(featureIndex)
        agg.update(featureIndex, binIndex, label, numSamples, sampleWeight)
        featureIndex += 1
      }
    }
  }

  /**
   * Given a group of nodes, this finds the best split for each node.
   *
   * @param input Training data: RDD of [[TreePoint]]
   * @param metadata Learning and dataset metadata
   * @param topNodesForGroup For each tree in group, tree index -> root node.
   *                         Used for matching instances with nodes.
   * @param nodesForGroup Mapping: treeIndex --> nodes to be split in tree
   * @param treeToNodeToIndexInfo Mapping: treeIndex --> nodeIndex --> nodeIndexInfo,
   *                              where nodeIndexInfo stores the index in the group and the
   *                              feature subsets (if using feature subsets).
   * @param bcSplits possible splits for all features, indexed (numFeatures)(numSplits)
   * @param nodeStack  Queue of nodes to split, with values (treeIndex, node).
   *                   Updated with new non-leaf nodes which are created.
   * @param nodeIds an RDD of Array[Int] where each value in the array is the data
   *                point's node Id for a corresponding tree. This is used to prevent
   *                the need to pass the entire tree to the executors during the node
   *                stat aggregation phase.
   */
  private[tree] def findBestSplits(
      input: RDD[BaggedPoint[TreePoint]],
      metadata: DecisionTreeMetadata,
      topNodesForGroup: Map[Int, LearningNode],
      nodesForGroup: Map[Int, Array[LearningNode]],
      treeToNodeToIndexInfo: Map[Int, Map[Int, NodeIndexInfo]],
      bcSplits: Broadcast[Array[Array[Split]]],
      nodeStack: mutable.ListBuffer[(Int, LearningNode)],
      timer: TimeTracker = new TimeTracker,
      nodeIds: RDD[Array[Int]] = null,
      outputBestSplits: Boolean = false): Array[Map[Int, Split]] = {

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

    val useNodeIdCache = nodeIds != null

    // numNodes:  Number of nodes in this group
    val numNodes = nodesForGroup.values.map(_.length).sum
    logDebug(s"numNodes = $numNodes")
    logDebug(s"numFeatures = ${metadata.numFeatures}")
    logDebug(s"numClasses = ${metadata.numClasses}")
    logDebug(s"isMulticlass = ${metadata.isMulticlass}")
    logDebug(s"isMulticlassWithCategoricalFeatures = " +
      s"${metadata.isMulticlassWithCategoricalFeatures}")
    logDebug(s"using nodeIdCache = $useNodeIdCache")

    /*
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
        baggedPoint: BaggedPoint[TreePoint],
        splits: Array[Array[Split]]): Unit = {
      if (nodeInfo != null) {
        val aggNodeIndex = nodeInfo.nodeIndexInGroup
        val featuresForNode = nodeInfo.featureSubset
        val numSamples = baggedPoint.subsampleCounts(treeIndex)
        val sampleWeight = baggedPoint.sampleWeight
        if (metadata.unorderedFeatures.isEmpty) {
          orderedBinSeqOp(agg(aggNodeIndex), baggedPoint.datum, numSamples, sampleWeight,
            featuresForNode)
        } else {
          mixedBinSeqOp(agg(aggNodeIndex), baggedPoint.datum, splits,
            metadata.unorderedFeatures, numSamples, sampleWeight, featuresForNode)
        }
        agg(aggNodeIndex).updateParent(baggedPoint.datum.label, numSamples, sampleWeight)
      }
    }

    /*
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
        baggedPoint: BaggedPoint[TreePoint],
        splits: Array[Array[Split]]): Array[DTStatsAggregator] = {
      treeToNodeToIndexInfo.foreach { case (treeIndex, nodeIndexToInfo) =>
        val nodeIndex =
          topNodesForGroup(treeIndex).predictImpl(baggedPoint.datum.binnedFeatures, splits)
        nodeBinSeqOp(treeIndex, nodeIndexToInfo.getOrElse(nodeIndex, null),
          agg, baggedPoint, splits)
      }
      agg
    }

    /**
     * Do the same thing as binSeqOp, but with nodeIdCache.
     */
    def binSeqOpWithNodeIdCache(
        agg: Array[DTStatsAggregator],
        dataPoint: (BaggedPoint[TreePoint], Array[Int]),
        splits: Array[Array[Split]]): Array[DTStatsAggregator] = {
      treeToNodeToIndexInfo.foreach { case (treeIndex, nodeIndexToInfo) =>
        val baggedPoint = dataPoint._1
        val nodeIdCache = dataPoint._2
        val nodeIndex = nodeIdCache(treeIndex)
        nodeBinSeqOp(treeIndex, nodeIndexToInfo.getOrElse(nodeIndex, null),
          agg, baggedPoint, splits)
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
    // yield a (nodeIndex, nodeAggregateStats) pair for each node.
    // After a `reduceByKey` operation,
    // stats of a node will be shuffled to a particular partition and be combined together,
    // then best splits for nodes are found there.
    // Finally, only best Splits for nodes are collected to driver to construct decision tree.
    val nodeToFeatures = getNodeToFeatures(treeToNodeToIndexInfo)
    val nodeToFeaturesBc = input.sparkContext.broadcast(nodeToFeatures)

    val partitionAggregates = if (useNodeIdCache) {

      input.zip(nodeIds).mapPartitions { points =>
        // Construct a nodeStatsAggregators array to hold node aggregate stats,
        // each node will have a nodeStatsAggregator
        val nodeStatsAggregators = Array.tabulate(numNodes) { nodeIndex =>
          val featuresForNode = nodeToFeaturesBc.value.map { nodeToFeatures =>
            nodeToFeatures(nodeIndex)
          }
          new DTStatsAggregator(metadata, featuresForNode)
        }

        // iterator all instances in current partition and update aggregate stats
        points.foreach(binSeqOpWithNodeIdCache(nodeStatsAggregators, _, bcSplits.value))

        // transform nodeStatsAggregators array to (nodeIndex, nodeAggregateStats) pairs,
        // which can be combined with other partition using `reduceByKey`
        nodeStatsAggregators.iterator.zipWithIndex.map(_.swap)
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
        points.foreach(binSeqOp(nodeStatsAggregators, _, bcSplits.value))

        // transform nodeStatsAggregators array to (nodeIndex, nodeAggregateStats) pairs,
        // which can be combined with other partition using `reduceByKey`
        nodeStatsAggregators.iterator.zipWithIndex.map(_.swap)
      }
    }

    val nodeToBestSplits = partitionAggregates.reduceByKey((a, b) => a.merge(b)).map {
      case (nodeIndex, aggStats) =>
        val featuresForNode = nodeToFeaturesBc.value.flatMap { nodeToFeatures =>
          Some(nodeToFeatures(nodeIndex))
        }

        // find best split for each node
        val (split: Split, stats: ImpurityStats) =
          binsToBestSplit(aggStats, bcSplits.value, featuresForNode, nodes(nodeIndex))
        (nodeIndex, (split, stats))
    }.collectAsMap()
    nodeToFeaturesBc.destroy()

    timer.stop("chooseSplits")

    val bestSplits = if (outputBestSplits) {
      Array.ofDim[mutable.Map[Int, Split]](metadata.numTrees)
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
        logDebug(s"best split = $split")

        // Extract info for this node.  Create children if not leaf.
        val isLeaf =
          (stats.gain <= 0) || (LearningNode.indexToLevel(nodeIndex) == metadata.maxDepth)
        node.isLeaf = isLeaf
        node.stats = stats
        logDebug(s"Node = $node")

        if (!isLeaf) {
          node.split = Some(split)
          val childIsLeaf = (LearningNode.indexToLevel(nodeIndex) + 1) == metadata.maxDepth
          val leftChildIsLeaf = childIsLeaf || (math.abs(stats.leftImpurity) < Utils.EPSILON)
          val rightChildIsLeaf = childIsLeaf || (math.abs(stats.rightImpurity) < Utils.EPSILON)
          node.leftChild = Some(LearningNode(LearningNode.leftChildIndex(nodeIndex),
            leftChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.leftImpurityCalculator)))
          node.rightChild = Some(LearningNode(LearningNode.rightChildIndex(nodeIndex),
            rightChildIsLeaf, ImpurityStats.getEmptyImpurityStats(stats.rightImpurityCalculator)))

          if (outputBestSplits) {
            val bestSplitsInTree = bestSplits(treeIndex)
            if (bestSplitsInTree == null) {
              bestSplits(treeIndex) = mutable.Map[Int, Split](nodeIndex -> split)
            } else {
              bestSplitsInTree.update(nodeIndex, split)
            }
          }

          // enqueue left child and right child if they are not leaves
          if (!leftChildIsLeaf) {
            nodeStack.prepend((treeIndex, node.leftChild.get))
          }
          if (!rightChildIsLeaf) {
            nodeStack.prepend((treeIndex, node.rightChild.get))
          }

          logDebug(s"leftChildIndex = ${node.leftChild.get.id}" +
            s", impurity = ${stats.leftImpurity}")
          logDebug(s"rightChildIndex = ${node.rightChild.get.id}" +
            s", impurity = ${stats.rightImpurity}")
        }
      }
    }

    if (outputBestSplits) {
      bestSplits.map { m => if (m == null) null else m.toMap }
    } else {
      null
    }
  }

  /**
   * Calculate the impurity statistics for a given (feature, split) based upon left/right
   * aggregates.
   *
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

    val leftRawCount = leftImpurityCalculator.rawCount
    val rightRawCount = rightImpurityCalculator.rawCount
    val leftCount = leftImpurityCalculator.count
    val rightCount = rightImpurityCalculator.count

    val totalCount = leftCount + rightCount

    val violatesMinInstancesPerNode = (leftRawCount < metadata.minInstancesPerNode) ||
      (rightRawCount < metadata.minInstancesPerNode)
    val violatesMinWeightPerNode = (leftCount < metadata.minWeightPerNode) ||
      (rightCount < metadata.minWeightPerNode)
    // If left child or right child doesn't satisfy minimum weight per node or minimum
    // instances per node, then this split is invalid, return invalid information gain stats.
    if (violatesMinInstancesPerNode || violatesMinWeightPerNode) {
      return ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator)
    }

    val leftImpurity = leftImpurityCalculator.calculate() // Note: This equals 0 if count = 0
    val rightImpurity = rightImpurityCalculator.calculate()

    val leftWeight = leftCount / totalCount
    val rightWeight = rightCount / totalCount

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
   *
   * @param binAggregates Bin statistics.
   * @return tuple for best split: (Split, information gain, prediction at node)
   */
  private[tree] def binsToBestSplit(
      binAggregates: DTStatsAggregator,
      splits: Array[Array[Split]],
      featuresForNode: Option[Array[Int]],
      node: LearningNode): (Split, ImpurityStats) = {

    // Calculate InformationGain and ImpurityStats if current node is top node
    val level = LearningNode.indexToLevel(node.id)
    var gainAndImpurityStats: ImpurityStats = if (level == 0) {
      null
    } else {
      node.stats
    }

    val validFeatureSplits =
      Iterator.range(0, binAggregates.metadata.numFeaturesPerNode).map { featureIndexIdx =>
        featuresForNode.map(features => (featureIndexIdx, features(featureIndexIdx)))
          .getOrElse((featureIndexIdx, featureIndexIdx))
      }.withFilter { case (_, featureIndex) =>
        binAggregates.metadata.numSplits(featureIndex) != 0
      }

    // For each (feature, split), calculate the gain, and select the best (feature, split).
    val splitsAndImpurityInfo =
      validFeatureSplits.map { case (featureIndexIdx, featureIndex) =>
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
            Range(0, numSplits).map { splitIdx =>
              val leftChildStats =
                binAggregates.getImpurityCalculator(nodeFeatureOffset, splitIdx)
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
          val leftChildOffset = binAggregates.getFeatureOffset(featureIndexIdx)
          val (bestFeatureSplitIndex, bestFeatureGainStats) =
            Range(0, numSplits).map { splitIndex =>
              val leftChildStats = binAggregates.getImpurityCalculator(leftChildOffset, splitIndex)
              val rightChildStats = binAggregates.getParentImpurityCalculator()
                .subtract(leftChildStats)
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
          val centroidForCategories = Range(0, numCategories).map { featureValue =>
            val categoryStats =
              binAggregates.getImpurityCalculator(nodeFeatureOffset, featureValue)
            val centroid = if (categoryStats.count != 0) {
              if (binAggregates.metadata.isMulticlass) {
                // multiclass classification
                // For categorical variables in multiclass classification,
                // the bins are ordered by the impurity of their corresponding labels.
                categoryStats.calculate()
              } else if (binAggregates.metadata.isClassification) {
                // binary classification
                // For categorical variables in binary classification,
                // the bins are ordered by the count of class 1.
                categoryStats.stats(1)
              } else {
                // regression
                // For categorical variables in regression and binary classification,
                // the bins are ordered by the prediction.
                categoryStats.predict
              }
            } else {
              Double.MaxValue
            }
            (featureValue, centroid)
          }

          logDebug(s"Centroids for categorical variable: " +
            s"${centroidForCategories.mkString(",")}")

          // bins sorted by centroids
          val categoriesSortedByCentroid = centroidForCategories.toList.sortBy(_._2)

          logDebug(s"Sorted centroids for categorical variable = " +
            s"${categoriesSortedByCentroid.mkString(",")}")

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
      }

    val (bestSplit, bestSplitStats) =
      if (splitsAndImpurityInfo.isEmpty) {
        // If no valid splits for features, then this split is invalid,
        // return invalid information gain stats.  Take any split and continue.
        // Splits is empty, so arbitrarily choose to split on any threshold
        val dummyFeatureIndex = featuresForNode.map(_.head).getOrElse(0)
        val parentImpurityCalculator = binAggregates.getParentImpurityCalculator()
        if (binAggregates.metadata.isContinuous(dummyFeatureIndex)) {
          (new ContinuousSplit(dummyFeatureIndex, 0),
            ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator))
        } else {
          val numCategories = binAggregates.metadata.featureArity(dummyFeatureIndex)
          (new CategoricalSplit(dummyFeatureIndex, Array(), numCategories),
            ImpurityStats.getInvalidImpurityStats(parentImpurityCalculator))
        }
      } else {
        splitsAndImpurityInfo.maxBy(_._2.gain)
      }
    (bestSplit, bestSplitStats)
  }

  /**
   * Returns splits for decision tree calculation.
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
   * @param input Training data: RDD of [[Instance]]
   * @param metadata Learning and dataset metadata
   * @param seed random seed
   * @return Splits, an Array of [[Split]]
   *          of size (numFeatures, numSplits)
   */
  protected[tree] def findSplits(
      input: RDD[Instance],
      metadata: DecisionTreeMetadata,
      seed: Long): Array[Array[Split]] = {

    logDebug(s"isMulticlass = ${metadata.isMulticlass}")

    val numFeatures = metadata.numFeatures

    // Sample the input only if there are continuous features.
    val continuousFeatures = Range(0, numFeatures).filter(metadata.isContinuous)
    val sampledInput = if (continuousFeatures.nonEmpty) {
      val fraction = samplesFractionForFindSplits(metadata)
      logDebug(s"fraction of data used for calculating quantiles = $fraction")
      if (fraction < 1) {
        input.sample(withReplacement = false, fraction, new XORShiftRandom(seed).nextInt())
      } else {
        input
      }
    } else {
      input.sparkContext.emptyRDD[Instance]
    }

    findSplitsBySorting(sampledInput, metadata, continuousFeatures)
  }

  private def findSplitsBySorting(
      input: RDD[Instance],
      metadata: DecisionTreeMetadata,
      continuousFeatures: IndexedSeq[Int]): Array[Array[Split]] = {

    val continuousSplits = if (continuousFeatures.nonEmpty) {
      // reduce the parallelism for split computations when there are less
      // continuous features than input partitions. this prevents tasks from
      // being spun up that will definitely do no work.
      val numPartitions = math.min(continuousFeatures.length, input.partitions.length)

      input.flatMap { point =>
        continuousFeatures.iterator
          .map(idx => (idx, (point.features(idx), point.weight)))
          .filter(_._2._1 != 0.0)
      }.aggregateByKey((new OpenHashMap[Double, Double], 0L), numPartitions)(
        seqOp = { case ((map, c), (v, w)) =>
          map.changeValue(v, w, _ + w)
          (map, c + 1L)
        },
        combOp = { case ((map1, c1), (map2, c2)) =>
          map2.foreach { case (v, w) =>
            map1.changeValue(v, w, _ + w)
          }
          (map1, c1 + c2)
        }
      ).map { case (idx, (map, c)) =>
        val thresholds = findSplitsForContinuousFeature(map.toMap, c, metadata, idx)
        val splits: Array[Split] = thresholds.map(thresh => new ContinuousSplit(idx, thresh))
        logDebug(s"featureIndex = $idx, numSplits = ${splits.length}")
        (idx, splits)
      }.collectAsMap()
    } else Map.empty[Int, Array[Split]]

    val numFeatures = metadata.numFeatures
    val splits: Array[Array[Split]] = Array.tabulate(numFeatures) {
      case i if metadata.isContinuous(i) =>
        // some features may contain only zero, so continuousSplits will not have a record
        val split = continuousSplits.getOrElse(i, Array.empty[Split])
        metadata.setNumSplits(i, split.length)
        split

      case i if metadata.isCategorical(i) && metadata.isUnordered(i) =>
        // Unordered features
        // 2^(maxFeatureValue - 1) - 1 combinations
        val featureArity = metadata.featureArity(i)
        Array.tabulate[Split](metadata.numSplits(i)) { splitIndex =>
          val categories = extractMultiClassCategories(splitIndex + 1, featureArity)
          new CategoricalSplit(i, categories.toArray, featureArity)
        }

      case i if metadata.isCategorical(i) =>
        // Ordered features
        //   Splits are constructed as needed during training.
        Array.empty[Split]
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
   *
   * @param featureSamples feature values and sample weights of each sample
   * @param metadata decision tree metadata
   *                 NOTE: `metadata.numbins` will be changed accordingly
   *                       if there are not enough splits to be found
   * @param featureIndex feature index to find splits
   * @return array of split thresholds
   */
  private[tree] def findSplitsForContinuousFeature(
      featureSamples: Iterable[(Double, Double)],
      metadata: DecisionTreeMetadata,
      featureIndex: Int): Array[Double] = {
    val valueWeights = new OpenHashMap[Double, Double]
    var count = 0L
    featureSamples.foreach { case (weight, value) =>
      valueWeights.changeValue(value, weight, _ + weight)
      count += 1L
    }
    findSplitsForContinuousFeature(valueWeights.toMap, count, metadata, featureIndex)
  }

  /**
   * Find splits for a continuous feature
   * NOTE: Returned number of splits is set based on `featureSamples` and
   *       could be different from the specified `numSplits`.
   *       The `numSplits` attribute in the `DecisionTreeMetadata` class will be set accordingly.
   *
   * @param partValueWeights non-zero distinct values and their weights
   * @param metadata decision tree metadata
   *                 NOTE: `metadata.numbins` will be changed accordingly
   *                       if there are not enough splits to be found
   * @param featureIndex feature index to find splits
   * @return array of split thresholds
   */
  private[tree] def findSplitsForContinuousFeature(
      partValueWeights: Map[Double, Double],
      count: Long,
      metadata: DecisionTreeMetadata,
      featureIndex: Int): Array[Double] = {
    require(metadata.isContinuous(featureIndex),
      "findSplitsForContinuousFeature can only be used to find splits for a continuous feature.")

    val splits = if (partValueWeights.isEmpty) {
      Array.emptyDoubleArray
    } else {
      val numSplits = metadata.numSplits(featureIndex)

      val partNumSamples = partValueWeights.values.sum

      // Calculate the expected number of samples for finding splits
      val weightedNumSamples = samplesFractionForFindSplits(metadata) *
        metadata.weightedNumExamples
      // scale tolerance by number of samples with constant factor
      // Note: constant factor was tuned by running some tests where there were no zero
      // feature values and validating we are never within tolerance
      val tolerance = Utils.EPSILON * count * 100
      // add expected zero value count and get complete statistics
      val valueCountMap = if (weightedNumSamples - partNumSamples > tolerance) {
        partValueWeights + (0.0 -> (weightedNumSamples - partNumSamples))
      } else {
        partValueWeights
      }

      // sort distinct values
      val valueCounts = valueCountMap.toSeq.sortBy(_._1).toArray

      val possibleSplits = valueCounts.length - 1
      if (possibleSplits == 0) {
        // constant feature
        Array.emptyDoubleArray
      } else if (possibleSplits <= numSplits) {
        // if possible splits is not enough or just enough, just return all possible splits
        (1 to possibleSplits)
          .map(index => (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0)
          .toArray
      } else {
        // stride between splits
        val stride: Double = weightedNumSamples / (numSplits + 1)
        logDebug(s"stride = $stride")

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
            splitsBuilder += (valueCounts(index - 1)._1 + valueCounts(index)._1) / 2.0
            targetCount += stride
          }
          index += 1
        }

        splitsBuilder.result()
      }
    }
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
   * @param nodeStack  Queue of nodes to split.
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
      nodeStack: mutable.ListBuffer[(Int, LearningNode)],
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
    // If maxMemoryInMB is set very small, we want to still try to split 1 node,
    // so we allow one iteration if memUsage == 0.
    var groupDone = false
    while (nodeStack.nonEmpty && !groupDone) {
      val (treeIndex, node) = nodeStack.head
      // Choose subset of features for node (if subsampling).
      val featureSubset: Option[Array[Int]] = if (metadata.subsamplingFeatures) {
        Some(SamplingUtils.reservoirSampleAndCount(Range(0,
          metadata.numFeatures).iterator, metadata.numFeaturesPerNode, rng.nextLong())._1)
      } else {
        None
      }
      // Check if enough memory remains to add this node to the group.
      val nodeMemUsage = RandomForest.aggregateSizeForNode(metadata, featureSubset) * 8L
      if (memUsage + nodeMemUsage <= maxMemoryUsage || memUsage == 0) {
        nodeStack.remove(0)
        mutableNodesForGroup.getOrElseUpdate(treeIndex, new mutable.ArrayBuffer[LearningNode]()) +=
          node
        mutableTreeToNodeToIndexInfo
          .getOrElseUpdate(treeIndex, new mutable.HashMap[Int, NodeIndexInfo]())(node.id)
          = new NodeIndexInfo(numNodesInGroup, featureSubset)
        numNodesInGroup += 1
        memUsage += nodeMemUsage
      } else {
        groupDone = true
      }
    }
    if (memUsage > maxMemoryUsage) {
      // If maxMemoryUsage is 0, we should still allow splitting 1 node.
      logWarning(log"Tree learning is using approximately ${MDC(MEMORY_SIZE, memUsage)} " +
        log"bytes per iteration, which exceeds requested limit " +
        log"maxMemoryUsage=${MDC(MAX_MEMORY_SIZE, maxMemoryUsage)}. This allows splitting " +
        log"${MDC(NUM_NODES, numNodesInGroup)} nodes in this iteration.")
    }
    // Convert mutable maps to immutable ones.
    val nodesForGroup: Map[Int, Array[LearningNode]] =
      mutableNodesForGroup.toMap.transform((_, v) => v.toArray)
    val treeToNodeToIndexInfo = mutableTreeToNodeToIndexInfo.toMap.transform((_, v) => v.toMap)
    (nodesForGroup, treeToNodeToIndexInfo)
  }

  /**
   * Get the number of values to be stored for this node in the bin aggregates.
   *
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
   * Calculate the subsample fraction for finding splits
   *
   * @param metadata decision tree metadata
   * @return subsample fraction
   */
  private def samplesFractionForFindSplits(
      metadata: DecisionTreeMetadata): Double = {
    // Calculate the number of samples for approximate quantile calculation.
    val requiredSamples = math.max(metadata.maxBins * metadata.maxBins, 10000)
    if (requiredSamples < metadata.numExamples) {
      requiredSamples.toDouble / metadata.numExamples
    } else {
      1.0
    }
  }
}
