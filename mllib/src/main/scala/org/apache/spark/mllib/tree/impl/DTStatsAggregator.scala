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

package org.apache.spark.mllib.tree.impl

import org.apache.spark.mllib.tree.impurity._

/**
 * DecisionTree statistics aggregator.
 * This holds a flat array of statistics for a set of (nodes, features, bins)
 * and helps with indexing.
 * This class is abstract to support learning with and without feature subsampling.
 */
private[tree] abstract class DTStatsAggregator(
    val metadata: DecisionTreeMetadata) extends Serializable {

  /**
   * [[ImpurityAggregator]] instance specifying the impurity type.
   */
  val impurityAggregator: ImpurityAggregator = metadata.impurity match {
    case Gini => new GiniAggregator(metadata.numClasses)
    case Entropy => new EntropyAggregator(metadata.numClasses)
    case Variance => new VarianceAggregator()
    case _ => throw new IllegalArgumentException(s"Bad impurity parameter: ${metadata.impurity}")
  }

  /**
   * Number of elements (Double values) used for the sufficient statistics of each bin.
   */
  val statsSize: Int = impurityAggregator.statsSize

  /**
   * Total number of features.
   * Note: This is the total number, not the subsampled number.
   */
  //val numFeatures: Int = metadata.numFeatures

  /**
   * Number of bins for each feature.  This is indexed by the feature index.
   */
  //val numBins: Array[Int] = metadata.numBins

  /**
   * Number of splits for the given feature.
   */
  //def numSplits(featureIndex: Int): Int = metadata.numSplits(featureIndex)

  /**
   * Indicator for each feature of whether that feature is an unordered feature.
   * TODO: Is Array[Boolean] any faster?
   */
  def isUnordered(featureIndex: Int): Boolean = metadata.isUnordered(featureIndex)

  /**
   * Total number of elements stored in this aggregator.
   */
  def allStatsSize: Int

  /**
   * Get flat array of elements stored in this aggregator.
   */
  protected def allStats: Array[Double]

  /**
   * Get an [[ImpurityCalculator]] for a given (node, feature, bin).
   * @param nodeFeatureOffset  For ordered features, this is a pre-computed (node, feature) offset
   *                           from [[getNodeFeatureOffset]].
   *                           For unordered features, this is a pre-computed
   *                           (node, feature, left/right child) offset from
   *                           [[getLeftRightNodeFeatureOffsets]].
   */
  def getImpurityCalculator(nodeFeatureOffset: Int, binIndex: Int): ImpurityCalculator = {
    impurityAggregator.getCalculator(allStats, nodeFeatureOffset + binIndex * statsSize)
  }

  /**
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   */
  def update(
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit

  /**
   * Pre-compute node offset for use with [[nodeUpdate]].
   */
  def getNodeOffset(nodeIndex: Int): Int

  /**
   * Faster version of [[update]].
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   * @param nodeOffset  Pre-computed node offset from [[getNodeOffset]].
   */
  def nodeUpdate(
      nodeOffset: Int,
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For ordered features only.
   */
  def getNodeFeatureOffset(nodeIndex: Int, featureIndex: Int): Int

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For unordered features only.
   */
  def getLeftRightNodeFeatureOffsets(nodeIndex: Int, featureIndex: Int): (Int, Int)

  /**
   * Faster version of [[update]].
   * Update the stats for a given (node, feature, bin), using the given label.
   * @param nodeFeatureOffset  For ordered features, this is a pre-computed (node, feature) offset
   *                           from [[getNodeFeatureOffset]].
   *                           For unordered features, this is a pre-computed
   *                           (node, feature, left/right child) offset from
   *                           [[getLeftRightNodeFeatureOffsets]].
   */
  def nodeFeatureUpdate(
      nodeFeatureOffset: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit = {
    impurityAggregator.update(allStats, nodeFeatureOffset + binIndex * statsSize, label,
      instanceWeight)
  }

  /**
   * For a given (node, feature), merge the stats for two bins.
   * @param nodeFeatureOffset  For ordered features, this is a pre-computed (node, feature) offset
   *                           from [[getNodeFeatureOffset]].
   *                           For unordered features, this is a pre-computed
   *                           (node, feature, left/right child) offset from
   *                           [[getLeftRightNodeFeatureOffsets]].
   * @param binIndex  The other bin is merged into this bin.
   * @param otherBinIndex  This bin is not modified.
   */
  def mergeForNodeFeature(nodeFeatureOffset: Int, binIndex: Int, otherBinIndex: Int): Unit = {
    impurityAggregator.merge(allStats, nodeFeatureOffset + binIndex * statsSize,
      nodeFeatureOffset + otherBinIndex * statsSize)
  }

  /**
   * Merge this aggregator with another, and returns this aggregator.
   * This method modifies this aggregator in-place.
   */
  def merge(other: DTStatsAggregator): DTStatsAggregator = {
    require(allStatsSize == other.allStatsSize,
      s"DTStatsAggregator.merge requires that both aggregators have the same length stats vectors."
      + s" This aggregator is of length $allStatsSize, but the other is ${other.allStatsSize}.")
    var i = 0
    // TODO: Test BLAS.axpy
    while (i < allStatsSize) {
      allStats(i) += other.allStats(i)
      i += 1
    }
    this
  }
}

/**
 * DecisionTree statistics aggregator.
 * This holds a flat array of statistics for a set of (nodes, features, bins)
 * and helps with indexing.
 *
 * This instance of [[DTStatsAggregator]] is used when not subsampling features.
 *
 * @param numNodes  Number of nodes to collect statistics for.
 */
private[tree] class DTStatsAggregatorFixedFeatures(
    metadata: DecisionTreeMetadata,
    numNodes: Int) extends DTStatsAggregator(metadata) {

  /**
   * Offset for each feature for calculating indices into the [[_allStats]] array.
   * Mapping: featureIndex --> offset
   */
  private val featureOffsets: Array[Int] = {
    metadata.numBins.scanLeft(0)((total, nBins) => total + statsSize * nBins)
  }

  /**
   * Number of elements for each node, corresponding to stride between nodes in [[_allStats]].
   */
  private val nodeStride: Int = featureOffsets.last

  /**
   * Total number of elements stored in this aggregator.
   */
  def allStatsSize: Int = numNodes * nodeStride

  /**
   * Flat array of elements.
   * Index for start of stats for a (node, feature, bin) is:
   *   index = nodeIndex * nodeStride + featureOffsets(featureIndex) + binIndex * statsSize
   * Note: For unordered features, the left child stats precede the right child stats
   *       in the binIndex order.
   */
  protected val _allStats: Array[Double] = new Array[Double](allStatsSize)

  /**
   * Get flat array of elements stored in this aggregator.
   */
  protected def allStats: Array[Double] = _allStats

  /**
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   */
  def update(
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit = {
    val i = nodeIndex * nodeStride + featureOffsets(featureIndex) + binIndex * statsSize
    impurityAggregator.update(_allStats, i, label, instanceWeight)
  }

  /**
   * Pre-compute node offset for use with [[nodeUpdate]].
   */
  def getNodeOffset(nodeIndex: Int): Int = nodeIndex * nodeStride

  /**
   * Faster version of [[update]].
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   * @param nodeOffset  Pre-computed node offset from [[getNodeOffset]].
   */
  def nodeUpdate(
      nodeOffset: Int,
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit = {
    val i = nodeOffset + featureOffsets(featureIndex) + binIndex * statsSize
    impurityAggregator.update(_allStats, i, label, instanceWeight)
  }

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For ordered features only.
   */
  def getNodeFeatureOffset(nodeIndex: Int, featureIndex: Int): Int = {
    require(!isUnordered(featureIndex),
      s"DTStatsAggregator.getNodeFeatureOffset is for ordered features only, but was called" +
        s" for unordered feature $featureIndex.")
    nodeIndex * nodeStride + featureOffsets(featureIndex)
  }

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For unordered features only.
   */
  def getLeftRightNodeFeatureOffsets(nodeIndex: Int, featureIndex: Int): (Int, Int) = {
    require(isUnordered(featureIndex),
      s"DTStatsAggregator.getLeftRightNodeFeatureOffsets is for unordered features only," +
        s" but was called for ordered feature $featureIndex.")
    val baseOffset = nodeIndex * nodeStride + featureOffsets(featureIndex)
    (baseOffset, baseOffset + (metadata.numBins(featureIndex) >> 1) * statsSize)
  }
}

/**
 * DecisionTree statistics aggregator.
 * This holds a flat array of statistics for a set of (nodes, features, bins)
 * and helps with indexing.
 *
 * This instance of [[DTStatsAggregator]] is used when not subsampling features.
 *
 * @param groupNodeIndex  Mapping: treeIndex --> nodeIndex in tree --> node index in group
 * @param featuresForNodes Mapping: treeIndex --> nodeIndex in tree --> features for splits,
 *                         or null if all features should be used.
 */
private[tree] class DTStatsAggregatorSubsampledFeatures(
    metadata: DecisionTreeMetadata,
    groupNodeIndex: Map[Int, Map[Int, Int]],
    featuresForNodes: Map[Int, Map[Int, Array[Int]]]) extends DTStatsAggregator(metadata) {

  /**
   * For each node, offset for each feature for calculating indices into the [[_allStats]] array.
   * Mapping: nodeIndex --> featureIndex --> offset
   */
  private val featureOffsets: Array[Array[Int]] = {
    val numNodes: Int = groupNodeIndex.values.map(_.size).sum
    val offsets = new Array[Array[Int]](numNodes)
    groupNodeIndex.foreach { case (treeIndex, globalToGroupNodeIndex) =>
      globalToGroupNodeIndex.foreach { case (globalIndex, groupIndex) => // indices of node
        offsets(groupIndex) = featuresForNodes(treeIndex)(globalIndex).map(metadata.numBins(_))
          .scanLeft(0)((total, nBins) => total + statsSize * nBins)
      }
    }
    offsets
  }

  /**
   * For each node, offset for each feature for calculating indices into the [[_allStats]] array.
   */
  protected val nodeOffsets: Array[Int] = featureOffsets.map(_.last).scanLeft(0)(_ + _)

  /**
   * Total number of elements stored in this aggregator.
   */
  def allStatsSize: Int = nodeOffsets.last

  /**
   * Flat array of elements.
   * Index for start of stats for a (node, feature, bin) is:
   *   index = nodeOffsets(nodeIndex) + featureOffsets(featureIndex) + binIndex * statsSize
   * Note: For unordered features, the left child stats precede the right child stats
   *       in the binIndex order.
   */
  protected val _allStats: Array[Double] = new Array[Double](allStatsSize)

  /**
   * Get flat array of elements stored in this aggregator.
   */
  protected def allStats: Array[Double] = _allStats

  /**
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   * @param featureIndex  Index of feature in featuresForNodes(nodeIndex).
   *                      Note: This is NOT the original feature index.
   */
  def update(
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit = {
    val i = nodeOffsets(nodeIndex) + featureOffsets(nodeIndex)(featureIndex) + binIndex * statsSize
    impurityAggregator.update(_allStats, i, label, instanceWeight)
  }

  /**
   * Pre-compute node offset for use with [[nodeUpdate]].
   */
  def getNodeOffset(nodeIndex: Int): Int = nodeOffsets(nodeIndex)

  /**
   * Faster version of [[update]].
   * Update the stats for a given (node, feature, bin) for ordered features, using the given label.
   * @param nodeOffset  Pre-computed node offset from [[getNodeOffset]].
   * @param featureIndex  Index of feature in featuresForNodes(nodeIndex).
   *                      Note: This is NOT the original feature index.
   */
  def nodeUpdate(
      nodeOffset: Int,
      nodeIndex: Int,
      featureIndex: Int,
      binIndex: Int,
      label: Double,
      instanceWeight: Double): Unit = {
    val i = nodeOffset + featureOffsets(nodeIndex)(featureIndex) + binIndex * statsSize
    impurityAggregator.update(_allStats, i, label, instanceWeight)
  }

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For ordered features only.
   * @param featureIndex  Index of feature in featuresForNodes(nodeIndex).
   *                      Note: This is NOT the original feature index.
   */
  def getNodeFeatureOffset(nodeIndex: Int, featureIndex: Int): Int = {
    require(!isUnordered(featureIndex),
      s"DTStatsAggregator.getNodeFeatureOffset is for ordered features only, but was called" +
        s" for unordered feature $featureIndex.")
    nodeOffsets(nodeIndex) + featureOffsets(nodeIndex)(featureIndex)
  }

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureUpdate]].
   * For unordered features only.
   * @param featureIndex  Index of feature in featuresForNodes(nodeIndex).
   *                      Note: This is NOT the original feature index.
   */
  def getLeftRightNodeFeatureOffsets(nodeIndex: Int, featureIndex: Int): (Int, Int) = {
    require(isUnordered(featureIndex),
      s"DTStatsAggregator.getLeftRightNodeFeatureOffsets is for unordered features only," +
        s" but was called for ordered feature $featureIndex.")
    val baseOffset = nodeOffsets(nodeIndex) + featureOffsets(nodeIndex)(featureIndex)
    (baseOffset, baseOffset + (metadata.numBins(featureIndex) >> 1) * statsSize)
  }

}

private[tree] object DTStatsAggregator extends Serializable {

  /**
   * Combines two aggregates (modifying the first) and returns the combination.
   */
  def binCombOp(
      agg1: DTStatsAggregator,
      agg2: DTStatsAggregator): DTStatsAggregator = {
    agg1.merge(agg2)
  }

}
