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

import scala.collection.mutable


/**
 * :: Experimental ::
 * DecisionTree statistics aggregator.
 * This holds a flat array of statistics for a set of (nodes, features, bins)
 * and helps with indexing.
 * TODO: Allow views of Vector types to replace some of the code in here.
 */
private[tree] class DTStatsAggregator(
    val numNodes: Int,
    val numBins: Array[Int],
    unorderedFeatures: Set[Int],
    val statsSize: Int) {

  val numFeatures: Int = numBins.size

  val isUnordered: Array[Boolean] =
    Range(0, numFeatures).map(f => unorderedFeatures.contains(f)).toArray

  private val featureOffsets: Array[Int] = {
    def featureOffsetsCalc(total: Int, featureIndex: Int): Int = {
      if (isUnordered(featureIndex)) {
        total + 2 * numBins(featureIndex)
      } else {
        total + numBins(featureIndex)
      }
    }
    Range(0, numFeatures).scanLeft(0)(featureOffsetsCalc).map(statsSize * _).toArray
  }

  /**
   * Number of elements for each node, corresponding to stride between nodes in [[allStats]].
   */
  private val nodeStride: Int = featureOffsets.last * statsSize

  /**
   * Total number of elements stored in this aggregator.
   */
  val allStatsSize: Int = numNodes * nodeStride

  /**
   * Flat array of elements.
   * Index for start of stats for a (node, feature, bin) is:
   *   index = nodeIndex * nodeStride + featureOffsets(featureIndex) + binIndex * statsSize
   * Note: For unordered features, the left child stats have binIndex in [0, numBins(featureIndex))
   *       and the right child stats in [numBins(featureIndex), 2 * numBins(featureIndex))
   */
  val allStats: Array[Double] = new Array[Double](allStatsSize)

  /**
   * Get a view of the stats for a given (node, feature, bin) for ordered features.
   * @return  (flat stats array, start index of stats)  The stats are contiguous in the array.
   */
  def view(nodeIndex: Int, featureIndex: Int, binIndex: Int): (Array[Double], Int) = {
    (allStats, nodeIndex * nodeStride + featureOffsets(featureIndex) + binIndex * statsSize)
  }

  /**
   * Pre-compute node offset for use with [[nodeView]].
   */
  def getNodeOffset(nodeIndex: Int): Int = nodeIndex * nodeStride

  /**
   * Get a view of the stats for a given (node, feature, bin) for ordered features.
   * This uses a pre-computed node offset from [[getNodeOffset]].
   * @return  (flat stats array, start index of stats)  The stats are contiguous in the array.
   */
  def nodeView(nodeOffset: Int, featureIndex: Int, binIndex: Int): (Array[Double], Int) = {
    (allStats, nodeOffset + featureOffsets(featureIndex) + binIndex * statsSize)
  }

  /**
   * Pre-compute (node, feature) offset for use with [[nodeFeatureView]].
   */
  def getNodeFeatureOffset(nodeIndex: Int, featureIndex: Int): Int =
    nodeIndex * nodeStride + featureOffsets(featureIndex)

  /**
   * Get a view of the stats for a given (node, feature, bin) for ordered features.
   * This uses a pre-computed (node, feature) offset from [[getNodeFeatureOffset]].
   * @return  (flat stats array, start index of stats)  The stats are contiguous in the array.
   */
  def nodeFeatureView(nodeFeatureOffset: Int, binIndex: Int): (Array[Double], Int) = {
    (allStats, nodeFeatureOffset + binIndex * statsSize)
  }

  /**
   * Merge this aggregator with another, and returns this aggregator.
   * This method modifies this aggregator in-place.
   */
  def merge(other: DTStatsAggregator): DTStatsAggregator = {
    //TODO
  }

  // TODO: Make views
  /*
  VIEWS TO MAKE:
  random access
    impurityAggregator.update(statsAggregator.view(nodeIndex, featureIndex, binIndex), label)
  random access for fixed nodeIndex
    statsAggregator.getNodeOffset(nodeIndex) = nodeIndex * nodeStride
    impurityAggregator.update(statsAggregator.nodeView(nodeOffset, featureIndex, binIndex), label)
  loop over bins, with rightChildOffset, for fixed node, feature (for unordered categorical)
    statsAggregator.getNodeFeatureOffset(nodeIndex, featureIndex) = nodeIndex * nodeStride + featureOffsets(featureIndex)
    impurityAggregator.update(statsAggregator.nodeFeatureView(nodeFeatureOffset, binIndex, isLeft), label)

  complete sum
   */

}
