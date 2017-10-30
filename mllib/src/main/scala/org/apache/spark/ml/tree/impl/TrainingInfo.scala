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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.ml.tree.{LearningNode, Split}
import org.apache.spark.util.collection.BitSet

/**
 * Maintains intermediate state of data (columns) and tree during local tree training.
 * Primary local tree training data structure; contains all information required to describe
 * the state of the algorithm at any point during learning.??
 *
 * Nodes are indexed left-to-right along the periphery of the tree, with 0-based indices.
 * The "periphery" is the set of leaf nodes (active and inactive).
 *
 * @param columns  Array of columns.
 *                 Each column is sorted first by nodes (left-to-right along the tree periphery);
 *                 all columns share this first level of sorting.
 * @param nodeOffsets  Offsets into the columns indicating the first level of sorting (by node).
 *                     The rows corresponding to the node activeNodes(i) are in the range
 *                     [nodeOffsets(i)(0), nodeOffsets(i)(1)) .
 * @param currentLevelActiveNodes  Nodes which are active (could still be split).
 *                                 Inactive nodes are known to be leaves in the final tree.
 */
private[impl] case class TrainingInfo(
    columns: Array[FeatureColumn],
    nodeOffsets: Array[(Int, Int)],
    currentLevelActiveNodes: Array[LearningNode],
    rowIndices: Option[Array[Int]] = None) extends Serializable {

  // pre-allocated temporary buffers that we use to sort
  // instances in left and right children during update
  val tempVals: Array[Int] = new Array[Int](columns.head.values.length)

  // Array of row indices for feature values, shared across all columns.
  // For each column (col) in [[columns]], col(j) is the feature value corresponding to the row
  // with index indices(j).
  val indices: Array[Int] = rowIndices.getOrElse(columns.head.values.indices.toArray)

  /** For debugging */
  override def toString: String = {
    "TrainingInfo(" +
      "  columns: {\n" +
      columns.mkString(",\n") +
      "  },\n" +
      s"  nodeOffsets: ${nodeOffsets.mkString(", ")},\n" +
      s"  activeNodes: ${currentLevelActiveNodes.iterator.mkString(", ")},\n" +
      ")\n"
  }

  /**
   * Update columns and nodeOffsets for the next level of the tree.
   *
   * Update columns:
   *   For each (previously) active node,
   *     Compute bitset indicating whether each training instance under the node splits left/right
   *     For each column,
   *       Sort corresponding range of instances based on bitset.
   * Update nodeOffsets, activeNodes:
   *   Split offsets for nodes which split (which can be identified using the bitset).
   *
   * @return Updated partition info
   */
  def update(splits: Array[Array[Split]], newActiveNodes: Array[LearningNode]): TrainingInfo = {
    // Create buffers for storing our new arrays of node offsets & impurities
    val newNodeOffsets = new ArrayBuffer[(Int, Int)]()
    // Update (per-node) sorting of each column to account for creation of new nodes
    var nodeIdx = 0
    while (nodeIdx < currentLevelActiveNodes.length) {
      val node = currentLevelActiveNodes(nodeIdx)
      // Get new active node offsets from active nodes that were split
      if (!node.isLeaf) {
        // Get split and FeatureVector corresponding to feature for split
        val split = node.split.get
        val col = columns(split.featureIndex)
        val (from, to) = nodeOffsets(nodeIdx)
        // Compute bitset indicating whether each training example splits left/right
        val bitset = TrainingInfo.bitSetFromSplit(col, from, to, split, splits(split.featureIndex))
        // Update each column according to the bitset
        val numRows = to - from
        // Allocate shared temp buffers (shared across all columns) for reordering
        // feature values/indices for current node.
        val tempVals = new Array[Int](numRows)
        val numLeftRows = numRows - bitset.cardinality()
        // Reorder values for each column
        columns.foreach { col =>
          LocalDecisionTreeUtils.updateArrayForSplit(col.values, from, to, numLeftRows, tempVals,
            bitset)
        }
        // Reorder indices (shared across all columns)
        LocalDecisionTreeUtils.updateArrayForSplit(indices, from, to, numLeftRows, tempVals, bitset)
        // Add new node offsets to array
        val leftIndices = (from, from + numLeftRows)
        val rightIndices = (from + numLeftRows, to)
        newNodeOffsets ++= Array(leftIndices, rightIndices)
      }
      nodeIdx += 1
    }
    TrainingInfo(columns, newNodeOffsets.toArray, newActiveNodes, Some(indices))
  }

}

/** Training-info specific utility methods. */
private[impl] object TrainingInfo {
  /**
   * For a given feature, for a given node, apply a split and return a bitset indicating the
   * outcome of the split for each instance at that node.
   *
   * @param col  Column for feature
   * @param from  Start offset in col for the node
   * @param to  End offset in col for the node
   * @param split  Split to apply to instances at this node.
   * @return  Bitset indicating splits for instances at this node.
   *          These bits are sorted by the row indices.
   *          bitset(i) = true if ith example for current node splits right, false otherwise.
   */
  private[impl] def bitSetFromSplit(
      col: FeatureColumn,
      from: Int,
      to: Int,
      split: Split,
      featureSplits: Array[Split]): BitSet = {
    val bitset = new BitSet(to - from)
    from.until(to).foreach { i =>
      if (!split.shouldGoLeft(col.values(i), featureSplits)) {
        bitset.set(i - from)
      }
    }
    bitset
  }
}
