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

import org.apache.spark.ml.tree.LearningNode
import org.apache.spark.util.collection.BitSet

/**
 * Intermediate data stored during learning.
 * TODO(smurching): Rename; maybe TrainingInfo?
 *
 * Node indexing for nodeOffsets, activeNodes:
 * Nodes are indexed left-to-right along the periphery of the tree, with 0-based indices.
 * The periphery is the set of leaf nodes (active and inactive).
 *
 * @param columns  Array of columns.
 *                 Each column is sorted first by nodes (left-to-right along the tree periphery);
 *                 all columns share this first level of sorting.
 *                 Within each node's group, each column is sorted based on feature value;
 *                 this second level of sorting differs across columns.
 * @param nodeOffsets  Offsets into the columns indicating the first level of sorting (by node).
 *                     The rows corresponding to the node activeNodes(i) are in the range
 *                     [nodeOffsets(i)(0), nodeOffsets(i)(1)) .
 * @param activeNodes  Nodes which are active (still being split).
 *                     Inactive nodes are known to be leaves in the final tree.
 */
private[impl] case class PartitionInfo(
    columns: Array[FeatureVector],
    nodeOffsets: Array[(Int, Int)],
    activeNodes: Array[LearningNode]) extends Serializable {

  // pre-allocated temporary buffers that we use to sort
  // instances in left and right children during update
  val tempVals: Array[Int] = new Array[Int](columns(0).values.length)
  val tempIndices: Array[Int] = new Array[Int](columns(0).values.length)

  /** For debugging */
  override def toString: String = {
    "PartitionInfo(" +
      "  columns: {\n" +
      columns.mkString(",\n") +
      "  },\n" +
      s"  nodeOffsets: ${nodeOffsets.mkString(", ")},\n" +
      s"  activeNodes: ${activeNodes.iterator.mkString(", ")},\n" +
      ")\n"
  }

  /**
   * Update columns and nodeOffsets for the next level of the tree.
   *
   * Update columns:
   *   For each column,
   *     For each (previously) active node,
   *       Sort corresponding range of instances based on bit vector.
   * Update nodeOffsets, activeNodes:
   *   Split offsets for nodes which split (which can be identified using the bit vector).
   *
   * @param instanceBitVector  Bit vector encoding splits for the next level of the tree.
   *                    These must follow a 2-level ordering, where the first level is by node
   *                    and the second level is by row index.
   *                    bitVector(i) = false iff instance i goes to the left child.
   *                    For instances at inactive (leaf) nodes, the value can be arbitrary.
   * @return Updated partition info
   */
  def update(
      instanceBitVector: BitSet,
      newActiveNodes: Array[LearningNode],
      labels: Array[Double],
      metadata: DecisionTreeMetadata): PartitionInfo = {

    // Create buffers for storing our new arrays of node offsets & impurities
    // TODO(smurching): Binary search for these offsets?
    val newNodeOffsets = new ArrayBuffer[(Int, Int)]()

    // Update first-level (per-node) sorting of each column to account for creation
    // of new nodes
    columns.zipWithIndex.foreach { case (col, index) =>
      // For the first column, determine the new offsets of active nodes & build
      // new impurity aggregators for each node.
      index match {
        case 0 => first(col, instanceBitVector, metadata, labels,
          newNodeOffsets)
        case _ => rest(col, instanceBitVector, newNodeOffsets)
      }
    }

    PartitionInfo(columns, newNodeOffsets.toArray, newActiveNodes)
  }

  /**
   * Sort the very first column in the [[PartitionInfo.columns]]. While
   * we sort the column, we also update [[PartitionInfo.nodeOffsets]]
   * (by modifying @param newNodeOffsets)
   *
   * @param col The very first column in [[PartitionInfo.columns]]
   * @param metadata Used to create new [[ImpurityAggregatorSingle]] for a new child
   *                 node in the tree
   * @param labels   Labels are read as we sort column to populate stats for each
   *                 new ImpurityAggregatorSingle
   */
  private def first(
      col: FeatureVector,
      instanceBitVector: BitSet,
      metadata: DecisionTreeMetadata,
      labels: Array[Double],
      newNodeOffsets: ArrayBuffer[(Int, Int)]): Unit = {

    activeNodes.indices.foreach { nodeIdx =>
      val (from, to) = nodeOffsets(nodeIdx)

      // If this is the very first time we split,
      // we don't use rangeIndices to count the number of bits set;
      // the entire bit vector will be used, so getCardinality
      // will give us the same result more cheaply.
      val numBitsSet = if (nodeOffsets.length == 1) {
        instanceBitVector.cardinality()
      } else {
        from.until(to).foldLeft(0) { case (count, i) =>
          count + (if (instanceBitVector.get(col.indices(i))) 1 else 0)
        }
      }

      val numBitsNotSet = to - from - numBitsSet // number of instances splitting left
      // If numBitsNotSet or numBitsSet equals 0, then this node was not split,
      // so we do not need to update its part of the column. Otherwise, we update it.
      val wasSplit = numBitsNotSet != 0 && numBitsSet != 0
      if (wasSplit) {
        val leftIndices = (from, from + numBitsNotSet)
        val rightIndices = (from + numBitsNotSet, to)
        // TODO(smurching): Check that this adds indices in the same order as activeNodes
        // are produced during splitting
        newNodeOffsets ++= Array(leftIndices, rightIndices)
        LocalDecisionTreeUtils.sortCol(col, from, to, numBitsNotSet,
          tempVals, tempIndices, instanceBitVector)
      }
    }

  }

  /**
   * Sort the remaining columns in the [[PartitionInfo.columns]]. Since
   * we already computed [[PartitionInfo.nodeOffsets]] while we
   * sorted the first column, we skip the computation for those here.
   *
   * @param col The very first column in [[PartitionInfo.columns]]
   * @param newNodeOffsets Instead of re-computing number of bits set/not set
   *                       per split, we read those values from here
   */
  private def rest(
      col: FeatureVector,
      instanceBitVector: BitSet,
      newNodeOffsets: ArrayBuffer[(Int, Int)]): Unit = {
    // Iterate over row offsets of pairs of sibling active nodes (e.g. pairs of nodes with a common
    // parent). Assumes that sibling active nodes' row offsets are located at adjacent positions in
    // the newNodeOffsets array
    0.until(newNodeOffsets.length, 2).foreach { nodeIdx =>

      val (leftFrom, leftTo) = newNodeOffsets(nodeIdx)
      val (rightFrom, rightTo) = newNodeOffsets(nodeIdx + 1)

      // Number of rows on the left side of the split
      val numBitsNotSet = leftTo - leftFrom
      LocalDecisionTreeUtils.sortCol(col, leftFrom, rightTo, numBitsNotSet,
        tempVals, tempIndices, instanceBitVector)
    }

  }

}
