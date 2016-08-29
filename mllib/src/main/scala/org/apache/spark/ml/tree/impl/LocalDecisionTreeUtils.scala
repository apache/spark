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

import org.roaringbitmap.RoaringBitmap

import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.impurity._
import org.apache.spark.mllib.tree.model.{ImpurityStats, Predict}
import org.apache.spark.util.collection.BitSet

/**
 * DecisionTree which partitions data by feature.
 *
 * Algorithm:
 *  - Repartition data, grouping by feature.
 *  - Prep data (sort continuous features).
 *  - On each partition, initialize instance--node map with each instance at root node.
 *  - Iterate, training 1 new level of the tree at a time:
 *     - On each partition, for each feature on the partition, select the best split for each node.
 *     - Aggregate best split for each node.
 *     - Aggregate bit vector (1 bit/instance) indicating whether each instance splits
 *       left or right.
 *     - Broadcast bit vector.  On each partition, update instance--node map.
 *
 * TODO: Update to use a sparse column store.
 */
private[ml] object LocalDecisionTreeUtils extends Logging {

  /**
   * Convert a dataset of [[Vector]] from row storage to column storage.
   * This can take any [[Vector]] type but stores data as [[DenseVector]].
   *
   * This maintains sparsity in the data.
   *
   * This maintains matrix structure.  I.e., each partition of the output RDD holds adjacent
   * columns.  The number of partitions will be min(input RDD's number of partitions, numColumns).
   *
   * @param rowStore  An array of input data rows, each represented as an
   *                  int array of binned feature values
   * @return Transpose of rowStore with
   *
   * TODO: Add implementation for sparse data.
   *       For sparse data, distribute more evenly based on number of non-zeros.
   *       (First collect stats to decide how to partition.)
   */
  private[impl] def rowToColumnStoreDense(rowStore: Array[Array[Int]]): Array[Array[Int]] = {
    // Compute the number of rows in the data
    val numRows = {
      val longNumRows: Long = rowStore.size
      require(longNumRows < Int.MaxValue, s"rowToColumnStore given RDD with $longNumRows rows," +
        s" but can handle at most ${Int.MaxValue} rows")
      longNumRows.toInt
    }

    // Return an empty array for a dataset with zero rows or columns, otherwise
    // return the transpose of the rowStore matrix
    if (numRows == 0 || rowStore(0).size == 0) {
      Array.empty
    } else {
      val numCols = rowStore(0).size
      0.until(numCols).map { colIdx =>
        rowStore.map(row => row(colIdx))
      }.toArray
    }
  }

  private[impl] def finalizeTree(
      rootNode: Node,
      algo: OldAlgo.Algo,
      numClasses: Int,
      numFeatures: Int,
      parentUID: Option[String]): DecisionTreeModel = {
    parentUID match {
      case Some(uid) =>
        if (algo == OldAlgo.Classification) {
          new DecisionTreeClassificationModel(uid, rootNode, numFeatures = numFeatures,
            numClasses = numClasses)
        } else {
          new DecisionTreeRegressionModel(uid, rootNode, numFeatures = numFeatures)
        }
      case None =>
        if (algo == OldAlgo.Classification) {
          new DecisionTreeClassificationModel(rootNode, numFeatures = numFeatures,
            numClasses = numClasses)
        } else {
          new DecisionTreeRegressionModel(rootNode, numFeatures = numFeatures)
        }
    }
  }

  private[impl] def getPredict(impurityCalculator: ImpurityCalculator): Predict = {
    val pred = impurityCalculator.predict
    new Predict(predict = pred, prob = impurityCalculator.prob(pred))
  }

  /**
   * On driver: Grow tree based on chosen splits, and compute new set of active nodes.
   *
   * @param oldPeriphery  Old periphery of active nodes.
   * @param bestSplitsAndGains  Best (split, gain) pairs, which can be zipped with the old
   *                            periphery.  These stats will be used to replace the stats in
   *                            any nodes which are split.
   * @param minInfoGain  Threshold for min info gain required to split a node.
   * @return  New active node periphery.
   *          If a node is split, then this method will update its fields.
   */
  private[impl] def computeActiveNodePeriphery(
      oldPeriphery: Array[LearningNode],
      bestSplitsAndGains: Array[(Option[Split], ImpurityStats)],
      minInfoGain: Double,
      minInstancesPerNode: Int): Array[LearningNode] = {
    bestSplitsAndGains.zipWithIndex.flatMap {
      case ((split, stats), nodeIdx) =>
        val node = oldPeriphery(nodeIdx)
        val leftCount = stats.leftImpurityCalculator.count
        val rightCount = stats.rightImpurityCalculator.count
        if (split.nonEmpty && stats.gain > minInfoGain
            && leftCount >= minInstancesPerNode && rightCount >= minInstancesPerNode) {
          // TODO: remove node id
          node.leftChild = Some(LearningNode(node.id * 2, isLeaf = false,
            ImpurityStats.getEmptyImpurityStats(stats.leftImpurityCalculator)))
          node.rightChild = Some(LearningNode(node.id * 2 + 1, isLeaf = false,
            ImpurityStats.getEmptyImpurityStats(stats.rightImpurityCalculator)))
          node.split = split
          node.isLeaf = false
          node.stats = stats
          Iterator(node.leftChild.get, node.rightChild.get)
        } else {
          node.stats = ImpurityStats.getInvalidImpurityStats(stats.impurityCalculator)
          node.isLeaf = true
          Iterator()
        }
    }
  }

  /**
   * Compute bit vector (1 bit/instance) indicating whether each instance goes left/right.
   * - For each node that we split during this round, produce a bitmap (one bit per row
   *   in the training set)
   *   in which we set entries corresponding to rows for that node that were split left.
   *   (TODO(smurching): Can we improve this by using reduceByKey?)
   * - Aggregate the partial bit vectors to create one vector (of length numRows).
   *   Correction: Aggregate only the pieces of that vector corresponding to instances at
   *   active nodes.
   *
   * @param partitionInfo  Contains feature data, plus current status metadata
   * @param bestSplits  Split for each active node, or None if that node will not be split
   * @return Array of bit vectors, ordered by offset ranges
   */
  private[impl] def aggregateBitVector(
      partitionInfo: PartitionInfo,
      bestSplits: Array[Option[Split]],
      numRows: Int,
      allSplits: Array[Array[Split]]): RoaringBitmap = {

    partitionInfo match {
      case PartitionInfo(columns: Array[FeatureVector], nodeOffsets: Array[Int],
        activeNodes: BitSet, fullImpurities: Array[ImpurityAggregatorSingle]) => {

        // localFeatureIndex[feature index] = index into PartitionInfo.columns
        val localFeatureIndex: Map[Int, Int] = columns.map(_.featureIndex).zipWithIndex.toMap

        // Zip active nodes with best splits for each node
        // TODO(smurching): Simplify this by filtering out splits where the bestSplit is None;
        val bitSetForNodes: Iterator[RoaringBitmap] = activeNodes.iterator
          .zip(bestSplits.iterator).flatMap {

          case (nodeIndexInLevel: Int, Some(split: Split)) =>
            // There's a split for the current active node
            val fromOffset = nodeOffsets(nodeIndexInLevel)
            val toOffset = nodeOffsets(nodeIndexInLevel + 1)
            // Get the column corresponding to the feature index of the current split
            // TODO(smurching): Can we do this without the localFeatureIndex map? e.g. is column i
            // equivalent to feature i when all data is local?
            val colIndex: Int = localFeatureIndex(split.featureIndex)
            Iterator(bitVectorFromSplit(columns(colIndex), fromOffset,
              toOffset, split, numRows, allSplits))

          case (nodeIndexInLevel: Int, None) =>
            // Do not create a bitVector when there is no split.
            // PartitionInfo.update will detect that there is no
            // split, by how many instances go left/right.
            Iterator()
        }

        // Result is a new empty bitmap if no nodes were split. Otherwise,
        // result is the OR of the bitmaps for each node
        if (bitSetForNodes.isEmpty) {
          new RoaringBitmap()
        } else {
          bitSetForNodes.reduce[RoaringBitmap] { (acc, bitv) => acc.or(bitv); acc }
        }
      }
    }

  }

  /**
   * For a given feature, for a given node, apply a split and return a bit vector indicating the
   * outcome of the split for each instance at that node.
   *
   * @param col  Column for feature
   * @param from  Start offset in col for the node
   * @param to  End offset in col for the node
   * @param split  Split to apply to instances at this node.
   * @return  Bits indicating splits for instances at this node.
   *          These bits are sorted by the row indices, in order to guarantee an ordering
   *          understood by all workers.
   *          Thus, the bit indices used are based on 2-level sorting: first by node, and
   *          second by sorted row indices within the node's rows.
   *          bit[index in sorted array of row indices] = false for left, true for right
   */
  private[impl] def bitVectorFromSplit(
      col: FeatureVector,
      from: Int,
      to: Int,
      split: Split,
      numRows: Int,
      allSplits: Array[Array[Split]]): RoaringBitmap = {
    val bitv = new RoaringBitmap()
    var i = from
    while (i < to) {
      val value = col.values(i)
      val idx = col.indices(i)
      if (!split.shouldGoLeft(value, allSplits(col.featureIndex))) {
        bitv.add(idx)
      }
      i += 1
    }
    bitv
  }

}
