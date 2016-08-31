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

  /** Converts the passed-in compressed bitmap to a bitset of the specified size */
  private[impl] def toBitset(bitmap: RoaringBitmap, size: Int): BitSet = {
    val result = new BitSet(size)
    val iter = bitmap.getIntIterator
    while(iter.hasNext) {
      result.set(iter.next)
    }
    result
  }

  /**
   * TODO(smurching): Update doc
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
      numRows: Int,
      allSplits: Array[Array[Split]]): BitSet = {

    val bitmap = partitionInfo match {
      case PartitionInfo(oldCols: Array[FeatureVector], oldNodeOffsets: Array[(Int, Int)],
      oldActiveNodes: Array[LearningNode], _) => {
        // localFeatureIndex[feature index] = index into PartitionInfo.columns
        val localFeatureIndex: Map[Int, Int] = oldCols.map(_.featureIndex).zipWithIndex.toMap
        // Build up a bitmap identifying whether each row splits left or right
        // TODO(smurching): copying oldActiveNodes array may be inefficient
        val splitNodes = oldActiveNodes.filter(node => node.split.isDefined)
        splitNodes.zipWithIndex.foldLeft(new RoaringBitmap()) {
          case (bitmap: RoaringBitmap, (node: LearningNode, nodeIdx: Int)) =>
            // Update bitmap for each node that was split
            val split = node.split.get
            val (fromOffset, toOffset) = oldNodeOffsets(nodeIdx)
            // Get the column corresponding to the feature index of the current split
            // TODO(smurching): Can we do this without the localFeatureIndex map?
            // e.g. is column i equivalent to feature i when all data is local?
            val colIndex: Int = localFeatureIndex(split.featureIndex)
            val bv: RoaringBitmap = bitVectorFromSplit(oldCols(colIndex), fromOffset,
              toOffset, split, allSplits)
            bitmap.or(bv)
            bitmap
        }
      }
    }
    toBitset(bitmap, numRows)
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
