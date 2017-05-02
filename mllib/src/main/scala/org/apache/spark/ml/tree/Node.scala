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

package org.apache.spark.ml.tree

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.mllib.tree.impurity.ImpurityCalculator
import org.apache.spark.mllib.tree.model.{ImpurityStats,
  InformationGainStats => OldInformationGainStats, Node => OldNode, Predict => OldPredict}

/**
 * Decision tree node interface.
 */
sealed abstract class Node extends Serializable {

  // TODO: Add aggregate stats (once available).  This will happen after we move the DecisionTree
  //       code into the new API and deprecate the old API.  SPARK-3727

  /** Prediction a leaf node makes, or which an internal node would make if it were a leaf node */
  def prediction: Double

  /** Impurity measure at this node (for training data) */
  def impurity: Double

  /**
   * Statistics aggregated from training data at this node, used to compute prediction, impurity,
   * and probabilities.
   * For classification, the array of class counts must be normalized to a probability distribution.
   */
  private[ml] def impurityStats: ImpurityCalculator

  /** Recursive prediction helper method */
  private[ml] def predictImpl(features: Vector): LeafNode

  /**
   * Get the number of nodes in tree below this node, including leaf nodes.
   * E.g., if this is a leaf, returns 0.  If both children are leaves, returns 2.
   */
  private[tree] def numDescendants: Int

  /**
   * Recursive print function.
   * @param indentFactor  The number of spaces to add to each level of indentation.
   */
  private[tree] def subtreeToString(indentFactor: Int = 0): String

  /**
   * Get depth of tree from this node.
   * E.g.: Depth 0 means this is a leaf node.  Depth 1 means 1 internal and 2 leaf nodes.
   */
  private[tree] def subtreeDepth: Int

  /**
   * Create a copy of this node in the old Node format, recursively creating child nodes as needed.
   * @param id  Node ID using old format IDs
   */
  private[ml] def toOld(id: Int): OldNode

  /**
   * Trace down the tree, and return the largest feature index used in any split.
   * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
   */
  private[ml] def maxSplitFeatureIndex(): Int

  /** Returns a deep copy of the subtree rooted at this node. */
  private[tree] def deepCopy(): Node
}

private[ml] object Node {

  /**
   * Create a new Node from the old Node format, recursively creating child nodes as needed.
   */
  def fromOld(oldNode: OldNode, categoricalFeatures: Map[Int, Int]): Node = {
    if (oldNode.isLeaf) {
      // TODO: Once the implementation has been moved to this API, then include sufficient
      //       statistics here.
      new LeafNode(prediction = oldNode.predict.predict,
        impurity = oldNode.impurity, impurityStats = null)
    } else {
      val gain = if (oldNode.stats.nonEmpty) {
        oldNode.stats.get.gain
      } else {
        0.0
      }
      new InternalNode(prediction = oldNode.predict.predict, impurity = oldNode.impurity,
        gain = gain, leftChild = fromOld(oldNode.leftNode.get, categoricalFeatures),
        rightChild = fromOld(oldNode.rightNode.get, categoricalFeatures),
        split = Split.fromOld(oldNode.split.get, categoricalFeatures), impurityStats = null)
    }
  }
}

/**
 * Decision tree leaf node.
 * @param prediction  Prediction this node makes
 * @param impurity  Impurity measure at this node (for training data)
 */
class LeafNode private[ml] (
    override val prediction: Double,
    override val impurity: Double,
    override private[ml] val impurityStats: ImpurityCalculator) extends Node {

  override def toString: String =
    s"LeafNode(prediction = $prediction, impurity = $impurity)"

  override private[ml] def predictImpl(features: Vector): LeafNode = this

  override private[tree] def numDescendants: Int = 0

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"Predict: $prediction\n"
  }

  override private[tree] def subtreeDepth: Int = 0

  override private[ml] def toOld(id: Int): OldNode = {
    new OldNode(id, new OldPredict(prediction, prob = impurityStats.prob(prediction)),
      impurity, isLeaf = true, None, None, None, None)
  }

  override private[ml] def maxSplitFeatureIndex(): Int = -1

  override private[tree] def deepCopy(): Node = {
    new LeafNode(prediction, impurity, impurityStats)
  }
}

/**
 * Internal Decision Tree node.
 * @param prediction  Prediction this node would make if it were a leaf node
 * @param impurity  Impurity measure at this node (for training data)
 * @param gain Information gain value. Values less than 0 indicate missing values;
 *             this quirk will be removed with future updates.
 * @param leftChild  Left-hand child node
 * @param rightChild  Right-hand child node
 * @param split  Information about the test used to split to the left or right child.
 */
class InternalNode private[ml] (
    override val prediction: Double,
    override val impurity: Double,
    val gain: Double,
    val leftChild: Node,
    val rightChild: Node,
    val split: Split,
    override private[ml] val impurityStats: ImpurityCalculator) extends Node {

  // Note to developers: The constructor argument impurityStats should be reconsidered before we
  //                     make the constructor public.  We may be able to improve the representation.

  override def toString: String = {
    s"InternalNode(prediction = $prediction, impurity = $impurity, split = $split)"
  }

  override private[ml] def predictImpl(features: Vector): LeafNode = {
    if (split.shouldGoLeft(features)) {
      leftChild.predictImpl(features)
    } else {
      rightChild.predictImpl(features)
    }
  }

  override private[tree] def numDescendants: Int = {
    2 + leftChild.numDescendants + rightChild.numDescendants
  }

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"If (${InternalNode.splitToString(split, left = true)})\n" +
      leftChild.subtreeToString(indentFactor + 1) +
      prefix + s"Else (${InternalNode.splitToString(split, left = false)})\n" +
      rightChild.subtreeToString(indentFactor + 1)
  }

  override private[tree] def subtreeDepth: Int = {
    1 + math.max(leftChild.subtreeDepth, rightChild.subtreeDepth)
  }

  override private[ml] def toOld(id: Int): OldNode = {
    assert(id.toLong * 2 < Int.MaxValue, "Decision Tree could not be converted from new to old API"
      + " since the old API does not support deep trees.")
    new OldNode(id, new OldPredict(prediction, prob = impurityStats.prob(prediction)), impurity,
      isLeaf = false, Some(split.toOld), Some(leftChild.toOld(OldNode.leftChildIndex(id))),
      Some(rightChild.toOld(OldNode.rightChildIndex(id))),
      Some(new OldInformationGainStats(gain, impurity, leftChild.impurity, rightChild.impurity,
        new OldPredict(leftChild.prediction, prob = 0.0),
        new OldPredict(rightChild.prediction, prob = 0.0))))
  }

  override private[ml] def maxSplitFeatureIndex(): Int = {
    math.max(split.featureIndex,
      math.max(leftChild.maxSplitFeatureIndex(), rightChild.maxSplitFeatureIndex()))
  }

  override private[tree] def deepCopy(): Node = {
    new InternalNode(prediction, impurity, gain, leftChild.deepCopy(), rightChild.deepCopy(),
      split, impurityStats)
  }
}

private object InternalNode {

  /**
   * Helper method for [[Node.subtreeToString()]].
   * @param split  Split to print
   * @param left  Indicates whether this is the part of the split going to the left,
   *              or that going to the right.
   */
  private def splitToString(split: Split, left: Boolean): String = {
    val featureStr = s"feature ${split.featureIndex}"
    split match {
      case contSplit: ContinuousSplit =>
        if (left) {
          s"$featureStr <= ${contSplit.threshold}"
        } else {
          s"$featureStr > ${contSplit.threshold}"
        }
      case catSplit: CategoricalSplit =>
        val categoriesStr = catSplit.leftCategories.mkString("{", ",", "}")
        if (left) {
          s"$featureStr in $categoriesStr"
        } else {
          s"$featureStr not in $categoriesStr"
        }
    }
  }
}

/**
 * Version of a node used in learning.  This uses vars so that we can modify nodes as we split the
 * tree by adding children, etc.
 *
 * For now, we use node IDs.  These will be kept internal since we hope to remove node IDs
 * in the future, or at least change the indexing (so that we can support much deeper trees).
 *
 * This node can either be:
 *  - a leaf node, with leftChild, rightChild, split set to null, or
 *  - an internal node, with all values set
 *
 * @param id  We currently use the same indexing as the old implementation in
 *            [[org.apache.spark.mllib.tree.model.Node]], but this will change later.
 * @param isLeaf  Indicates whether this node will definitely be a leaf in the learned tree,
 *                so that we do not need to consider splitting it further.
 * @param stats  Impurity statistics for this node.
 */
private[tree] class LearningNode(
    var id: Int,
    var leftChild: Option[LearningNode],
    var rightChild: Option[LearningNode],
    var split: Option[Split],
    var isLeaf: Boolean,
    var stats: ImpurityStats) extends Serializable {

  /**
   * Convert this [[LearningNode]] to a regular [[Node]], and recurse on any children.
   */
  def toNode: Node = {
    if (leftChild.nonEmpty) {
      assert(rightChild.nonEmpty && split.nonEmpty && stats != null,
        "Unknown error during Decision Tree learning.  Could not convert LearningNode to Node.")
      new InternalNode(stats.impurityCalculator.predict, stats.impurity, stats.gain,
        leftChild.get.toNode, rightChild.get.toNode, split.get, stats.impurityCalculator)
    } else {
      if (stats.valid) {
        new LeafNode(stats.impurityCalculator.predict, stats.impurity,
          stats.impurityCalculator)
      } else {
        // Here we want to keep same behavior with the old mllib.DecisionTreeModel
        new LeafNode(stats.impurityCalculator.predict, -1.0, stats.impurityCalculator)
      }

    }
  }

  /**
   * Get the node index corresponding to this data point.
   * This function mimics prediction, passing an example from the root node down to a leaf
   * or unsplit node; that node's index is returned.
   *
   * @param binnedFeatures  Binned feature vector for data point.
   * @param splits possible splits for all features, indexed (numFeatures)(numSplits)
   * @return Leaf index if the data point reaches a leaf.
   *         Otherwise, last node reachable in tree matching this example.
   *         Note: This is the global node index, i.e., the index used in the tree.
   *         This index is different from the index used during training a particular
   *         group of nodes on one call to
   *         [[org.apache.spark.ml.tree.impl.RandomForest.findBestSplits()]].
   */
  def predictImpl(binnedFeatures: Array[Int], splits: Array[Array[Split]]): Int = {
    if (this.isLeaf || this.split.isEmpty) {
      this.id
    } else {
      val split = this.split.get
      val featureIndex = split.featureIndex
      val splitLeft = split.shouldGoLeft(binnedFeatures(featureIndex), splits(featureIndex))
      if (this.leftChild.isEmpty) {
        // Not yet split. Return next layer of nodes to train
        if (splitLeft) {
          LearningNode.leftChildIndex(this.id)
        } else {
          LearningNode.rightChildIndex(this.id)
        }
      } else {
        if (splitLeft) {
          this.leftChild.get.predictImpl(binnedFeatures, splits)
        } else {
          this.rightChild.get.predictImpl(binnedFeatures, splits)
        }
      }
    }
  }

}

private[tree] object LearningNode {

  /** Create a node with some of its fields set. */
  def apply(
      id: Int,
      isLeaf: Boolean,
      stats: ImpurityStats): LearningNode = {
    new LearningNode(id, None, None, None, false, stats)
  }

  /** Create an empty node with the given node index.  Values must be set later on. */
  def emptyNode(nodeIndex: Int): LearningNode = {
    new LearningNode(nodeIndex, None, None, None, false, null)
  }

  // The below indexing methods were copied from spark.mllib.tree.model.Node

  /**
   * Return the index of the left child of this node.
   */
  def leftChildIndex(nodeIndex: Int): Int = nodeIndex << 1

  /**
   * Return the index of the right child of this node.
   */
  def rightChildIndex(nodeIndex: Int): Int = (nodeIndex << 1) + 1

  /**
   * Get the parent index of the given node, or 0 if it is the root.
   */
  def parentIndex(nodeIndex: Int): Int = nodeIndex >> 1

  /**
   * Return the level of a tree which the given node is in.
   */
  def indexToLevel(nodeIndex: Int): Int = if (nodeIndex == 0) {
    throw new IllegalArgumentException(s"0 is not a valid node index.")
  } else {
    java.lang.Integer.numberOfTrailingZeros(java.lang.Integer.highestOneBit(nodeIndex))
  }

  /**
   * Returns true if this is a left child.
   * Note: Returns false for the root.
   */
  def isLeftChild(nodeIndex: Int): Boolean = nodeIndex > 1 && nodeIndex % 2 == 0

  /**
   * Return the maximum number of nodes which can be in the given level of the tree.
   * @param level  Level of tree (0 = root).
   */
  def maxNodesInLevel(level: Int): Int = 1 << level

  /**
   * Return the index of the first node in the given level.
   * @param level  Level of tree (0 = root).
   */
  def startIndexInLevel(level: Int): Int = 1 << level

  /**
   * Traces down from a root node to get the node with the given node index.
   * This assumes the node exists.
   */
  def getNode(nodeIndex: Int, rootNode: LearningNode): LearningNode = {
    var tmpNode: LearningNode = rootNode
    var levelsToGo = indexToLevel(nodeIndex)
    while (levelsToGo > 0) {
      if ((nodeIndex & (1 << levelsToGo - 1)) == 0) {
        tmpNode = tmpNode.leftChild.get
      } else {
        tmpNode = tmpNode.rightChild.get
      }
      levelsToGo -= 1
    }
    tmpNode
  }

}
