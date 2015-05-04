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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.{InformationGainStats => OldInformationGainStats,
  Node => OldNode, Predict => OldPredict}


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

  /** Recursive prediction helper method */
  private[ml] def predict(features: Vector): Double = prediction

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
}

private[ml] object Node {

  /**
   * Create a new Node from the old Node format, recursively creating child nodes as needed.
   */
  def fromOld(oldNode: OldNode, categoricalFeatures: Map[Int, Int]): Node = {
    if (oldNode.isLeaf) {
      // TODO: Once the implementation has been moved to this API, then include sufficient
      //       statistics here.
      new LeafNode(prediction = oldNode.predict.predict, impurity = oldNode.impurity)
    } else {
      val gain = if (oldNode.stats.nonEmpty) {
        oldNode.stats.get.gain
      } else {
        0.0
      }
      new InternalNode(prediction = oldNode.predict.predict, impurity = oldNode.impurity,
        gain = gain, leftChild = fromOld(oldNode.leftNode.get, categoricalFeatures),
        rightChild = fromOld(oldNode.rightNode.get, categoricalFeatures),
        split = Split.fromOld(oldNode.split.get, categoricalFeatures))
    }
  }
}

/**
 * Decision tree leaf node.
 * @param prediction  Prediction this node makes
 * @param impurity  Impurity measure at this node (for training data)
 */
final class LeafNode private[ml] (
    override val prediction: Double,
    override val impurity: Double) extends Node {

  override def toString: String = s"LeafNode(prediction = $prediction, impurity = $impurity)"

  override private[ml] def predict(features: Vector): Double = prediction

  override private[tree] def numDescendants: Int = 0

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"Predict: $prediction\n"
  }

  override private[tree] def subtreeDepth: Int = 0

  override private[ml] def toOld(id: Int): OldNode = {
    // NOTE: We do NOT store 'prob' in the new API currently.
    new OldNode(id, new OldPredict(prediction, prob = 0.0), impurity, isLeaf = true,
      None, None, None, None)
  }
}

/**
 * Internal Decision Tree node.
 * @param prediction  Prediction this node would make if it were a leaf node
 * @param impurity  Impurity measure at this node (for training data)
 * @param gain Information gain value.
 *             Values < 0 indicate missing values; this quirk will be removed with future updates.
 * @param leftChild  Left-hand child node
 * @param rightChild  Right-hand child node
 * @param split  Information about the test used to split to the left or right child.
 */
final class InternalNode private[ml] (
    override val prediction: Double,
    override val impurity: Double,
    val gain: Double,
    val leftChild: Node,
    val rightChild: Node,
    val split: Split) extends Node {

  override def toString: String = {
    s"InternalNode(prediction = $prediction, impurity = $impurity, split = $split)"
  }

  override private[ml] def predict(features: Vector): Double = {
    if (split.shouldGoLeft(features)) {
      leftChild.predict(features)
    } else {
      rightChild.predict(features)
    }
  }

  override private[tree] def numDescendants: Int = {
    2 + leftChild.numDescendants + rightChild.numDescendants
  }

  override private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"If (${InternalNode.splitToString(split, left=true)})\n" +
      leftChild.subtreeToString(indentFactor + 1) +
      prefix + s"Else (${InternalNode.splitToString(split, left=false)})\n" +
      rightChild.subtreeToString(indentFactor + 1)
  }

  override private[tree] def subtreeDepth: Int = {
    1 + math.max(leftChild.subtreeDepth, rightChild.subtreeDepth)
  }

  override private[ml] def toOld(id: Int): OldNode = {
    assert(id.toLong * 2 < Int.MaxValue, "Decision Tree could not be converted from new to old API"
      + " since the old API does not support deep trees.")
    // NOTE: We do NOT store 'prob' in the new API currently.
    new OldNode(id, new OldPredict(prediction, prob = 0.0), impurity, isLeaf = false,
      Some(split.toOld), Some(leftChild.toOld(OldNode.leftChildIndex(id))),
      Some(rightChild.toOld(OldNode.rightChildIndex(id))),
      Some(new OldInformationGainStats(gain, impurity, leftChild.impurity, rightChild.impurity,
        new OldPredict(leftChild.prediction, prob = 0.0),
        new OldPredict(rightChild.prediction, prob = 0.0))))
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
