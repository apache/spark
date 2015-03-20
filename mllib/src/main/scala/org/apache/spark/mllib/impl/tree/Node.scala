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

package org.apache.spark.mllib.impl.tree

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.tree.model.{Node => OldNode}

// TODO: Add aggregate stats (once available).  This will happen after we move the DecisionTree
//       code into the new API and deprecate the old API.

/**
 * Decision tree node.  This serves as both the API for tree nodes and a leaf node.
 * Internal tree nodes extend this API.
 * @param prediction  Prediction this node makes (or would make, if it is an internal node)
 * @param impurity  Impurity measure at this node (for training data)
 */
class Node private[mllib] (
    val prediction: Double,
    val impurity: Double) {

  override def toString = s"LeafNode(prediction = $prediction, impurity = $impurity)"

  /**
   * predict value if node is not leaf
   * @param features feature value
   * @return predicted value
   */
  private[mllib] def predict(features: Vector): Double = prediction

  /**
   * Get the number of nodes in tree below this node, including leaf nodes.
   * E.g., if this is a leaf, returns 0.  If both children are leaves, returns 2.
   */
  private[tree] def numDescendants: Int = 0

  /**
   * Recursive print function.
   * @param indentFactor  The number of spaces to add to each level of indentation.
   */
  private[tree] def subtreeToString(indentFactor: Int = 0): String = {
    val prefix: String = " " * indentFactor
    prefix + s"Predict: $prediction\n"
  }

  /**
   * Get depth of tree from this node.
   * E.g.: Depth 0 means this is a leaf node.  Depth 1 means 1 internal and 2 leaf nodes.
   */
  private[tree] def subtreeDepth: Int = 0
}

private[mllib] object Node {

  /**
   * Create a new Node from the old Node format, recursively creating child nodes as needed.
   */
  def fromOld(oldNode: OldNode): Node = {
    if (oldNode.isLeaf) {
      // TODO: Once the implementation has been moved to this API, then include sufficient
      //       statistics here.
      new Node(prediction = oldNode.predict.predict, impurity = oldNode.impurity)
    } else {
      new InternalNode(prediction = oldNode.predict.predict, impurity = oldNode.impurity,
        leftChild = fromOld(oldNode.leftNode.get), rightChild = fromOld(oldNode.rightNode.get),
        split = Split.fromOld(oldNode.split.get))
    }
  }

  /**
   * Helper method for [[Node.subtreeToString()]].
   * @param split  Split to print
   * @param left  Indicates whether this is the part of the split going to the left,
   *              or that going to the right.
   */
  private[tree] def splitToString(split: Split, left: Boolean): String = {
    val featureStr = s"feature ${split.feature}"
    split match {
      case contSplit: ContinuousSplit =>
        if (left) {
          s"$featureStr <= ${contSplit.threshold}"
        } else {
          s"$featureStr > ${contSplit.threshold}"
        }
      case catSplit: CategoricalSplit =>
        val categoriesStr = catSplit.categories.toSeq.sorted.mkString("{", ",", "}")
        if (left) {
          s"$featureStr in $categoriesStr"
        } else {
          s"$featureStr not in $categoriesStr"
        }
    }
  }
}

/**
 * Internal Decision Tree node
 * @param prediction  Prediction this node makes (or would make, if it is an internal node)
 * @param impurity  Impurity measure at this node (for training data)
 * @param leftChild  Left-hand child node
 * @param rightChild  Right-hand child node
 * @param split  Information about the test used to split to the left or right child.
 */
class InternalNode private[mllib] (
    prediction: Double,
    impurity: Double,
    val leftChild: Node,
    val rightChild: Node,
    val split: Split) extends Node(prediction, impurity) {

  override def toString = {
    s"InternalNode(prediction = $prediction, impurity = $impurity, split = $split)"
  }

  override private[mllib] def predict(features: Vector): Double = {
    if (split.goLeft(features)) {
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
    prefix + s"If (${Node.splitToString(split, left=true)})\n" +
      leftChild.subtreeToString(indentFactor + 1) +
      prefix + s"Else (${Node.splitToString(split, left=false)})\n" +
      rightChild.subtreeToString(indentFactor + 1)
  }

  override private[tree] def subtreeDepth: Int = {
    1 + math.max(leftChild.subtreeDepth, rightChild.subtreeDepth)
  }
}
