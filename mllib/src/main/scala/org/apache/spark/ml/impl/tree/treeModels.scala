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

package org.apache.spark.ml.impl.tree

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.mllib.linalg.Vector


/**
 * :: AlphaComponent ::
 *
 * Abstraction for Decision Tree models.
 *
 * TODO: Add support for predicting probabilities and raw predictions
 */
@AlphaComponent
private[ml] trait DecisionTreeModel {

  /** Root of the decision tree */
  def rootNode: Node

  //def predict(features: Vector): Double

  /** Number of nodes in tree, including leaf nodes. */
  def numNodes: Int = {
    1 + rootNode.numDescendants
  }

  /**
   * Depth of the tree.
   * E.g.: Depth 0 means 1 leaf node.  Depth 1 means 1 internal node and 2 leaf nodes.
   */
  lazy val depth: Int = {
    rootNode.subtreeDepth
  }

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"DecisionTreeModel of depth $depth with $numNodes nodes"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + rootNode.subtreeToString(2)
  }
}

/** Abstraction for models which are ensembles of decision trees */
private[ml] trait TreeEnsembleModel {

  /** Trees in this ensemble */
  def getTrees: Array[DecisionTreeModel]

  /** Weights for each tree, zippable with [[getTrees]] */
  def getTreeWeights: Array[Double]

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"TreeEnsembleModel with $numTrees trees"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + getTrees.zip(getTreeWeights).zipWithIndex.map { case ((tree, weight), treeIndex) =>
      s"  Tree $treeIndex (weight $weight):\n" + tree.rootNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

  /** Number of trees in ensemble */
  val numTrees: Int = getTrees.size

  /** Total number of nodes, summed over all trees in the ensemble. */
  lazy val totalNumNodes: Int = getTrees.map(_.numNodes).sum
}
