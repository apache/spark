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

import org.apache.spark.mllib.linalg.{Vectors, Vector}

import scala.language.experimental.macros
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

/**
 * Abstraction for Decision Tree models.
 *
 * TODO: Add support for predicting probabilities and raw predictions  SPARK-3727
 */
private[ml] trait DecisionTreeModel {

  /** Root of the decision tree */
  def rootNode: Node

  /** Returns a predictor based on the root node optionally using codegen. */
  def predictor(codeGen: Boolean): Vector => Double = {
    if (codeGen) {
      CodeGenerationDecisionTreeModel.getScorer(rootNode)
    } else {
      ((f: Vector) => rootNode.predictImpl(f).prediction)
    }
  }

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

  /**
   * Trace down the tree, and return the largest feature index used in any split.
   * @return  Max feature index used in a split, or -1 if there are no splits (single leaf node).
   */
  private[ml] def maxSplitFeatureIndex(): Int = rootNode.maxSplitFeatureIndex()
}

/**
 * Abstraction for models which are ensembles of decision trees
 *
 * TODO: Add support for predicting probabilities and raw predictions  SPARK-3727
 */
private[ml] trait TreeEnsembleModel {

  // Note: We use getTrees since subclasses of TreeEnsembleModel will store subclasses of
  //       DecisionTreeModel.

  /** Trees in this ensemble. Warning: These have null parent Estimators. */
  def trees: Array[DecisionTreeModel]

  /** Weights for each tree, zippable with [[trees]] */
  def treeWeights: Array[Double]

  /** Weights used by the python wrappers. */
  // Note: An array cannot be returned directly due to serialization problems.
  private[spark] def javaTreeWeights: Vector = Vectors.dense(treeWeights)

  /** Summary of the model */
  override def toString: String = {
    // Implementing classes should generally override this method to be more descriptive.
    s"TreeEnsembleModel with $numTrees trees"
  }

  /** Full description of model */
  def toDebugString: String = {
    val header = toString + "\n"
    header + trees.zip(treeWeights).zipWithIndex.map { case ((tree, weight), treeIndex) =>
      s"  Tree $treeIndex (weight $weight):\n" + tree.rootNode.subtreeToString(4)
    }.fold("")(_ + _)
  }

  /** Number of trees in ensemble */
  val numTrees: Int = trees.length

  /** Total number of nodes, summed over all trees in the ensemble. */
  lazy val totalNumNodes: Int = trees.map(_.numNodes).sum
}

/**
 * An object for creating a code generated decision tree model.
 * NodeToTree is used to convert a node to a series if code gen
 * if/else statements conditions returning the predicition for a
 * given vector.
 * getScorer wraps this and provides a function we can use to get
 * the prediction.
 */
private[spark] object CodeGenerationDecisionTreeModel {
  val universe: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe

  import universe._

  // Convert a node to code gened if/else statements
  def NodeToTree(root: Node): universe.Tree = {
    root match {
      case node: InternalNode => {
        val nodeSplit = node.split
        nodeSplit match {
          case split: CategoricalSplit => {
            val isLeft = split.isLeft
            isLeft match {
              case true => q"if (categories.contains(input(${split.featureIndex}))) { ${NodeToTree(node.leftChild)} } else { ${NodeToTree(node.rightChild)} }"
              case false => q"if (categories.contains(input(${split.featureIndex}))) { ${NodeToTree(node.leftChild)} } else { ${NodeToTree(node.rightChild)} }"
            }
          }
          case split: ContinuousSplit => {
            q"if (input(${split.featureIndex}) <= ${split.threshold}) { ${NodeToTree(node.leftChild)} } else { ${NodeToTree(node.rightChild)} }"
          }
        }
      }
      case node: LeafNode => q"${node.prediction}"
    }
  }

  // Create a codegened scorer for a given node
  def getScorer(root: Node): Vector => Double = {
    val toolbox = currentMirror.mkToolBox()
    val code =
      q"""
         import org.apache.spark.mllib.linalg.{Vectors, Vector}
         (input: Vector) => {
            ${NodeToTree(root)}
         }
       """
    toolbox.eval(code).asInstanceOf[Vector => Double]
  }
}
