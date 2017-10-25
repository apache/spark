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

import org.apache.spark.internal.Logging
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.rdd.RDD


/** Object providing test-only methods for local decision tree training. */
private[impl] object LocalTreeTests extends Logging {

  /**
   * Given the root node of a decision tree, returns a corresponding DecisionTreeModel
   * @param algo Enum describing the algorithm used to fit the tree
   * @param numClasses Number of label classes (for classification trees)
   * @param parentUID UID of parent estimator
   */
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

  /**
   * Method to locally train a decision tree model over an RDD. Assumes the RDD is small enough
   * to be collected at a single worker and used to fit a decision tree locally.
   * Only used for testing.
   */
  private[impl] def train(
      input: RDD[LabeledPoint],
      strategy: OldStrategy,
      seed: Long,
      parentUID: Option[String] = None): DecisionTreeModel = {

    // Validate input data
    require(input.count() > 0, "Local decision tree training requires > 0 training examples.")
    val numFeatures = input.first().features.size
    require(numFeatures > 0, "Local decision tree training requires > 0 features.")

    // Construct metadata, find splits
    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val splits = RandomForest.findSplits(input, metadata, seed)

    // Bin feature values (convert to TreePoint representation).
    val treeInput = TreePoint.convertToTreeRDD(input, splits, metadata).collect()
    val instanceWeights = Array.fill[Double](treeInput.length)(1.0)

    // Create tree root node
    val initialRoot = LearningNode.emptyNode(nodeIndex = 1)
    // TODO: Create rng for feature subsampling (using seed), pass to fitNode
    // Fit tree
    val rootNode = LocalDecisionTree.fitNode(treeInput, instanceWeights,
      initialRoot, metadata, splits)
    finalizeTree(rootNode, strategy.algo, strategy.numClasses, numFeatures, parentUID)
  }

  /**
   * Returns an array of continuous splits for the feature with index featureIndex and the passed-in
   * set of values. Creates one continuous split per value in values.
   */
  private[impl] def getContinuousSplits(
      values: Array[Int],
      featureIndex: Int): Array[Split] = {
    val splits = values.sorted.map {
      new ContinuousSplit(featureIndex, _).asInstanceOf[Split]
    }
    splits
  }
}
