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
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.tree.{DecisionTreeModel, LearningNode}
import org.apache.spark.mllib.tree.configuration.{Strategy => OldStrategy}
import org.apache.spark.rdd.RDD

/** Object providing test-only methods for local decision tree training. */
private[tree] object LocalTreeTests extends Logging {

  /**
   * Method to train a decision tree model over an RDD. Assumes the RDD is small enough
   * to be collected at a single worker and used to fit a decision tree locally.
   * Primarily used for testing.
   */
  def train(
      input: RDD[LabeledPoint],
      strategy: OldStrategy,
      parentUID: Option[String] = None,
      seed: Long): DecisionTreeModel = {
    // TODO: Check validity of params
    // TODO: Check for empty dataset

    val metadata = DecisionTreeMetadata.buildMetadata(input, strategy)
    val numFeatures = input.first().features.size

    val splits = RandomForest.findSplits(input, metadata, seed)

    logDebug("numBins: feature: number of bins")
    logDebug(Range(0, metadata.numFeatures).map { featureIndex =>
      s"\t$featureIndex\t${metadata.numBins(featureIndex)}"
    }.mkString("\n"))

    // Bin feature values (TreePoint representation).
    // Cache input RDD for speedup during multiple passes.
    val treeInput = TreePoint.convertToTreeRDD(input, splits, metadata)

    // For normal RF training, withReplacement is set to 1 iff numTrees > 1, which is not the
    // case here (we're training a single tree)
    val withReplacement = false

    val baggedInput: Array[BaggedPoint[TreePoint]] = BaggedPoint
      .convertToBaggedRDD(treeInput, strategy.subsamplingRate, numSubsamples = 1,
        withReplacement, seed)
      .collect()

    val initialRoot = LearningNode.emptyNode(nodeIndex = 1)
    val rootNode = LocalDecisionTree.fitNode(baggedInput, initialRoot, metadata, splits)
    LocalDecisionTreeUtils.finalizeTree(rootNode, strategy.algo, strategy.numClasses, numFeatures,
      parentUID)
  }
}
