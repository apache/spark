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

package org.apache.spark.mllib.tree.configuration

import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._
import org.apache.spark.mllib.tree.impurity.Impurity
import org.apache.spark.mllib.tree.loss.{LeastSquaresError, Loss}

/**
 * :: Experimental ::
 * Stores all the configuration options for the boosting algorithms
 * @param algo  Learning goal.  Supported:
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Classification]],
 *              [[org.apache.spark.mllib.tree.configuration.Algo.Regression]]
 * @param numEstimators Number of estimators used in boosting stages. In other words,
 *                      number of boosting iterations performed.
 * @param loss Loss function used for minimization during gradient boosting.
 * @param impurity Criterion used for information gain calculation.
 *                 Supported for Classification: [[org.apache.spark.mllib.tree.impurity.Gini]],
 *                  [[org.apache.spark.mllib.tree.impurity.Entropy]].
 *                 Supported for Regression: [[org.apache.spark.mllib.tree.impurity.Variance]].
 * @param maxDepth Maximum depth of the tree.
 *                 E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
 * @param learningRate Learning rate for shrinking the contribution of each estimator. The
 *                     learning rate should be between in the interval (0, 1]
 * @param subsample  Fraction of the training data used for learning the decision tree.
 * @param checkpointPeriod Checkpointing the dataset in memory to avoid long lineage chains.
 * @param numClassesForClassification Number of classes for classification.
 *                                    (Ignored for regression.)
 *                                    Default value is 2 (binary classification).
 * @param maxBins Maximum number of bins used for discretizing continuous features and
 *                for choosing how to split on features at each node.
 *                More bins give higher granularity.
 * @param quantileCalculationStrategy Algorithm for calculating quantiles.  Supported:
 *                             [[org.apache.spark.mllib.tree.configuration.QuantileStrategy.Sort]]
 * @param categoricalFeaturesInfo A map storing information about the categorical variables and the
 *                                number of discrete values they take. For example, an entry (n ->
 *                                k) implies the feature n is categorical with k categories 0,
 *                                1, 2, ... , k-1. It's important to note that features are
 *                                zero-indexed.
 * @param minInstancesPerNode Minimum number of instances each child must have after split.
 *                            Default value is 1. If a split cause left or right child
 *                            to have less than minInstancesPerNode,
 *                            this split will not be considered as a valid split.
 * @param minInfoGain Minimum information gain a split must get. Default value is 0.0.
 *                    If a split has less information gain than minInfoGain,
 *                    this split will not be considered as a valid split.
 * @param maxMemoryInMB Maximum memory in MB allocated to histogram aggregation. Default value is
 *                      256 MB.
 */
@Experimental
case class BoostingStrategy(
    // Required boosting parameters
    algo: Algo,
    numEstimators: Int,
    loss: Loss,
    // Required tree parameters
    impurity: Impurity,
    maxDepth: Int,
    // Optional boosting parameters
    learningRate: Double = 0.1,
    subsample: Double = 1,
    checkpointPeriod: Int = 20,
    numClassesForClassification: Int = 2,
    // Optional tree parametes
    maxBins: Int = 32,
    quantileCalculationStrategy: QuantileStrategy = Sort,
    categoricalFeaturesInfo: Map[Int, Int] = Map[Int, Int](),
    minInstancesPerNode: Int = 1,
    minInfoGain: Double = 0.0,
    maxMemoryInMB: Int = 256) extends Serializable {

  val strategy = new Strategy(algo, impurity, maxDepth, numClassesForClassification, maxBins,
    quantileCalculationStrategy, categoricalFeaturesInfo, minInstancesPerNode, minInfoGain,
    maxMemoryInMB, subsample)

  // TODO: Add require for learning rate
}
