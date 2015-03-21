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

import org.apache.spark.mllib.classification.tree.ClassificationImpurity
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Gini => OldGini, Entropy => OldEntropy}
import org.apache.spark.util.Utils


/**
 * (private trait) Parameters for Decision Trees.
 * @tparam M  Concrete class implementing this parameter trait
 */
private[mllib] trait DecisionTreeParams[M] {

  protected var maxDepth: Int = 5

  protected var maxBins: Int = 32

  protected var minInstancesPerNode: Int = 1

  protected var minInfoGain: Double = 0.0

  protected var maxMemoryInMB: Int = 256

  protected var cacheNodeIds: Boolean = false

  protected var checkpointInterval: Int = 10

  /**
   * Maximum depth of the tree.
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   * @group setParam
   */
  def setMaxDepth(maxDepth: Int): M = {
    this.maxDepth = maxDepth
    this.asInstanceOf[M]
  }

  /**
   * Maximum depth of the tree.
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   * @group getParam
   */
  def getMaxDepth: Int = maxDepth

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be >= 2 and >= number of categories in any categorical feature.
   * Values < 0 are interpreted as "auto" (algorithm chooses automatically).
   * (default = 32)
   * @group setParam
   */
  def setMaxBins(maxBins: Int): M = {
    this.maxBins = maxBins
    this.asInstanceOf[M]
  }

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be >= 2 and >= number of categories in any categorical feature.
   * Values < 0 are interpreted as "auto" (algorithm chooses automatically).
   * (default = 32)
   * @group getParam
   */
  def getMaxBins: Int = maxBins

  /**
   * Minimum number of instances each child must have after split.
   * If a split cause left or right child to have less than minInstancesPerNode,
   * this split will not be considered as a valid split.
   * (default = 1)
   * @group setParam
   */
  def setMinInstancesPerNode(minInstancesPerNode: Int): M = {
    this.minInstancesPerNode = minInstancesPerNode
    this.asInstanceOf[M]
  }

  /**
   * Minimum number of instances each child must have after split.
   * If a split cause left or right child to have less than minInstancesPerNode,
   * this split will not be considered as a valid split.
   * (default = 1)
   * @group getParam
   */
  def getMinInstancesPerNode: Int = minInstancesPerNode

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * (default = 0.0)
   * @group setParam
   */
  def setMinInfoGain(minInfoGain: Double): M = {
    this.minInfoGain = minInfoGain
    this.asInstanceOf[M]
  }

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * (default = 0.0)
   * @group getParam
   */
  def getMinInfoGain: Double = minInfoGain

  /**
   * Maximum memory in MB allocated to histogram aggregation.
   * (default = 256 MB)
   * @group expert
   */
  def setMaxMemoryInMB(maxMemoryInMB: Int): M = {
    this.maxMemoryInMB = maxMemoryInMB
    this.asInstanceOf[M]
  }

  /**
   * Maximum memory in MB allocated to histogram aggregation.
   * (default = 256 MB)
   * @group expert
   */
  def getMaxMemoryInMB: Int = maxMemoryInMB

  /**
   * If false, the algorithm will pass trees to executors to match instances with nodes.
   * If true, the algorithm will cache node IDs for each instance.
   * Caching can speed up training of deeper trees.
   * (default = false)
   * @group expert
   */
  def setCacheNodeIds(cacheNodeIds: Boolean): M = {
    this.cacheNodeIds = cacheNodeIds
    this.asInstanceOf[M]
  }

  /**
   * If false, the algorithm will pass trees to executors to match instances with nodes.
   * If true, the algorithm will cache node IDs for each instance.
   * Caching can speed up training of deeper trees.
   * (default = false)
   * @group expert
   */
  def getCacheNodeIds: Boolean = cacheNodeIds

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * (default = 10)
   * @group expert
   */
  def setCheckpointInterval(checkpointInterval: Int): M = {
    this.checkpointInterval = checkpointInterval
    this.asInstanceOf[M]
  }

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * (default = 10)
   * @group expert
   */
  def getCheckpointInterval: Int = checkpointInterval

  /**
   * Create a Strategy instance to use with the old API.
   * NOTE: The caller should set subsamplingRate by hand based on the model type!
   * TODO: Make this protected once we deprecate the old API.
   */
  private[mllib] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    val strategy = OldStrategy.defaultStategy(OldAlgo.Classification)
    strategy.checkpointInterval = checkpointInterval
    strategy.maxBins = maxBins
    strategy.maxDepth = maxDepth
    strategy.maxMemoryInMB = maxMemoryInMB
    strategy.minInfoGain = minInfoGain
    strategy.minInstancesPerNode = minInstancesPerNode
    strategy.useNodeIdCache = cacheNodeIds
    strategy.numClasses = numClasses
    strategy.subsamplingRate = 1.0 // default for individual trees
    strategy
  }

}

private[mllib] trait TreeClassifierParams[M] extends DecisionTreeParams[M] {

  protected var impurity: ClassificationImpurity = ClassificationImpurity.Gini

  /**
   * Criterion used for information gain calculation.
   * Supported: [[org.apache.spark.mllib.classification.tree.ClassificationImpurity.Gini]],
   * [[org.apache.spark.mllib.classification.tree.ClassificationImpurity.Entropy]].
   * (default = Gini)
   * @group setParam
   */
  def setImpurity(impurity: ClassificationImpurity): M = {
    this.impurity = impurity
    this.asInstanceOf[M]
  }

  /**
   * Criterion used for information gain calculation.
   * Supported: [[org.apache.spark.mllib.classification.tree.ClassificationImpurity.Gini]],
   * [[org.apache.spark.mllib.classification.tree.ClassificationImpurity.Entropy]].
   * (default = Gini)
   * @group getParam
   */
  def getImpurity: ClassificationImpurity = impurity

  /**
   * Create a Strategy instance to use with the old API.
   * NOTE: The caller should set subsamplingRate by hand based on the model type!
   * TODO: Make this protected once we deprecate the old API.
   */
  override private[mllib] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses)
    val oldImpurity = impurity match {
      case ClassificationImpurity.Entropy => OldEntropy
      case ClassificationImpurity.Gini => OldGini
      case _ => throw new RuntimeException(
        s"TreeClassifierParams was given unrecognized impurity: $impurity")
    }
    strategy.setImpurity(oldImpurity)
    strategy
  }
}

/**
 * (private trait) Parameters for Decision Trees.
 * @tparam M  Concrete class implementing this parameter trait
 */
private[mllib] trait TreeEnsembleParams[M] {

  protected var subsamplingRate: Double = 1.0

  protected var seed: Long = Utils.random.nextLong()

  /**
   * Fraction of the training data used for learning each decision tree.
   * (default = 1.0)
   * @group setParam
   */
  def setSubsamplingRate(subsamplingRate: Double): M = {
    this.subsamplingRate = subsamplingRate
    this.asInstanceOf[M]
  }

  /**
   * Fraction of the training data used for learning each decision tree.
   * (default = 1.0)
   * @group getParam
   */
  def getSubsamplingRate: Double = subsamplingRate

  /**
   * Random seed.
   * @group setParam
   */
  def setSeed(seed: Long): M = {
    this.seed = seed
    this.asInstanceOf[M]
  }

  /**
   * Random seed.
   * @group getParam
   */
  def getSeed: Long = seed

}

private[mllib] trait RandomForestParams[M] extends TreeEnsembleParams[M] {

  protected var numTrees: Int = 20

  protected var featureSubsetStrategy: FeatureSubsetStrategy = FeatureSubsetStrategy.Auto

  /** Paired with [[featureSubsetStrategy]] to track built-in named options */
  protected var featureSubsetStrategyStr: String = "auto"

  /**
   * Number of trees to train (>= 1).
   * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).
   * (default = 20)
   * @group setParam
   */
  def setNumTrees(numTrees: Int): M = {
    require(numTrees >= 1,
      s"Random Forest numTrees parameter cannot be $numTrees; it must be >= 1.")
    this.numTrees = numTrees
    this.asInstanceOf[M]
  }

  /**
   * Number of trees to train (>= 1).
   * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).
   * (default = 20)
   * @group getParam
   */
  def getNumTrees: Int = numTrees

  /**
   * Specifies the number of features to consider for splits at each tree node.
   * Use featureSubsetStrategies to select supported options.
   * (default = [[FeatureSubsetStrategy.Auto]])
   * @group setParam
   */
  def setFeatureSubsetStrategy(featureSubsetStrategy: String): M = {
    this.featureSubsetStrategy = FeatureSubsetStrategies.fromString(featureSubsetStrategy)
    this.featureSubsetStrategyStr = featureSubsetStrategy
    this.asInstanceOf[M]
  }

  /**
   * Specifies the number of features to consider for splits at each tree node.
   * Use featureSubsetStrategies to select supported options.
   * (default = "auto")
   * Note: This returns the strategy as a String to permit us to make this strategy
   *       more pluggable in the future.
   * @group getParam
   */
  def getFeatureSubsetStrategyStr: String = featureSubsetStrategyStr
}

private[mllib] trait GBTParams[M] extends TreeEnsembleParams[M] {

  protected var numIterations: Int = 20

  /**
   * Number of trees to train (>= 1).
   * (default = 20)
   * @group setParam
   */
  def setNumIterations(numIterations: Int): M = {
    require(numIterations >= 1,
      s"Gradient Boosting numIterations parameter cannot be $numIterations; it must be >= 1.")
    this.numIterations = numIterations
    this.asInstanceOf[M]
  }

  /**
   * Number of trees to train (>= 1).
   * (default = 20)
   * @group getParam
   */
  def getNumIterations: Int = numIterations
}
