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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.impl.estimator.PredictorParams
import org.apache.spark.ml.param._
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Gini => OldGini, Entropy => OldEntropy,
  Impurity => OldImpurity, Variance => OldVariance}


/**
 * :: DeveloperApi ::
 * Parameters for Decision Tree-based algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
@DeveloperApi
private[ml] trait DecisionTreeParams extends PredictorParams {

  /**
   * Maximum depth of the tree.
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   * @group param
   */
  final val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "Maximum depth of the tree." +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.")

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be >= 2 and >= number of categories in any categorical feature.
   * (default = 32)
   * @group param
   */
  final val maxBins: IntParam = new IntParam(this, "maxBins", "Max number of bins for" +
    " discretizing continuous features.  Must be >=2 and >= number of categories for any" +
    " categorical feature.")

  /**
   * Minimum number of instances each child must have after split.
   * If a split causes the left or right child to have fewer than minInstancesPerNode,
   * the split will be discarded as invalid.
   * Should be >= 1.
   * (default = 1)
   * @group param
   */
  final val minInstancesPerNode: IntParam = new IntParam(this, "minInstancesPerNode", "Minimum" +
    " number of instances each child must have after split.  If a split causes the left or right" +
    " child to have fewer than minInstancesPerNode, the split will be discarded as invalid." +
    " Should be >= 1.")

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * (default = 0.0)
   * @group param
   */
  final val minInfoGain: DoubleParam = new DoubleParam(this, "minInfoGain",
    "Minimum information gain for a split to be considered at a tree node.")

  /**
   * Maximum memory in MB allocated to histogram aggregation.
   * (default = 256 MB)
   * @group expertParam
   */
  final val maxMemoryInMB: IntParam = new IntParam(this, "maxMemoryInMB",
    "Maximum memory in MB allocated to histogram aggregation.")

  /**
   * If false, the algorithm will pass trees to executors to match instances with nodes.
   * If true, the algorithm will cache node IDs for each instance.
   * Caching can speed up training of deeper trees.
   * (default = false)
   * @group expertParam
   */
  final val cacheNodeIds: BooleanParam = new BooleanParam(this, "cacheNodeIds", "If false, the" +
    " algorithm will pass trees to executors to match instances with nodes. If true, the" +
    " algorithm will cache node IDs for each instance. Caching can speed up training of deeper" +
    " trees.")

  /**
   * Specifies how often to checkpoint the cached node IDs.
   * E.g. 10 means that the cache will get checkpointed every 10 iterations.
   * This is only used if cacheNodeIds is true and if the checkpoint directory is set in
   * [[org.apache.spark.SparkContext]].
   * Must be >= 1.
   * (default = 10)
   * @group expertParam
   */
  final val checkpointInterval: IntParam = new IntParam(this, "checkpointInterval", "Specifies" +
    " how often to checkpoint the cached node IDs.  E.g. 10 means that the cache will get" +
    " checkpointed every 10 iterations. This is only used if cacheNodeIds is true and if the" +
    " checkpoint directory is set in the SparkContext. Must be >= 1.")

  setDefault(maxDepth -> 5, maxBins -> 32, minInstancesPerNode -> 1, minInfoGain -> 0.0,
    maxMemoryInMB -> 256, cacheNodeIds -> false, checkpointInterval -> 10)

  /** @group setParam */
  def setMaxDepth(value: Int): this.type = {
    require(value >= 0, s"maxDepth parameter must be >= 0.  Given bad value: $value")
    set(maxDepth, value)
    this
  }

  /** @group getParam */
  def getMaxDepth: Int = getOrDefault(maxDepth)

  /** @group setParam */
  def setMaxBins(value: Int): this.type = {
    require(value >= 2, s"maxBins parameter must be >= 2.  Given bad value: $value")
    set(maxBins, value)
    this
  }

  /** @group getParam */
  def getMaxBins: Int = getOrDefault(maxBins)

  /** @group setParam */
  def setMinInstancesPerNode(value: Int): this.type = {
    require(value >= 1, s"minInstancesPerNode parameter must be >= 1.  Given bad value: $value")
    set(minInstancesPerNode, value)
    this
  }

  /** @group getParam */
  def getMinInstancesPerNode: Int = getOrDefault(minInstancesPerNode)

  /** @group setParam */
  def setMinInfoGain(value: Double): this.type = {
    set(minInfoGain, value)
    this
  }

  /** @group getParam */
  def getMinInfoGain: Double = getOrDefault(minInfoGain)

  /** @group expertSetParam */
  def setMaxMemoryInMB(value: Int): this.type = {
    require(value > 0, s"maxMemoryInMB parameter must be > 0.  Given bad value: $value")
    set(maxMemoryInMB, value)
    this
  }

  /** @group expertGetParam */
  def getMaxMemoryInMB: Int = getOrDefault(maxMemoryInMB)

  /** @group expertSetParam */
  def setCacheNodeIds(value: Boolean): this.type = {
    set(cacheNodeIds, value)
    this
  }

  /** @group expertGetParam */
  def getCacheNodeIds: Boolean = getOrDefault(cacheNodeIds)

  /** @group expertSetParam */
  def setCheckpointInterval(value: Int): this.type = {
    require(value >= 1, s"checkpointInterval parameter must be >= 1.  Given bad value: $value")
    set(checkpointInterval, value)
    this
  }

  /** @group expertGetParam */
  def getCheckpointInterval: Int = getOrDefault(checkpointInterval)

  /**
   * Create a Strategy instance to use with the old API.
   * NOTE: The caller should set impurity and subsamplingRate (which is set to 1.0,
   *       the default for single trees).
   */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int): OldStrategy = {
    val strategy = OldStrategy.defaultStategy(OldAlgo.Classification)
    strategy.checkpointInterval = getCheckpointInterval
    strategy.maxBins = getMaxBins
    strategy.maxDepth = getMaxDepth
    strategy.maxMemoryInMB = getMaxMemoryInMB
    strategy.minInfoGain = getMinInfoGain
    strategy.minInstancesPerNode = getMinInstancesPerNode
    strategy.useNodeIdCache = getCacheNodeIds
    strategy.numClasses = numClasses
    strategy.categoricalFeaturesInfo = categoricalFeatures
    strategy.subsamplingRate = 1.0 // default for individual trees
    strategy
  }
}

/**
 * (private trait) Parameters for Decision Tree-based classification algorithms.
 */
private[ml] trait TreeClassifierParams extends Params {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Supported: "entropy" and "gini".
   * (default = gini)
   * @group param
   */
  val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeClassifierParams.supportedImpurities.mkString(", ")}")

  setDefault(impurity -> "gini")

  /** @group setParam */
  def setImpurity(value: String): this.type = {
    val impurityStr = value.toLowerCase
    require(TreeClassifierParams.supportedImpurities.contains(impurityStr),
      s"Tree-based classifier was given unrecognized impurity: $value." +
      s"  Supported options: ${TreeClassifierParams.supportedImpurities.mkString(", ")}")
    set(impurity, impurityStr)
    this
  }

  /** @group getParam */
  def getImpurity: String = getOrDefault(impurity)

  /** Convert new impurity to old impurity. */
  private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "entropy" => OldEntropy
      case "gini" => OldGini
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeClassifierParams was given unrecognized impurity: $impurity.")
    }
  }
}

private[ml] object TreeClassifierParams {
  // These options should be lowercase.
  val supportedImpurities: Array[String] = Array("entropy", "gini").map(_.toLowerCase)
}

/**
 * (private trait) Parameters for Decision Tree-based regression algorithms.
 */
private[ml] trait TreeRegressorParams extends Params {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Supported: "variance".
   * (default = variance)
   * @group param
   */
  val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeRegressorParams.supportedImpurities.mkString(", ")}")

  setDefault(impurity -> "variance")

  /** @group setParam */
  def setImpurity(value: String): this.type = {
    val impurityStr = value.toLowerCase
    require(TreeRegressorParams.supportedImpurities.contains(impurityStr),
      s"Tree-based regressor was given unrecognized impurity: $value." +
        s"  Supported options: ${TreeRegressorParams.supportedImpurities.mkString(", ")}")
    set(impurity, impurityStr)
    this
  }

  /** @group getParam */
  def getImpurity: String = getOrDefault(impurity)

  /** Convert new impurity to old impurity. */
  private[ml] def getOldImpurity: OldImpurity = {
    getImpurity match {
      case "variance" => OldVariance
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(
          s"TreeRegressorParams was given unrecognized impurity: $impurity")
    }
  }
}

private[ml] object TreeRegressorParams {
  // These options should be lowercase.
  val supportedImpurities: Array[String] = Array("variance").map(_.toLowerCase)
}
