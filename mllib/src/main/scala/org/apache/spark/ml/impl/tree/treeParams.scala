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
import org.apache.spark.ml.param.shared.{HasSeed, HasMaxIter}
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo,
  BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Gini => OldGini, Entropy => OldEntropy,
  Impurity => OldImpurity, Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{Loss => OldLoss}


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
  }

  /** @group getParam */
  final def getMaxDepth: Int = getOrDefault(maxDepth)

  /** @group setParam */
  def setMaxBins(value: Int): this.type = {
    require(value >= 2, s"maxBins parameter must be >= 2.  Given bad value: $value")
    set(maxBins, value)
  }

  /** @group getParam */
  final def getMaxBins: Int = getOrDefault(maxBins)

  /** @group setParam */
  def setMinInstancesPerNode(value: Int): this.type = {
    require(value >= 1, s"minInstancesPerNode parameter must be >= 1.  Given bad value: $value")
    set(minInstancesPerNode, value)
  }

  /** @group getParam */
  final def getMinInstancesPerNode: Int = getOrDefault(minInstancesPerNode)

  /** @group setParam */
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group getParam */
  final def getMinInfoGain: Double = getOrDefault(minInfoGain)

  /** @group expertSetParam */
  def setMaxMemoryInMB(value: Int): this.type = {
    require(value > 0, s"maxMemoryInMB parameter must be > 0.  Given bad value: $value")
    set(maxMemoryInMB, value)
  }

  /** @group expertGetParam */
  final def getMaxMemoryInMB: Int = getOrDefault(maxMemoryInMB)

  /** @group expertSetParam */
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /** @group expertGetParam */
  final def getCacheNodeIds: Boolean = getOrDefault(cacheNodeIds)

  /** @group expertSetParam */
  def setCheckpointInterval(value: Int): this.type = {
    require(value >= 1, s"checkpointInterval parameter must be >= 1.  Given bad value: $value")
    set(checkpointInterval, value)
  }

  /** @group expertGetParam */
  final def getCheckpointInterval: Int = getOrDefault(checkpointInterval)

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity,
      subsamplingRate: Double): OldStrategy = {
    val strategy = OldStrategy.defaultStategy(oldAlgo)
    strategy.impurity = oldImpurity
    strategy.checkpointInterval = getCheckpointInterval
    strategy.maxBins = getMaxBins
    strategy.maxDepth = getMaxDepth
    strategy.maxMemoryInMB = getMaxMemoryInMB
    strategy.minInfoGain = getMinInfoGain
    strategy.minInstancesPerNode = getMinInstancesPerNode
    strategy.useNodeIdCache = getCacheNodeIds
    strategy.numClasses = numClasses
    strategy.categoricalFeaturesInfo = categoricalFeatures
    strategy.subsamplingRate = subsamplingRate
    strategy
  }
}

/**
 * Parameters for Decision Tree-based classification algorithms.
 */
private[ml] trait TreeClassifierParams extends Params {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Supported: "entropy" and "gini".
   * (default = gini)
   * @group param
   */
  final val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
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
  }

  /** @group getParam */
  final def getImpurity: String = getOrDefault(impurity)

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
  final val supportedImpurities: Array[String] = Array("entropy", "gini").map(_.toLowerCase)
}

/**
 * Parameters for Decision Tree-based regression algorithms.
 */
private[ml] trait TreeRegressorParams extends Params {

  /**
   * Criterion used for information gain calculation (case-insensitive).
   * Supported: "variance".
   * (default = variance)
   * @group param
   */
  final val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
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
  }

  /** @group getParam */
  final def getImpurity: String = getOrDefault(impurity)

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
  final val supportedImpurities: Array[String] = Array("variance").map(_.toLowerCase)
}

/**
 * :: DeveloperApi ::
 * Parameters for Decision Tree-based ensemble algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
@DeveloperApi
private[ml] trait TreeEnsembleParams extends DecisionTreeParams with HasSeed {

  /**
   * Fraction of the training data used for learning each decision tree.
   * (default = 1.0)
   * @group param
   */
  final val subsamplingRate: DoubleParam = new DoubleParam(this, "subsamplingRate",
    "Fraction of the training data used for learning each decision tree.")

  setDefault(subsamplingRate -> 1.0)

  /** @group setParam */
  def setSubsamplingRate(value: Double): this.type = {
    require(value > 0.0 && value <= 1.0,
      s"Subsampling rate must be in range (0,1]. Bad rate: $value")
    set(subsamplingRate, value)
  }

  /** @group getParam */
  final def getSubsamplingRate: Double = getOrDefault(subsamplingRate)

  /** @group setParam */
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Create a Strategy instance to use with the old API.
   * NOTE: The caller should set impurity and seed.
   */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity): OldStrategy = {
    super.getOldStrategy(categoricalFeatures, numClasses, oldAlgo, oldImpurity, getSubsamplingRate)
  }
}

/**
 * :: DeveloperApi ::
 * Parameters for Random Forest algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
@DeveloperApi
private[ml] trait RandomForestParams extends TreeEnsembleParams {

  /**
   * Number of trees to train (>= 1).
   * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).  SPARK-7130
   * (default = 20)
   * @group param
   */
  final val numTrees: IntParam = new IntParam(this, "numTrees", "Number of trees to train (>= 1)")

  /**
   * The number of features to consider for splits at each tree node.
   * Supported options:
   *  - "auto": Choose automatically for task:
   *            If numTrees == 1, set to "all."
   *            If numTrees > 1 (forest), set to "sqrt" for classification and
   *              to "onethird" for regression.
   *  - "all": use all features
   *  - "onethird": use 1/3 of the features
   *  - "sqrt": use sqrt(number of features)
   *  - "log2": use log2(number of features)
   * (default = "auto")
   *
   * These various settings are based on the following references:
   *  - log2: tested in Breiman (2001)
   *  - sqrt: recommended by Breiman manual for random forests
   *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
   *    package.
   * @see [[http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf  Breiman (2001)]]
   * @see [[http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf  Breiman manual for
   *     random forests]]
   *
   * @group param
   */
  final val featureSubsetStrategy: Param[String] = new Param[String](this, "featureSubsetStrategy",
    "The number of features to consider for splits at each tree node." +
      s" Supported options: ${RandomForestParams.supportedFeatureSubsetStrategies.mkString(", ")}")

  setDefault(numTrees -> 20, featureSubsetStrategy -> "auto")

  /** @group setParam */
  def setNumTrees(value: Int): this.type = {
    require(value >= 1, s"Random Forest numTrees parameter cannot be $value; it must be >= 1.")
    set(numTrees, value)
  }

  /** @group getParam */
  final def getNumTrees: Int = getOrDefault(numTrees)

  /** @group setParam */
  def setFeatureSubsetStrategy(value: String): this.type = {
    val strategyStr = value.toLowerCase
    require(RandomForestParams.supportedFeatureSubsetStrategies.contains(strategyStr),
      s"RandomForestParams was given unrecognized featureSubsetStrategy: $value. Supported" +
        s" options: ${RandomForestParams.supportedFeatureSubsetStrategies.mkString(", ")}")
    set(featureSubsetStrategy, strategyStr)
  }

  /** @group getParam */
  final def getFeatureSubsetStrategy: String = getOrDefault(featureSubsetStrategy)
}

private[ml] object RandomForestParams {
  // These options should be lowercase.
  final val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "onethird", "sqrt", "log2").map(_.toLowerCase)
}

/**
 * :: DeveloperApi ::
 * Parameters for Gradient-Boosted Tree algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
@DeveloperApi
private[ml] trait GBTParams extends TreeEnsembleParams with HasMaxIter {

  /**
   * Step size (a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each
   * estimator.
   * (default = 0.1)
   * @group param
   */
  final val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "Step size (a.k.a." +
    " learning rate) in interval (0, 1] for shrinking the contribution of each estimator")

  /* TODO: Add this doc when we add this param.  SPARK-7132
   * Threshold for stopping early when runWithValidation is used.
   * If the error rate on the validation input changes by less than the validationTol,
   * then learning will stop early (before [[numIterations]]).
   * This parameter is ignored when run is used.
   * (default = 1e-5)
   * @group param
   */
  // final val validationTol: DoubleParam = new DoubleParam(this, "validationTol", "")
  // validationTol -> 1e-5

  setDefault(maxIter -> 20, stepSize -> 0.1)

  /** @group setParam */
  def setMaxIter(value: Int): this.type = {
    require(value >= 1, s"Gradient Boosting maxIter parameter cannot be $value; it must be >= 1.")
    set(maxIter, value)
  }

  /** @group setParam */
  def setStepSize(value: Double): this.type = {
    require(value > 0.0 && value <= 1.0,
      s"GBT given invalid step size ($value).  Value should be in (0,1].")
    set(stepSize, value)
  }

  /** @group getParam */
  final def getStepSize: Double = getOrDefault(stepSize)

  /** (private[ml]) Create a BoostingStrategy instance to use with the old API. */
  private[ml] def getOldBoostingStrategy(
      categoricalFeatures: Map[Int, Int],
      oldAlgo: OldAlgo.Algo): OldBoostingStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 2, oldAlgo, OldVariance)
    // NOTE: The old API does not support "seed" so we ignore it.
    new OldBoostingStrategy(strategy, getOldLossType, getMaxIter, getStepSize)
  }

  /** Get old Gradient Boosting Loss type */
  private[ml] def getOldLossType: OldLoss
}
