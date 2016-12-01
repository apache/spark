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

import scala.util.Try

import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Entropy => OldEntropy, Gini => OldGini, Impurity => OldImpurity, Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, LogLoss => OldLogLoss, Loss => OldLoss, SquaredError => OldSquaredError}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * Parameters for Decision Tree-based algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait DecisionTreeParams extends PredictorParams
  with HasCheckpointInterval with HasSeed {

  /**
   * Maximum depth of the tree (>= 0).
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   * @group param
   */
  final val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "Maximum depth of the tree. (>= 0)" +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.",
      ParamValidators.gtEq(0))

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be >= 2 and >= number of categories in any categorical feature.
   * (default = 32)
   * @group param
   */
  final val maxBins: IntParam = new IntParam(this, "maxBins", "Max number of bins for" +
    " discretizing continuous features.  Must be >=2 and >= number of categories for any" +
    " categorical feature.", ParamValidators.gtEq(2))

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
    " Should be >= 1.", ParamValidators.gtEq(1))

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * Should be >= 0.0.
   * (default = 0.0)
   * @group param
   */
  final val minInfoGain: DoubleParam = new DoubleParam(this, "minInfoGain",
    "Minimum information gain for a split to be considered at a tree node.",
    ParamValidators.gtEq(0.0))

  /**
   * Maximum memory in MB allocated to histogram aggregation. If too small, then 1 node will be
   * split per iteration, and its aggregates may exceed this size.
   * (default = 256 MB)
   * @group expertParam
   */
  final val maxMemoryInMB: IntParam = new IntParam(this, "maxMemoryInMB",
    "Maximum memory in MB allocated to histogram aggregation.",
    ParamValidators.gtEq(0))

  /**
   * If false, the algorithm will pass trees to executors to match instances with nodes.
   * If true, the algorithm will cache node IDs for each instance.
   * Caching can speed up training of deeper trees. Users can set how often should the
   * cache be checkpointed or disable it by setting checkpointInterval.
   * (default = false)
   * @group expertParam
   */
  final val cacheNodeIds: BooleanParam = new BooleanParam(this, "cacheNodeIds", "If false, the" +
    " algorithm will pass trees to executors to match instances with nodes. If true, the" +
    " algorithm will cache node IDs for each instance. Caching can speed up training of deeper" +
    " trees.")

  setDefault(maxDepth -> 5, maxBins -> 32, minInstancesPerNode -> 1, minInfoGain -> 0.0,
    maxMemoryInMB -> 256, cacheNodeIds -> false, checkpointInterval -> 10)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group getParam */
  final def getMaxDepth: Int = $(maxDepth)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMaxBins(value: Int): this.type = set(maxBins, value)

  /** @group getParam */
  final def getMaxBins: Int = $(maxBins)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMinInstancesPerNode(value: Int): this.type = set(minInstancesPerNode, value)

  /** @group getParam */
  final def getMinInstancesPerNode: Int = $(minInstancesPerNode)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMinInfoGain(value: Double): this.type = set(minInfoGain, value)

  /** @group getParam */
  final def getMinInfoGain: Double = $(minInfoGain)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group expertSetParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMaxMemoryInMB(value: Int): this.type = set(maxMemoryInMB, value)

  /** @group expertGetParam */
  final def getMaxMemoryInMB: Int = $(maxMemoryInMB)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group expertSetParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setCacheNodeIds(value: Boolean): this.type = set(cacheNodeIds, value)

  /** @group expertGetParam */
  final def getCacheNodeIds: Boolean = $(cacheNodeIds)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setCheckpointInterval(value: Int): this.type = set(checkpointInterval, value)

  /** (private[ml]) Create a Strategy instance to use with the old API. */
  private[ml] def getOldStrategy(
      categoricalFeatures: Map[Int, Int],
      numClasses: Int,
      oldAlgo: OldAlgo.Algo,
      oldImpurity: OldImpurity,
      subsamplingRate: Double): OldStrategy = {
    val strategy = OldStrategy.defaultStrategy(oldAlgo)
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
    s" ${TreeClassifierParams.supportedImpurities.mkString(", ")}",
    (value: String) => TreeClassifierParams.supportedImpurities.contains(value.toLowerCase))

  setDefault(impurity -> "gini")

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setImpurity(value: String): this.type = set(impurity, value)

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase

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

private[ml] trait DecisionTreeClassifierParams
  extends DecisionTreeParams with TreeClassifierParams

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
    s" ${TreeRegressorParams.supportedImpurities.mkString(", ")}",
    (value: String) => TreeRegressorParams.supportedImpurities.contains(value.toLowerCase))

  setDefault(impurity -> "variance")

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setImpurity(value: String): this.type = set(impurity, value)

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase

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

private[ml] trait DecisionTreeRegressorParams extends DecisionTreeParams
  with TreeRegressorParams with HasVarianceCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    val newSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if (isDefined(varianceCol) && $(varianceCol).nonEmpty) {
      SchemaUtils.appendColumn(newSchema, $(varianceCol), DoubleType)
    } else {
      newSchema
    }
  }
}

/**
 * Parameters for Decision Tree-based ensemble algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait TreeEnsembleParams extends DecisionTreeParams {

  /**
   * Fraction of the training data used for learning each decision tree, in range (0, 1].
   * (default = 1.0)
   * @group param
   */
  final val subsamplingRate: DoubleParam = new DoubleParam(this, "subsamplingRate",
    "Fraction of the training data used for learning each decision tree, in range (0, 1].",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  setDefault(subsamplingRate -> 1.0)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setSubsamplingRate(value: Double): this.type = set(subsamplingRate, value)

  /** @group getParam */
  final def getSubsamplingRate: Double = $(subsamplingRate)

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
 * Parameters for Random Forest algorithms.
 */
private[ml] trait RandomForestParams extends TreeEnsembleParams {

  /**
   * Number of trees to train (>= 1).
   * If 1, then no bootstrapping is used.  If > 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).  SPARK-7130
   * (default = 20)
   *
   * Note: The reason that we cannot add this to both GBT and RF (i.e. in TreeEnsembleParams)
   * is the param `maxIter` controls how many trees a GBT has. The semantics in the algorithms
   * are a bit different.
   * @group param
   */
  final val numTrees: IntParam = new IntParam(this, "numTrees", "Number of trees to train (>= 1)",
    ParamValidators.gtEq(1))

  setDefault(numTrees -> 20)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setNumTrees(value: Int): this.type = set(numTrees, value)

  /** @group getParam */
  final def getNumTrees: Int = $(numTrees)

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
   *  - "n": when n is in the range (0, 1.0], use n * number of features. When n
   *         is in the range (1, number of features), use n features.
   * (default = "auto")
   *
   * These various settings are based on the following references:
   *  - log2: tested in Breiman (2001)
   *  - sqrt: recommended by Breiman manual for random forests
   *  - The defaults of sqrt (classification) and onethird (regression) match the R randomForest
   *    package.
   * @see <a href="http://www.stat.berkeley.edu/~breiman/randomforest2001.pdf">Breiman (2001)</a>
   * @see <a href="http://www.stat.berkeley.edu/~breiman/Using_random_forests_V3.1.pdf">
   * Breiman manual for random forests</a>
   *
   * @group param
   */
  final val featureSubsetStrategy: Param[String] = new Param[String](this, "featureSubsetStrategy",
    "The number of features to consider for splits at each tree node." +
      s" Supported options: ${RandomForestParams.supportedFeatureSubsetStrategies.mkString(", ")}" +
      s", (0.0-1.0], [1-n].",
    (value: String) =>
      RandomForestParams.supportedFeatureSubsetStrategies.contains(value.toLowerCase)
      || Try(value.toInt).filter(_ > 0).isSuccess
      || Try(value.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess)

  setDefault(featureSubsetStrategy -> "auto")

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setFeatureSubsetStrategy(value: String): this.type = set(featureSubsetStrategy, value)

  /** @group getParam */
  final def getFeatureSubsetStrategy: String = $(featureSubsetStrategy).toLowerCase
}

private[spark] object RandomForestParams {
  // These options should be lowercase.
  final val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "onethird", "sqrt", "log2").map(_.toLowerCase)
}

private[ml] trait RandomForestClassifierParams
  extends RandomForestParams with TreeClassifierParams

private[ml] trait RandomForestRegressorParams
  extends RandomForestParams with TreeRegressorParams

/**
 * Parameters for Gradient-Boosted Tree algorithms.
 *
 * Note: Marked as private and DeveloperApi since this may be made public in the future.
 */
private[ml] trait GBTParams extends TreeEnsembleParams with HasMaxIter {

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

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking
   * the contribution of each estimator.
   * (default = 0.1)
   * @group param
   */
  final val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "Step size " +
    "(a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator.",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  /** @group getParam */
  final def getStepSize: Double = $(stepSize)

  /**
   * @deprecated This method is deprecated and will be removed in 2.2.0.
   * @group setParam
   */
  @deprecated("This method is deprecated and will be removed in 2.2.0.", "2.1.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  setDefault(maxIter -> 20, stepSize -> 0.1)

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

private[ml] object GBTClassifierParams {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: logistic */
  final val supportedLossTypes: Array[String] = Array("logistic").map(_.toLowerCase)
}

private[ml] trait GBTClassifierParams extends GBTParams with TreeClassifierParams {

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "logistic"
   * (default = logistic)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifierParams.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTClassifierParams.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "logistic")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier was given bad loss type: $getLossType")
    }
  }
}

private[ml] object GBTRegressorParams {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: squared (L2), absolute (L1) */
  final val supportedLossTypes: Array[String] = Array("squared", "absolute").map(_.toLowerCase)
}

private[ml] trait GBTRegressorParams extends GBTParams with TreeRegressorParams {

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "squared" (L2) and "absolute" (L1)
   * (default = squared)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTRegressorParams.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTRegressorParams.supportedLossTypes.contains(value.toLowerCase))

  setDefault(lossType -> "squared")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "squared" => OldSquaredError
      case "absolute" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss type: $getLossType")
    }
  }
}
