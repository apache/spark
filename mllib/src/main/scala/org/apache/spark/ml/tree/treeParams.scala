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

import java.util.Locale

import scala.util.Try

import org.apache.spark.annotation.Since
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.classification.ProbabilisticClassifierParams
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo, BoostingStrategy => OldBoostingStrategy, Strategy => OldStrategy}
import org.apache.spark.mllib.tree.impurity.{Entropy => OldEntropy, Gini => OldGini, Impurity => OldImpurity, Variance => OldVariance}
import org.apache.spark.mllib.tree.loss.{AbsoluteError => OldAbsoluteError, ClassificationLoss => OldClassificationLoss, LogLoss => OldLogLoss, Loss => OldLoss, SquaredError => OldSquaredError}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * Parameters for Decision Tree-based algorithms.
 *
 * Note: Marked as private since this may be made public in the future.
 */
private[ml] trait DecisionTreeParams extends PredictorParams
  with HasCheckpointInterval with HasSeed with HasWeightCol {

  /**
   * Leaf indices column name.
   * Predicted leaf index of each instance in each tree by preorder.
   * (default = "")
   * @group param
   */
  @Since("3.0.0")
  final val leafCol: Param[String] =
    new Param[String](this, "leafCol", "Leaf indices column name. " +
      "Predicted leaf index of each instance in each tree by preorder")

  /**
   * Maximum depth of the tree (nonnegative).
   * E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.
   * (default = 5)
   * @group param
   */
  final val maxDepth: IntParam =
    new IntParam(this, "maxDepth", "Maximum depth of the tree. (Nonnegative)" +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes." +
      " Must be in range [0, 30].",
      ParamValidators.inRange(0, 30))

  /**
   * Maximum number of bins used for discretizing continuous features and for choosing how to split
   * on features at each node.  More bins give higher granularity.
   * Must be at least 2 and at least number of categories in any categorical feature.
   * (default = 32)
   * @group param
   */
  final val maxBins: IntParam = new IntParam(this, "maxBins", "Max number of bins for" +
    " discretizing continuous features.  Must be at least 2 and at least number of categories" +
    " for any categorical feature.", ParamValidators.gtEq(2))

  /**
   * Minimum number of instances each child must have after split.
   * If a split causes the left or right child to have fewer than minInstancesPerNode,
   * the split will be discarded as invalid.
   * Must be at least 1.
   * (default = 1)
   * @group param
   */
  final val minInstancesPerNode: IntParam = new IntParam(this, "minInstancesPerNode", "Minimum" +
    " number of instances each child must have after split.  If a split causes the left or right" +
    " child to have fewer than minInstancesPerNode, the split will be discarded as invalid." +
    " Must be at least 1.", ParamValidators.gtEq(1))

  /**
   * Minimum fraction of the weighted sample count that each child must have after split.
   * If a split causes the fraction of the total weight in the left or right child to be less than
   * minWeightFractionPerNode, the split will be discarded as invalid.
   * Should be in the interval [0.0, 0.5).
   * (default = 0.0)
   * @group param
   */
  final val minWeightFractionPerNode: DoubleParam = new DoubleParam(this,
    "minWeightFractionPerNode", "Minimum fraction of the weighted sample count that each child " +
    "must have after split. If a split causes the fraction of the total weight in the left or " +
    "right child to be less than minWeightFractionPerNode, the split will be discarded as " +
    "invalid. Should be in interval [0.0, 0.5)",
    ParamValidators.inRange(0.0, 0.5, lowerInclusive = true, upperInclusive = false))

  /**
   * Minimum information gain for a split to be considered at a tree node.
   * Should be at least 0.0.
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

  setDefault(leafCol -> "", maxDepth -> 5, maxBins -> 32, minInstancesPerNode -> 1,
    minWeightFractionPerNode -> 0.0, minInfoGain -> 0.0, maxMemoryInMB -> 256,
    cacheNodeIds -> false, checkpointInterval -> 10)

  /** @group setParam */
  @Since("3.0.0")
  final def setLeafCol(value: String): this.type = set(leafCol, value)

  /** @group getParam */
  @Since("3.0.0")
  final def getLeafCol: String = $(leafCol)

  /** @group getParam */
  final def getMaxDepth: Int = $(maxDepth)

  /** @group getParam */
  final def getMaxBins: Int = $(maxBins)

  /** @group getParam */
  final def getMinInstancesPerNode: Int = $(minInstancesPerNode)

  /** @group getParam */
  final def getMinWeightFractionPerNode: Double = $(minWeightFractionPerNode)

  /** @group getParam */
  final def getMinInfoGain: Double = $(minInfoGain)

  /** @group expertGetParam */
  final def getMaxMemoryInMB: Int = $(maxMemoryInMB)

  /** @group expertGetParam */
  final def getCacheNodeIds: Boolean = $(cacheNodeIds)

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
    strategy.minWeightFractionPerNode = getMinWeightFractionPerNode
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
   * This impurity type is used in DecisionTreeClassifier and RandomForestClassifier,
   * Supported: "entropy" and "gini".
   * (default = gini)
   * @group param
   */
  final val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${TreeClassifierParams.supportedImpurities.mkString(", ")}",
    (value: String) =>
      TreeClassifierParams.supportedImpurities.contains(value.toLowerCase(Locale.ROOT)))

  setDefault(impurity -> "gini")

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase(Locale.ROOT)

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
  final val supportedImpurities: Array[String] =
    Array("entropy", "gini").map(_.toLowerCase(Locale.ROOT))
}

private[ml] trait DecisionTreeClassifierParams
  extends DecisionTreeParams with TreeClassifierParams with ProbabilisticClassifierParams {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    var outputSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.appendColumn(outputSchema, $(leafCol), DoubleType)
    }
    outputSchema
  }
}

private[ml] trait HasVarianceImpurity extends Params {
  /**
   * Criterion used for information gain calculation (case-insensitive).
   * This impurity type is used in DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
   * and GBTClassifier (since GBTClassificationModel is internally composed of
   * DecisionTreeRegressionModels).
   * Supported: "variance".
   * (default = variance)
   * @group param
   */
  final val impurity: Param[String] = new Param[String](this, "impurity", "Criterion used for" +
    " information gain calculation (case-insensitive). Supported options:" +
    s" ${HasVarianceImpurity.supportedImpurities.mkString(", ")}",
    (value: String) =>
      HasVarianceImpurity.supportedImpurities.contains(value.toLowerCase(Locale.ROOT)))

  setDefault(impurity -> "variance")

  /** @group getParam */
  final def getImpurity: String = $(impurity).toLowerCase(Locale.ROOT)

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

private[ml] object HasVarianceImpurity {
  // These options should be lowercase.
  final val supportedImpurities: Array[String] =
    Array("variance").map(_.toLowerCase(Locale.ROOT))
}

/**
 * Parameters for Decision Tree-based regression algorithms.
 */
private[ml] trait TreeRegressorParams extends HasVarianceImpurity

private[ml] trait DecisionTreeRegressorParams extends DecisionTreeParams
  with TreeRegressorParams with HasVarianceCol {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    var outputSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if (isDefined(varianceCol) && $(varianceCol).nonEmpty) {
      outputSchema = SchemaUtils.appendColumn(outputSchema, $(varianceCol), DoubleType)
    }
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.appendColumn(outputSchema, $(leafCol), DoubleType)
    }
    outputSchema
  }
}

private[spark] object TreeEnsembleParams {
  // These options should be lowercase.
  final val supportedFeatureSubsetStrategies: Array[String] =
    Array("auto", "all", "onethird", "sqrt", "log2").map(_.toLowerCase(Locale.ROOT))
}

/**
 * Parameters for Decision Tree-based ensemble algorithms.
 *
 * Note: Marked as private since this may be made public in the future.
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

  /**
   * The number of features to consider for splits at each tree node.
   * Supported options:
   *  - "auto": Choose automatically for task:
   *            If numTrees == 1, set to "all."
   *            If numTrees greater than 1 (forest), set to "sqrt" for classification and
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
      s" Supported options: ${TreeEnsembleParams.supportedFeatureSubsetStrategies.mkString(", ")}" +
      s", (0.0-1.0], [1-n].",
    (value: String) =>
      TreeEnsembleParams.supportedFeatureSubsetStrategies.contains(
        value.toLowerCase(Locale.ROOT))
      || Try(value.toInt).filter(_ > 0).isSuccess
      || Try(value.toDouble).filter(_ > 0).filter(_ <= 1.0).isSuccess)

  /** @group getParam */
  final def getFeatureSubsetStrategy: String = $(featureSubsetStrategy).toLowerCase(Locale.ROOT)

  setDefault(subsamplingRate -> 1.0, featureSubsetStrategy -> "auto")
}

/**
 * Parameters for Decision Tree-based ensemble classification algorithms.
 */
private[ml] trait TreeEnsembleClassifierParams
  extends TreeEnsembleParams with ProbabilisticClassifierParams {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    var outputSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.appendColumn(outputSchema, $(leafCol), new VectorUDT)
    }
    outputSchema
  }
}

/**
 * Parameters for Decision Tree-based ensemble regression algorithms.
 */
private[ml] trait TreeEnsembleRegressorParams
  extends TreeEnsembleParams {

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    var outputSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)
    if ($(leafCol).nonEmpty) {
      outputSchema = SchemaUtils.appendColumn(outputSchema, $(leafCol), new VectorUDT)
    }
    outputSchema
  }
}

/**
 * Parameters for Random Forest algorithms.
 */
private[ml] trait RandomForestParams extends TreeEnsembleParams {

  /**
   * Number of trees to train (at least 1).
   * If 1, then no bootstrapping is used.  If greater than 1, then bootstrapping is done.
   * TODO: Change to always do bootstrapping (simpler).  SPARK-7130
   * (default = 20)
   *
   * Note: The reason that we cannot add this to both GBT and RF (i.e. in TreeEnsembleParams)
   * is the param `maxIter` controls how many trees a GBT has. The semantics in the algorithms
   * are a bit different.
   * @group param
   */
  final val numTrees: IntParam =
    new IntParam(this, "numTrees", "Number of trees to train (at least 1)",
    ParamValidators.gtEq(1))

  /** @group getParam */
  final def getNumTrees: Int = $(numTrees)

  /**
   * Whether bootstrap samples are used when building trees.
   * @group expertParam
   */
  @Since("3.0.0")
  final val bootstrap: BooleanParam = new BooleanParam(this, "bootstrap",
    "Whether bootstrap samples are used when building trees.")

  /** @group getParam */
  @Since("3.0.0")
  final def getBootstrap: Boolean = $(bootstrap)

  setDefault(numTrees -> 20, bootstrap -> true)
}

private[ml] trait RandomForestClassifierParams
  extends RandomForestParams with TreeEnsembleClassifierParams with TreeClassifierParams

private[ml] trait RandomForestRegressorParams
  extends RandomForestParams with TreeEnsembleRegressorParams with TreeRegressorParams

/**
 * Parameters for Gradient-Boosted Tree algorithms.
 *
 * Note: Marked as private since this may be made public in the future.
 */
private[ml] trait GBTParams extends TreeEnsembleParams with HasMaxIter with HasStepSize
  with HasValidationIndicatorCol {

  /**
   * Threshold for stopping early when fit with validation is used.
   * (This parameter is ignored when fit without validation is used.)
   * The decision to stop early is decided based on this logic:
   * If the current loss on the validation set is greater than 0.01, the diff
   * of validation error is compared to relative tolerance which is
   * validationTol * (current loss on the validation set).
   * If the current loss on the validation set is less than or equal to 0.01,
   * the diff of validation error is compared to absolute tolerance which is
   * validationTol * 0.01.
   * @group param
   * @see validationIndicatorCol
   */
  @Since("2.4.0")
  final val validationTol: DoubleParam = new DoubleParam(this, "validationTol",
    "Threshold for stopping early when fit with validation is used." +
    "If the error rate on the validation input changes by less than the validationTol," +
    "then learning will stop early (before `maxIter`)." +
    "This parameter is ignored when fit without validation is used.",
    ParamValidators.gtEq(0.0)
  )

  /** @group getParam */
  @Since("2.4.0")
  final def getValidationTol: Double = $(validationTol)

  /**
   * Param for Step size (a.k.a. learning rate) in interval (0, 1] for shrinking
   * the contribution of each estimator.
   * (default = 0.1)
   * @group param
   */
  final override val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "Step size " +
    "(a.k.a. learning rate) in interval (0, 1] for shrinking the contribution of each estimator.",
    ParamValidators.inRange(0, 1, lowerInclusive = false, upperInclusive = true))

  setDefault(maxIter -> 20, stepSize -> 0.1, validationTol -> 0.01, featureSubsetStrategy -> "all")

  /** (private[ml]) Create a BoostingStrategy instance to use with the old API. */
  private[ml] def getOldBoostingStrategy(
      categoricalFeatures: Map[Int, Int],
      oldAlgo: OldAlgo.Algo): OldBoostingStrategy = {
    val strategy = super.getOldStrategy(categoricalFeatures, numClasses = 2, oldAlgo, OldVariance)
    // NOTE: The old API does not support "seed" so we ignore it.
    new OldBoostingStrategy(strategy, getOldLossType, getMaxIter, getStepSize, getValidationTol)
  }

  /** Get old Gradient Boosting Loss type */
  private[ml] def getOldLossType: OldLoss
}

private[ml] object GBTClassifierParams {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: logistic */
  final val supportedLossTypes: Array[String] =
    Array("logistic").map(_.toLowerCase(Locale.ROOT))
}

private[ml] trait GBTClassifierParams
  extends GBTParams with TreeEnsembleClassifierParams with HasVarianceImpurity {

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "logistic"
   * (default = logistic)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifierParams.supportedLossTypes.mkString(", ")}",
    (value: String) =>
      GBTClassifierParams.supportedLossTypes.contains(value.toLowerCase(Locale.ROOT)))

  setDefault(lossType -> "logistic")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase(Locale.ROOT)

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldClassificationLoss = {
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
  final val supportedLossTypes: Array[String] =
    Array("squared", "absolute").map(_.toLowerCase(Locale.ROOT))
}

private[ml] trait GBTRegressorParams
  extends GBTParams with TreeEnsembleRegressorParams with TreeRegressorParams {

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "squared" (L2) and "absolute" (L1)
   * (default = squared)
   * @group param
   */
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTRegressorParams.supportedLossTypes.mkString(", ")}",
    (value: String) =>
      GBTRegressorParams.supportedLossTypes.contains(value.toLowerCase(Locale.ROOT)))

  setDefault(lossType -> "squared")

  /** @group getParam */
  def getLossType: String = $(lossType).toLowerCase(Locale.ROOT)

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    convertToOldLossType(getLossType)
  }

  private[ml] def convertToOldLossType(loss: String): OldLoss = {
    loss match {
      case "squared" => OldSquaredError
      case "absolute" => OldAbsoluteError
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTRegressorParams was given bad loss type: $getLossType")
    }
  }
}
