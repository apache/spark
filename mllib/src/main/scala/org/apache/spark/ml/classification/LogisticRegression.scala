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

package org.apache.spark.ml.classification

import java.util.Locale

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, LBFGS => BreezeLBFGS, LBFGSB => BreezeLBFGSB, OWLQN => BreezeOWLQN}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator.LogisticAggregator
import org.apache.spark.ml.optim.loss.{L2Regularization, RDDLossFunction}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.VersionUtils

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
  with HasStandardization with HasWeightCol with HasThreshold with HasAggregationDepth {

  import org.apache.spark.ml.classification.LogisticRegression.supportedFamilyNames

  /**
   * Set threshold in binary classification, in range [0, 1].
   *
   * If the estimated probability of class label 1 is greater than threshold, then predict 1,
   * else 0. A high threshold encourages the model to predict 0 more often;
   * a low threshold encourages the model to predict 1 more often.
   *
   * Note: Calling this with threshold p is equivalent to calling `setThresholds(Array(1-p, p))`.
   *       When `setThreshold()` is called, any user-set value for `thresholds` will be cleared.
   *       If both `threshold` and `thresholds` are set in a ParamMap, then they must be
   *       equivalent.
   *
   * Default is 0.5.
   *
   * @group setParam
   */
  // TODO: Implement SPARK-11543?
  def setThreshold(value: Double): this.type = {
    if (isSet(thresholds)) clear(thresholds)
    set(threshold, value)
  }

  /**
   * Param for the name of family which is a description of the label distribution
   * to be used in the model.
   * Supported options:
   *  - "auto": Automatically select the family based on the number of classes:
   *            If numClasses == 1 || numClasses == 2, set to "binomial".
   *            Else, set to "multinomial"
   *  - "binomial": Binary logistic regression with pivoting.
   *  - "multinomial": Multinomial logistic (softmax) regression without pivoting.
   * Default is "auto".
   *
   * @group param
   */
  @Since("2.1.0")
  final val family: Param[String] = new Param(this, "family",
    "The name of family which is a description of the label distribution to be used in the " +
      s"model. Supported options: ${supportedFamilyNames.mkString(", ")}.",
    (value: String) => supportedFamilyNames.contains(value.toLowerCase(Locale.ROOT)))

  /** @group getParam */
  @Since("2.1.0")
  def getFamily: String = $(family)

  /**
   * Get threshold for binary classification.
   *
   * If `thresholds` is set with length 2 (i.e., binary classification),
   * this returns the equivalent threshold: {{{1 / (1 + thresholds(0) / thresholds(1))}}}.
   * Otherwise, returns `threshold` if set, or its default value if unset.
   *
   * @group getParam
   * @throws IllegalArgumentException if `thresholds` is set to an array of length other than 2.
   */
  override def getThreshold: Double = {
    checkThresholdConsistency()
    if (isSet(thresholds)) {
      val ts = $(thresholds)
      require(ts.length == 2, "Logistic Regression getThreshold only applies to" +
        " binary classification, but thresholds has length != 2.  thresholds: " + ts.mkString(","))
      1.0 / (1.0 + ts(0) / ts(1))
    } else {
      $(threshold)
    }
  }

  /**
   * Set thresholds in multiclass (or binary) classification to adjust the probability of
   * predicting each class. Array must have length equal to the number of classes,
   * with values greater than 0, excepting that at most one value may be 0.
   * The class with largest value p/t is predicted, where p is the original probability of that
   * class and t is the class's threshold.
   *
   * Note: When `setThresholds()` is called, any user-set value for `threshold` will be cleared.
   *       If both `threshold` and `thresholds` are set in a ParamMap, then they must be
   *       equivalent.
   *
   * @group setParam
   */
  def setThresholds(value: Array[Double]): this.type = {
    if (isSet(threshold)) clear(threshold)
    set(thresholds, value)
  }

  /**
   * Get thresholds for binary or multiclass classification.
   *
   * If `thresholds` is set, return its value.
   * Otherwise, if `threshold` is set, return the equivalent thresholds for binary
   * classification: (1-threshold, threshold).
   * If neither are set, throw an exception.
   *
   * @group getParam
   */
  override def getThresholds: Array[Double] = {
    checkThresholdConsistency()
    if (!isSet(thresholds) && isSet(threshold)) {
      val t = $(threshold)
      Array(1-t, t)
    } else {
      $(thresholds)
    }
  }

  /**
   * If `threshold` and `thresholds` are both set, ensures they are consistent.
   *
   * @throws IllegalArgumentException if `threshold` and `thresholds` are not equivalent
   */
  protected def checkThresholdConsistency(): Unit = {
    if (isSet(threshold) && isSet(thresholds)) {
      val ts = $(thresholds)
      require(ts.length == 2, "Logistic Regression found inconsistent values for threshold and" +
        s" thresholds.  Param threshold is set (${$(threshold)}), indicating binary" +
        s" classification, but Param thresholds is set with length ${ts.length}." +
        " Clear one Param value to fix this problem.")
      val t = 1.0 / (1.0 + ts(0) / ts(1))
      require(math.abs($(threshold) - t) < 1E-5, "Logistic Regression getThreshold found" +
        s" inconsistent values for threshold (${$(threshold)}) and thresholds (equivalent to $t)")
    }
  }

  /**
   * The lower bounds on coefficients if fitting under bound constrained optimization.
   * The bound matrix must be compatible with the shape (1, number of features) for binomial
   * regression, or (number of classes, number of features) for multinomial regression.
   * Otherwise, it throws exception.
   * Default is none.
   *
   * @group expertParam
   */
  @Since("2.2.0")
  val lowerBoundsOnCoefficients: Param[Matrix] = new Param(this, "lowerBoundsOnCoefficients",
    "The lower bounds on coefficients if fitting under bound constrained optimization.")

  /** @group expertGetParam */
  @Since("2.2.0")
  def getLowerBoundsOnCoefficients: Matrix = $(lowerBoundsOnCoefficients)

  /**
   * The upper bounds on coefficients if fitting under bound constrained optimization.
   * The bound matrix must be compatible with the shape (1, number of features) for binomial
   * regression, or (number of classes, number of features) for multinomial regression.
   * Otherwise, it throws exception.
   * Default is none.
   *
   * @group expertParam
   */
  @Since("2.2.0")
  val upperBoundsOnCoefficients: Param[Matrix] = new Param(this, "upperBoundsOnCoefficients",
    "The upper bounds on coefficients if fitting under bound constrained optimization.")

  /** @group expertGetParam */
  @Since("2.2.0")
  def getUpperBoundsOnCoefficients: Matrix = $(upperBoundsOnCoefficients)

  /**
   * The lower bounds on intercepts if fitting under bound constrained optimization.
   * The bounds vector size must be equal to 1 for binomial regression, or the number
   * of classes for multinomial regression. Otherwise, it throws exception.
   * Default is none.
   *
   * @group expertParam
   */
  @Since("2.2.0")
  val lowerBoundsOnIntercepts: Param[Vector] = new Param(this, "lowerBoundsOnIntercepts",
    "The lower bounds on intercepts if fitting under bound constrained optimization.")

  /** @group expertGetParam */
  @Since("2.2.0")
  def getLowerBoundsOnIntercepts: Vector = $(lowerBoundsOnIntercepts)

  /**
   * The upper bounds on intercepts if fitting under bound constrained optimization.
   * The bound vector size must be equal to 1 for binomial regression, or the number
   * of classes for multinomial regression. Otherwise, it throws exception.
   * Default is none.
   *
   * @group expertParam
   */
  @Since("2.2.0")
  val upperBoundsOnIntercepts: Param[Vector] = new Param(this, "upperBoundsOnIntercepts",
    "The upper bounds on intercepts if fitting under bound constrained optimization.")

  /** @group expertGetParam */
  @Since("2.2.0")
  def getUpperBoundsOnIntercepts: Vector = $(upperBoundsOnIntercepts)

  protected def usingBoundConstrainedOptimization: Boolean = {
    isSet(lowerBoundsOnCoefficients) || isSet(upperBoundsOnCoefficients) ||
      isSet(lowerBoundsOnIntercepts) || isSet(upperBoundsOnIntercepts)
  }

  override protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    checkThresholdConsistency()
    if (usingBoundConstrainedOptimization) {
      require($(elasticNetParam) == 0.0, "Fitting under bound constrained optimization only " +
        s"supports L2 regularization, but got elasticNetParam = $getElasticNetParam.")
    }
    if (!$(fitIntercept)) {
      require(!isSet(lowerBoundsOnIntercepts) && !isSet(upperBoundsOnIntercepts),
        "Please don't set bounds on intercepts if fitting without intercept.")
    }
    super.validateAndTransformSchema(schema, fitting, featuresDataType)
  }
}

/**
 * Logistic regression. Supports:
 *  - Multinomial logistic (softmax) regression.
 *  - Binomial logistic regression.
 *
 * This class supports fitting traditional logistic regression model by LBFGS/OWLQN and
 * bound (box) constrained logistic regression model by LBFGSB.
 */
@Since("1.2.0")
class LogisticRegression @Since("1.2.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("logreg"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty.
   * For alpha = 1, it is an L1 penalty.
   * For alpha in (0,1), the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   *
   * Note: Fitting under bound constrained optimization only supports L2 regularization,
   * so throws exception if this param is non-zero value.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Sets the value of param [[family]].
   * Default is "auto".
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setFamily(value: String): this.type = set(family, value)
  setDefault(family -> "auto")

  /**
   * Whether to standardize the training features before fitting the model.
   * The coefficients of models will be always returned on the original scale,
   * so it will be transparent for users. Note that with/without standardization,
   * the models should be always converged to the same solution when no regularization
   * is applied. In R's GLMNET package, the default behavior is true as well.
   * Default is true.
   *
   * @group setParam
   */
  @Since("1.5.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)
  setDefault(threshold -> 0.5)

  @Since("1.5.0")
  override def getThreshold: Double = super.getThreshold

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   *
   * @group setParam
   */
  @Since("1.6.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  @Since("1.5.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("1.5.0")
  override def getThresholds: Array[Double] = super.getThresholds

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   *
   * @group expertSetParam
   */
  @Since("2.1.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  /**
   * Set the lower bounds on coefficients if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setLowerBoundsOnCoefficients(value: Matrix): this.type = set(lowerBoundsOnCoefficients, value)

  /**
   * Set the upper bounds on coefficients if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setUpperBoundsOnCoefficients(value: Matrix): this.type = set(upperBoundsOnCoefficients, value)

  /**
   * Set the lower bounds on intercepts if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setLowerBoundsOnIntercepts(value: Vector): this.type = set(lowerBoundsOnIntercepts, value)

  /**
   * Set the upper bounds on intercepts if fitting under bound constrained optimization.
   *
   * @group expertSetParam
   */
  @Since("2.2.0")
  def setUpperBoundsOnIntercepts(value: Vector): this.type = set(upperBoundsOnIntercepts, value)

  private def assertBoundConstrainedOptimizationParamsValid(
      numCoefficientSets: Int,
      numFeatures: Int): Unit = {
    if (isSet(lowerBoundsOnCoefficients)) {
      require($(lowerBoundsOnCoefficients).numRows == numCoefficientSets &&
        $(lowerBoundsOnCoefficients).numCols == numFeatures,
        "The shape of LowerBoundsOnCoefficients must be compatible with (1, number of features) " +
          "for binomial regression, or (number of classes, number of features) for multinomial " +
          "regression, but found: " +
          s"(${getLowerBoundsOnCoefficients.numRows}, ${getLowerBoundsOnCoefficients.numCols}).")
    }
    if (isSet(upperBoundsOnCoefficients)) {
      require($(upperBoundsOnCoefficients).numRows == numCoefficientSets &&
        $(upperBoundsOnCoefficients).numCols == numFeatures,
        "The shape of upperBoundsOnCoefficients must be compatible with (1, number of features) " +
          "for binomial regression, or (number of classes, number of features) for multinomial " +
          "regression, but found: " +
          s"(${getUpperBoundsOnCoefficients.numRows}, ${getUpperBoundsOnCoefficients.numCols}).")
    }
    if (isSet(lowerBoundsOnIntercepts)) {
      require($(lowerBoundsOnIntercepts).size == numCoefficientSets, "The size of " +
        "lowerBoundsOnIntercepts must be equal to 1 for binomial regression, or the number of " +
        s"classes for multinomial regression, but found: ${getLowerBoundsOnIntercepts.size}.")
    }
    if (isSet(upperBoundsOnIntercepts)) {
      require($(upperBoundsOnIntercepts).size == numCoefficientSets, "The size of " +
        "upperBoundsOnIntercepts must be equal to 1 for binomial regression, or the number of " +
        s"classes for multinomial regression, but found: ${getUpperBoundsOnIntercepts.size}.")
    }
    if (isSet(lowerBoundsOnCoefficients) && isSet(upperBoundsOnCoefficients)) {
      require($(lowerBoundsOnCoefficients).toArray.zip($(upperBoundsOnCoefficients).toArray)
        .forall(x => x._1 <= x._2), "LowerBoundsOnCoefficients should always be " +
        "less than or equal to upperBoundsOnCoefficients, but found: " +
        s"lowerBoundsOnCoefficients = $getLowerBoundsOnCoefficients, " +
        s"upperBoundsOnCoefficients = $getUpperBoundsOnCoefficients.")
    }
    if (isSet(lowerBoundsOnIntercepts) && isSet(upperBoundsOnIntercepts)) {
      require($(lowerBoundsOnIntercepts).toArray.zip($(upperBoundsOnIntercepts).toArray)
        .forall(x => x._1 <= x._2), "LowerBoundsOnIntercepts should always be " +
        "less than or equal to upperBoundsOnIntercepts, but found: " +
        s"lowerBoundsOnIntercepts = $getLowerBoundsOnIntercepts, " +
        s"upperBoundsOnIntercepts = $getUpperBoundsOnIntercepts.")
    }
  }

  private var optInitialModel: Option[LogisticRegressionModel] = None

  private[spark] def setInitialModel(model: LogisticRegressionModel): this.type = {
    this.optInitialModel = Some(model)
    this
  }

  override protected[spark] def train(dataset: Dataset[_]): LogisticRegressionModel = {
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    train(dataset, handlePersistence)
  }

  protected[spark] def train(
      dataset: Dataset[_],
      handlePersistence: Boolean): LogisticRegressionModel = instrumented { instr =>
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, regParam, elasticNetParam, standardization, threshold,
      maxIter, tol, fitIntercept)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        (new MultivariateOnlineSummarizer, new MultiClassSummarizer)
      )(seqOp, combOp, $(aggregationDepth))
    }
    instr.logNumExamples(summarizer.count)
    instr.logNamedValue("lowestLabelWeight", labelSummarizer.histogram.min.toString)
    instr.logNamedValue("highestLabelWeight", labelSummarizer.histogram.max.toString)

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numFeatures = summarizer.mean.size
    val numFeaturesPlusIntercept = if (getFitIntercept) numFeatures + 1 else numFeatures

    val numClasses = MetadataUtils.getNumClasses(dataset.schema($(labelCol))) match {
      case Some(n: Int) =>
        require(n >= histogram.length, s"Specified number of classes $n was " +
          s"less than the number of unique labels ${histogram.length}.")
        n
      case None => histogram.length
    }

    val isMultinomial = getFamily.toLowerCase(Locale.ROOT) match {
      case "binomial" =>
        require(numClasses == 1 || numClasses == 2, s"Binomial family only supports 1 or 2 " +
        s"outcome classes but found $numClasses.")
        false
      case "multinomial" => true
      case "auto" => numClasses > 2
      case other => throw new IllegalArgumentException(s"Unsupported family: $other")
    }
    val numCoefficientSets = if (isMultinomial) numClasses else 1

    // Check params interaction is valid if fitting under bound constrained optimization.
    if (usingBoundConstrainedOptimization) {
      assertBoundConstrainedOptimizationParamsValid(numCoefficientSets, numFeatures)
    }

    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficientMatrix, interceptVector, objectiveHistory) = {
      if (numInvalid != 0) {
        val msg = s"Classification labels should be in [0 to ${numClasses - 1}]. " +
          s"Found $numInvalid invalid labels."
        instr.logError(msg)
        throw new SparkException(msg)
      }

      val isConstantLabel = histogram.count(_ != 0.0) == 1

      if ($(fitIntercept) && isConstantLabel && !usingBoundConstrainedOptimization) {
        instr.logWarning(s"All labels are the same value and fitIntercept=true, so the " +
          s"coefficients will be zeros. Training is not needed.")
        val constantLabelIndex = Vectors.dense(histogram).argmax
        val coefMatrix = new SparseMatrix(numCoefficientSets, numFeatures,
          new Array[Int](numCoefficientSets + 1), Array.empty[Int], Array.empty[Double],
          isTransposed = true).compressed
        val interceptVec = if (isMultinomial) {
          Vectors.sparse(numClasses, Seq((constantLabelIndex, Double.PositiveInfinity)))
        } else {
          Vectors.dense(if (numClasses == 2) Double.PositiveInfinity else Double.NegativeInfinity)
        }
        (coefMatrix, interceptVec, Array.empty[Double])
      } else {
        if (!$(fitIntercept) && isConstantLabel) {
          instr.logWarning(s"All labels belong to a single class and fitIntercept=false. It's a " +
            s"dangerous ground, so the algorithm may not converge.")
        }

        val featuresMean = summarizer.mean.toArray
        val featuresStd = summarizer.variance.toArray.map(math.sqrt)

        if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
          featuresStd(i) == 0.0 && featuresMean(i) != 0.0 }) {
          instr.logWarning("Fitting LogisticRegressionModel without intercept on dataset with " +
            "constant nonzero column, Spark MLlib outputs zero coefficients for constant " +
            "nonzero columns. This behavior is the same as R glmnet but different from LIBSVM.")
        }

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val bcFeaturesStd = instances.context.broadcast(featuresStd)
        val getAggregatorFunc = new LogisticAggregator(bcFeaturesStd, numClasses, $(fitIntercept),
          multinomial = isMultinomial)(_)
        val getFeaturesStd = (j: Int) => if (j >= 0 && j < numCoefficientSets * numFeatures) {
          featuresStd(j / numCoefficientSets)
        } else {
          0.0
        }

        val regularization = if (regParamL2 != 0.0) {
          val shouldApply = (idx: Int) => idx >= 0 && idx < numFeatures * numCoefficientSets
          Some(new L2Regularization(regParamL2, shouldApply,
            if ($(standardization)) None else Some(getFeaturesStd)))
        } else {
          None
        }

        val costFun = new RDDLossFunction(instances, getAggregatorFunc, regularization,
          $(aggregationDepth))

        val numCoeffsPlusIntercepts = numFeaturesPlusIntercept * numCoefficientSets

        val (lowerBounds, upperBounds): (Array[Double], Array[Double]) = {
          if (usingBoundConstrainedOptimization) {
            val lowerBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.NegativeInfinity)
            val upperBounds = Array.fill[Double](numCoeffsPlusIntercepts)(Double.PositiveInfinity)
            val isSetLowerBoundsOnCoefficients = isSet(lowerBoundsOnCoefficients)
            val isSetUpperBoundsOnCoefficients = isSet(upperBoundsOnCoefficients)
            val isSetLowerBoundsOnIntercepts = isSet(lowerBoundsOnIntercepts)
            val isSetUpperBoundsOnIntercepts = isSet(upperBoundsOnIntercepts)

            var i = 0
            while (i < numCoeffsPlusIntercepts) {
              val coefficientSetIndex = i % numCoefficientSets
              val featureIndex = i / numCoefficientSets
              if (featureIndex < numFeatures) {
                if (isSetLowerBoundsOnCoefficients) {
                  lowerBounds(i) = $(lowerBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
                if (isSetUpperBoundsOnCoefficients) {
                  upperBounds(i) = $(upperBoundsOnCoefficients)(
                    coefficientSetIndex, featureIndex) * featuresStd(featureIndex)
                }
              } else {
                if (isSetLowerBoundsOnIntercepts) {
                  lowerBounds(i) = $(lowerBoundsOnIntercepts)(coefficientSetIndex)
                }
                if (isSetUpperBoundsOnIntercepts) {
                  upperBounds(i) = $(upperBoundsOnIntercepts)(coefficientSetIndex)
                }
              }
              i += 1
            }
            (lowerBounds, upperBounds)
          } else {
            (null, null)
          }
        }

        val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
          if (lowerBounds != null && upperBounds != null) {
            new BreezeLBFGSB(
              BDV[Double](lowerBounds), BDV[Double](upperBounds), $(maxIter), 10, $(tol))
          } else {
            new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
          }
        } else {
          val standardizationParam = $(standardization)
          def regParamL1Fun = (index: Int) => {
            // Remove the L1 penalization on the intercept
            val isIntercept = $(fitIntercept) && index >= numFeatures * numCoefficientSets
            if (isIntercept) {
              0.0
            } else {
              if (standardizationParam) {
                regParamL1
              } else {
                val featureIndex = index / numCoefficientSets
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                if (featuresStd(featureIndex) != 0.0) {
                  regParamL1 / featuresStd(featureIndex)
                } else {
                  0.0
                }
              }
            }
          }
          new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
        }

        /*
          The coefficients are laid out in column major order during training. Here we initialize
          a column major matrix of initial coefficients.
         */
        val initialCoefWithInterceptMatrix =
          Matrices.zeros(numCoefficientSets, numFeaturesPlusIntercept)

        val initialModelIsValid = optInitialModel match {
          case Some(_initialModel) =>
            val providedCoefs = _initialModel.coefficientMatrix
            val modelIsValid = (providedCoefs.numRows == numCoefficientSets) &&
              (providedCoefs.numCols == numFeatures) &&
              (_initialModel.interceptVector.size == numCoefficientSets) &&
              (_initialModel.getFitIntercept == $(fitIntercept))
            if (!modelIsValid) {
              instr.logWarning(s"Initial coefficients will be ignored! Its dimensions " +
                s"(${providedCoefs.numRows}, ${providedCoefs.numCols}) did not match the " +
                s"expected size ($numCoefficientSets, $numFeatures)")
            }
            modelIsValid
          case None => false
        }

        if (initialModelIsValid) {
          val providedCoef = optInitialModel.get.coefficientMatrix
          providedCoef.foreachActive { (classIndex, featureIndex, value) =>
            // We need to scale the coefficients since they will be trained in the scaled space
            initialCoefWithInterceptMatrix.update(classIndex, featureIndex,
              value * featuresStd(featureIndex))
          }
          if ($(fitIntercept)) {
            optInitialModel.get.interceptVector.foreachActive { (classIndex, value) =>
              initialCoefWithInterceptMatrix.update(classIndex, numFeatures, value)
            }
          }
        } else if ($(fitIntercept) && isMultinomial) {
          /*
             For multinomial logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercepts such that
             it follows the distribution of the labels.
             {{{
               P(1) = \exp(b_1) / Z
               ...
               P(K) = \exp(b_K) / Z
               where Z = \sum_{k=1}^{K} \exp(b_k)
             }}}
             Since this doesn't have a unique solution, one of the solutions that satisfies the
             above equations is
             {{{
               \exp(b_k) = count_k * \exp(\lambda)
               b_k = \log(count_k) * \lambda
             }}}
             \lambda is a free parameter, so choose the phase \lambda such that the
             mean is centered. This yields
             {{{
               b_k = \log(count_k)
               b_k' = b_k - \mean(b_k)
             }}}
           */
          val rawIntercepts = histogram.map(math.log1p) // add 1 for smoothing (log1p(x) = log(1+x))
          val rawMean = rawIntercepts.sum / rawIntercepts.length
          rawIntercepts.indices.foreach { i =>
            initialCoefWithInterceptMatrix.update(i, numFeatures, rawIntercepts(i) - rawMean)
          }
        } else if ($(fitIntercept)) {
          /*
             For binary logistic regression, when we initialize the coefficients as zeros,
             it will converge faster if we initialize the intercept such that
             it follows the distribution of the labels.

             {{{
               P(0) = 1 / (1 + \exp(b)), and
               P(1) = \exp(b) / (1 + \exp(b))
             }}}, hence
             {{{
               b = \log{P(1) / P(0)} = \log{count_1 / count_0}
             }}}
           */
          initialCoefWithInterceptMatrix.update(0, numFeatures,
            math.log(histogram(1) / histogram(0)))
        }

        if (usingBoundConstrainedOptimization) {
          // Make sure all initial values locate in the corresponding bound.
          var i = 0
          while (i < numCoeffsPlusIntercepts) {
            val coefficientSetIndex = i % numCoefficientSets
            val featureIndex = i / numCoefficientSets
            if (initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) < lowerBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, lowerBounds(i))
            } else if (
              initialCoefWithInterceptMatrix(coefficientSetIndex, featureIndex) > upperBounds(i))
            {
              initialCoefWithInterceptMatrix.update(
                coefficientSetIndex, featureIndex, upperBounds(i))
            }
            i += 1
          }
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          new BDV[Double](initialCoefWithInterceptMatrix.toArray))

        /*
           Note that in Logistic Regression, the objective history (loss + regularization)
           is log-likelihood which is invariant under feature standardization. As a result,
           the objective history from optimizer is the same as the one in the original space.
         */
        val arrayBuilder = mutable.ArrayBuilder.make[Double]
        var state: optimizer.State = null
        while (states.hasNext) {
          state = states.next()
          arrayBuilder += state.adjustedValue
        }
        bcFeaturesStd.destroy(blocking = false)

        if (state == null) {
          val msg = s"${optimizer.getClass.getName} failed."
          instr.logError(msg)
          throw new SparkException(msg)
        }

        /*
           The coefficients are trained in the scaled space; we're converting them back to
           the original space.

           Additionally, since the coefficients were laid out in column major order during training
           to avoid extra computation, we convert them back to row major before passing them to the
           model.

           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val allCoefficients = state.x.toArray.clone()
        val allCoefMatrix = new DenseMatrix(numCoefficientSets, numFeaturesPlusIntercept,
          allCoefficients)
        val denseCoefficientMatrix = new DenseMatrix(numCoefficientSets, numFeatures,
          new Array[Double](numCoefficientSets * numFeatures), isTransposed = true)
        val interceptVec = if ($(fitIntercept) || !isMultinomial) {
          Vectors.zeros(numCoefficientSets)
        } else {
          Vectors.sparse(numCoefficientSets, Seq.empty)
        }
        // separate intercepts and coefficients from the combined matrix
        allCoefMatrix.foreachActive { (classIndex, featureIndex, value) =>
          val isIntercept = $(fitIntercept) && (featureIndex == numFeatures)
          if (!isIntercept && featuresStd(featureIndex) != 0.0) {
            denseCoefficientMatrix.update(classIndex, featureIndex,
              value / featuresStd(featureIndex))
          }
          if (isIntercept) interceptVec.toArray(classIndex) = value
        }

        if ($(regParam) == 0.0 && isMultinomial && !usingBoundConstrainedOptimization) {
          /*
            When no regularization is applied, the multinomial coefficients lack identifiability
            because we do not use a pivot class. We can add any constant value to the coefficients
            and get the same likelihood. So here, we choose the mean centered coefficients for
            reproducibility. This method follows the approach in glmnet, described here:

            Friedman, et al. "Regularization Paths for Generalized Linear Models via
              Coordinate Descent," https://core.ac.uk/download/files/153/6287975.pdf
           */
          val centers = Array.fill(numFeatures)(0.0)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            centers(j) += v
          }
          centers.transform(_ / numCoefficientSets)
          denseCoefficientMatrix.foreachActive { case (i, j, v) =>
            denseCoefficientMatrix.update(i, j, v - centers(j))
          }
        }

        // center the intercepts when using multinomial algorithm
        if ($(fitIntercept) && isMultinomial && !usingBoundConstrainedOptimization) {
          val interceptArray = interceptVec.toArray
          val interceptMean = interceptArray.sum / interceptArray.length
          (0 until interceptVec.size).foreach { i => interceptArray(i) -= interceptMean }
        }
        (denseCoefficientMatrix.compressed, interceptVec.compressed, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LogisticRegressionModel(uid, coefficientMatrix, interceptVector,
      numClasses, isMultinomial))

    val (summaryModel, probabilityColName, predictionColName) = model.findSummaryModel()
    val logRegSummary = if (numClasses <= 2) {
      new BinaryLogisticRegressionTrainingSummaryImpl(
        summaryModel.transform(dataset),
        probabilityColName,
        predictionColName,
        $(labelCol),
        $(featuresCol),
        objectiveHistory)
    } else {
      new LogisticRegressionTrainingSummaryImpl(
        summaryModel.transform(dataset),
        probabilityColName,
        predictionColName,
        $(labelCol),
        $(featuresCol),
        objectiveHistory)
    }
    model.setSummary(Some(logRegSummary))
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LogisticRegression = defaultCopy(extra)
}

@Since("1.6.0")
object LogisticRegression extends DefaultParamsReadable[LogisticRegression] {

  @Since("1.6.0")
  override def load(path: String): LogisticRegression = super.load(path)

  private[classification] val supportedFamilyNames =
    Array("auto", "binomial", "multinomial").map(_.toLowerCase(Locale.ROOT))
}

/**
 * Model produced by [[LogisticRegression]].
 */
@Since("1.4.0")
class LogisticRegressionModel private[spark] (
    @Since("1.4.0") override val uid: String,
    @Since("2.1.0") val coefficientMatrix: Matrix,
    @Since("2.1.0") val interceptVector: Vector,
    @Since("1.3.0") override val numClasses: Int,
    private val isMultinomial: Boolean)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
  with LogisticRegressionParams with MLWritable {

  require(coefficientMatrix.numRows == interceptVector.size, s"Dimension mismatch! Expected " +
    s"coefficientMatrix.numRows == interceptVector.size, but ${coefficientMatrix.numRows} != " +
    s"${interceptVector.size}")

  private[spark] def this(uid: String, coefficients: Vector, intercept: Double) =
    this(uid, new DenseMatrix(1, coefficients.size, coefficients.toArray, isTransposed = true),
      Vectors.dense(intercept), 2, isMultinomial = false)

  /**
   * A vector of model coefficients for "binomial" logistic regression. If this model was trained
   * using the "multinomial" family then an exception is thrown.
   *
   * @return Vector
   */
  @Since("2.0.0")
  def coefficients: Vector = if (isMultinomial) {
    throw new SparkException("Multinomial models contain a matrix of coefficients, use " +
      "coefficientMatrix instead.")
  } else {
    _coefficients
  }

  // convert to appropriate vector representation without replicating data
  private lazy val _coefficients: Vector = {
    require(coefficientMatrix.isTransposed,
      "LogisticRegressionModel coefficients should be row major for binomial model.")
    coefficientMatrix match {
      case dm: DenseMatrix => Vectors.dense(dm.values)
      case sm: SparseMatrix => Vectors.sparse(coefficientMatrix.numCols, sm.rowIndices, sm.values)
    }
  }

  /**
   * The model intercept for "binomial" logistic regression. If this model was fit with the
   * "multinomial" family then an exception is thrown.
   *
   * @return Double
   */
  @Since("1.3.0")
  def intercept: Double = if (isMultinomial) {
    throw new SparkException("Multinomial models contain a vector of intercepts, use " +
      "interceptVector instead.")
  } else {
    _intercept
  }

  private lazy val _intercept = interceptVector.toArray.head

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  @Since("1.5.0")
  override def getThreshold: Double = super.getThreshold

  @Since("1.5.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("1.5.0")
  override def getThresholds: Array[Double] = super.getThresholds

  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, _coefficients) + _intercept
  }

  /** Margin (rawPrediction) for each class label. */
  private val margins: Vector => Vector = (features) => {
    val m = interceptVector.toDense.copy
    BLAS.gemv(1.0, coefficientMatrix, features, 1.0, m)
    m
  }

  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  @Since("1.6.0")
  override val numFeatures: Int = coefficientMatrix.numCols

  private var trainingSummary: Option[LogisticRegressionTrainingSummary] = None

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `trainingSummary == None`.
   */
  @Since("1.5.0")
  def summary: LogisticRegressionTrainingSummary = trainingSummary.getOrElse {
    throw new SparkException("No training summary available for this LogisticRegressionModel")
  }

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `trainingSummary == None` or it is a multiclass model.
   */
  @Since("2.3.0")
  def binarySummary: BinaryLogisticRegressionTrainingSummary = summary match {
    case b: BinaryLogisticRegressionTrainingSummary => b
    case _ =>
      throw new RuntimeException("Cannot create a binary summary for a non-binary model" +
        s"(numClasses=${numClasses}), use summary instead.")
  }

  /**
   * If the probability and prediction columns are set, this method returns the current model,
   * otherwise it generates new columns for them and sets them as columns on a new copy of
   * the current model
   */
  private[classification] def findSummaryModel():
      (LogisticRegressionModel, String, String) = {
    val model = if ($(probabilityCol).isEmpty && $(predictionCol).isEmpty) {
      copy(ParamMap.empty)
        .setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
        .setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else if ($(probabilityCol).isEmpty) {
      copy(ParamMap.empty).setProbabilityCol("probability_" + java.util.UUID.randomUUID.toString)
    } else if ($(predictionCol).isEmpty) {
      copy(ParamMap.empty).setPredictionCol("prediction_" + java.util.UUID.randomUUID.toString)
    } else {
      this
    }
    (model, model.getProbabilityCol, model.getPredictionCol)
  }

  private[classification]
  def setSummary(summary: Option[LogisticRegressionTrainingSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /** Indicates whether a training summary exists for this model instance. */
  @Since("1.5.0")
  def hasSummary: Boolean = trainingSummary.isDefined

  /**
   * Evaluates the model on a test dataset.
   *
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_]): LogisticRegressionSummary = {
    // Handle possible missing or invalid prediction columns
    val (summaryModel, probabilityColName, predictionColName) = findSummaryModel()
    if (numClasses > 2) {
      new LogisticRegressionSummaryImpl(summaryModel.transform(dataset),
        probabilityColName, predictionColName, $(labelCol), $(featuresCol))
    } else {
      new BinaryLogisticRegressionSummaryImpl(summaryModel.transform(dataset),
        probabilityColName, predictionColName, $(labelCol), $(featuresCol))
    }
  }

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using `thresholds`.
   */
  override def predict(features: Vector): Double = if (isMultinomial) {
    super.predict(features)
  } else {
    // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
    if (score(features) > getThreshold) 1 else 0
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        if (isMultinomial) {
          val size = dv.size
          val values = dv.values

          // get the maximum margin
          val maxMarginIndex = rawPrediction.argmax
          val maxMargin = rawPrediction(maxMarginIndex)

          if (maxMargin == Double.PositiveInfinity) {
            var k = 0
            while (k < size) {
              values(k) = if (k == maxMarginIndex) 1.0 else 0.0
              k += 1
            }
          } else {
            val sum = {
              var temp = 0.0
              var k = 0
              while (k < numClasses) {
                values(k) = if (maxMargin > 0) {
                  math.exp(values(k) - maxMargin)
                } else {
                  math.exp(values(k))
                }
                temp += values(k)
                k += 1
              }
              temp
            }
            BLAS.scal(1 / sum, dv)
          }
          dv
        } else {
          var i = 0
          val size = dv.size
          while (i < size) {
            dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
            i += 1
          }
          dv
        }
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  override protected def predictRaw(features: Vector): Vector = {
    if (isMultinomial) {
      margins(features)
    } else {
      val m = margin(features)
      Vectors.dense(-m, m)
    }
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LogisticRegressionModel = {
    val newModel = copyValues(new LogisticRegressionModel(uid, coefficientMatrix, interceptVector,
      numClasses, isMultinomial), extra)
    newModel.setSummary(trainingSummary).setParent(parent)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
    if (isMultinomial) {
      super.raw2prediction(rawPrediction)
    } else {
      // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
      val t = getThreshold
      val rawThreshold = if (t == 0.0) {
        Double.NegativeInfinity
      } else if (t == 1.0) {
        Double.PositiveInfinity
      } else {
        math.log(t / (1.0 - t))
      }
      if (rawPrediction(1) > rawThreshold) 1 else 0
    }
  }

  override protected def probability2prediction(probability: Vector): Double = {
    if (isMultinomial) {
      super.probability2prediction(probability)
    } else {
      // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
      if (probability(1) > getThreshold) 1 else 0
    }
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * For [[LogisticRegressionModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   * This also does not save the [[parent]] currently.
   */
  @Since("1.6.0")
  override def write: MLWriter = new LogisticRegressionModel.LogisticRegressionModelWriter(this)

  override def toString: String = {
    s"LogisticRegressionModel: " +
    s"uid = ${super.toString}, numClasses = $numClasses, numFeatures = $numFeatures"
  }
}


@Since("1.6.0")
object LogisticRegressionModel extends MLReadable[LogisticRegressionModel] {

  @Since("1.6.0")
  override def read: MLReader[LogisticRegressionModel] = new LogisticRegressionModelReader

  @Since("1.6.0")
  override def load(path: String): LogisticRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[LogisticRegressionModel]] */
  private[LogisticRegressionModel]
  class LogisticRegressionModelWriter(instance: LogisticRegressionModel)
    extends MLWriter with Logging {

    private case class Data(
        numClasses: Int,
        numFeatures: Int,
        interceptVector: Vector,
        coefficientMatrix: Matrix,
        isMultinomial: Boolean)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: numClasses, numFeatures, intercept, coefficients
      val data = Data(instance.numClasses, instance.numFeatures, instance.interceptVector,
        instance.coefficientMatrix, instance.isMultinomial)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LogisticRegressionModelReader extends MLReader[LogisticRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LogisticRegressionModel].getName

    override def load(path: String): LogisticRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val (major, minor) = VersionUtils.majorMinorVersion(metadata.sparkVersion)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)

      val model = if (major.toInt < 2 || (major.toInt == 2 && minor.toInt == 0)) {
        // 2.0 and before
        val Row(numClasses: Int, numFeatures: Int, intercept: Double, coefficients: Vector) =
          MLUtils.convertVectorColumnsToML(data, "coefficients")
            .select("numClasses", "numFeatures", "intercept", "coefficients")
            .head()
        val coefficientMatrix =
          new DenseMatrix(1, coefficients.size, coefficients.toArray, isTransposed = true)
        val interceptVector = Vectors.dense(intercept)
        new LogisticRegressionModel(metadata.uid, coefficientMatrix,
          interceptVector, numClasses, isMultinomial = false)
      } else {
        // 2.1+
        val Row(numClasses: Int, numFeatures: Int, interceptVector: Vector,
        coefficientMatrix: Matrix, isMultinomial: Boolean) = data
          .select("numClasses", "numFeatures", "interceptVector", "coefficientMatrix",
            "isMultinomial").head()
        new LogisticRegressionModel(metadata.uid, coefficientMatrix, interceptVector,
          numClasses, isMultinomial)
      }

      metadata.getAndSetParams(model)
      model
    }
  }
}


/**
 * MultiClassSummarizer computes the number of distinct labels and corresponding counts,
 * and validates the data to see if the labels used for k class multi-label classification
 * are in the range of {0, 1, ..., k - 1} in an online fashion.
 *
 * Two MultilabelSummarizer can be merged together to have a statistical summary of the
 * corresponding joint dataset.
 */
private[ml] class MultiClassSummarizer extends Serializable {
  // The first element of value in distinctMap is the actually number of instances,
  // and the second element of value is sum of the weights.
  private val distinctMap = new mutable.HashMap[Int, (Long, Double)]
  private var totalInvalidCnt: Long = 0L

  /**
   * Add a new label into this MultilabelSummarizer, and update the distinct map.
   *
   * @param label The label for this data point.
   * @param weight The weight of this instances.
   * @return This MultilabelSummarizer
   */
  def add(label: Double, weight: Double = 1.0): this.type = {
    require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

    if (weight == 0.0) return this

    if (label - label.toInt != 0.0 || label < 0) {
      totalInvalidCnt += 1
      this
    }
    else {
      val (counts: Long, weightSum: Double) = distinctMap.getOrElse(label.toInt, (0L, 0.0))
      distinctMap.put(label.toInt, (counts + 1L, weightSum + weight))
      this
    }
  }

  /**
   * Merge another MultilabelSummarizer, and update the distinct map.
   * (Note that it will merge the smaller distinct map into the larger one using in-place
   * merging, so either `this` or `other` object will be modified and returned.)
   *
   * @param other The other MultilabelSummarizer to be merged.
   * @return Merged MultilabelSummarizer object.
   */
  def merge(other: MultiClassSummarizer): MultiClassSummarizer = {
    val (largeMap, smallMap) = if (this.distinctMap.size > other.distinctMap.size) {
      (this, other)
    } else {
      (other, this)
    }
    smallMap.distinctMap.foreach {
      case (key, value) =>
        val (counts: Long, weightSum: Double) = largeMap.distinctMap.getOrElse(key, (0L, 0.0))
        largeMap.distinctMap.put(key, (counts + value._1, weightSum + value._2))
    }
    largeMap.totalInvalidCnt += smallMap.totalInvalidCnt
    largeMap
  }

  /** @return The total invalid input counts. */
  def countInvalid: Long = totalInvalidCnt

  /** @return The number of distinct labels in the input dataset. */
  def numClasses: Int = if (distinctMap.isEmpty) 0 else distinctMap.keySet.max + 1

  /** @return The weightSum of each label in the input dataset. */
  def histogram: Array[Double] = {
    val result = Array.ofDim[Double](numClasses)
    var i = 0
    val len = result.length
    while (i < len) {
      result(i) = distinctMap.getOrElse(i, (0L, 0.0))._2
      i += 1
    }
    result
  }
}

/**
 * :: Experimental ::
 * Abstraction for logistic regression results for a given model.
 *
 * Currently, the summary ignores the instance weights.
 */
@Experimental
sealed trait LogisticRegressionSummary extends Serializable {

  /**
   * Dataframe output by the model's `transform` method.
   */
  @Since("1.5.0")
  def predictions: DataFrame

  /** Field in "predictions" which gives the probability of each class as a vector. */
  @Since("1.5.0")
  def probabilityCol: String

  /** Field in "predictions" which gives the prediction of each class. */
  @Since("2.3.0")
  def predictionCol: String

  /** Field in "predictions" which gives the true label of each instance (if available). */
  @Since("1.5.0")
  def labelCol: String

  /** Field in "predictions" which gives the features of each instance as a vector. */
  @Since("1.6.0")
  def featuresCol: String

  @transient private val multiclassMetrics = {
    new MulticlassMetrics(
      predictions.select(
        col(predictionCol),
        col(labelCol).cast(DoubleType))
        .rdd.map { case Row(prediction: Double, label: Double) => (prediction, label) })
  }

  /**
   * Returns the sequence of labels in ascending order. This order matches the order used
   * in metrics which are specified as arrays over labels, e.g., truePositiveRateByLabel.
   *
   * Note: In most cases, it will be values {0.0, 1.0, ..., numClasses-1}, However, if the
   * training set is missing a label, then all of the arrays over labels
   * (e.g., from truePositiveRateByLabel) will be of length numClasses-1 instead of the
   * expected numClasses.
   */
  @Since("2.3.0")
  def labels: Array[Double] = multiclassMetrics.labels

  /** Returns true positive rate for each label (category). */
  @Since("2.3.0")
  def truePositiveRateByLabel: Array[Double] = recallByLabel

  /** Returns false positive rate for each label (category). */
  @Since("2.3.0")
  def falsePositiveRateByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.falsePositiveRate(label))
  }

  /** Returns precision for each label (category). */
  @Since("2.3.0")
  def precisionByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.precision(label))
  }

  /** Returns recall for each label (category). */
  @Since("2.3.0")
  def recallByLabel: Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.recall(label))
  }

  /** Returns f-measure for each label (category). */
  @Since("2.3.0")
  def fMeasureByLabel(beta: Double): Array[Double] = {
    multiclassMetrics.labels.map(label => multiclassMetrics.fMeasure(label, beta))
  }

  /** Returns f1-measure for each label (category). */
  @Since("2.3.0")
  def fMeasureByLabel: Array[Double] = fMeasureByLabel(1.0)

  /**
   * Returns accuracy.
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("2.3.0")
  def accuracy: Double = multiclassMetrics.accuracy

  /**
   * Returns weighted true positive rate.
   * (equals to precision, recall and f-measure)
   */
  @Since("2.3.0")
  def weightedTruePositiveRate: Double = weightedRecall

  /** Returns weighted false positive rate. */
  @Since("2.3.0")
  def weightedFalsePositiveRate: Double = multiclassMetrics.weightedFalsePositiveRate

  /**
   * Returns weighted averaged recall.
   * (equals to precision, recall and f-measure)
   */
  @Since("2.3.0")
  def weightedRecall: Double = multiclassMetrics.weightedRecall

  /** Returns weighted averaged precision. */
  @Since("2.3.0")
  def weightedPrecision: Double = multiclassMetrics.weightedPrecision

  /** Returns weighted averaged f-measure. */
  @Since("2.3.0")
  def weightedFMeasure(beta: Double): Double = multiclassMetrics.weightedFMeasure(beta)

  /** Returns weighted averaged f1-measure. */
  @Since("2.3.0")
  def weightedFMeasure: Double = multiclassMetrics.weightedFMeasure(1.0)

  /**
   * Convenient method for casting to binary logistic regression summary.
   * This method will throw an Exception if the summary is not a binary summary.
   */
  @Since("2.3.0")
  def asBinary: BinaryLogisticRegressionSummary = this match {
    case b: BinaryLogisticRegressionSummary => b
    case _ =>
      throw new RuntimeException("Cannot cast to a binary summary.")
  }
}

/**
 * :: Experimental ::
 * Abstraction for multiclass logistic regression training results.
 * Currently, the training summary ignores the training weights except
 * for the objective trace.
 */
@Experimental
sealed trait LogisticRegressionTrainingSummary extends LogisticRegressionSummary {

  /** objective function (scaled loss + regularization) at each iteration. */
  @Since("1.5.0")
  def objectiveHistory: Array[Double]

  /** Number of training iterations. */
  @Since("1.5.0")
  def totalIterations: Int = objectiveHistory.length

}

/**
 * :: Experimental ::
 * Abstraction for binary logistic regression results for a given model.
 *
 * Currently, the summary ignores the instance weights.
 */
@Experimental
sealed trait BinaryLogisticRegressionSummary extends LogisticRegressionSummary {

  private val sparkSession = predictions.sparkSession
  import sparkSession.implicits._

  // TODO: Allow the user to vary the number of bins using a setBins method in
  // BinaryClassificationMetrics. For now the default is set to 100.
  @transient private val binaryMetrics = new BinaryClassificationMetrics(
    predictions.select(col(probabilityCol), col(labelCol).cast(DoubleType)).rdd.map {
      case Row(score: Vector, label: Double) => (score(1), label)
    }, 100
  )

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is a Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * See http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is a Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val fMeasureByThreshold: DataFrame = {
    binaryMetrics.fMeasureByThreshold().toDF("threshold", "F-Measure")
  }

  /**
   * Returns a dataframe with two fields (threshold, precision) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the precision.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val precisionByThreshold: DataFrame = {
    binaryMetrics.precisionByThreshold().toDF("threshold", "precision")
  }

  /**
   * Returns a dataframe with two fields (threshold, recall) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the recall.
   *
   * @note This ignores instance weights (setting all to 1.0) from `LogisticRegression.weightCol`.
   * This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
  }
}

/**
 * :: Experimental ::
 * Abstraction for binary logistic regression training results.
 * Currently, the training summary ignores the training weights except
 * for the objective trace.
 */
@Experimental
sealed trait BinaryLogisticRegressionTrainingSummary extends BinaryLogisticRegressionSummary
  with LogisticRegressionTrainingSummary

/**
 * Multiclass logistic regression training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class LogisticRegressionTrainingSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    override val objectiveHistory: Array[Double])
  extends LogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
  with LogisticRegressionTrainingSummary

/**
 * Multiclass logistic regression results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 */
private class LogisticRegressionSummaryImpl(
    @transient override val predictions: DataFrame,
    override val probabilityCol: String,
    override val predictionCol: String,
    override val labelCol: String,
    override val featuresCol: String)
  extends LogisticRegressionSummary

/**
 * Binary logistic regression training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class BinaryLogisticRegressionTrainingSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String,
    override val objectiveHistory: Array[Double])
  extends BinaryLogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
  with BinaryLogisticRegressionTrainingSummary

/**
 * Binary logistic regression results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param predictionCol field in "predictions" which gives the prediction of
 *                      each class as a double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 */
private class BinaryLogisticRegressionSummaryImpl(
    predictions: DataFrame,
    probabilityCol: String,
    predictionCol: String,
    labelCol: String,
    featuresCol: String)
  extends LogisticRegressionSummaryImpl(
    predictions, probabilityCol, predictionCol, labelCol, featuresCol)
  with BinaryLogisticRegressionSummary
