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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param.Param
import org.apache.spark.ml.param.shared.{
  HasAggregationDepth, HasElasticNetParam, HasFitIntercept, HasMaxBlockSizeInMB,
  HasMaxIter, HasRegParam, HasStandardization, HasThreshold, HasTol, HasWeightCol
}
import org.apache.spark.sql.types.{DataType, StructType}

private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
  with HasStandardization with HasWeightCol with HasThreshold with HasAggregationDepth
  with HasMaxBlockSizeInMB {

  import org.apache.spark.ml.classification.LogisticRegressionParams.supportedFamilyNames

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

  setDefault(regParam -> 0.0, elasticNetParam -> 0.0, maxIter -> 100, tol -> 1E-6,
    fitIntercept -> true, family -> "auto", standardization -> true, threshold -> 0.5,
    aggregationDepth -> 2, maxBlockSizeInMB -> 0.0)

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


object LogisticRegressionParams {

  private[classification] val supportedFamilyNames =
    Array("auto", "binomial", "multinomial").map(_.toLowerCase(Locale.ROOT))

}

private[classification] trait LogisticRegressionEstimatorParams extends LogisticRegressionParams {
  /**
   * Set the regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

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

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("1.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy at the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("1.4.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Sets the value of param [[family]].
   * Default is "auto".
   *
   * @group setParam
   */
  @Since("2.1.0")
  def setFamily(value: String): this.type = set(family, value)

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

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)

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

  /**
   * Sets the value of param [[maxBlockSizeInMB]].
   * Default is 0.0, then 1.0 MB will be chosen.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)
}
