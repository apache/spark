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

package org.apache.spark.ml.regression

import java.util.Locale

import breeze.stats.{distributions => dist}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.{Instance, OffsetInstance}
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.ml.optim._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, StructType}

/**
 * Params for Generalized Linear Regression.
 */
private[regression] trait GeneralizedLinearRegressionBase extends PredictorParams
  with HasFitIntercept with HasMaxIter with HasTol with HasRegParam with HasWeightCol
  with HasSolver with Logging {

  import GeneralizedLinearRegression._

  /**
   * Param for the name of family which is a description of the error distribution
   * to be used in the model.
   * Supported options: "gaussian", "binomial", "poisson", "gamma" and "tweedie".
   * Default is "gaussian".
   *
   * @group param
   */
  @Since("2.0.0")
  final val family: Param[String] = new Param(this, "family",
    "The name of family which is a description of the error distribution to be used in the " +
      s"model. Supported options: ${supportedFamilyNames.mkString(", ")}.",
    (value: String) => supportedFamilyNames.contains(value.toLowerCase(Locale.ROOT)))

  /** @group getParam */
  @Since("2.0.0")
  def getFamily: String = $(family)

  /**
   * Param for the power in the variance function of the Tweedie distribution which provides
   * the relationship between the variance and mean of the distribution.
   * Only applicable to the Tweedie family.
   * (see <a href="https://en.wikipedia.org/wiki/Tweedie_distribution">
   * Tweedie Distribution (Wikipedia)</a>)
   * Supported values: 0 and [1, Inf).
   * Note that variance power 0, 1, or 2 corresponds to the Gaussian, Poisson or Gamma
   * family, respectively.
   *
   * @group param
   */
  @Since("2.2.0")
  final val variancePower: DoubleParam = new DoubleParam(this, "variancePower",
    "The power in the variance function of the Tweedie distribution which characterizes " +
    "the relationship between the variance and mean of the distribution. " +
    "Only applicable to the Tweedie family. Supported values: 0 and [1, Inf).",
    (x: Double) => x >= 1.0 || x == 0.0)

  /** @group getParam */
  @Since("2.2.0")
  def getVariancePower: Double = $(variancePower)

  /**
   * Param for the name of link function which provides the relationship
   * between the linear predictor and the mean of the distribution function.
   * Supported options: "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt".
   * This is used only when family is not "tweedie". The link function for the "tweedie" family
   * must be specified through [[linkPower]].
   *
   * @group param
   */
  @Since("2.0.0")
  final val link: Param[String] = new Param(this, "link", "The name of link function " +
    "which provides the relationship between the linear predictor and the mean of the " +
    s"distribution function. Supported options: ${supportedLinkNames.mkString(", ")}",
    (value: String) => supportedLinkNames.contains(value.toLowerCase(Locale.ROOT)))

  /** @group getParam */
  @Since("2.0.0")
  def getLink: String = $(link)

  /**
   * Param for the index in the power link function. Only applicable to the Tweedie family.
   * Note that link power 0, 1, -1 or 0.5 corresponds to the Log, Identity, Inverse or Sqrt
   * link, respectively.
   * When not set, this value defaults to 1 - [[variancePower]], which matches the R "statmod"
   * package.
   *
   * @group param
   */
  @Since("2.2.0")
  final val linkPower: DoubleParam = new DoubleParam(this, "linkPower",
    "The index in the power link function. Only applicable to the Tweedie family.")

  /** @group getParam */
  @Since("2.2.0")
  def getLinkPower: Double = $(linkPower)

  /**
   * Param for link prediction (linear predictor) column name.
   * Default is not set, which means we do not output link prediction.
   *
   * @group param
   */
  @Since("2.0.0")
  final val linkPredictionCol: Param[String] = new Param[String](this, "linkPredictionCol",
    "link prediction (linear predictor) column name")

  /** @group getParam */
  @Since("2.0.0")
  def getLinkPredictionCol: String = $(linkPredictionCol)

  /**
   * Param for offset column name. If this is not set or empty, we treat all instance offsets
   * as 0.0. The feature specified as offset has a constant coefficient of 1.0.
   *
   * @group param
   */
  @Since("2.3.0")
  final val offsetCol: Param[String] = new Param[String](this, "offsetCol", "The offset " +
    "column name. If this is not set or empty, we treat all instance offsets as 0.0")

  /** @group getParam */
  @Since("2.3.0")
  def getOffsetCol: String = $(offsetCol)

  /** Checks whether weight column is set and nonempty. */
  private[regression] def hasWeightCol: Boolean =
    isSet(weightCol) && $(weightCol).nonEmpty

  /** Checks whether offset column is set and nonempty. */
  private[regression] def hasOffsetCol: Boolean =
    isSet(offsetCol) && $(offsetCol).nonEmpty

  /** Checks whether we should output link prediction. */
  private[regression] def hasLinkPredictionCol: Boolean = {
    isDefined(linkPredictionCol) && $(linkPredictionCol).nonEmpty
  }

  /**
   * The solver algorithm for optimization.
   * Supported options: "irls" (iteratively reweighted least squares).
   * Default: "irls"
   *
   * @group param
   */
  @Since("2.0.0")
  final override val solver: Param[String] = new Param[String](this, "solver",
    "The solver algorithm for optimization. Supported options: " +
      s"${supportedSolvers.mkString(", ")}. (Default irls)",
    ParamValidators.inArray[String](supportedSolvers))

  @Since("2.0.0")
  override def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    if ($(family).toLowerCase(Locale.ROOT) == "tweedie") {
      if (isSet(link)) {
        logWarning("When family is tweedie, use param linkPower to specify link function. " +
          "Setting param link will take no effect.")
      }
    } else {
      if (isSet(variancePower)) {
        logWarning("When family is not tweedie, setting param variancePower will take no effect.")
      }
      if (isSet(linkPower)) {
        logWarning("When family is not tweedie, use param link to specify link function. " +
          "Setting param linkPower will take no effect.")
      }
      if (isSet(link)) {
        require(supportedFamilyAndLinkPairs.contains(
          Family.fromParams(this) -> Link.fromParams(this)),
          s"Generalized Linear Regression with ${$(family)} family " +
            s"does not support ${$(link)} link function.")
      }
    }

    val newSchema = super.validateAndTransformSchema(schema, fitting, featuresDataType)

    if (hasOffsetCol) {
      SchemaUtils.checkNumericType(schema, $(offsetCol))
    }

    if (hasLinkPredictionCol) {
      SchemaUtils.appendColumn(newSchema, $(linkPredictionCol), DoubleType)
    } else {
      newSchema
    }
  }
}

/**
 * :: Experimental ::
 *
 * Fit a Generalized Linear Model
 * (see <a href="https://en.wikipedia.org/wiki/Generalized_linear_model">
 * Generalized linear model (Wikipedia)</a>)
 * specified by giving a symbolic description of the linear
 * predictor (link function) and a description of the error distribution (family).
 * It supports "gaussian", "binomial", "poisson", "gamma" and "tweedie" as family.
 * Valid link functions for each family is listed below. The first link function of each family
 * is the default one.
 *  - "gaussian" : "identity", "log", "inverse"
 *  - "binomial" : "logit", "probit", "cloglog"
 *  - "poisson"  : "log", "identity", "sqrt"
 *  - "gamma"    : "inverse", "identity", "log"
 *  - "tweedie"  : power link function specified through "linkPower". The default link power in
 *  the tweedie family is 1 - variancePower.
 */
@Experimental
@Since("2.0.0")
class GeneralizedLinearRegression @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Regressor[Vector, GeneralizedLinearRegression, GeneralizedLinearRegressionModel]
  with GeneralizedLinearRegressionBase with DefaultParamsWritable with Logging {

  import GeneralizedLinearRegression._

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("glm"))

  /**
   * Sets the value of param [[family]].
   * Default is "gaussian".
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setFamily(value: String): this.type = set(family, value)
  setDefault(family -> Gaussian.name)

  /**
   * Sets the value of param [[variancePower]].
   * Used only when family is "tweedie".
   * Default is 0.0, which corresponds to the "gaussian" family.
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setVariancePower(value: Double): this.type = set(variancePower, value)
  setDefault(variancePower -> 0.0)

  /**
   * Sets the value of param [[linkPower]].
   * Used only when family is "tweedie".
   *
   * @group setParam
   */
  @Since("2.2.0")
  def setLinkPower(value: Double): this.type = set(linkPower, value)

  /**
   * Sets the value of param [[link]].
   * Used only when family is not "tweedie".
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setLink(value: String): this.type = set(link, value)

  /**
   * Sets if we should fit the intercept.
   * Default is true.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Sets the maximum number of iterations (applicable for solver "irls").
   * Default is 25.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 25)

  /**
   * Sets the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Sets the regularization parameter for L2 regularization.
   * The regularization term is
   * <blockquote>
   *    $$
   *    0.5 * regParam * L2norm(coefficients)^2
   *    $$
   * </blockquote>
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is not set, so all instances have weight one.
   * In the Binomial family, weights correspond to number of trials and should be integer.
   * Non-integer weights are rounded to integer in AIC calculation.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  /**
   * Sets the value of param [[offsetCol]].
   * If this is not set or empty, we treat all instance offsets as 0.0.
   * Default is not set, so all instances have offset 0.0.
   *
   * @group setParam
   */
  @Since("2.3.0")
  def setOffsetCol(value: String): this.type = set(offsetCol, value)

  /**
   * Sets the solver algorithm used for optimization.
   * Currently only supports "irls" which is also the default solver.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setSolver(value: String): this.type = set(solver, value)
  setDefault(solver -> IRLS)

  /**
   * Sets the link prediction (linear predictor) column name.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setLinkPredictionCol(value: String): this.type = set(linkPredictionCol, value)

  override protected def train(
      dataset: Dataset[_]): GeneralizedLinearRegressionModel = instrumented { instr =>
    val familyAndLink = FamilyAndLink(this)

    val numFeatures = dataset.select(col($(featuresCol))).first().getAs[Vector](0).size
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, weightCol, offsetCol, predictionCol,
      linkPredictionCol, family, solver, fitIntercept, link, maxIter, regParam, tol)
    instr.logNumFeatures(numFeatures)

    if (numFeatures > WeightedLeastSquares.MAX_NUM_FEATURES) {
      val msg = "Currently, GeneralizedLinearRegression only supports number of features" +
        s" <= ${WeightedLeastSquares.MAX_NUM_FEATURES}. Found $numFeatures in the input dataset."
      throw new SparkException(msg)
    }

    require(numFeatures > 0 || $(fitIntercept),
      "GeneralizedLinearRegression was given data with 0 features, and with Param fitIntercept " +
        "set to false. To fit a model with 0 features, fitIntercept must be set to true." )

    val w = if (!hasWeightCol) lit(1.0) else col($(weightCol))
    val offset = if (!hasOffsetCol) lit(0.0) else col($(offsetCol)).cast(DoubleType)

    val model = if (familyAndLink.family == Gaussian && familyAndLink.link == Identity) {
      // TODO: Make standardizeFeatures and standardizeLabel configurable.
      val instances: RDD[Instance] =
        dataset.select(col($(labelCol)), w, offset, col($(featuresCol))).rdd.map {
          case Row(label: Double, weight: Double, offset: Double, features: Vector) =>
            Instance(label - offset, weight, features)
        }
      val optimizer = new WeightedLeastSquares($(fitIntercept), $(regParam), elasticNetParam = 0.0,
        standardizeFeatures = true, standardizeLabel = true)
      val wlsModel = optimizer.fit(instances, instr = OptionalInstrumentation.create(instr))
      val model = copyValues(
        new GeneralizedLinearRegressionModel(uid, wlsModel.coefficients, wlsModel.intercept)
          .setParent(this))
      val trainingSummary = new GeneralizedLinearRegressionTrainingSummary(dataset, model,
        wlsModel.diagInvAtWA.toArray, 1, getSolver)
      model.setSummary(Some(trainingSummary))
    } else {
      val instances: RDD[OffsetInstance] =
        dataset.select(col($(labelCol)), w, offset, col($(featuresCol))).rdd.map {
          case Row(label: Double, weight: Double, offset: Double, features: Vector) =>
            OffsetInstance(label, weight, offset, features)
        }
      // Fit Generalized Linear Model by iteratively reweighted least squares (IRLS).
      val initialModel = familyAndLink.initialize(instances, $(fitIntercept), $(regParam),
        instr = OptionalInstrumentation.create(instr))
      val optimizer = new IterativelyReweightedLeastSquares(initialModel,
        familyAndLink.reweightFunc, $(fitIntercept), $(regParam), $(maxIter), $(tol))
      val irlsModel = optimizer.fit(instances, instr = OptionalInstrumentation.create(instr))
      val model = copyValues(
        new GeneralizedLinearRegressionModel(uid, irlsModel.coefficients, irlsModel.intercept)
          .setParent(this))
      val trainingSummary = new GeneralizedLinearRegressionTrainingSummary(dataset, model,
        irlsModel.diagInvAtWA.toArray, irlsModel.numIterations, getSolver)
      model.setSummary(Some(trainingSummary))
    }

    model
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegression = defaultCopy(extra)
}

@Since("2.0.0")
object GeneralizedLinearRegression extends DefaultParamsReadable[GeneralizedLinearRegression] {

  @Since("2.0.0")
  override def load(path: String): GeneralizedLinearRegression = super.load(path)

  /**
   * Set of family (except for tweedie) and link pairs that GeneralizedLinearRegression supports.
   * The link function of the Tweedie family is specified through param linkPower.
   */
  private[regression] lazy val supportedFamilyAndLinkPairs = Set(
    Gaussian -> Identity, Gaussian -> Log, Gaussian -> Inverse,
    Binomial -> Logit, Binomial -> Probit, Binomial -> CLogLog,
    Poisson -> Log, Poisson -> Identity, Poisson -> Sqrt,
    Gamma -> Inverse, Gamma -> Identity, Gamma -> Log
  )

  /** String name for "irls" (iteratively reweighted least squares) solver. */
  private[regression] val IRLS = "irls"

  /** Set of solvers that GeneralizedLinearRegression supports. */
  private[regression] val supportedSolvers = Array(IRLS)

  /** Set of family names that GeneralizedLinearRegression supports. */
  private[regression] lazy val supportedFamilyNames =
    supportedFamilyAndLinkPairs.map(_._1.name).toArray :+ "tweedie"

  /** Set of link names that GeneralizedLinearRegression supports. */
  private[regression] lazy val supportedLinkNames =
    supportedFamilyAndLinkPairs.map(_._2.name).toArray

  private[regression] val epsilon: Double = 1E-16

  private[regression] def ylogy(y: Double, mu: Double): Double = {
    if (y == 0) 0.0 else y * math.log(y / mu)
  }

  /**
   * Wrapper of family and link combination used in the model.
   */
  private[regression] class FamilyAndLink(val family: Family, val link: Link) extends Serializable {

    /** Linear predictor based on given mu. */
    def predict(mu: Double): Double = link.link(family.project(mu))

    /** Fitted value based on linear predictor eta. */
    def fitted(eta: Double): Double = family.project(link.unlink(eta))

    /**
     * Get the initial guess model for [[IterativelyReweightedLeastSquares]].
     */
    def initialize(
        instances: RDD[OffsetInstance],
        fitIntercept: Boolean,
        regParam: Double,
        instr: OptionalInstrumentation = OptionalInstrumentation.create(
          classOf[GeneralizedLinearRegression])
      ): WeightedLeastSquaresModel = {
      val newInstances = instances.map { instance =>
        val mu = family.initialize(instance.label, instance.weight)
        val eta = predict(mu) - instance.offset
        Instance(eta, instance.weight, instance.features)
      }
      // TODO: Make standardizeFeatures and standardizeLabel configurable.
      val initialModel = new WeightedLeastSquares(fitIntercept, regParam, elasticNetParam = 0.0,
        standardizeFeatures = true, standardizeLabel = true)
        .fit(newInstances, instr)
      initialModel
    }

    /**
     * The reweight function used to update working labels and weights
     * at each iteration of [[IterativelyReweightedLeastSquares]].
     */
    def reweightFunc(
        instance: OffsetInstance, model: WeightedLeastSquaresModel): (Double, Double) = {
      val eta = model.predict(instance.features) + instance.offset
      val mu = fitted(eta)
      val newLabel = eta - instance.offset + (instance.label - mu) * link.deriv(mu)
      val newWeight = instance.weight / (math.pow(this.link.deriv(mu), 2.0) * family.variance(mu))
      (newLabel, newWeight)
    }
  }

  private[regression] object FamilyAndLink {

    /**
     * Constructs the FamilyAndLink object from a parameter map
     */
    def apply(params: GeneralizedLinearRegressionBase): FamilyAndLink = {
      val familyObj = Family.fromParams(params)
      val linkObj =
        if ((params.getFamily.toLowerCase(Locale.ROOT) != "tweedie" &&
              params.isSet(params.link)) ||
            (params.getFamily.toLowerCase(Locale.ROOT) == "tweedie" &&
              params.isSet(params.linkPower))) {
          Link.fromParams(params)
        } else {
          familyObj.defaultLink
        }
      new FamilyAndLink(familyObj, linkObj)
    }
  }

  /**
   * A description of the error distribution to be used in the model.
   *
   * @param name the name of the family.
   */
  private[regression] abstract class Family(val name: String) extends Serializable {

    /** The default link instance of this family. */
    val defaultLink: Link

    /** Initialize the starting value for mu. */
    def initialize(y: Double, weight: Double): Double

    /** The variance of the endogenous variable's mean, given the value mu. */
    def variance(mu: Double): Double

    /** Deviance of (y, mu) pair. */
    def deviance(y: Double, mu: Double, weight: Double): Double

    /**
     * Akaike Information Criterion (AIC) value of the family for a given dataset.
     *
     * @param predictions an RDD of (y, mu, weight) of instances in evaluation dataset
     * @param deviance the deviance for the fitted model in evaluation dataset
     * @param numInstances number of instances in evaluation dataset
     * @param weightSum weights sum of instances in evaluation dataset
     */
    def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double

    /** Trim the fitted value so that it will be in valid range. */
    def project(mu: Double): Double = mu
  }

  private[regression] object Family {

    /**
     * Gets the [[Family]] object based on param family and variancePower.
     * If param family is set with "gaussian", "binomial", "poisson" or "gamma",
     * return the corresponding object directly; otherwise, construct a Tweedie object
     * according to variancePower.
     *
     * @param params the parameter map containing family name and variance power
     */
    def fromParams(params: GeneralizedLinearRegressionBase): Family = {
      params.getFamily.toLowerCase(Locale.ROOT) match {
        case Gaussian.name => Gaussian
        case Binomial.name => Binomial
        case Poisson.name => Poisson
        case Gamma.name => Gamma
        case "tweedie" =>
          params.getVariancePower match {
            case 0.0 => Gaussian
            case 1.0 => Poisson
            case 2.0 => Gamma
            case others => new Tweedie(others)
          }
      }
    }
  }

  /**
   * Tweedie exponential family distribution.
   * This includes the special cases of Gaussian, Poisson and Gamma.
   */
  private[regression] class Tweedie(val variancePower: Double)
    extends Family("tweedie") {

    override val defaultLink: Link = new Power(1.0 - variancePower)

    override def initialize(y: Double, weight: Double): Double = {
      if (variancePower >= 1.0 && variancePower < 2.0) {
        require(y >= 0.0, s"The response variable of $name($variancePower) family " +
          s"should be non-negative, but got $y")
      } else if (variancePower >= 2.0) {
        require(y > 0.0, s"The response variable of $name($variancePower) family " +
          s"should be positive, but got $y")
      }
      if (y == 0) Tweedie.delta else y
    }

    override def variance(mu: Double): Double = math.pow(mu, variancePower)

    private def yp(y: Double, mu: Double, p: Double): Double = {
      if (p == 0) {
        math.log(y / mu)
      } else {
        (math.pow(y, p) - math.pow(mu, p)) / p
      }
    }

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      // Force y >= delta for Poisson or compound Poisson
      val y1 = if (variancePower >= 1.0 && variancePower < 2.0) {
        math.max(y, Tweedie.delta)
      } else {
        y
      }
      2.0 * weight *
        (y * yp(y1, mu, 1.0 - variancePower) - yp(y, mu, 2.0 - variancePower))
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      /*
       This depends on the density of the Tweedie distribution.
       Only implemented for Gaussian, Poisson and Gamma at this point.
      */
      throw new UnsupportedOperationException("No AIC available for the tweedie family")
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  private[regression] object Tweedie{

    /** Constant used in initialization and deviance to avoid numerical issues. */
    val delta: Double = 0.1
  }

  /**
   * Gaussian exponential family distribution.
   * The default link for the Gaussian family is the identity link.
   */
  private[regression] object Gaussian extends Tweedie(0.0) {

    override val name: String = "gaussian"

    override val defaultLink: Link = Identity

    override def initialize(y: Double, weight: Double): Double = y

    override def variance(mu: Double): Double = 1.0

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      weight * (y - mu) * (y - mu)
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      val wt = predictions.map(x => math.log(x._3)).sum()
      numInstances * (math.log(deviance / numInstances * 2.0 * math.Pi) + 1.0) + 2.0 - wt
    }

    override def project(mu: Double): Double = {
      if (mu.isNegInfinity) {
        Double.MinValue
      } else if (mu.isPosInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
   * Binomial exponential family distribution.
   * The default link for the Binomial family is the logit link.
   */
  private[regression] object Binomial extends Family("binomial") {

    val defaultLink: Link = Logit

    override def initialize(y: Double, weight: Double): Double = {
      val mu = (weight * y + 0.5) / (weight + 1.0)
      require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
        s"should be in range (0, 1), but got $mu")
      mu
    }

    override def variance(mu: Double): Double = mu * (1.0 - mu)

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (ylogy(y, mu) + ylogy(1.0 - y, 1.0 - mu))
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        // weights for Binomial distribution correspond to number of trials
        val wt = math.round(weight).toInt
        if (wt == 0) {
          0.0
        } else {
          dist.Binomial(wt, mu).logProbabilityOf(math.round(y * weight).toInt)
        }
      }.sum()
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu > 1.0 - epsilon) {
        1.0 - epsilon
      } else {
        mu
      }
    }
  }

  /**
   * Poisson exponential family distribution.
   * The default link for the Poisson family is the log link.
   */
  private[regression] object Poisson extends Tweedie(1.0) {

    override val name: String = "poisson"

    override val defaultLink: Link = Log

    override def initialize(y: Double, weight: Double): Double = {
      require(y >= 0.0, "The response variable of Poisson family " +
        s"should be non-negative, but got $y")
      /*
        Force Poisson mean > 0 to avoid numerical instability in IRLS.
        R uses y + delta for initialization. See poisson()$initialize.
       */
      math.max(y, Tweedie.delta)
    }

    override def variance(mu: Double): Double = mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (ylogy(y, mu) - (y - mu))
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        weight * dist.Poisson(mu).logProbabilityOf(y.toInt)
      }.sum()
    }
  }

  /**
   * Gamma exponential family distribution.
   * The default link for the Gamma family is the inverse link.
   */
  private[regression] object Gamma extends Tweedie(2.0) {

    override val name: String = "gamma"

    override val defaultLink: Link = Inverse

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Gamma family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu * mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      -2.0 * weight * (math.log(y / mu) - (y - mu)/mu)
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      val disp = deviance / weightSum
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        weight * dist.Gamma(1.0 / disp, mu * disp).logPdf(y)
      }.sum() + 2.0
    }
  }

  /**
   * A description of the link function to be used in the model.
   * The link function provides the relationship between the linear predictor
   * and the mean of the distribution function.
   *
   * @param name the name of link function.
   */
  private[regression] abstract class Link(val name: String) extends Serializable {

    /** The link function. */
    def link(mu: Double): Double

    /** Derivative of the link function. */
    def deriv(mu: Double): Double

    /** The inverse link function. */
    def unlink(eta: Double): Double
  }

  private[regression] object Link {

    /**
     * Gets the [[Link]] object based on param family, link and linkPower.
     * If param family is set with "tweedie", return or construct link function object
     * according to linkPower; otherwise, return link function object according to link.
     *
     * @param params the parameter map containing family, link and linkPower
     */
    def fromParams(params: GeneralizedLinearRegressionBase): Link = {
      if (params.getFamily.toLowerCase(Locale.ROOT) == "tweedie") {
        params.getLinkPower match {
          case 0.0 => Log
          case 1.0 => Identity
          case -1.0 => Inverse
          case 0.5 => Sqrt
          case others => new Power(others)
        }
      } else {
        params.getLink.toLowerCase(Locale.ROOT) match {
          case Identity.name => Identity
          case Logit.name => Logit
          case Log.name => Log
          case Inverse.name => Inverse
          case Probit.name => Probit
          case CLogLog.name => CLogLog
          case Sqrt.name => Sqrt
        }
      }
    }
  }

  /** Power link function class */
  private[regression] class Power(val linkPower: Double)
    extends Link("power") {

    override def link(mu: Double): Double = {
      if (linkPower == 0.0) {
        math.log(mu)
      } else {
        math.pow(mu, linkPower)
      }
    }

    override def deriv(mu: Double): Double = {
      if (linkPower == 0.0) {
        1.0 / mu
      } else {
        linkPower * math.pow(mu, linkPower - 1.0)
      }
    }

    override def unlink(eta: Double): Double = {
      if (linkPower == 0.0) {
        math.exp(eta)
      } else {
        math.pow(eta, 1.0 / linkPower)
      }
    }
  }

  private[regression] object Identity extends Power(1.0) {

    override val name: String = "identity"

    override def link(mu: Double): Double = mu

    override def deriv(mu: Double): Double = 1.0

    override def unlink(eta: Double): Double = eta
  }

  private[regression] object Logit extends Link("logit") {

    override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

    override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
  }

  private[regression] object Log extends Power(0.0) {

    override val name: String = "log"

    override def link(mu: Double): Double = math.log(mu)

    override def deriv(mu: Double): Double = 1.0 / mu

    override def unlink(eta: Double): Double = math.exp(eta)
  }

  private[regression] object Inverse extends Power(-1.0) {

    override val name: String = "inverse"

    override def link(mu: Double): Double = 1.0 / mu

    override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

    override def unlink(eta: Double): Double = 1.0 / eta
  }

  private[regression] object Probit extends Link("probit") {

    override def link(mu: Double): Double = dist.Gaussian(0.0, 1.0).inverseCdf(mu)

    override def deriv(mu: Double): Double = {
      1.0 / dist.Gaussian(0.0, 1.0).pdf(dist.Gaussian(0.0, 1.0).inverseCdf(mu))
    }

    override def unlink(eta: Double): Double = dist.Gaussian(0.0, 1.0).cdf(eta)
  }

  private[regression] object CLogLog extends Link("cloglog") {

    override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

    override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
  }

  private[regression] object Sqrt extends Power(0.5) {

    override val name: String = "sqrt"

    override def link(mu: Double): Double = math.sqrt(mu)

    override def deriv(mu: Double): Double = 1.0 / (2.0 * math.sqrt(mu))

    override def unlink(eta: Double): Double = eta * eta
  }
}

/**
 * :: Experimental ::
 * Model produced by [[GeneralizedLinearRegression]].
 */
@Experimental
@Since("2.0.0")
class GeneralizedLinearRegressionModel private[ml] (
    @Since("2.0.0") override val uid: String,
    @Since("2.0.0") val coefficients: Vector,
    @Since("2.0.0") val intercept: Double)
  extends RegressionModel[Vector, GeneralizedLinearRegressionModel]
  with GeneralizedLinearRegressionBase with MLWritable {

  /**
   * Sets the link prediction (linear predictor) column name.
   *
   * @group setParam
   */
  @Since("2.0.0")
  def setLinkPredictionCol(value: String): this.type = set(linkPredictionCol, value)

  import GeneralizedLinearRegression._

  private lazy val familyAndLink = FamilyAndLink(this)

  override def predict(features: Vector): Double = {
    predict(features, 0.0)
  }

  /**
   * Calculates the predicted value when offset is set.
   */
  private def predict(features: Vector, offset: Double): Double = {
    val eta = predictLink(features, offset)
    familyAndLink.fitted(eta)
  }

  /**
   * Calculates the link prediction (linear predictor) of the given instance.
   */
  private def predictLink(features: Vector, offset: Double): Double = {
    BLAS.dot(features, coefficients) + intercept + offset
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema)
    transformImpl(dataset)
  }

  override protected def transformImpl(dataset: Dataset[_]): DataFrame = {
    val predictUDF = udf { (features: Vector, offset: Double) => predict(features, offset) }
    val predictLinkUDF = udf { (features: Vector, offset: Double) => predictLink(features, offset) }

    val offset = if (!hasOffsetCol) lit(0.0) else col($(offsetCol)).cast(DoubleType)
    var output = dataset
    if ($(predictionCol).nonEmpty) {
      output = output.withColumn($(predictionCol), predictUDF(col($(featuresCol)), offset))
    }
    if (hasLinkPredictionCol) {
      output = output.withColumn($(linkPredictionCol), predictLinkUDF(col($(featuresCol)), offset))
    }
    output.toDF()
  }

  private var trainingSummary: Option[GeneralizedLinearRegressionTrainingSummary] = None

  /**
   * Gets R-like summary of model on training set. An exception is
   * thrown if there is no summary available.
   */
  @Since("2.0.0")
  def summary: GeneralizedLinearRegressionTrainingSummary = trainingSummary.getOrElse {
    throw new SparkException(
      "No training summary available for this GeneralizedLinearRegressionModel")
  }

  /**
   * Indicates if [[summary]] is available.
   */
  @Since("2.0.0")
  def hasSummary: Boolean = trainingSummary.nonEmpty

  private[regression]
  def setSummary(summary: Option[GeneralizedLinearRegressionTrainingSummary]): this.type = {
    this.trainingSummary = summary
    this
  }

  /**
   * Evaluate the model on the given dataset, returning a summary of the results.
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_]): GeneralizedLinearRegressionSummary = {
    new GeneralizedLinearRegressionSummary(dataset, this)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegressionModel = {
    val copied = copyValues(new GeneralizedLinearRegressionModel(uid, coefficients, intercept),
      extra)
    copied.setSummary(trainingSummary).setParent(parent)
  }

  /**
   * Returns a [[org.apache.spark.ml.util.MLWriter]] instance for this ML instance.
   *
   * For [[GeneralizedLinearRegressionModel]], this does NOT currently save the
   * training [[summary]]. An option to save [[summary]] may be added in the future.
   *
   */
  @Since("2.0.0")
  override def write: MLWriter =
    new GeneralizedLinearRegressionModel.GeneralizedLinearRegressionModelWriter(this)

  override val numFeatures: Int = coefficients.size
}

@Since("2.0.0")
object GeneralizedLinearRegressionModel extends MLReadable[GeneralizedLinearRegressionModel] {

  @Since("2.0.0")
  override def read: MLReader[GeneralizedLinearRegressionModel] =
    new GeneralizedLinearRegressionModelReader

  @Since("2.0.0")
  override def load(path: String): GeneralizedLinearRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[GeneralizedLinearRegressionModel]] */
  private[GeneralizedLinearRegressionModel]
  class GeneralizedLinearRegressionModelWriter(instance: GeneralizedLinearRegressionModel)
    extends MLWriter with Logging {

    private case class Data(intercept: Double, coefficients: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: intercept, coefficients
      val data = Data(instance.intercept, instance.coefficients)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class GeneralizedLinearRegressionModelReader
    extends MLReader[GeneralizedLinearRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GeneralizedLinearRegressionModel].getName

    override def load(path: String): GeneralizedLinearRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("intercept", "coefficients").head()
      val intercept = data.getDouble(0)
      val coefficients = data.getAs[Vector](1)

      val model = new GeneralizedLinearRegressionModel(metadata.uid, coefficients, intercept)

      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * :: Experimental ::
 * Summary of [[GeneralizedLinearRegression]] model and predictions.
 *
 * @param dataset Dataset to be summarized.
 * @param origModel Model to be summarized.  This is copied to create an internal
 *                  model which cannot be modified from outside.
 */
@Since("2.0.0")
@Experimental
class GeneralizedLinearRegressionSummary private[regression] (
    dataset: Dataset[_],
    origModel: GeneralizedLinearRegressionModel) extends Serializable {

  import GeneralizedLinearRegression._

  /**
   * Field in "predictions" which gives the predicted value of each instance.
   * This is set to a new column name if the original model's `predictionCol` is not set.
   */
  @Since("2.0.0")
  val predictionCol: String = {
    if (origModel.isDefined(origModel.predictionCol) && origModel.getPredictionCol.nonEmpty) {
      origModel.getPredictionCol
    } else {
      "prediction_" + java.util.UUID.randomUUID.toString
    }
  }

  /**
   * Private copy of model to ensure Params are not modified outside this class.
   * Coefficients is not a deep copy, but that is acceptable.
   *
   * @note [[predictionCol]] must be set correctly before the value of [[model]] is set,
   * and [[model]] must be set before [[predictions]] is set!
   */
  protected val model: GeneralizedLinearRegressionModel =
    origModel.copy(ParamMap.empty).setPredictionCol(predictionCol)

  /**
   * Predictions output by the model's `transform` method.
   */
  @Since("2.0.0") @transient val predictions: DataFrame = model.transform(dataset)

  private[regression] lazy val familyLink: FamilyAndLink = FamilyAndLink(model)

  private[regression] lazy val family: Family = familyLink.family

  private[regression] lazy val link: Link = familyLink.link

  /** Number of instances in DataFrame predictions. */
  @Since("2.2.0")
  lazy val numInstances: Long = predictions.count()


  /**
   * Name of features. If the name cannot be retrieved from attributes,
   * set default names to feature column name with numbered suffix "_0", "_1", and so on.
   */
  private[ml] lazy val featureNames: Array[String] = {
    val featureAttrs = AttributeGroup.fromStructField(
      dataset.schema(model.getFeaturesCol)).attributes
    if (featureAttrs.isDefined) {
      featureAttrs.get.map(_.name.get)
    } else {
      Array.tabulate[String](origModel.numFeatures)((x: Int) => model.getFeaturesCol + "_" + x)
    }
  }

  /** The numeric rank of the fitted linear model. */
  @Since("2.0.0")
  lazy val rank: Long = if (model.getFitIntercept) {
    model.coefficients.size + 1
  } else {
    model.coefficients.size
  }

  /** Degrees of freedom. */
  @Since("2.0.0")
  lazy val degreesOfFreedom: Long = numInstances - rank

  /** The residual degrees of freedom. */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedom: Long = degreesOfFreedom

  /** The residual degrees of freedom for the null model. */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedomNull: Long = {
    if (model.getFitIntercept) numInstances - 1 else numInstances
  }

  private def label: Column = col(model.getLabelCol).cast(DoubleType)

  private def prediction: Column = col(predictionCol)

  private def weight: Column = {
    if (!model.hasWeightCol) lit(1.0) else col(model.getWeightCol)
  }

  private def offset: Column = {
    if (!model.hasOffsetCol) lit(0.0) else col(model.getOffsetCol).cast(DoubleType)
  }

  private[regression] lazy val devianceResiduals: DataFrame = {
    val drUDF = udf { (y: Double, mu: Double, weight: Double) =>
      val r = math.sqrt(math.max(family.deviance(y, mu, weight), 0.0))
      if (y > mu) r else -1.0 * r
    }
    predictions.select(
      drUDF(label, prediction, weight).as("devianceResiduals"))
  }

  private[regression] lazy val pearsonResiduals: DataFrame = {
    val prUDF = udf { mu: Double => family.variance(mu) }
    predictions.select(label.minus(prediction)
      .multiply(sqrt(weight)).divide(sqrt(prUDF(prediction))).as("pearsonResiduals"))
  }

  private[regression] lazy val workingResiduals: DataFrame = {
    val wrUDF = udf { (y: Double, mu: Double) => (y - mu) * link.deriv(mu) }
    predictions.select(wrUDF(label, prediction).as("workingResiduals"))
  }

  private[regression] lazy val responseResiduals: DataFrame = {
    predictions.select(label.minus(prediction).as("responseResiduals"))
  }

  /**
   * Get the default residuals (deviance residuals) of the fitted model.
   */
  @Since("2.0.0")
  def residuals(): DataFrame = devianceResiduals

  /**
   * Get the residuals of the fitted model by type.
   *
   * @param residualsType The type of residuals which should be returned.
   *                      Supported options: deviance, pearson, working and response.
   */
  @Since("2.0.0")
  def residuals(residualsType: String): DataFrame = {
    residualsType match {
      case "deviance" => devianceResiduals
      case "pearson" => pearsonResiduals
      case "working" => workingResiduals
      case "response" => responseResiduals
      case other => throw new UnsupportedOperationException(
        s"The residuals type $other is not supported by Generalized Linear Regression.")
    }
  }

  /**
   * The deviance for the null model.
   */
  @Since("2.0.0")
  lazy val nullDeviance: Double = {
    val intercept: Double = if (!model.getFitIntercept) {
      0.0
    } else {
      /*
        Estimate intercept analytically when there is no offset, or when there is offset but
        the model is Gaussian family with identity link. Otherwise, fit an intercept only model.
       */
      if (!model.hasOffsetCol ||
        (model.hasOffsetCol && family == Gaussian && link == Identity)) {
        val agg = predictions.agg(sum(weight.multiply(
          label.minus(offset))), sum(weight)).first()
        link.link(agg.getDouble(0) / agg.getDouble(1))
      } else {
        // Create empty feature column and fit intercept only model using param setting from model
        val featureNull = "feature_" + java.util.UUID.randomUUID.toString
        val paramMap = model.extractParamMap()
        paramMap.put(model.featuresCol, featureNull)
        if (family.name != "tweedie") {
          paramMap.remove(model.variancePower)
        }
        val emptyVectorUDF = udf{ () => Vectors.zeros(0) }
        model.parent.fit(
          dataset.withColumn(featureNull, emptyVectorUDF()), paramMap
        ).intercept
      }
    }
    predictions.select(label, offset, weight).rdd.map {
      case Row(y: Double, offset: Double, weight: Double) =>
        family.deviance(y, link.unlink(intercept + offset), weight)
    }.sum()
  }

  /**
   * The deviance for the fitted model.
   */
  @Since("2.0.0")
  lazy val deviance: Double = {
    predictions.select(label, prediction, weight).rdd.map {
      case Row(label: Double, pred: Double, weight: Double) =>
        family.deviance(label, pred, weight)
    }.sum()
  }

  /**
   * The dispersion of the fitted model.
   * It is taken as 1.0 for the "binomial" and "poisson" families, and otherwise
   * estimated by the residual Pearson's Chi-Squared statistic (which is defined as
   * sum of the squares of the Pearson residuals) divided by the residual degrees of freedom.
   */
  @Since("2.0.0")
  lazy val dispersion: Double = if (
    model.getFamily.toLowerCase(Locale.ROOT) == Binomial.name ||
      model.getFamily.toLowerCase(Locale.ROOT) == Poisson.name) {
    1.0
  } else {
    val rss = pearsonResiduals.agg(sum(pow(col("pearsonResiduals"), 2.0))).first().getDouble(0)
    rss / degreesOfFreedom
  }

  /** Akaike Information Criterion (AIC) for the fitted model. */
  @Since("2.0.0")
  lazy val aic: Double = {
    val weightSum = predictions.select(weight).agg(sum(weight)).first().getDouble(0)
    val t = predictions.select(
      label, prediction, weight).rdd.map {
        case Row(label: Double, pred: Double, weight: Double) =>
          (label, pred, weight)
    }
    family.aic(t, deviance, numInstances, weightSum) + 2 * rank
  }
}

/**
 * :: Experimental ::
 * Summary of [[GeneralizedLinearRegression]] fitting and model.
 *
 * @param dataset Dataset to be summarized.
 * @param origModel Model to be summarized.  This is copied to create an internal
 *                  model which cannot be modified from outside.
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1 in the last iteration
 * @param numIterations number of iterations
 * @param solver the solver algorithm used for model training
 */
@Since("2.0.0")
@Experimental
class GeneralizedLinearRegressionTrainingSummary private[regression] (
    dataset: Dataset[_],
    origModel: GeneralizedLinearRegressionModel,
    private val diagInvAtWA: Array[Double],
    @Since("2.0.0") val numIterations: Int,
    @Since("2.0.0") val solver: String)
  extends GeneralizedLinearRegressionSummary(dataset, origModel) with Serializable {

  import GeneralizedLinearRegression._

  /**
   * Whether the underlying `WeightedLeastSquares` using the "normal" solver.
   */
  private[ml] val isNormalSolver: Boolean = {
    diagInvAtWA.length != 1 || diagInvAtWA(0) != 0
  }

  /**
   * Standard error of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val coefficientStandardErrors: Array[Double] = {
    if (isNormalSolver) {
      diagInvAtWA.map(_ * dispersion).map(math.sqrt)
    } else {
      throw new UnsupportedOperationException(
        "No Std. Error of coefficients available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val tValues: Array[Double] = {
    if (isNormalSolver) {
      val estimate = if (model.getFitIntercept) {
        Array.concat(model.coefficients.toArray, Array(model.intercept))
      } else {
        model.coefficients.toArray
      }
      estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
    } else {
      throw new UnsupportedOperationException(
        "No t-statistic available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   * This value is only available when the underlying `WeightedLeastSquares`
   * using the "normal" solver.
   *
   * If `GeneralizedLinearRegression.fitIntercept` is set to true,
   * then the last element returned corresponds to the intercept.
   */
  @Since("2.0.0")
  lazy val pValues: Array[Double] = {
    if (isNormalSolver) {
      if (model.getFamily.toLowerCase(Locale.ROOT) == Binomial.name ||
        model.getFamily.toLowerCase(Locale.ROOT) == Poisson.name) {
        tValues.map { x => 2.0 * (1.0 - dist.Gaussian(0.0, 1.0).cdf(math.abs(x))) }
      } else {
        tValues.map { x =>
          2.0 * (1.0 - dist.StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x)))
        }
      }
    } else {
      throw new UnsupportedOperationException(
        "No p-value available for this GeneralizedLinearRegressionModel")
    }
  }

  /**
   * Coefficients with statistics: feature name, coefficients, standard error, tValue and pValue.
   */
  private[ml] lazy val coefficientsWithStatistics: Array[
    (String, Double, Double, Double, Double)] = {
    var featureNamesLocal = featureNames
    var coefficientsArray = model.coefficients.toArray
    var index = Array.range(0, coefficientsArray.length)
    if (model.getFitIntercept) {
      featureNamesLocal = featureNamesLocal :+ "(Intercept)"
      coefficientsArray = coefficientsArray :+ model.intercept
      // Reorder so that intercept comes first
      index = (coefficientsArray.length - 1) +: index
    }
    index.map { i =>
      (featureNamesLocal(i), coefficientsArray(i), coefficientStandardErrors(i),
        tValues(i), pValues(i))
    }
  }

  override def toString: String = {
    if (isNormalSolver) {

      def round(x: Double): String = {
        BigDecimal(x).setScale(4, BigDecimal.RoundingMode.HALF_UP).toString
      }

      val colNames = Array("Feature", "Estimate", "Std Error", "T Value", "P Value")

      val data = coefficientsWithStatistics.map { row =>
        val strRow = row.productIterator.map { cell =>
          val str = cell match {
            case s: String => s
            case n: Double => round(n)
          }
          // Truncate if length > 20
          if (str.length > 20) {
            str.substring(0, 17) + "..."
          } else {
            str
          }
        }
        strRow.toArray
      }

      // Compute the width of each column
      val colWidths = colNames.map(_.length)
      data.foreach { strRow =>
        strRow.zipWithIndex.foreach { case (cell: String, i: Int) =>
          colWidths(i) = math.max(colWidths(i), cell.length)
        }
      }

      val sb = new StringBuilder

      // Output coefficients with statistics
      sb.append("Coefficients:\n")
      colNames.zipWithIndex.map { case (colName: String, i: Int) =>
        StringUtils.leftPad(colName, colWidths(i))
      }.addString(sb, "", " ", "\n")

      data.foreach { case strRow: Array[String] =>
        strRow.zipWithIndex.map { case (cell: String, i: Int) =>
          StringUtils.leftPad(cell.toString, colWidths(i))
        }.addString(sb, "", " ", "\n")
      }

      sb.append("\n")
      sb.append(s"(Dispersion parameter for ${family.name} family taken to be " +
        s"${round(dispersion)})")

      sb.append("\n")
      val nd = s"Null deviance: ${round(nullDeviance)} on $degreesOfFreedom degrees of freedom"
      val rd = s"Residual deviance: ${round(deviance)} on $residualDegreeOfFreedom degrees of " +
        "freedom"
      val l = math.max(nd.length, rd.length)
      sb.append(StringUtils.leftPad(nd, l))
      sb.append("\n")
      sb.append(StringUtils.leftPad(rd, l))

      if (family.name != "tweedie") {
        sb.append("\n")
        sb.append(s"AIC: " + round(aic))
      }

      sb.toString()
    } else {
      throw new UnsupportedOperationException(
        "No summary available for this GeneralizedLinearRegressionModel")
    }
  }
}
