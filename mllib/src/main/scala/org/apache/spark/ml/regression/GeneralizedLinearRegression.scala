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

import breeze.stats.{distributions => dist}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.optim._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Params for Generalized Linear Regression.
 */
private[regression] trait GeneralizedLinearRegressionBase extends PredictorParams
  with HasFitIntercept with HasMaxIter with HasTol with HasRegParam with HasWeightCol
  with HasSolver with Logging {

  /**
   * Param for the name of family which is a description of the error distribution
   * to be used in the model.
   * Supported options: "gaussian", "binomial", "poisson" and "gamma".
   * Default is "gaussian".
   * @group param
   */
  @Since("2.0.0")
  final val family: Param[String] = new Param(this, "family",
    "The name of family which is a description of the error distribution to be used in the " +
      "model. Supported options: gaussian(default), binomial, poisson and gamma.",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedFamilyNames.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getFamily: String = $(family)

  /**
   * Param for the name of link function which provides the relationship
   * between the linear predictor and the mean of the distribution function.
   * Supported options: "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt".
   * @group param
   */
  @Since("2.0.0")
  final val link: Param[String] = new Param(this, "link", "The name of link function " +
    "which provides the relationship between the linear predictor and the mean of the " +
    "distribution function. Supported options: identity, log, inverse, logit, probit, " +
    "cloglog and sqrt.",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedLinkNames.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getLink: String = $(link)

  import GeneralizedLinearRegression._

  @Since("2.0.0")
  override def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType): StructType = {
    if ($(solver) == "irls") {
      setDefault(maxIter -> 25)
    }
    if (isDefined(link)) {
      require(supportedFamilyAndLinkPairs.contains(
        Family.fromName($(family)) -> Link.fromName($(link))), "Generalized Linear Regression " +
        s"with ${$(family)} family does not support ${$(link)} link function.")
    }
    super.validateAndTransformSchema(schema, fitting, featuresDataType)
  }
}

/**
 * :: Experimental ::
 *
 * Fit a Generalized Linear Model ([[https://en.wikipedia.org/wiki/Generalized_linear_model]])
 * specified by giving a symbolic description of the linear predictor (link function) and
 * a description of the error distribution (family).
 * It supports "gaussian", "binomial", "poisson" and "gamma" as family.
 * Valid link functions for each family is listed below. The first link function of each family
 * is the default one.
 *  - "gaussian" -> "identity", "log", "inverse"
 *  - "binomial" -> "logit", "probit", "cloglog"
 *  - "poisson"  -> "log", "identity", "sqrt"
 *  - "gamma"    -> "inverse", "identity", "log"
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
   * @group setParam
   */
  @Since("2.0.0")
  def setFamily(value: String): this.type = set(family, value)
  setDefault(family -> Gaussian.name)

  /**
   * Sets the value of param [[link]].
   * @group setParam
   */
  @Since("2.0.0")
  def setLink(value: String): this.type = set(link, value)

  /**
   * Sets if we should fit the intercept.
   * Default is true.
   * @group setParam
   */
  @Since("2.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Sets the maximum number of iterations.
   * Default is 25 if the solver algorithm is "irls".
   * @group setParam
   */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Sets the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("2.0.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Sets the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("2.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Sets the value of param [[weightCol]].
   * If this is not set or empty, we treat all instance weights as 1.0.
   * Default is empty, so all instances have weight one.
   * @group setParam
   */
  @Since("2.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "")

  /**
   * Sets the solver algorithm used for optimization.
   * Currently only support "irls" which is also the default solver.
   * @group setParam
   */
  @Since("2.0.0")
  def setSolver(value: String): this.type = set(solver, value)
  setDefault(solver -> "irls")

  override protected def train(dataset: DataFrame): GeneralizedLinearRegressionModel = {
    val familyObj = Family.fromName($(family))
    val linkObj = if (isDefined(link)) {
      Link.fromName($(link))
    } else {
      familyObj.defaultLink
    }
    val familyAndLink = new FamilyAndLink(familyObj, linkObj)

    val numFeatures = dataset.select(col($(featuresCol))).limit(1).rdd
      .map { case Row(features: Vector) =>
        features.size
      }.first()
    if (numFeatures > WeightedLeastSquares.MAX_NUM_FEATURES) {
      val msg = "Currently, GeneralizedLinearRegression only supports number of features" +
        s" <= ${WeightedLeastSquares.MAX_NUM_FEATURES}. Found $numFeatures in the input dataset."
      throw new SparkException(msg)
    }

    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] = dataset.select(col($(labelCol)), w, col($(featuresCol))).rdd
      .map { case Row(label: Double, weight: Double, features: Vector) =>
        Instance(label, weight, features)
      }

    if (familyObj == Gaussian && linkObj == Identity) {
      // TODO: Make standardizeFeatures and standardizeLabel configurable.
      val optimizer = new WeightedLeastSquares($(fitIntercept), $(regParam),
        standardizeFeatures = true, standardizeLabel = true)
      val wlsModel = optimizer.fit(instances)
      val model = copyValues(
        new GeneralizedLinearRegressionModel(uid, wlsModel.coefficients, wlsModel.intercept)
          .setParent(this))
      // Handle possible missing or invalid prediction columns
      val (summaryModel, predictionColName) = model.findSummaryModelAndPredictionCol()
      val trainingSummary = new GeneralizedLinearRegressionSummary(
        summaryModel.transform(dataset),
        predictionColName,
        model,
        wlsModel.diagInvAtWA.toArray,
        1)
      return model.setSummary(trainingSummary)
    }

    // Fit Generalized Linear Model by iteratively reweighted least squares (IRLS).
    val initialModel = familyAndLink.initialize(instances, $(fitIntercept), $(regParam))
    val optimizer = new IterativelyReweightedLeastSquares(initialModel, familyAndLink.reweightFunc,
      $(fitIntercept), $(regParam), $(maxIter), $(tol))
    val irlsModel = optimizer.fit(instances)

    val model = copyValues(
      new GeneralizedLinearRegressionModel(uid, irlsModel.coefficients, irlsModel.intercept)
        .setParent(this))
    // Handle possible missing or invalid prediction columns
    val (summaryModel, predictionColName) = model.findSummaryModelAndPredictionCol()
    val trainingSummary = new GeneralizedLinearRegressionSummary(
      summaryModel.transform(dataset),
      predictionColName,
      model,
      irlsModel.diagInvAtWA.toArray,
      irlsModel.numIterations)

    model.setSummary(trainingSummary)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegression = defaultCopy(extra)
}

@Since("2.0.0")
object GeneralizedLinearRegression extends DefaultParamsReadable[GeneralizedLinearRegression] {

  @Since("2.0.0")
  override def load(path: String): GeneralizedLinearRegression = super.load(path)

  /** Set of family and link pairs that GeneralizedLinearRegression supports. */
  private[ml] lazy val supportedFamilyAndLinkPairs = Set(
    Gaussian -> Identity, Gaussian -> Log, Gaussian -> Inverse,
    Binomial -> Logit, Binomial -> Probit, Binomial -> CLogLog,
    Poisson -> Log, Poisson -> Identity, Poisson -> Sqrt,
    Gamma -> Inverse, Gamma -> Identity, Gamma -> Log
  )

  /** Set of family names that GeneralizedLinearRegression supports. */
  private[ml] lazy val supportedFamilyNames = supportedFamilyAndLinkPairs.map(_._1.name)

  /** Set of link names that GeneralizedLinearRegression supports. */
  private[ml] lazy val supportedLinkNames = supportedFamilyAndLinkPairs.map(_._2.name)

  private[ml] val epsilon: Double = 1E-16

  /**
   * Wrapper of family and link combination used in the model.
   */
  private[ml] class FamilyAndLink(val family: Family, val link: Link) extends Serializable {

    /** Linear predictor based on given mu. */
    def predict(mu: Double): Double = link.link(family.project(mu))

    /** Fitted value based on linear predictor eta. */
    def fitted(eta: Double): Double = family.project(link.unlink(eta))

    /**
     * Get the initial guess model for [[IterativelyReweightedLeastSquares]].
     */
    def initialize(
        instances: RDD[Instance],
        fitIntercept: Boolean,
        regParam: Double): WeightedLeastSquaresModel = {
      val newInstances = instances.map { instance =>
        val mu = family.initialize(instance.label, instance.weight)
        val eta = predict(mu)
        Instance(eta, instance.weight, instance.features)
      }
      // TODO: Make standardizeFeatures and standardizeLabel configurable.
      val initialModel = new WeightedLeastSquares(fitIntercept, regParam,
        standardizeFeatures = true, standardizeLabel = true)
        .fit(newInstances)
      initialModel
    }

    /**
     * The reweight function used to update offsets and weights
     * at each iteration of [[IterativelyReweightedLeastSquares]].
     */
    val reweightFunc: (Instance, WeightedLeastSquaresModel) => (Double, Double) = {
      (instance: Instance, model: WeightedLeastSquaresModel) => {
        val eta = model.predict(instance.features)
        val mu = fitted(eta)
        val offset = eta + (instance.label - mu) * link.deriv(mu)
        val weight = instance.weight / (math.pow(this.link.deriv(mu), 2.0) * family.variance(mu))
        (offset, weight)
      }
    }
  }

  /**
   * A description of the error distribution to be used in the model.
   * @param name the name of the family.
   */
  private[ml] abstract class Family(val name: String) extends Serializable {

    /** The default link instance of this family. */
    val defaultLink: Link

    /** Initialize the starting value for mu. */
    def initialize(y: Double, weight: Double): Double

    /** The variance of the endogenous variable's mean, given the value mu. */
    def variance(mu: Double): Double

    /** Deviance of (y, mu) pair. */
    def deviance(y: Double, mu: Double, weight: Double): Double

    /**
     * Akaike's 'An Information Criterion'(AIC) value of the family for a given dataset.
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

  private[ml] object Family {

    /**
     * Gets the [[Family]] object from its name.
     * @param name family name: "gaussian", "binomial", "poisson" or "gamma".
     */
    def fromName(name: String): Family = {
      name match {
        case Gaussian.name => Gaussian
        case Binomial.name => Binomial
        case Poisson.name => Poisson
        case Gamma.name => Gamma
      }
    }
  }

  /**
   * Gaussian exponential family distribution.
   * The default link for the Gaussian family is the identity link.
   */
  private[ml] object Gaussian extends Family("gaussian") {

    val defaultLink: Link = Identity

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
  private[ml] object Binomial extends Family("binomial") {

    val defaultLink: Link = Logit

    override def initialize(y: Double, weight: Double): Double = {
      val mu = (weight * y + 0.5) / (weight + 1.0)
      require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
        s"should be in range (0, 1), but got $mu")
      mu
    }

    override def variance(mu: Double): Double = mu * (1.0 - mu)

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      val my = 1.0 - y
      2.0 * weight * (y * math.log(math.max(y, 1.0) / mu) +
        my * math.log(math.max(my, 1.0) / (1.0 - mu)))
    }

    override def aic(
        predictions: RDD[(Double, Double, Double)],
        deviance: Double,
        numInstances: Double,
        weightSum: Double): Double = {
      -2.0 * predictions.map { case (y: Double, mu: Double, weight: Double) =>
        weight * dist.Binomial(1, mu).logProbabilityOf(math.round(y).toInt)
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
  private[ml] object Poisson extends Family("poisson") {

    val defaultLink: Link = Log

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Poisson family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (y * math.log(y / mu) - (y - mu))
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

  /**
   * Gamma exponential family distribution.
   * The default link for the Gamma family is the inverse link.
   */
  private[ml] object Gamma extends Family("gamma") {

    val defaultLink: Link = Inverse

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

  /**
   * A description of the link function to be used in the model.
   * The link function provides the relationship between the linear predictor
   * and the mean of the distribution function.
   * @param name the name of link function.
   */
  private[ml] abstract class Link(val name: String) extends Serializable {

    /** The link function. */
    def link(mu: Double): Double

    /** Derivative of the link function. */
    def deriv(mu: Double): Double

    /** The inverse link function. */
    def unlink(eta: Double): Double
  }

  private[ml] object Link {

    /**
     * Gets the [[Link]] object from its name.
     * @param name link name: "identity", "logit", "log",
     *             "inverse", "probit", "cloglog" or "sqrt".
     */
    def fromName(name: String): Link = {
      name match {
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

  private[ml] object Identity extends Link("identity") {

    override def link(mu: Double): Double = mu

    override def deriv(mu: Double): Double = 1.0

    override def unlink(eta: Double): Double = eta
  }

  private[ml] object Logit extends Link("logit") {

    override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

    override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
  }

  private[ml] object Log extends Link("log") {

    override def link(mu: Double): Double = math.log(mu)

    override def deriv(mu: Double): Double = 1.0 / mu

    override def unlink(eta: Double): Double = math.exp(eta)
  }

  private[ml] object Inverse extends Link("inverse") {

    override def link(mu: Double): Double = 1.0 / mu

    override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

    override def unlink(eta: Double): Double = 1.0 / eta
  }

  private[ml] object Probit extends Link("probit") {

    override def link(mu: Double): Double = dist.Gaussian(0.0, 1.0).icdf(mu)

    override def deriv(mu: Double): Double = {
      1.0 / dist.Gaussian(0.0, 1.0).pdf(dist.Gaussian(0.0, 1.0).icdf(mu))
    }

    override def unlink(eta: Double): Double = dist.Gaussian(0.0, 1.0).cdf(eta)
  }

  private[ml] object CLogLog extends Link("cloglog") {

    override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

    override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
  }

  private[ml] object Sqrt extends Link("sqrt") {

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

  import GeneralizedLinearRegression._

  lazy val familyObj = Family.fromName($(family))
  lazy val linkObj = if (isDefined(link)) {
    Link.fromName($(link))
  } else {
    familyObj.defaultLink
  }
  lazy val familyAndLink = new FamilyAndLink(familyObj, linkObj)

  override protected def predict(features: Vector): Double = {
    val eta = BLAS.dot(features, coefficients) + intercept
    familyAndLink.fitted(eta)
  }

  private var trainingSummary: Option[GeneralizedLinearRegressionSummary] = None

  /**
   * Gets R-like summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("2.0.0")
  def summary: GeneralizedLinearRegressionSummary = trainingSummary.getOrElse {
    throw new SparkException(
      "No training summary available for this GeneralizedLinearRegressionModel")
  }

  private[regression] def setSummary(summary: GeneralizedLinearRegressionSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /**
   * If the prediction column is set returns the current model and prediction column,
   * otherwise generates a new column and sets it as the prediction column on a new copy
   * of the current model.
   */
  private[regression] def findSummaryModelAndPredictionCol()
    : (GeneralizedLinearRegressionModel, String) = {
    $(predictionCol) match {
      case "" =>
        val predictionColName = "prediction_" + java.util.UUID.randomUUID.toString()
        (copy(ParamMap.empty).setPredictionCol(predictionColName), predictionColName)
      case p => (this, p)
    }
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegressionModel = {
    copyValues(new GeneralizedLinearRegressionModel(uid, coefficients, intercept), extra)
      .setParent(parent)
  }

  @Since("2.0.0")
  override def write: MLWriter =
    new GeneralizedLinearRegressionModel.GeneralizedLinearRegressionModelWriter(this)
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
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class GeneralizedLinearRegressionModelReader
    extends MLReader[GeneralizedLinearRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GeneralizedLinearRegressionModel].getName

    override def load(path: String): GeneralizedLinearRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("intercept", "coefficients").head()
      val intercept = data.getDouble(0)
      val coefficients = data.getAs[Vector](1)

      val model = new GeneralizedLinearRegressionModel(metadata.uid, coefficients, intercept)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * :: Experimental ::
 * Summarizing Generalized Linear regression Fits.
 *
 * @param predictions predictions outputted by the model's `transform` method
 * @param predictionCol field in "predictions" which gives the prediction value of each instance
 * @param model the model that should be summarized
 * @param diagInvAtWA diagonal of matrix (A^T * W * A)^-1 in the last iteration
 * @param numIterations number of iterations
 */
@Since("2.0.0")
@Experimental
class GeneralizedLinearRegressionSummary private[regression] (
    @Since("2.0.0") @transient val predictions: DataFrame,
    @Since("2.0.0") val predictionCol: String,
    @Since("2.0.0") val model: GeneralizedLinearRegressionModel,
    private val diagInvAtWA: Array[Double],
    @Since("2.0.0") val numIterations: Int) extends Serializable {

  import GeneralizedLinearRegression._

  private lazy val family = Family.fromName(model.getFamily)
  private lazy val link = if (model.isDefined(model.getParam("link"))) {
    Link.fromName(model.getLink)
  } else {
    family.defaultLink
  }

  /** Number of instances in DataFrame predictions */
  private lazy val numInstances: Long = predictions.count()

  /** The numeric rank of the fitted linear model */
  @Since("2.0.0")
  lazy val rank: Long = if (model.getFitIntercept) {
    model.coefficients.size + 1
  } else {
    model.coefficients.size
  }

  /** Degrees of freedom */
  @Since("2.0.0")
  lazy val degreesOfFreedom: Long = {
    numInstances - rank
  }

  /** The residual degrees of freedom */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedom: Long = degreesOfFreedom

  /** The residual degrees of freedom for the null model */
  @Since("2.0.0")
  lazy val residualDegreeOfFreedomNull: Long = if (model.getFitIntercept) {
    numInstances - 1
  } else {
    numInstances
  }

  private lazy val devianceResiduals: DataFrame = {
    val drUDF = udf { (y: Double, mu: Double, weight: Double) =>
      val r = math.sqrt(math.max(family.deviance(y, mu, weight), 0.0))
      if (y > mu) r else -1.0 * r
    }
    val w = if (model.getWeightCol.isEmpty) lit(1.0) else col(model.getWeightCol)
    predictions.select(
      drUDF(col(model.getLabelCol), col(predictionCol), w).as("devianceResiduals"))
  }

  private lazy val pearsonResiduals: DataFrame = {
    val prUDF = udf { mu: Double => family.variance(mu) }
    val w = if (model.getWeightCol.isEmpty) lit(1.0) else col(model.getWeightCol)
    predictions.select(col(model.getLabelCol).minus(col(predictionCol))
      .multiply(sqrt(w)).divide(sqrt(prUDF(col(predictionCol)))).as("pearsonResiduals"))
  }

  private lazy val workingResiduals: DataFrame = {
    val wrUDF = udf { (y: Double, mu: Double) => (y - mu) * link.deriv(mu) }
    predictions.select(wrUDF(col(model.getLabelCol), col(predictionCol)).as("workingResiduals"))
  }

  private lazy val responseResiduals: DataFrame = {
    predictions.select(col(model.getLabelCol).minus(col(predictionCol)).as("responseResiduals"))
  }

  /**
   * Get the default residuals(deviance residuals) of the fitted model.
   */
  @Since("2.0.0")
  def residuals(): DataFrame = devianceResiduals

  /**
   * Get the residuals of the fitted model by type.
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
    val w = if (model.getWeightCol.isEmpty) lit(1.0) else col(model.getWeightCol)
    val wtdmu: Double = if (model.getFitIntercept) {
      val agg = predictions.agg(sum(w.multiply(col(model.getLabelCol))), sum(w)).first()
      agg.getDouble(0) / agg.getDouble(1)
    } else {
      link.unlink(0.0)
    }
    predictions.select(col(model.getLabelCol), w).rdd.map {
      case Row(y: Double, weight: Double) =>
        family.deviance(y, wtdmu, weight)
    }.sum()
  }

  /**
   * The deviance for the fitted model.
   */
  @Since("2.0.0")
  lazy val deviance: Double = {
    val w = if (model.getWeightCol.isEmpty) lit(1.0) else col(model.getWeightCol)
    predictions.select(col(model.getLabelCol), col(predictionCol), w).rdd.map {
      case Row(label: Double, pred: Double, weight: Double) =>
        family.deviance(label, pred, weight)
    }.sum()
  }

  /**
   * The dispersion of the fitted model.
   * It is taken as 1.0 for the "binomial" and "poisson" families, and otherwise
   * estimated by the residual Pearson's Chi-Squared statistic(which is defined as
   * sum of the squares of the Pearson residuals) divided by the residual degrees of freedom.
   */
  @Since("2.0.0")
  lazy val dispersion: Double = if (
    model.getFamily == Binomial.name || model.getFamily == Poisson.name) {
    1.0
  } else {
    val rss = pearsonResiduals.agg(sum(pow(col("pearsonResiduals"), 2.0))).first().getDouble(0)
    rss / degreesOfFreedom
  }

  /** Akaike's "An Information Criterion"(AIC) for the fitted model. */
  @Since("2.0.0")
  lazy val aic: Double = {
    val w = if (model.getWeightCol.isEmpty) lit(1.0) else col(model.getWeightCol)
    val weightSum = predictions.select(w).agg(sum(w)).first().getDouble(0)
    val t = predictions.select(col(model.getLabelCol), col(predictionCol), w).rdd.map {
      case Row(label: Double, pred: Double, weight: Double) =>
        (label, pred, weight)
    }
    family.aic(t, deviance, numInstances, weightSum) + 2 * rank
  }

  /**
   * Standard error of estimated coefficients and intercept.
   */
  @Since("2.0.0")
  lazy val coefficientStandardErrors: Array[Double] = {
    diagInvAtWA.map(_ * dispersion).map(math.sqrt)
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   */
  @Since("2.0.0")
  lazy val tValues: Array[Double] = {
    val estimate = if (model.getFitIntercept) {
      Array.concat(model.coefficients.toArray, Array(model.intercept))
    } else {
      model.coefficients.toArray
    }
    estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   */
  @Since("2.0.0")
  lazy val pValues: Array[Double] = {
    if (model.getFamily == Binomial.name || model.getFamily == Poisson.name) {
      tValues.map { x => 2.0 * (1.0 - dist.Gaussian(0.0, 1.0).cdf(math.abs(x))) }
    } else {
      tValues.map { x => 2.0 * (1.0 - dist.StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x))) }
    }
  }
}
