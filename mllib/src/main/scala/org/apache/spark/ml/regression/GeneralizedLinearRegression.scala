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

import breeze.stats.distributions.{Gaussian => GD}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.optim._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{BLAS, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

/**
 * Params for Generalized Linear Regression.
 */
private[regression] trait GeneralizedLinearRegressionParams extends PredictorParams
  with HasFitIntercept with HasMaxIter with HasTol with HasRegParam with HasWeightCol
  with HasSolver with Logging {

  /**
   * Param for the name of family which is a description of the error distribution
   * to be used in the model.
   * Supported options: "gaussian", "binomial", "poisson" and "gamma".
   * @group param
   */
  @Since("2.0.0")
  final val family: Param[String] = new Param(this, "family",
    "the name of family which is a description of the error distribution to be used in the model",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedFamilies.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getFamily: String = $(family)

  /**
   * Param for the name of the model link function.
   * Supported options: "identity", "log", "inverse", "logit", "probit", "cloglog" and "sqrt".
   * @group param
   */
  @Since("2.0.0")
  final val link: Param[String] = new Param(this, "link", "the name of the model link function",
    ParamValidators.inArray[String](GeneralizedLinearRegression.supportedLinks.toArray))

  /** @group getParam */
  @Since("2.0.0")
  def getLink: String = $(link)

  @Since("2.0.0")
  override def validateParams(): Unit = {
    if (isDefined(link)) {
      require(GeneralizedLinearRegression.supportedFamilyLinkPairs.contains($(family) -> $(link)),
        s"Generalized Linear Regression with ${$(family)} family does not support ${$(link)} " +
          s"link function.")
    }
  }
}

/**
 * :: Experimental ::
 *
 * Fit a Generalized Linear Model ([[https://en.wikipedia.org/wiki/Generalized_linear_model]])
 * specified by giving a symbolic description of the linear predictor and
 * a description of the error distribution.
 */
@Experimental
@Since("2.0.0")
class GeneralizedLinearRegression @Since("2.0.0") (@Since("2.0.0") override val uid: String)
  extends Regressor[Vector, GeneralizedLinearRegression, GeneralizedLinearRegressionModel]
  with GeneralizedLinearRegressionParams with Logging {

  @Since("2.0.0")
  def this() = this(Identifiable.randomUID("genLinReg"))

  /**
   * Set the name of family which is a description of the error distribution
   * to be used in the model.
   * @group setParam
   */
  @Since("2.0.0")
  def setFamily(value: String): this.type = set(family, value)

  /**
   * Set the name of the model link function.
   * @group setParam
   */
  @Since("2.0.0")
  def setLink(value: String): this.type = set(link, value)

  /**
   * Set if we should fit the intercept.
   * Default is true.
   * @group setParam
   */
  @Since("2.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("2.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("2.0.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("2.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Whether to over-/under-sample training instances according to the given weights in weightCol.
   * If empty, all instances are treated equally (weight 1.0).
   * Default is empty, so all instances have weight one.
   * @group setParam
   */
  @Since("2.0.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "")

  /**
   * Set the solver algorithm used for optimization.
   * Currently only support "irls" which is also the default solver.
   * @group setParam
   */
  @Since("2.0.0")
  def setSolver(value: String): this.type = set(solver, value)
  setDefault(solver -> "irls")

  override protected def train(dataset: DataFrame): GeneralizedLinearRegressionModel = {
    val familyLink = $(family) match {
      case "gaussian" => if (isDefined(link)) Gaussian($(link)) else Gaussian("identity")
      case "binomial" => if (isDefined(link)) Binomial($(link)) else Binomial("logit")
      case "poisson" => if (isDefined(link)) Poisson($(link)) else Poisson("log")
      case "gamma" => if (isDefined(link)) Gamma($(link)) else Gamma("inverse")
    }

    val numFeatures = dataset.select(col($(featuresCol))).limit(1).map {
      case Row(features: Vector) => features.size
    }.first()
    if (numFeatures > 4096) {
      val msg = "Currently, GeneralizedLinearRegression only supports number of features" +
        s" <= 4096. Found $numFeatures in the input dataset."
      throw new SparkException(msg)
    }

    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] = dataset.select(
      col($(labelCol)), w, col($(featuresCol))).map {
      case Row(label: Double, weight: Double, features: Vector) =>
        Instance(label, weight, features)
    }

    if ($(family) == "gaussian" && $(link) == "identity") {
      val optimizer = new WeightedLeastSquares($(fitIntercept), $(regParam),
        standardizeFeatures = true, standardizeLabel = true)
      val wlsModel = optimizer.fit(instances)
      val model = copyValues(new GeneralizedLinearRegressionModel(uid,
        wlsModel.coefficients, wlsModel.intercept).setParent(this))
      return model
    }

    val newInstances = instances.map { instance =>
      val mu = familyLink.initialize(instance.label, instance.weight)
      val eta = familyLink.predict(mu)
      Instance(eta, instance.weight, instance.features)
    }

    val initialModel = new WeightedLeastSquares($(fitIntercept), $(regParam),
      standardizeFeatures = true, standardizeLabel = true).fit(newInstances)

    val reweightFunc: (Instance, WeightedLeastSquaresModel) => (Double, Double) = {
      (instance: Instance, model: WeightedLeastSquaresModel) => {
        val eta = model.predict(instance.features)
        val mu = familyLink.fitted(eta)
        val z = familyLink.adjusted(instance.label, mu, eta)
        val w = familyLink.weights(mu) * instance.weight
        (z, w)
      }
    }

    val optimizer = new IterativelyReweightedLeastSquares(initialModel, reweightFunc,
      $(fitIntercept), $(regParam), $(maxIter), $(tol))

    val irlsModel = optimizer.fit(instances)

    val model = copyValues(new GeneralizedLinearRegressionModel(uid,
      irlsModel.coefficients, irlsModel.intercept).setParent(this))
    model
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegression = defaultCopy(extra)
}

@Since("2.0.0")
object GeneralizedLinearRegression {

  /** Set of families that GeneralizedLinearRegression supports */
  private[ml] val supportedFamilies = Set("gaussian", "binomial", "poisson", "gamma")

  /** Set of links that GeneralizedLinearRegression supports */
  private[ml] val supportedLinks = Set("identity", "log", "inverse", "logit", "probit",
    "cloglog", "sqrt")

  /** Set of family and link pairs that GeneralizedLinearRegression supports */
  private[ml] val supportedFamilyLinkPairs = Set(
    "gaussian" -> "identity", "gaussian" -> "log", "gaussian" -> "inverse",
    "binomial" -> "logit", "binomial" -> "probit", "binomial" -> "cloglog",
    "poisson" -> "log", "poisson" -> "identity", "poisson" -> "sqrt",
    "gamma" -> "inverse", "gamma" -> "identity", "gamma" -> "log"
  )
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
  with GeneralizedLinearRegressionParams {

  private lazy val familyLink = $(family) match {
    case "gaussian" => if (isDefined(link)) Gaussian($(link)) else Gaussian("identity")
    case "binomial" => if (isDefined(link)) Binomial($(link)) else Binomial("logit")
    case "poisson" => if (isDefined(link)) Poisson($(link)) else Poisson("log")
    case "gamma" => if (isDefined(link)) Gamma($(link)) else Gamma("inverse")
  }

  override protected def predict(features: Vector): Double = {
    val eta = BLAS.dot(features, coefficients) + intercept
    familyLink.fitted(eta)
  }

  @Since("2.0.0")
  override def copy(extra: ParamMap): GeneralizedLinearRegressionModel = {
    copyValues(new GeneralizedLinearRegressionModel(uid, coefficients, intercept), extra)
      .setParent(parent)
  }
}

/**
 * A description of the error distribution and link function to be used in the model.
 * @param link a link function instance
 */
private[ml] abstract class Family(val link: Link) extends Serializable {

  /** Initialize the starting value for mu. */
  def initialize(y: Double, weight: Double): Double

  /** The variance of the endogenous variable's mean, given the value mu. */
  def variance(mu: Double): Double

  /** Weights for IRLS steps. */
  def weights(mu: Double): Double = {
    val x = clean(mu)
    1.0 / (math.pow(this.link.deriv(x), 2.0) * this.variance(x))
  }

  /** The adjusted response variable. */
  def adjusted(y: Double, mu: Double, eta: Double): Double = {
    val x = clean(mu)
    eta + (y - x) * link.deriv(x)
  }

  /** Linear predictors based on given mu. */
  def predict(mu: Double): Double = this.link.link(clean(mu))

  /** Fitted values based on linear predictors eta. */
  def fitted(eta: Double): Double = clean(this.link.unlink(eta))

  def clean(mu: Double): Double = mu

  val epsilon: Double = 1E-16
}

/**
 * Gaussian exponential family distribution.
 * The default link for the Gaussian family is the identity link.
 * @param link a link function instance
 */
private[ml] class Gaussian(link: Link = Identity) extends Family(link) {

  override def initialize(y: Double, weight: Double): Double = {
    if (link == Log) {
      require(y > 0.0, "The response variable of Gaussian family with Log link " +
        s"should be positive, but got $y")
    }
    y
  }

  def variance(mu: Double): Double = 1.0
}

private[ml] object Gaussian {

  def apply(link: String): Gaussian = {
    link match {
      case "identity" => new Gaussian(Identity)
      case "log" => new Gaussian(Log)
      case "inverse" => new Gaussian(Inverse)
    }
  }
}

/**
 * Binomial exponential family distribution.
 * The default link for the Binomial family is the logit link.
 * @param link a link function instance
 */
private[ml] class Binomial(link: Link = Logit) extends Family(link) {

  override def initialize(y: Double, weight: Double): Double = {
    val mu = (weight * y + 0.5) / (weight + 1.0)
    require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
      s"should be in range (0, 1), but got $mu")
    mu
  }

  override def variance(mu: Double): Double = mu * (1.0 - mu)

  override def clean(mu: Double): Double = {
    if (mu < epsilon) {
      epsilon
    } else if (mu > 1.0 - epsilon) {
      1.0 - epsilon
    } else {
      mu
    }
  }
}

private[ml] object Binomial {

  def apply(link: String): Binomial = {
    link match {
      case "logit" => new Binomial(Logit)
      case "probit" => new Binomial(Probit)
      case "cloglog" => new Binomial(CLogLog)
    }
  }
}

/**
 * Poisson exponential family distribution.
 * The default link for the Poisson family is the log link.
 * @param link a link function instance
 */
private[ml] class Poisson(link: Link = Log) extends Family(link) {

  override def initialize(y: Double, weight: Double): Double = {
    require(y > 0.0, "The response variable of Poisson family " +
      s"should be positive, but got $y")
    y
  }

  override def variance(mu: Double): Double = mu
}

private[ml] object Poisson {

  def apply(link: String): Poisson = {
    link match {
      case "log" => new Poisson(Log)
      case "sqrt" => new Poisson(Sqrt)
      case "identity" => new Poisson(Identity)
    }
  }
}

/**
 * Gamma exponential family distribution.
 * The default link for the Gamma family is the inverse link.
 * @param link a link function instance
 */
private[ml] class Gamma(link: Link = Inverse) extends Family(link) {

  override def initialize(y: Double, weight: Double): Double = {
    require(y > 0.0, "The response variable of Gamma family " +
      s"should be positive, but got $y")
    y
  }

  override def variance(mu: Double): Double = math.pow(mu, 2.0)
}

private[ml] object Gamma {

  def apply(link: String): Gamma = {
    link match {
      case "inverse" => new Gamma(Inverse)
      case "identity" => new Gamma(Identity)
      case "log" => new Gamma(Log)
    }
  }
}

/**
 * A description of the link function to be used in the model.
 */
private[ml] trait Link extends Serializable {

  /** The link function. */
  def link(mu: Double): Double

  /** Derivative of the link function. */
  def deriv(mu: Double): Double

  /** The inverse link function. */
  def unlink(eta: Double): Double
}

private[ml] object Identity extends Link {

  override def link(mu: Double): Double = mu

  override def deriv(mu: Double): Double = 1.0

  override def unlink(eta: Double): Double = eta
}

private[ml] object Logit extends Link {

  override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

  override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

  override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
}

private[ml] object Log extends Link {

  override def link(mu: Double): Double = math.log(mu)

  override def deriv(mu: Double): Double = 1.0 / mu

  override def unlink(eta: Double): Double = math.exp(eta)
}

private[ml] object Inverse extends Link {

  override def link(mu: Double): Double = 1.0 / mu

  override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

  override def unlink(eta: Double): Double = 1.0 / eta
}

private[ml] object Probit extends Link {

  override def link(mu: Double): Double = GD(0.0, 1.0).icdf(mu)

  override def deriv(mu: Double): Double = 1.0 / GD(0.0, 1.0).pdf(GD(0.0, 1.0).icdf(mu))

  override def unlink(eta: Double): Double = GD(0.0, 1.0).cdf(eta)
}

private[ml] object CLogLog extends Link {

  override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

  override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

  override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
}

private[ml] object Sqrt extends Link {

  override def link(mu: Double): Double = math.sqrt(mu)

  override def deriv(mu: Double): Double = 1.0 / (2.0 * math.sqrt(mu))

  override def unlink(eta: Double): Double = math.pow(eta, 2.0)
}
