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

import scala.collection.mutable

import breeze.linalg.{norm => brznorm, DenseVector => BDV}
import breeze.optimize.{LBFGS => BreezeLBFGS, _}

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.param.{DoubleParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Params for robust regression.
 */
private[regression] trait RobustRegressionParams extends PredictorParams with HasRegParam
  with HasMaxIter with HasTol with HasFitIntercept with HasStandardization with HasWeightCol {

  /**
   * The shape parameter to control the amount of robustness. Must be > 1.0.
   * At larger values of M, the huber criterion becomes more similar to least squares regression;
   * for small values of M, the criterion is more similar to L1 regression.
   * Default is 1.35 to get as much robustness as possible while retaining
   * 95% statistical efficiency for normally distributed data.
   */
  @Since("2.1.0")
  final val m = new DoubleParam(this, "m", "The shape parameter to control the amount of " +
    "robustness. Must be > 1.0.", ParamValidators.gt(1.0))

  /** @group getParam */
  @Since("2.1.0")
  def getM: Double = $(m)
}

/**
 * Robust regression.
 *
 * The learning objective is to minimize the huber loss, with regularization.
 *
 * The robust regression optimizes the squared loss for the samples where
 * {{{ |\frac{(y - X \beta)}{\sigma}|\leq M }}}
 * and the absolute loss for the samples where
 * {{{ |\frac{(y - X \beta)}{\sigma}|\geq M }}},
 * where \beta and \sigma are parameters to be optimized.
 *
 * This supports two types of regularization: None and L2.
 *
 * This estimator is different from the R implementation of Robust Regression
 * ([[http://www.ats.ucla.edu/stat/r/dae/rreg.htm]]) because the R implementation does a
 * weighted least squares implementation with weights given to each sample on the basis
 * of how much the residual is greater than a certain threshold.
 */
@Since("2.1.0")
class RobustRegression @Since("2.1.0") (@Since("2.1.0") override val uid: String)
  extends Regressor[Vector, RobustRegression, RobustRegressionModel]
  with RobustRegressionParams with Logging {

  @Since("2.1.0")
  def this() = this(Identifiable.randomUID("robReg"))

  /**
   * Sets the value of param [[m]].
   * Default is 1.35.
   * @group setParam
   */
  @Since("2.1.0")
  def setM(value: Double): this.type = set(m, value)
  setDefault(m -> 1.35)

  /**
   * Sets the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("2.1.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Sets if we should fit the intercept.
   * Default is true.
   * @group setParam
   */
  @Since("2.1.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Whether to standardize the training features before fitting the model.
   * The coefficients of models will be always returned on the original scale,
   * so it will be transparent for users. Note that with/without standardization,
   * the models should be always converged to the same solution when no regularization
   * is applied.
   * Default is true.
   * @group setParam
   */
  @Since("2.1.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  /**
   * Sets the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("2.1.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Sets the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("2.1.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to over-/under-sample training instances according to the given weights in weightCol.
   * If not set or empty, all instances are treated equally (weight 1.0).
   * Default is not set, so all instances have weight one.
   * @group setParam
   */
  @Since("2.1.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  override protected def train(dataset: Dataset[_]): RobustRegressionModel = {
    val numFeatures = dataset.select(col($(featuresCol))).first().getAs[Vector](0).size
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))

    val sqlContext = dataset.sqlContext
    import sqlContext.implicits._
    val instances: RDD[Instance] = dataset.select(col($(labelCol)).as("label"),
      w.as("weight"), col($(featuresCol)).as("features")).as[Instance].rdd

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val featuresSummarizer = {
      val seqOp = (c: MultivariateOnlineSummarizer, v: Instance) => c.add(v.features, v.weight)
      val combOp = (c1: MultivariateOnlineSummarizer, c2: MultivariateOnlineSummarizer) => {
        c1.merge(c2)
      }
      instances.treeAggregate(new MultivariateOnlineSummarizer)(seqOp, combOp)
    }

    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)
    val regParamL2 = $(regParam)

    val costFun = new HuberCostFun(instances, $(fitIntercept), $(standardization),
      featuresStd, regParamL2, $(m))

    /**
     * Huber loss function should be optimized by LBFGS-B, but breeze LBFGS-B has bug and
     * can't be used currently. We figure out one work around with modified LBFGS.
     * Since we known huber loss function is convex in space "\sigma > 0" and the bound
     * "\sigma = 0" is unreachable. The solution of loss function will not be on the bound.
     * We still optimize the loss function by LBFGS but limit the step size when doing line search
     * of each iteration. We should verify the step size generated by each line search in the
     * space "\sigma > 0".
     */
    val optimizer = new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol)) {
      override protected def determineStepSize(
          state: State,
          f: DiffFunction[BDV[Double]],
          dir: BDV[Double]) = {
        val x = state.x
        val grad = state.grad

        val ff = LineSearch.functionFromSearchDirection(f, x, dir)
        val maxAlpha = if (dir(0) >= -1E-10) 1E10 else x(0) / (-dir(0)) - 1E-10
        val search = new BacktrackingLineSearch(initfval = state.value, maxAlpha = maxAlpha)
        var initAlpha = if (state.iter < 1) .5 / brznorm(state.grad) else 1.0
        if (initAlpha > maxAlpha) initAlpha = maxAlpha
        val alpha = search.minimize(ff, initAlpha)

        if (alpha * brznorm(grad) < 1E-10) {
          throw new StepSizeUnderflow
        }
        alpha
      }
    }

    val initialArray = Array.fill(numFeatures + 2)(1.0)
    if (!$(fitIntercept)) initialArray(1) = 0.0
    val initialParameters = Vectors.dense(initialArray)

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialParameters.asBreeze.toDenseVector)

    val parameters = {
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext && !(state != null && state.searchFailed)) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }
      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        throw new SparkException(msg)
      }

      state.x.toArray.clone()
    }

    if (handlePersistence) instances.unpersist()

    val rawCoefficients = parameters.slice(2, parameters.length)
    var i = 0
    while (i < numFeatures) {
      rawCoefficients(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
      i += 1
    }
    val coefficients = Vectors.dense(rawCoefficients)
    val intercept = parameters(1)
    val scale = parameters(0)
    val model = new RobustRegressionModel(uid, coefficients, intercept, scale)
    copyValues(model.setParent(this))
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): RobustRegression = defaultCopy(extra)

}

/**
 * Model produced by [[RobustRegression]].
 */
@Since("2.1.0")
class RobustRegressionModel private[ml] (
     @Since("2.1.0") override val uid: String,
     @Since("2.1.0") val coefficients: Vector,
     @Since("2.1.0") val intercept: Double,
     @Since("2.1.0") val scale: Double)
  extends RegressionModel[Vector, RobustRegressionModel] with LinearRegressionParams {

  override protected def predict(features: Vector): Double = {
    dot(features, coefficients) + intercept
  }

  @Since("2.1.0")
  override def copy(extra: ParamMap): RobustRegressionModel = {
    val newModel = copyValues(new RobustRegressionModel(uid, coefficients, intercept, scale), extra)
    newModel.setParent(parent)
  }
}

/**
 * HuberAggregator computes the gradient and loss for a huber loss function,
 * as used in robust regression for samples in sparse or dense vector in an online fashion.
 *
 * The huber loss function based on:
 * Art B. Owen (2006), A robust hybrid of lasso and ridge regression.
 * ([[http://statweb.stanford.edu/~owen/reports/hhu.pdf]])
 *
 * Two HuberAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * The huber loss function is given by
 * {{{
 *   {min\,} {\sum_{i=1}^n\left(\sigma +
 *   H_m\left(\frac{X_{i}w - y_{i}}{\sigma}\right)\sigma\right) + \alpha {||w||_2}^2}
 * }}}
 * where
 * {{{
 *   H_m(z) = \begin{cases}
 *            z^2, & \text {if } |z| < \epsilon, \\
 *            2\epsilon|z| - \epsilon^2, & \text{otherwise}
 *            \end{cases}
 * }}}
 *
 * @param bcParameters including three part: The scale parameter (sigma), the intercept and
 *                regression coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 * @param bcFeaturesStd The broadcast standard deviation values of the features.
 * @param m The shape parameter to control the amount of robustness.
 */
private class HuberAggregator(
    bcParameters: Broadcast[Vector],
    fitIntercept: Boolean,
    bcFeaturesStd: Broadcast[Array[Double]],
    m: Double) extends Serializable {

  private val parameterLength = bcParameters.value.size

  @transient private lazy val coefficients: Array[Double] =
    bcParameters.value.toArray.slice(2, parameterLength)
  private val intercept: Double = bcParameters.value(1)
  private val sigma: Double = bcParameters.value(0)

  @transient private lazy val featuresStd = bcFeaturesStd.value

  private val dim: Int = coefficients.length

  private var totalCnt: Long = 0L
  private var weightSum: Double = 0.0
  private var lossSum = 0.0
  // Here we optimize loss function over sigma, intercept and coefficients
  private val gradientSumArray = Array.ofDim[Double](parameterLength)

  def count: Long = totalCnt
  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum
  }
  def gradient: Vector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    val result = Vectors.dense(gradientSumArray.clone())
    result
  }

  /**
   * Add a new training instance to this HuberAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This HuberAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(dim == features.size, s"Dimensions mismatch when adding new sample." +
        s" Expecting $dim but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this

      val margin = {
        var sum = 0.0
        features.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            sum += coefficients(index) * (value / featuresStd(index))
          }
        }
        sum + intercept
      }
      val linearLoss = label - margin

      if (math.abs(linearLoss) <= sigma * m) {
        lossSum += weight * (sigma +  math.pow(linearLoss, 2.0) / sigma)

        gradientSumArray(0) += weight * (1.0 - math.pow(linearLoss / sigma, 2.0))
        if (fitIntercept) {
          gradientSumArray(1) += weight * -2.0 * linearLoss / sigma
        }
        features.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            gradientSumArray(index + 2) +=
              weight * -2.0 * linearLoss / sigma * (value / featuresStd(index))
          }
        }
      } else {
        val sign = if (linearLoss >= 0) -1.0 else 1.0
        lossSum += weight * (sigma + 2.0 * m * math.abs(linearLoss) - sigma * m * m)
        gradientSumArray(0) += weight * (1.0 - m * m)
        if (fitIntercept) {
          gradientSumArray(1) += weight * (sign * 2.0 * m)
        }
        features.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            gradientSumArray(index + 2) += weight * sign * 2.0 * m * (value / featuresStd(index))
          }
        }
      }

      totalCnt += 1
      weightSum += weight
      this
    }
  }

  /**
   * Merge another [[HuberAggregator]], and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other HuberAggregator to be merged.
   * @return This HuberAggregator object.
   */
  def merge(other: HuberAggregator): this.type = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"HuberAggregator. Expecting $dim but got ${other.dim}.")

    if (other.weightSum != 0) {
      totalCnt += other.totalCnt
      weightSum += other.weightSum
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      while (i < gradientSumArray.length) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    }
    this
  }
}

/**
 * HuberCostFun implements Breeze's DiffFunction[T] for huber cost.
 * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
 * It's used in Breeze's convex optimization routines.
 */
private class HuberCostFun(
    instances: RDD[Instance],
    fitIntercept: Boolean,
    standardization: Boolean,
    featuresStd: Array[Double],
    regParamL2: Double,
    m: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(parameters: BDV[Double]): (Double, BDV[Double]) = {
    val parametersVector = Vectors.fromBreeze(parameters)
    val bcFeaturesStd = instances.context.broadcast(featuresStd)
    val bcParameters = instances.context.broadcast(parametersVector)

    val huberAggregator = {
      val seqOp = (c: HuberAggregator, instance: Instance) => c.add(instance)
      val combOp = (c1: HuberAggregator, c2: HuberAggregator) => c1.merge(c2)

      instances.treeAggregate(
        new HuberAggregator(bcParameters, fitIntercept, bcFeaturesStd, m))(seqOp, combOp)
    }

    val totalGradientArray = huberAggregator.gradient.toArray

    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      parametersVector.foreachActive { (index, value) =>
        // The first two elements (scale, intercept) don't contribute to the regularization.
        if (index > 1) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value * 2.0
              value * value
            } else {
              if (featuresStd(index - 2) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index - 2) * featuresStd(index - 2))
                totalGradientArray(index) += regParamL2 * temp * 2.0
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      regParamL2 * sum
    }

    (huberAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}
