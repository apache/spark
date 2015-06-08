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

import breeze.linalg.{DenseVector => BDV, norm => brzNorm}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasElasticNetParam, HasMaxIter, HasRegParam, HasTol}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.StatCounter

/**
 * Params for linear regression.
 */
private[regression] trait LinearRegressionParams extends PredictorParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasTol

/**
 * :: Experimental ::
 * Linear regression.
 *
 * The learning objective is to minimize the squared error, with regularization.
 * The specific squared error loss function used is:
 *   L = 1/2n ||A weights - y||^2^
 *
 * This support multiple types of regularization:
 *  - none (a.k.a. ordinary least squares)
 *  - L2 (ridge regression)
 *  - L1 (Lasso)
 *  - L2 + L1 (elastic net)
 */
@Experimental
class LinearRegression(override val uid: String)
  extends Regressor[Vector, LinearRegression, LinearRegressionModel]
  with LinearRegressionParams with Logging {

  def this() = this(Identifiable.randomUID("linReg"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   * For 0 < alpha < 1, the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   * @group setParam
   */
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  override protected def train(dataset: DataFrame): LinearRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist instances.
    val instances = extractLabeledPoints(dataset).map {
      case LabeledPoint(label: Double, features: Vector) => (label, features)
    }
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (summarizer, statCounter) = instances.treeAggregate(
      (new MultivariateOnlineSummarizer, new StatCounter))(
        seqOp = (c, v) => (c, v) match {
          case ((summarizer: MultivariateOnlineSummarizer, statCounter: StatCounter),
          (label: Double, features: Vector)) =>
            (summarizer.add(features), statCounter.merge(label))
      },
        combOp = (c1, c2) => (c1, c2) match {
          case ((summarizer1: MultivariateOnlineSummarizer, statCounter1: StatCounter),
          (summarizer2: MultivariateOnlineSummarizer, statCounter2: StatCounter)) =>
            (summarizer1.merge(summarizer2), statCounter1.merge(statCounter2))
      })

    val numFeatures = summarizer.mean.size
    val yMean = statCounter.mean
    val yStd = math.sqrt(statCounter.variance)

    // If the yStd is zero, then the intercept is yMean with zero weights;
    // as a result, training is not needed.
    if (yStd == 0.0) {
      logWarning(s"The standard deviation of the label is zero, so the weights will be zeros " +
        s"and the intercept will be the mean of the label; as a result, training is not needed.")
      if (handlePersistence) instances.unpersist()
      return new LinearRegressionModel(uid, Vectors.sparse(numFeatures, Seq()), yMean)
    }

    val featuresMean = summarizer.mean.toArray
    val featuresStd = summarizer.variance.toArray.map(math.sqrt)

    // Since we implicitly do the feature scaling when we compute the cost function
    // to improve the convergence, the effective regParam will be changed.
    val effectiveRegParam = $(regParam) / yStd
    val effectiveL1RegParam = $(elasticNetParam) * effectiveRegParam
    val effectiveL2RegParam = (1.0 - $(elasticNetParam)) * effectiveRegParam

    val costFun = new LeastSquaresCostFun(instances, yStd, yMean,
      featuresStd, featuresMean, effectiveL2RegParam)

    val optimizer = if ($(elasticNetParam) == 0.0 || effectiveRegParam == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    } else {
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, effectiveL1RegParam, $(tol))
    }

    val initialWeights = Vectors.zeros(numFeatures)
    val states =
      optimizer.iterations(new CachedDiffFunction(costFun), initialWeights.toBreeze.toDenseVector)

    var state = states.next()
    val lossHistory = mutable.ArrayBuilder.make[Double]

    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value

    // The weights are trained in the scaled space; we're converting them back to
    // the original space.
    val weights = {
      val rawWeights = state.x.toArray.clone()
      var i = 0
      val len = rawWeights.length
      while (i < len) {
        rawWeights(i) *= { if (featuresStd(i) != 0.0) yStd / featuresStd(i) else 0.0 }
        i += 1
      }
      Vectors.dense(rawWeights)
    }

    // The intercept in R's GLMNET is computed using closed form after the coefficients are
    // converged. See the following discussion for detail.
    // http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
    val intercept = yMean - dot(weights, Vectors.dense(featuresMean))
    if (handlePersistence) instances.unpersist()

    // TODO: Converts to sparse format based on the storage, but may base on the scoring speed.
    copyValues(new LinearRegressionModel(uid, weights.compressed, intercept))
  }
}

/**
 * :: Experimental ::
 * Model produced by [[LinearRegression]].
 */
@Experimental
class LinearRegressionModel private[ml] (
    override val uid: String,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[Vector, LinearRegressionModel]
  with LinearRegressionParams {

  override protected def predict(features: Vector): Double = {
    dot(features, weights) + intercept
  }

  override def copy(extra: ParamMap): LinearRegressionModel = {
    copyValues(new LinearRegressionModel(uid, weights, intercept), extra)
  }
}

/**
 * LeastSquaresAggregator computes the gradient and loss for a Least-squared loss function,
 * as used in linear regression for samples in sparse or dense vector in a online fashion.
 *
 * Two LeastSquaresAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * For improving the convergence rate during the optimization process, and also preventing against
 * features with very large variances exerting an overly large influence during model training,
 * package like R's GLMNET performs the scaling to unit variance and removing the mean to reduce
 * the condition number, and then trains the model in scaled space but returns the weights in
 * the original scale. See page 9 in http://cran.r-project.org/web/packages/glmnet/glmnet.pdf
 *
 * However, we don't want to apply the `StandardScaler` on the training dataset, and then cache
 * the standardized dataset since it will create a lot of overhead. As a result, we perform the
 * scaling implicitly when we compute the objective function. The following is the mathematical
 * derivation.
 *
 * Note that we don't deal with intercept by adding bias here, because the intercept
 * can be computed using closed form after the coefficients are converged.
 * See this discussion for detail.
 * http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
 *
 * The objective function in the scaled space is given by
 * {{{
 * L = 1/2n ||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2,
 * }}}
 * where \bar{x_i} is the mean of x_i, \hat{x_i} is the standard deviation of x_i,
 * \bar{y} is the mean of label, and \hat{y} is the standard deviation of label.
 *
 * This can be rewritten as
 * {{{
 * L = 1/2n ||\sum_i (w_i/\hat{x_i})x_i - \sum_i (w_i/\hat{x_i})\bar{x_i} - y / \hat{y}
 *     + \bar{y} / \hat{y}||^2
 *   = 1/2n ||\sum_i w_i^\prime x_i - y / \hat{y} + offset||^2 = 1/2n diff^2
 * }}}
 * where w_i^\prime^ is the effective weights defined by w_i/\hat{x_i}, offset is
 * {{{
 * - \sum_i (w_i/\hat{x_i})\bar{x_i} + \bar{y} / \hat{y}.
 * }}}, and diff is
 * {{{
 * \sum_i w_i^\prime x_i - y / \hat{y} + offset
 * }}}
 *
 * Note that the effective weights and offset don't depend on training dataset,
 * so they can be precomputed.
 *
 * Now, the first derivative of the objective function in scaled space is
 * {{{
 * \frac{\partial L}{\partial\w_i} = diff/N (x_i - \bar{x_i}) / \hat{x_i}
 * }}}
 * However, ($x_i - \bar{x_i}$) will densify the computation, so it's not
 * an ideal formula when the training dataset is sparse format.
 *
 * This can be addressed by adding the dense \bar{x_i} / \har{x_i} terms
 * in the end by keeping the sum of diff. The first derivative of total
 * objective function from all the samples is
 * {{{
 * \frac{\partial L}{\partial\w_i} =
 *     1/N \sum_j diff_j (x_{ij} - \bar{x_i}) / \hat{x_i}
 *   = 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) - diffSum \bar{x_i}) / \hat{x_i})
 *   = 1/N ((\sum_j diff_j x_{ij} / \hat{x_i}) + correction_i)
 * }}},
 * where correction_i = - diffSum \bar{x_i}) / \hat{x_i}
 *
 * A simple math can show that diffSum is actually zero, so we don't even
 * need to add the correction terms in the end. From the definition of diff,
 * {{{
 * diffSum = \sum_j (\sum_i w_i(x_{ij} - \bar{x_i}) / \hat{x_i} - (y_j - \bar{y}) / \hat{y})
 *         = N * (\sum_i w_i(\bar{x_i} - \bar{x_i}) / \hat{x_i} - (\bar{y_j} - \bar{y}) / \hat{y})
 *         = 0
 * }}}
 *
 * As a result, the first derivative of the total objective function only depends on
 * the training dataset, which can be easily computed in distributed fashion, and is
 * sparse format friendly.
 * {{{
 * \frac{\partial L}{\partial\w_i} = 1/N ((\sum_j diff_j x_{ij} / \hat{x_i})
 * }}},
 *
 * @param weights The weights/coefficients corresponding to the features.
 * @param labelStd The standard deviation value of the label.
 * @param labelMean The mean value of the label.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class LeastSquaresAggregator(
    weights: Vector,
    labelStd: Double,
    labelMean: Double,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var totalCnt: Long = 0L
  private var lossSum = 0.0

  private val (effectiveWeightsArray: Array[Double], offset: Double, dim: Int) = {
    val weightsArray = weights.toArray.clone()
    var sum = 0.0
    var i = 0
    val len = weightsArray.length
    while (i < len) {
      if (featuresStd(i) != 0.0) {
        weightsArray(i) /=  featuresStd(i)
        sum += weightsArray(i) * featuresMean(i)
      } else {
        weightsArray(i) = 0.0
      }
      i += 1
    }
    (weightsArray, -sum + labelMean / labelStd, weightsArray.length)
  }

  private val effectiveWeightsVector = Vectors.dense(effectiveWeightsArray)

  private val gradientSumArray = Array.ofDim[Double](dim)

  /**
   * Add a new training data to this LeastSquaresAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param label The label for this data point.
   * @param data The features for one data point in dense/sparse vector format to be added
   *             into this aggregator.
   * @return This LeastSquaresAggregator object.
   */
  def add(label: Double, data: Vector): this.type = {
    require(dim == data.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $dim but got ${data.size}.")

    val diff = dot(data, effectiveWeightsVector) - label / labelStd + offset

    if (diff != 0) {
      val localGradientSumArray = gradientSumArray
      data.foreachActive { (index, value) =>
        if (featuresStd(index) != 0.0 && value != 0.0) {
          localGradientSumArray(index) += diff * value / featuresStd(index)
        }
      }
      lossSum += diff * diff / 2.0
    }

    totalCnt += 1
    this
  }

  /**
   * Merge another LeastSquaresAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other LeastSquaresAggregator to be merged.
   * @return This LeastSquaresAggregator object.
   */
  def merge(other: LeastSquaresAggregator): this.type = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting $dim but got ${other.dim}.")

    if (other.totalCnt != 0) {
      totalCnt += other.totalCnt
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      while (i < dim) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    }
    this
  }

  def count: Long = totalCnt

  def loss: Double = lossSum / totalCnt

  def gradient: Vector = {
    val result = Vectors.dense(gradientSumArray.clone())
    scal(1.0 / totalCnt, result)
    result
  }
}

/**
 * LeastSquaresCostFun implements Breeze's DiffFunction[T] for Least Squares cost.
 * It returns the loss and gradient with L2 regularization at a particular point (weights).
 * It's used in Breeze's convex optimization routines.
 */
private class LeastSquaresCostFun(
    data: RDD[(Double, Vector)],
    labelStd: Double,
    labelMean: Double,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    effectiveL2regParam: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    val w = Vectors.fromBreeze(weights)

    val leastSquaresAggregator = data.treeAggregate(new LeastSquaresAggregator(w, labelStd,
      labelMean, featuresStd, featuresMean))(
        seqOp = (c, v) => (c, v) match {
          case (aggregator, (label, features)) => aggregator.add(label, features)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
        })

    // regVal is the sum of weight squares for L2 regularization
    val norm = brzNorm(weights, 2.0)
    val regVal = 0.5 * effectiveL2regParam * norm * norm

    val loss = leastSquaresAggregator.loss + regVal
    val gradient = leastSquaresAggregator.gradient
    axpy(effectiveL2regParam, w, gradient)

    (loss, gradient.toBreeze.asInstanceOf[BDV[Double]])
  }
}
