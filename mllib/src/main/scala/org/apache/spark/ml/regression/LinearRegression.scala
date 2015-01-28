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

import org.apache.spark.mllib.linalg.BLAS.dot
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{norm => brzNorm, DenseVector => BDV, SparseVector => BSV}
import breeze.optimize.{LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import breeze.optimize.{CachedDiffFunction, DiffFunction}

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param.shared.{HasElasticNetParam, HasMaxIter, HasTol}
import org.apache.spark.ml.param.{Params, ParamMap}
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * Params for linear regression.
 */
private[regression] trait LinearRegressionParams extends RegressorParams
  with HasElasticNetParam with HasMaxIter with HasTol

/**
 * :: AlphaComponent ::
 *
 * Linear regression.
 */
@AlphaComponent
class LinearRegression extends Regressor[Vector, LinearRegression, LinearRegressionModel]
  with LinearRegressionParams {

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  def setRegParam(value: Double): this.type = set(regParam, value)
  setRegParam(0.0)

  /**
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   * For 0 < alpha < 1, the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   * @group setParam
   */
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setElasticNetParam(0.0)

  /**
   * Set the maximal number of iterations.
   * Default is 100.
   * @group setParam
   */
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setMaxIter(100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-9.
   * @group setParam
   */
  def setTol(value: Double): this.type = set(tol, value)
  setTol(1E-9)

  override protected def train(dataset: DataFrame, paramMap: ParamMap): LinearRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist instances.
    val instances = extractLabeledPoints(dataset, paramMap).map {
      case LabeledPoint(label: Double, features: Vector) => (label, features)
    }
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) {
      instances.persist(StorageLevel.MEMORY_AND_DISK)
    }

    // TODO: Benchmark if using two MultivariateOnlineSummarizer will be faster
    //       than appending the label into the vector.
    val summary = instances.map { case (label: Double, features: Vector) =>
        Vectors.fromBreeze(features.toBreeze match {
          case dv: BDV[Double] => BDV.vertcat(dv, new BDV[Double](Array(label)))
          case sv: BSV[Double] => BSV.vertcat(sv, new BSV[Double](Array(0), Array(label), 1))
          case v: Any =>
            throw new IllegalArgumentException("Do not support vector type " + v.getClass)
        })}.treeAggregate(new MultivariateOnlineSummarizer)(
        (aggregator, data) => aggregator.add(data),
        (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    val numFeatures = summary.mean.size - 1
    val yMean = summary.mean(numFeatures)
    val yStd = math.sqrt(summary.variance(numFeatures))

    val effectiveRegParam = paramMap(regParam) / yStd
    val effectiveL1RegParam = paramMap(elasticNetParam) * effectiveRegParam
    val effectiveL2RegParam = (1.0 - paramMap(elasticNetParam)) * effectiveRegParam

    val costFun = new LeastSquaresCostFun(
      instances,
      yStd, yMean,
      summary.variance.toArray.slice(0, numFeatures).map(Math.sqrt(_)).toArray,
      summary.mean.toArray.slice(0, numFeatures).toArray,
      effectiveL2RegParam)

    val optimizer = if (paramMap(elasticNetParam) == 0.0 || effectiveRegParam == 0.0) {
      new BreezeLBFGS[BDV[Double]](paramMap(maxIter), 10, paramMap(tol))
    } else {
      new BreezeOWLQN[Int, BDV[Double]](
        paramMap(maxIter), 10, effectiveL1RegParam, paramMap(tol))
    }

    val initialWeights = Vectors.zeros(numFeatures)
    val states =
      optimizer.iterations(new CachedDiffFunction(costFun), initialWeights.toBreeze.toDenseVector)

    var state = states.next()
    val lossHistory = new ArrayBuffer[Double](paramMap(maxIter))
    while(states.hasNext) {
      lossHistory.append(state.value)
      state = states.next()
    }
    lossHistory.append(state.value)

    val weights = {
      val rawWeights = state.x.toArray
      val std = summary.variance.toArray.slice(0, numFeatures).map(Math.sqrt(_)).toArray
      require(rawWeights.size == std.size)

      var i = 0
      while (i < rawWeights.size) {
        rawWeights(i) = if (std(i) != 0.0) rawWeights(i) * yStd / std(i) else 0.0
        i += 1
      }
      Vectors.dense(rawWeights)
    }

    val intercept = yMean - dot(weights, Vectors.dense(summary.mean.toArray.slice(0, numFeatures)))

    if (handlePersistence) {
      instances.unpersist()
    }
    new LinearRegressionModel(this, paramMap, weights, intercept)
  }
}

/**
 * :: AlphaComponent ::
 *
 * Model produced by [[LinearRegression]].
 */
@AlphaComponent
class LinearRegressionModel private[ml] (
    override val parent: LinearRegression,
    override val fittingParamMap: ParamMap,
    val weights: Vector,
    val intercept: Double)
  extends RegressionModel[Vector, LinearRegressionModel]
  with LinearRegressionParams {

  override protected def predict(features: Vector): Double = {
    BLAS.dot(features, weights) + intercept
  }

  override protected def copy(): LinearRegressionModel = {
    val m = new LinearRegressionModel(parent, fittingParamMap, weights, intercept)
    Params.inheritValues(extractParamMap(), this, m)
    m
  }
}

private class LeastSquaresAggregator(
    weights: Vector,
    labelStd: Double,
    labelMean: Double,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var totalCnt: Long = 0
  private var lossSum = 0.0
  private var diffSum = 0.0

  private val (effectiveWeightsArray: Array[Double], offset: Double, dim: Int) = {
    val weightsArray = weights.toArray.clone()
    var sum = 0.0
    var i = 0
    while (i < weights.size) {
      if (featuresStd(i) != 0.0) {
        weightsArray(i) /=  featuresStd(i)
        sum += weightsArray(i) * featuresMean(i)
      } else {
        weightsArray(i) = 0.0
      }
      i += 1
    }
    (weightsArray, -sum, weightsArray.length)
  }

  private val effectiveWeightsVector = Vectors.dense(effectiveWeightsArray)
  private var gradientSumArray: Array[Double] = Array.ofDim[Double](dim)

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

    val diff = dot(data, effectiveWeightsVector) - (label - labelMean) / labelStd + offset

    if (diff != 0) {
      val localGradientSumArray = gradientSumArray
      data.foreachActive { (index, value) =>
        if (featuresStd(index) != 0.0 && value != 0.0) {
          localGradientSumArray(index) += diff * value / featuresStd(index)
        }
      }
      lossSum += diff * diff / 2.0
      diffSum += diff
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
    if (this.totalCnt != 0 && other.totalCnt != 0) {
      require(dim == other.dim, s"Dimensions mismatch when merging with another summarizer. " +
        s"Expecting $dim but got ${other.dim}.")
      totalCnt += other.totalCnt
      lossSum += other.lossSum
      diffSum += other.diffSum

      var i = 0
      val localThisGradientSumArray = gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      while (i < dim) {
        localThisGradientSumArray(i) += localOtherGradientSumArray(i)
        i += 1
      }
    } else if (totalCnt == 0 && other.totalCnt != 0) {
      this.totalCnt = other.totalCnt
      this.lossSum = other.lossSum
      this.diffSum = other.diffSum
      this.gradientSumArray = other.gradientSumArray.clone
    }
    this
  }

  def count: Long = totalCnt

  def loss: Double = lossSum / totalCnt

  def gradient: Vector = {
    val result = Vectors.dense(gradientSumArray.clone)

    val correction = {
      val temp = effectiveWeightsArray.clone
      var i = 0
      while (i < temp.size) {
        temp(i) *= featuresMean(i)
        i += 1
      }
      Vectors.dense(temp)
    }

    axpy(-diffSum, result, correction)
    scal(1.0 / totalCnt, result)
    result
  }
}

private class LeastSquaresCostFun(
    data: RDD[(Double, Vector)],
    labelStd: Double,
    labelMean: Double,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    effectiveL2regParam: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
   val w = Vectors.fromBreeze(weights)

   val leastSquaresAggregator = data.treeAggregate(
     new LeastSquaresAggregator(w, labelStd, labelMean, featuresStd, featuresMean))(
     seqOp = (c, v) => (c, v) match {
       case (aggregator, (label, features)) => aggregator.add(label, features)
     },
     combOp = (c1, c2) => (c1, c2) match {
       case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
     })

   /**
    * regVal is sum of weight squares if it's L2 updater;
    * for other updater, the same logic is followed.
    */
   val norm = brzNorm(weights, 2.0)
   val regVal = 0.5 * effectiveL2regParam * norm * norm

   val loss = leastSquaresAggregator.loss + regVal
   // The following gradientTotal is actually the regularization part of gradient.
   // Will add the gradientSum computed from the data with weights in the next step.
   val gradientTotal = w.copy
   scal(effectiveL2regParam, gradientTotal)

   // gradientTotal = gradient + gradientTotal
   axpy(1.0, leastSquaresAggregator.gradient, gradientTotal)

   (loss, gradientTotal.toBreeze.asInstanceOf[BDV[Double]])
 }
}