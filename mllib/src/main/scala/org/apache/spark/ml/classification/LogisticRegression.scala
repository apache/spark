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

import scala.collection.mutable

import breeze.linalg.{DenseVector => BDV, norm => brzNorm}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
  with HasThreshold

/**
 * :: Experimental ::
 * Logistic regression.
 * Currently, this class only supports binary classification.
 */
@Experimental
class LogisticRegression(override val uid: String)
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams with Logging {

  def this() = this(Identifiable.randomUID("logreg"))

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

  /**
   * Whether to fit an intercept term.
   * Default is true.
   * @group setParam
   * */
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold -> 0.5)

  override protected def train(dataset: DataFrame): LogisticRegressionModel = {
    // Extract columns from data.  If dataset is persisted, do not persist oldDataset.
    val instances = extractLabeledPoints(dataset).map {
      case LabeledPoint(label: Double, features: Vector) => (label, features)
    }
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (summarizer, labelSummarizer) = instances.treeAggregate(
      (new MultivariateOnlineSummarizer, new MultiClassSummarizer))(
        seqOp = (c, v) => (c, v) match {
          case ((summarizer: MultivariateOnlineSummarizer, labelSummarizer: MultiClassSummarizer),
          (label: Double, features: Vector)) =>
            (summarizer.add(features), labelSummarizer.add(label))
      },
        combOp = (c1, c2) => (c1, c2) match {
          case ((summarizer1: MultivariateOnlineSummarizer,
          classSummarizer1: MultiClassSummarizer), (summarizer2: MultivariateOnlineSummarizer,
          classSummarizer2: MultiClassSummarizer)) =>
            (summarizer1.merge(summarizer2), classSummarizer1.merge(classSummarizer2))
      })

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numClasses = histogram.length
    val numFeatures = summarizer.mean.size

    if (numInvalid != 0) {
      val msg = s"Classification labels should be in {0 to ${numClasses - 1} " +
        s"Found $numInvalid invalid labels."
      logError(msg)
      throw new SparkException(msg)
    }

    if (numClasses > 2) {
      val msg = s"Currently, LogisticRegression with ElasticNet in ML package only supports " +
        s"binary classification. Found $numClasses in the input dataset."
      logError(msg)
      throw new SparkException(msg)
    }

    val featuresMean = summarizer.mean.toArray
    val featuresStd = summarizer.variance.toArray.map(math.sqrt)

    val regParamL1 = $(elasticNetParam) * $(regParam)
    val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

    val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept),
      featuresStd, featuresMean, regParamL2)

    val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    } else {
      // Remove the L1 penalization on the intercept
      def regParamL1Fun = (index: Int) => {
        if (index == numFeatures) 0.0 else regParamL1
      }
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
    }

    val initialWeightsWithIntercept =
      Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)

    if ($(fitIntercept)) {
      /**
       * For binary logistic regression, when we initialize the weights as zeros,
       * it will converge faster if we initialize the intercept such that
       * it follows the distribution of the labels.
       *
       * {{{
       * P(0) = 1 / (1 + \exp(b)), and
       * P(1) = \exp(b) / (1 + \exp(b))
       * }}}, hence
       * {{{
       * b = \log{P(1) / P(0)} = \log{count_1 / count_0}
       * }}}
       */
      initialWeightsWithIntercept.toArray(numFeatures)
        = math.log(histogram(1).toDouble / histogram(0).toDouble)
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialWeightsWithIntercept.toBreeze.toDenseVector)

    var state = states.next()
    val lossHistory = mutable.ArrayBuilder.make[Double]

    while (states.hasNext) {
      lossHistory += state.value
      state = states.next()
    }
    lossHistory += state.value

    // The weights are trained in the scaled space; we're converting them back to
    // the original space.
    val weightsWithIntercept = {
      val rawWeights = state.x.toArray.clone()
      var i = 0
      // Note that the intercept in scaled space and original space is the same;
      // as a result, no scaling is needed.
      while (i < numFeatures) {
        rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        i += 1
      }
      Vectors.dense(rawWeights)
    }

    if (handlePersistence) instances.unpersist()

    val (weights, intercept) = if ($(fitIntercept)) {
      (Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
        weightsWithIntercept(weightsWithIntercept.size - 1))
    } else {
      (weightsWithIntercept, 0.0)
    }

    new LogisticRegressionModel(uid, weights.compressed, intercept)
  }

  override def copy(extra: ParamMap): LogisticRegression = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Model produced by [[LogisticRegression]].
 */
@Experimental
class LogisticRegressionModel private[ml] (
    override val uid: String,
    val weights: Vector,
    val intercept: Double)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
  with LogisticRegressionParams {

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)

  /** Margin (rawPrediction) for class label 1.  For binary classification only. */
  private val margin: Vector => Double = (features) => {
    BLAS.dot(features, weights) + intercept
  }

  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  override val numClasses: Int = 2

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[threshold]].
   */
  override protected def predict(features: Vector): Double = {
    if (score(features) > getThreshold) 1 else 0
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        while (i < size) {
          dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
          i += 1
        }
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }

  override protected def predictRaw(features: Vector): Vector = {
    val m = margin(features)
    Vectors.dense(-m, m)
  }

  override def copy(extra: ParamMap): LogisticRegressionModel = {
    copyValues(new LogisticRegressionModel(uid, weights, intercept), extra)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
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

  override protected def probability2prediction(probability: Vector): Double = {
    if (probability(1) > getThreshold) 1 else 0
  }
}

/**
 * MultiClassSummarizer computes the number of distinct labels and corresponding counts,
 * and validates the data to see if the labels used for k class multi-label classification
 * are in the range of {0, 1, ..., k - 1} in a online fashion.
 *
 * Two MultilabelSummarizer can be merged together to have a statistical summary of the
 * corresponding joint dataset.
 */
private[classification] class MultiClassSummarizer extends Serializable {
  private val distinctMap = new mutable.HashMap[Int, Long]
  private var totalInvalidCnt: Long = 0L

  /**
   * Add a new label into this MultilabelSummarizer, and update the distinct map.
   * @param label The label for this data point.
   * @return This MultilabelSummarizer
   */
  def add(label: Double): this.type = {
    if (label - label.toInt != 0.0 || label < 0) {
      totalInvalidCnt += 1
      this
    }
    else {
      val counts: Long = distinctMap.getOrElse(label.toInt, 0L)
      distinctMap.put(label.toInt, counts + 1)
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
        val counts = largeMap.distinctMap.getOrElse(key, 0L)
        largeMap.distinctMap.put(key, counts + value)
    }
    largeMap.totalInvalidCnt += smallMap.totalInvalidCnt
    largeMap
  }

  /** @return The total invalid input counts. */
  def countInvalid: Long = totalInvalidCnt

  /** @return The number of distinct labels in the input dataset. */
  def numClasses: Int = distinctMap.keySet.max + 1

  /** @return The counts of each label in the input dataset. */
  def histogram: Array[Long] = {
    val result = Array.ofDim[Long](numClasses)
    var i = 0
    val len = result.length
    while (i < len) {
      result(i) = distinctMap.getOrElse(i, 0L)
      i += 1
    }
    result
  }
}

/**
 * LogisticAggregator computes the gradient and loss for binary logistic loss function, as used
 * in binary classification for samples in sparse or dense vector in a online fashion.
 *
 * Note that multinomial logistic loss is not supported yet!
 *
 * Two LogisticAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * @param weights The weights/coefficients corresponding to the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class LogisticAggregator(
    weights: Vector,
    numClasses: Int,
    fitIntercept: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var totalCnt: Long = 0L
  private var lossSum = 0.0

  private val weightsArray = weights match {
    case dv: DenseVector => dv.values
    case _ =>
      throw new IllegalArgumentException(
        s"weights only supports dense vector but got type ${weights.getClass}.")
  }

  private val dim = if (fitIntercept) weightsArray.length - 1 else weightsArray.length

  private val gradientSumArray = Array.ofDim[Double](weightsArray.length)

  /**
   * Add a new training data to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param label The label for this data point.
   * @param data The features for one data point in dense/sparse vector format to be added
   *             into this aggregator.
   * @return This LogisticAggregator object.
   */
  def add(label: Double, data: Vector): this.type = {
    require(dim == data.size, s"Dimensions mismatch when adding new sample." +
      s" Expecting $dim but got ${data.size}.")

    val dataSize = data.size

    val localWeightsArray = weightsArray
    val localGradientSumArray = gradientSumArray

    numClasses match {
      case 2 =>
        /**
         * For Binary Logistic Regression.
         */
        val margin = - {
          var sum = 0.0
          data.foreachActive { (index, value) =>
            if (featuresStd(index) != 0.0 && value != 0.0) {
              sum += localWeightsArray(index) * (value / featuresStd(index))
            }
          }
          sum + { if (fitIntercept) localWeightsArray(dim) else 0.0 }
        }

        val multiplier = (1.0 / (1.0 + math.exp(margin))) - label

        data.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            localGradientSumArray(index) += multiplier * (value / featuresStd(index))
          }
        }

        if (fitIntercept) {
          localGradientSumArray(dim) += multiplier
        }

        if (label > 0) {
          // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
          lossSum += MLUtils.log1pExp(margin)
        } else {
          lossSum += MLUtils.log1pExp(margin) - margin
        }
      case _ =>
        new NotImplementedError("LogisticRegression with ElasticNet in ML package only supports " +
          "binary classification for now.")
    }
    totalCnt += 1
    this
  }

  /**
   * Merge another LogisticAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other LogisticAggregator to be merged.
   * @return This LogisticAggregator object.
   */
  def merge(other: LogisticAggregator): this.type = {
    require(dim == other.dim, s"Dimensions mismatch when merging with another " +
      s"LeastSquaresAggregator. Expecting $dim but got ${other.dim}.")

    if (other.totalCnt != 0) {
      totalCnt += other.totalCnt
      lossSum += other.lossSum

      var i = 0
      val localThisGradientSumArray = this.gradientSumArray
      val localOtherGradientSumArray = other.gradientSumArray
      val len = localThisGradientSumArray.length
      while (i < len) {
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
 * LogisticCostFun implements Breeze's DiffFunction[T] for a multinomial logistic loss function,
 * as used in multi-class classification (it is also used in binary logistic regression).
 * It returns the loss and gradient with L2 regularization at a particular point (weights).
 * It's used in Breeze's convex optimization routines.
 */
private class LogisticCostFun(
    data: RDD[(Double, Vector)],
    numClasses: Int,
    fitIntercept: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    regParamL2: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    val w = Vectors.fromBreeze(weights)

    val logisticAggregator = data.treeAggregate(new LogisticAggregator(w, numClasses, fitIntercept,
      featuresStd, featuresMean))(
        seqOp = (c, v) => (c, v) match {
          case (aggregator, (label, features)) => aggregator.add(label, features)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
        })

    // regVal is the sum of weight squares for L2 regularization
    val norm = if (regParamL2 == 0.0) {
      0.0
    } else if (fitIntercept) {
      brzNorm(Vectors.dense(weights.toArray.slice(0, weights.size -1)).toBreeze, 2.0)
    } else {
      brzNorm(weights, 2.0)
    }
    val regVal = 0.5 * regParamL2 * norm * norm

    val loss = logisticAggregator.loss + regVal
    val gradient = logisticAggregator.gradient

    if (fitIntercept) {
      val wArray = w.toArray.clone()
      wArray(wArray.length - 1) = 0.0
      axpy(regParamL2, Vectors.dense(wArray), gradient)
    } else {
      axpy(regParamL2, w, gradient)
    }

    (loss, gradient.toBreeze.asInstanceOf[BDV[Double]])
  }
}
