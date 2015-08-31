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

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
  with HasStandardization with HasThreshold {

  /**
   * Set threshold in binary classification, in range [0, 1].
   *
   * If the estimated probability of class label 1 is > threshold, then predict 1, else 0.
   * A high threshold encourages the model to predict 0 more often;
   * a low threshold encourages the model to predict 1 more often.
   *
   * Note: Calling this with threshold p is equivalent to calling `setThresholds(Array(1-p, p))`.
   *       When [[setThreshold()]] is called, any user-set value for [[thresholds]] will be cleared.
   *       If both [[threshold]] and [[thresholds]] are set in a ParamMap, then they must be
   *       equivalent.
   *
   * Default is 0.5.
   * @group setParam
   */
  def setThreshold(value: Double): this.type = {
    if (isSet(thresholds)) clear(thresholds)
    set(threshold, value)
  }

  /**
   * Get threshold for binary classification.
   *
   * If [[threshold]] is set, returns that value.
   * Otherwise, if [[thresholds]] is set with length 2 (i.e., binary classification),
   * this returns the equivalent threshold: {{{1 / (1 + thresholds(0) / thresholds(1))}}}.
   * Otherwise, returns [[threshold]] default value.
   *
   * @group getParam
   * @throws IllegalArgumentException if [[thresholds]] is set to an array of length other than 2.
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
   * predicting each class. Array must have length equal to the number of classes, with values >= 0.
   * The class with largest value p/t is predicted, where p is the original probability of that
   * class and t is the class' threshold.
   *
   * Note: When [[setThresholds()]] is called, any user-set value for [[threshold]] will be cleared.
   *       If both [[threshold]] and [[thresholds]] are set in a ParamMap, then they must be
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
   * If [[thresholds]] is set, return its value.
   * Otherwise, if [[threshold]] is set, return the equivalent thresholds for binary
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
   * If [[threshold]] and [[thresholds]] are both set, ensures they are consistent.
   * @throws IllegalArgumentException if [[threshold]] and [[thresholds]] are not equivalent
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

  override def validateParams(): Unit = {
    checkThresholdConsistency()
  }
}

/**
 * :: Experimental ::
 * Logistic regression.
 * Currently, this class only supports binary classification.  It will support multiclass
 * in the future.
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
   */
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Whether to standardize the training features before fitting the model.
   * The coefficients of models will be always returned on the original scale,
   * so it will be transparent for users. Note that with/without standardization,
   * the models should be always converged to the same solution when no regularization
   * is applied. In R's GLMNET package, the default behavior is true as well.
   * Default is true.
   * @group setParam
   */
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  override def getThreshold: Double = super.getThreshold

  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  override def getThresholds: Array[Double] = super.getThresholds

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

    val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept), $(standardization),
      featuresStd, featuresMean, regParamL2)

    val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    } else {
      def regParamL1Fun = (index: Int) => {
        // Remove the L1 penalization on the intercept
        if (index == numFeatures) {
          0.0
        } else {
          if ($(standardization)) {
            regParamL1
          } else {
            // If `standardization` is false, we still standardize the data
            // to improve the rate of convergence; as a result, we have to
            // perform this reverse standardization by penalizing each component
            // differently to get effectively the same objective function when
            // the training dataset is not standardized.
            if (featuresStd(index) != 0.0) regParamL1 / featuresStd(index) else 0.0
          }
        }
      }
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, regParamL1Fun, $(tol))
    }

    val initialWeightsWithIntercept =
      Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)

    if ($(fitIntercept)) {
      /*
         For binary logistic regression, when we initialize the weights as zeros,
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
      initialWeightsWithIntercept.toArray(numFeatures)
        = math.log(histogram(1).toDouble / histogram(0).toDouble)
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialWeightsWithIntercept.toBreeze.toDenseVector)

    val (weights, intercept, objectiveHistory) = {
      /*
         Note that in Logistic Regression, the objective history (loss + regularization)
         is log-likelihood which is invariance under feature standardization. As a result,
         the objective history from optimizer is the same as the one in the original space.
       */
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
        state = states.next()
        arrayBuilder += state.adjustedValue
      }

      if (state == null) {
        val msg = s"${optimizer.getClass.getName} failed."
        logError(msg)
        throw new SparkException(msg)
      }

      /*
         The weights are trained in the scaled space; we're converting them back to
         the original space.
         Note that the intercept in scaled space and original space is the same;
         as a result, no scaling is needed.
       */
      val rawWeights = state.x.toArray.clone()
      var i = 0
      while (i < numFeatures) {
        rawWeights(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
        i += 1
      }

      if ($(fitIntercept)) {
        (Vectors.dense(rawWeights.dropRight(1)).compressed, rawWeights.last, arrayBuilder.result())
      } else {
        (Vectors.dense(rawWeights).compressed, 0.0, arrayBuilder.result())
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LogisticRegressionModel(uid, weights, intercept))
    val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
      model.transform(dataset),
      $(probabilityCol),
      $(labelCol),
      objectiveHistory)
    model.setSummary(logRegSummary)
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

  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  override def getThreshold: Double = super.getThreshold

  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  override def getThresholds: Array[Double] = super.getThresholds

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

  private var trainingSummary: Option[LogisticRegressionTrainingSummary] = None

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  def summary: LogisticRegressionTrainingSummary = trainingSummary match {
    case Some(summ) => summ
    case None =>
      throw new SparkException(
        "No training summary available for this LogisticRegressionModel",
        new NullPointerException())
  }

  private[classification] def setSummary(
      summary: LogisticRegressionTrainingSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /** Indicates whether a training summary exists for this model instance. */
  def hasSummary: Boolean = trainingSummary.isDefined

  /**
   * Evaluates the model on a testset.
   * @param dataset Test dataset to evaluate model on.
   */
  // TODO: decide on a good name before exposing to public API
  private[classification] def evaluate(dataset: DataFrame): LogisticRegressionSummary = {
    new BinaryLogisticRegressionSummary(this.transform(dataset), $(probabilityCol), $(labelCol))
  }

  /**
   * Predict label for the given feature vector.
   * The behavior of this can be adjusted using [[thresholds]].
   */
  override protected def predict(features: Vector): Double = {
    // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
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
    copyValues(new LogisticRegressionModel(uid, weights, intercept), extra).setParent(parent)
  }

  override protected def raw2prediction(rawPrediction: Vector): Double = {
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

  override protected def probability2prediction(probability: Vector): Double = {
    // Note: We should use getThreshold instead of $(threshold) since getThreshold is overridden.
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
 * Abstraction for multinomial Logistic Regression Training results.
 */
sealed trait LogisticRegressionTrainingSummary extends LogisticRegressionSummary {

  /** objective function (scaled loss + regularization) at each iteration. */
  def objectiveHistory: Array[Double]

  /** Number of training iterations until termination */
  def totalIterations: Int = objectiveHistory.length

}

/**
 * Abstraction for Logistic Regression Results for a given model.
 */
sealed trait LogisticRegressionSummary extends Serializable {

  /** Dataframe outputted by the model's `transform` method. */
  def predictions: DataFrame

  /** Field in "predictions" which gives the calibrated probability of each sample as a vector. */
  def probabilityCol: String

  /** Field in "predictions" which gives the the true label of each sample. */
  def labelCol: String

}

/**
 * :: Experimental ::
 * Logistic regression training results.
 * @param predictions dataframe outputted by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the calibrated probability of
 *                       each sample as a vector.
 * @param labelCol field in "predictions" which gives the true label of each sample.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
@Experimental
class BinaryLogisticRegressionTrainingSummary private[classification] (
    predictions: DataFrame,
    probabilityCol: String,
    labelCol: String,
    val objectiveHistory: Array[Double])
  extends BinaryLogisticRegressionSummary(predictions, probabilityCol, labelCol)
  with LogisticRegressionTrainingSummary {

}

/**
 * :: Experimental ::
 * Binary Logistic regression results for a given model.
 * @param predictions dataframe outputted by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the calibrated probability of
 *                       each sample.
 * @param labelCol field in "predictions" which gives the true label of each sample.
 */
@Experimental
class BinaryLogisticRegressionSummary private[classification] (
    @transient override val predictions: DataFrame,
    override val probabilityCol: String,
    override val labelCol: String) extends LogisticRegressionSummary {

  private val sqlContext = predictions.sqlContext
  import sqlContext.implicits._

  /**
   * Returns a BinaryClassificationMetrics object.
   */
  // TODO: Allow the user to vary the number of bins using a setBins method in
  // BinaryClassificationMetrics. For now the default is set to 100.
  @transient private val binaryMetrics = new BinaryClassificationMetrics(
    predictions.select(probabilityCol, labelCol).map {
      case Row(score: Vector, label: Double) => (score(1), label)
    }, 100
  )

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is an Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   */
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   */
  @transient lazy val fMeasureByThreshold: DataFrame = {
    binaryMetrics.fMeasureByThreshold().toDF("threshold", "F-Measure")
  }

  /**
   * Returns a dataframe with two fields (threshold, precision) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the precision.
   */
  @transient lazy val precisionByThreshold: DataFrame = {
    binaryMetrics.precisionByThreshold().toDF("threshold", "precision")
  }

  /**
   * Returns a dataframe with two fields (threshold, recall) curve.
   * Every possible probability obtained in transforming the dataset are used
   * as thresholds used in calculating the recall.
   */
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
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

    val localWeightsArray = weightsArray
    val localGradientSumArray = gradientSumArray

    numClasses match {
      case 2 =>
        // For Binary Logistic Regression.
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
    standardization: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    regParamL2: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
    val numFeatures = featuresStd.length
    val w = Vectors.fromBreeze(weights)

    val logisticAggregator = data.treeAggregate(new LogisticAggregator(w, numClasses, fitIntercept,
      featuresStd, featuresMean))(
        seqOp = (c, v) => (c, v) match {
          case (aggregator, (label, features)) => aggregator.add(label, features)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
        })

    val totalGradientArray = logisticAggregator.gradient.toArray

    // regVal is the sum of weight squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      w.foreachActive { (index, value) =>
        // If `fitIntercept` is true, the last term which is intercept doesn't
        // contribute to the regularization.
        if (index != numFeatures) {
          // The following code will compute the loss of the regularization; also
          // the gradient of the regularization, and add back to totalGradientArray.
          sum += {
            if (standardization) {
              totalGradientArray(index) += regParamL2 * value
              value * value
            } else {
              if (featuresStd(index) != 0.0) {
                // If `standardization` is false, we still standardize the data
                // to improve the rate of convergence; as a result, we have to
                // perform this reverse standardization by penalizing each component
                // differently to get effectively the same objective function when
                // the training dataset is not standardized.
                val temp = value / (featuresStd(index) * featuresStd(index))
                totalGradientArray(index) += regParamL2 * temp
                value * temp
              } else {
                0.0
              }
            }
          }
        }
      }
      0.5 * regParamL2 * sum
    }

    (logisticAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}
