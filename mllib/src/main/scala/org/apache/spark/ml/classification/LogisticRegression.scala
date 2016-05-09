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
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.linalg.BLAS._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.storage.StorageLevel

/**
 * Params for logistic regression.
 */
private[classification] trait LogisticRegressionParams extends ProbabilisticClassifierParams
  with HasRegParam with HasElasticNetParam with HasMaxIter with HasFitIntercept with HasTol
  with HasStandardization with HasWeightCol with HasThreshold {

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
@Since("1.2.0")
@Experimental
class LogisticRegression @Since("1.2.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
  with LogisticRegressionParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("logreg"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("1.2.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set the ElasticNet mixing parameter.
   * For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty.
   * For 0 < alpha < 1, the penalty is a combination of L1 and L2.
   * Default is 0.0 which is an L2 penalty.
   * @group setParam
   */
  @Since("1.4.0")
  def setElasticNetParam(value: Double): this.type = set(elasticNetParam, value)
  setDefault(elasticNetParam -> 0.0)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("1.2.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("1.4.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Whether to fit an intercept term.
   * Default is true.
   * @group setParam
   */
  @Since("1.4.0")
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
  @Since("1.5.0")
  def setStandardization(value: Boolean): this.type = set(standardization, value)
  setDefault(standardization -> true)

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  @Since("1.5.0")
  override def getThreshold: Double = super.getThreshold

  /**
   * Whether to over-/under-sample training instances according to the given weights in weightCol.
   * If not set or empty String, all instances are treated equally (weight 1.0).
   * Default is not set, so all instances have weight one.
   * @group setParam
   */
  @Since("1.6.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)

  @Since("1.5.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("1.5.0")
  override def getThresholds: Array[Double] = super.getThresholds

  private var optInitialModel: Option[LogisticRegressionModel] = None

  /** @group setParam */
  private[spark] def setInitialModel(model: LogisticRegressionModel): this.type = {
    this.optInitialModel = Some(model)
    this
  }

  override protected[spark] def train(dataset: Dataset[_]): LogisticRegressionModel = {
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    train(dataset, handlePersistence)
  }

  protected[spark] def train(dataset: Dataset[_], handlePersistence: Boolean):
      LogisticRegressionModel = {
    val w = if (!isDefined(weightCol) || $(weightCol).isEmpty) lit(1.0) else col($(weightCol))
    val instances: RDD[Instance] =
      dataset.select(col($(labelCol)).cast(DoubleType), w, col($(featuresCol))).rdd.map {
        case Row(label: Double, weight: Double, features: Vector) =>
          Instance(label, weight, features)
      }

    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val instr = Instrumentation.create(this, instances)
    instr.logParams(regParam, elasticNetParam, standardization, threshold,
      maxIter, tol, fitIntercept)

    val (summarizer, labelSummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight), c._2.add(instance.label, instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultiClassSummarizer),
        c2: (MultivariateOnlineSummarizer, MultiClassSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        new MultivariateOnlineSummarizer, new MultiClassSummarizer)(seqOp, combOp)
    }

    val histogram = labelSummarizer.histogram
    val numInvalid = labelSummarizer.countInvalid
    val numClasses = histogram.length
    val numFeatures = summarizer.mean.size

    instr.logNumClasses(numClasses)
    instr.logNumFeatures(numFeatures)

    val (coefficients, intercept, objectiveHistory) = {
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
      } else if ($(fitIntercept) && numClasses == 2 && histogram(0) == 0.0) {
        logWarning(s"All labels are one and fitIntercept=true, so the coefficients will be " +
          s"zeros and the intercept will be positive infinity; as a result, " +
          s"training is not needed.")
        (Vectors.sparse(numFeatures, Seq()), Double.PositiveInfinity, Array.empty[Double])
      } else if ($(fitIntercept) && numClasses == 1) {
        logWarning(s"All labels are zero and fitIntercept=true, so the coefficients will be " +
          s"zeros and the intercept will be negative infinity; as a result, " +
          s"training is not needed.")
        (Vectors.sparse(numFeatures, Seq()), Double.NegativeInfinity, Array.empty[Double])
      } else {
        if (!$(fitIntercept) && numClasses == 2 && histogram(0) == 0.0) {
          logWarning(s"All labels are one and fitIntercept=false. It's a dangerous ground, " +
            s"so the algorithm may not converge.")
        } else if (!$(fitIntercept) && numClasses == 1) {
          logWarning(s"All labels are zero and fitIntercept=false. It's a dangerous ground, " +
            s"so the algorithm may not converge.")
        }

        val featuresMean = summarizer.mean.toArray
        val featuresStd = summarizer.variance.toArray.map(math.sqrt)

        val regParamL1 = $(elasticNetParam) * $(regParam)
        val regParamL2 = (1.0 - $(elasticNetParam)) * $(regParam)

        val costFun = new LogisticCostFun(instances, numClasses, $(fitIntercept),
          $(standardization), featuresStd, featuresMean, regParamL2)

        val optimizer = if ($(elasticNetParam) == 0.0 || $(regParam) == 0.0) {
          new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
        } else {
          val standardizationParam = $(standardization)
          def regParamL1Fun = (index: Int) => {
            // Remove the L1 penalization on the intercept
            if (index == numFeatures) {
              0.0
            } else {
              if (standardizationParam) {
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

        val initialCoefficientsWithIntercept =
          Vectors.zeros(if ($(fitIntercept)) numFeatures + 1 else numFeatures)

        if (optInitialModel.isDefined && optInitialModel.get.coefficients.size != numFeatures) {
          val vecSize = optInitialModel.get.coefficients.size
          logWarning(
            s"Initial coefficients will be ignored!! As its size $vecSize did not match the " +
            s"expected size $numFeatures")
        }

        if (optInitialModel.isDefined && optInitialModel.get.coefficients.size == numFeatures) {
          val initialCoefficientsWithInterceptArray = initialCoefficientsWithIntercept.toArray
          optInitialModel.get.coefficients.foreachActive { case (index, value) =>
            initialCoefficientsWithInterceptArray(index) = value
          }
          if ($(fitIntercept)) {
            initialCoefficientsWithInterceptArray(numFeatures) == optInitialModel.get.intercept
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
          initialCoefficientsWithIntercept.toArray(numFeatures) = math.log(
            histogram(1) / histogram(0))
        }

        val states = optimizer.iterations(new CachedDiffFunction(costFun),
          initialCoefficientsWithIntercept.toBreeze.toDenseVector)

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
           The coefficients are trained in the scaled space; we're converting them back to
           the original space.
           Note that the intercept in scaled space and original space is the same;
           as a result, no scaling is needed.
         */
        val rawCoefficients = state.x.toArray.clone()
        var i = 0
        while (i < numFeatures) {
          rawCoefficients(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
          i += 1
        }

        if ($(fitIntercept)) {
          (Vectors.dense(rawCoefficients.dropRight(1)).compressed, rawCoefficients.last,
            arrayBuilder.result())
        } else {
          (Vectors.dense(rawCoefficients).compressed, 0.0, arrayBuilder.result())
        }
      }
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LogisticRegressionModel(uid, coefficients, intercept))
    val (summaryModel, probabilityColName) = model.findSummaryModelAndProbabilityCol()
    val logRegSummary = new BinaryLogisticRegressionTrainingSummary(
      summaryModel.transform(dataset),
      probabilityColName,
      $(labelCol),
      $(featuresCol),
      objectiveHistory)
    val m = model.setSummary(logRegSummary)
    instr.logSuccess(m)
    m
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LogisticRegression = defaultCopy(extra)
}

@Since("1.6.0")
object LogisticRegression extends DefaultParamsReadable[LogisticRegression] {

  @Since("1.6.0")
  override def load(path: String): LogisticRegression = super.load(path)
}

/**
 * :: Experimental ::
 * Model produced by [[LogisticRegression]].
 */
@Since("1.4.0")
@Experimental
class LogisticRegressionModel private[spark] (
    @Since("1.4.0") override val uid: String,
    @Since("1.6.0") val coefficients: Vector,
    @Since("1.3.0") val intercept: Double)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
  with LogisticRegressionParams with MLWritable {

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
    BLAS.dot(features, coefficients) + intercept
  }

  /** Score (probability) for class label 1.  For binary classification only. */
  private val score: Vector => Double = (features) => {
    val m = margin(features)
    1.0 / (1.0 + math.exp(-m))
  }

  @Since("1.6.0")
  override val numFeatures: Int = coefficients.size

  @Since("1.3.0")
  override val numClasses: Int = 2

  private var trainingSummary: Option[LogisticRegressionTrainingSummary] = None

  /**
   * Gets summary of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("1.5.0")
  def summary: LogisticRegressionTrainingSummary = trainingSummary.getOrElse {
    throw new SparkException("No training summary available for this LogisticRegressionModel")
  }

  /**
   * If the probability column is set returns the current model and probability column,
   * otherwise generates a new column and sets it as the probability column on a new copy
   * of the current model.
   */
  private[classification] def findSummaryModelAndProbabilityCol():
      (LogisticRegressionModel, String) = {
    $(probabilityCol) match {
      case "" =>
        val probabilityColName = "probability_" + java.util.UUID.randomUUID.toString
        (copy(ParamMap.empty).setProbabilityCol(probabilityColName), probabilityColName)
      case p => (this, p)
    }
  }

  private[classification] def setSummary(
      summary: LogisticRegressionTrainingSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /** Indicates whether a training summary exists for this model instance. */
  @Since("1.5.0")
  def hasSummary: Boolean = trainingSummary.isDefined

  /**
   * Evaluates the model on a test dataset.
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_]): LogisticRegressionSummary = {
    // Handle possible missing or invalid prediction columns
    val (summaryModel, probabilityColName) = findSummaryModelAndProbabilityCol()
    new BinaryLogisticRegressionSummary(summaryModel.transform(dataset),
      probabilityColName, $(labelCol), $(featuresCol))
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

  @Since("1.4.0")
  override def copy(extra: ParamMap): LogisticRegressionModel = {
    val newModel = copyValues(new LogisticRegressionModel(uid, coefficients, intercept), extra)
    if (trainingSummary.isDefined) newModel.setSummary(trainingSummary.get)
    newModel.setParent(parent)
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
        intercept: Double,
        coefficients: Vector)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: numClasses, numFeatures, intercept, coefficients
      val data = Data(instance.numClasses, instance.numFeatures, instance.intercept,
        instance.coefficients)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class LogisticRegressionModelReader
    extends MLReader[LogisticRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LogisticRegressionModel].getName

    override def load(path: String): LogisticRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.format("parquet").load(dataPath)
        .select("numClasses", "numFeatures", "intercept", "coefficients").head()
      // We will need numClasses, numFeatures in the future for multinomial logreg support.
      // val numClasses = data.getInt(0)
      // val numFeatures = data.getInt(1)
      val intercept = data.getDouble(2)
      val coefficients = data.getAs[Vector](3)
      val model = new LogisticRegressionModel(metadata.uid, coefficients, intercept)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
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
  // The first element of value in distinctMap is the actually number of instances,
  // and the second element of value is sum of the weights.
  private val distinctMap = new mutable.HashMap[Int, (Long, Double)]
  private var totalInvalidCnt: Long = 0L

  /**
   * Add a new label into this MultilabelSummarizer, and update the distinct map.
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
 * Abstraction for multinomial Logistic Regression Training results.
 * Currently, the training summary ignores the training weights except
 * for the objective trace.
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

  /** Dataframe output by the model's `transform` method. */
  def predictions: DataFrame

  /** Field in "predictions" which gives the probability of each class as a vector. */
  def probabilityCol: String

  /** Field in "predictions" which gives the true label of each instance (if available). */
  def labelCol: String

  /** Field in "predictions" which gives the features of each instance as a vector. */
  def featuresCol: String

}

/**
 * :: Experimental ::
 * Logistic regression training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
@Experimental
@Since("1.5.0")
class BinaryLogisticRegressionTrainingSummary private[classification] (
    predictions: DataFrame,
    probabilityCol: String,
    labelCol: String,
    featuresCol: String,
    @Since("1.5.0") val objectiveHistory: Array[Double])
  extends BinaryLogisticRegressionSummary(predictions, probabilityCol, labelCol, featuresCol)
  with LogisticRegressionTrainingSummary {

}

/**
 * :: Experimental ::
 * Binary Logistic regression results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param probabilityCol field in "predictions" which gives the probability of
 *                       each class as a vector.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param featuresCol field in "predictions" which gives the features of each instance as a vector.
 */
@Experimental
@Since("1.5.0")
class BinaryLogisticRegressionSummary private[classification] (
    @Since("1.5.0") @transient override val predictions: DataFrame,
    @Since("1.5.0") override val probabilityCol: String,
    @Since("1.5.0") override val labelCol: String,
    @Since("1.6.0") override val featuresCol: String) extends LogisticRegressionSummary {


  private val sparkSession = predictions.sparkSession
  import sparkSession.implicits._

  /**
   * Returns a BinaryClassificationMetrics object.
   */
  // TODO: Allow the user to vary the number of bins using a setBins method in
  // BinaryClassificationMetrics. For now the default is set to 100.
  @transient private val binaryMetrics = new BinaryClassificationMetrics(
    predictions.select(probabilityCol, labelCol).rdd.map {
      case Row(score: Vector, label: Double) => (score(1), label)
    }, 100
  )

  /**
   * Returns the receiver operating characteristic (ROC) curve,
   * which is an Dataframe having two fields (FPR, TPR)
   * with (0.0, 0.0) prepended and (1.0, 1.0) appended to it.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
   * @see http://en.wikipedia.org/wiki/Receiver_operating_characteristic
   */
  @Since("1.5.0")
  @transient lazy val roc: DataFrame = binaryMetrics.roc().toDF("FPR", "TPR")

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  lazy val areaUnderROC: Double = binaryMetrics.areaUnderROC()

  /**
   * Returns the precision-recall curve, which is an Dataframe containing
   * two fields recall, precision with (0.0, 1.0) prepended to it.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val pr: DataFrame = binaryMetrics.pr().toDF("recall", "precision")

  /**
   * Returns a dataframe with two fields (threshold, F-Measure) curve with beta = 1.0.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
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
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
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
   * Note: This ignores instance weights (setting all to 1.0) from [[LogisticRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  @transient lazy val recallByThreshold: DataFrame = {
    binaryMetrics.recallByThreshold().toDF("threshold", "recall")
  }
}

/**
 * LogisticAggregator computes the gradient and loss for binary logistic loss function, as used
 * in binary classification for instances in sparse or dense vector in a online fashion.
 *
 * Note that multinomial logistic loss is not supported yet!
 *
 * Two LogisticAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * @param coefficients The coefficients corresponding to the features.
 * @param numClasses the number of possible outcomes for k classes classification problem in
 *                   Multinomial Logistic Regression.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class LogisticAggregator(
    coefficients: Vector,
    numClasses: Int,
    fitIntercept: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var weightSum = 0.0
  private var lossSum = 0.0

  private val coefficientsArray = coefficients match {
    case dv: DenseVector => dv.values
    case _ =>
      throw new IllegalArgumentException(
        s"coefficients only supports dense vector but got type ${coefficients.getClass}.")
  }

  private val dim = if (fitIntercept) coefficientsArray.length - 1 else coefficientsArray.length

  private val gradientSumArray = Array.ofDim[Double](coefficientsArray.length)

  /**
   * Add a new training instance to this LogisticAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This LogisticAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(dim == features.size, s"Dimensions mismatch when adding new instance." +
        s" Expecting $dim but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, $weight has to be >= 0.0")

      if (weight == 0.0) return this

      val localCoefficientsArray = coefficientsArray
      val localGradientSumArray = gradientSumArray

      numClasses match {
        case 2 =>
          // For Binary Logistic Regression.
          val margin = - {
            var sum = 0.0
            features.foreachActive { (index, value) =>
              if (featuresStd(index) != 0.0 && value != 0.0) {
                sum += localCoefficientsArray(index) * (value / featuresStd(index))
              }
            }
            sum + {
              if (fitIntercept) localCoefficientsArray(dim) else 0.0
            }
          }

          val multiplier = weight * (1.0 / (1.0 + math.exp(margin)) - label)

          features.foreachActive { (index, value) =>
            if (featuresStd(index) != 0.0 && value != 0.0) {
              localGradientSumArray(index) += multiplier * (value / featuresStd(index))
            }
          }

          if (fitIntercept) {
            localGradientSumArray(dim) += multiplier
          }

          if (label > 0) {
            // The following is equivalent to log(1 + exp(margin)) but more numerically stable.
            lossSum += weight * MLUtils.log1pExp(margin)
          } else {
            lossSum += weight * (MLUtils.log1pExp(margin) - margin)
          }
        case _ =>
          new NotImplementedError("LogisticRegression with ElasticNet in ML package " +
            "only supports binary classification for now.")
      }
      weightSum += weight
      this
    }
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

    if (other.weightSum != 0.0) {
      weightSum += other.weightSum
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

  def loss: Double = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    lossSum / weightSum
  }

  def gradient: Vector = {
    require(weightSum > 0.0, s"The effective number of instances should be " +
      s"greater than 0.0, but $weightSum.")
    val result = Vectors.dense(gradientSumArray.clone())
    scal(1.0 / weightSum, result)
    result
  }
}

/**
 * LogisticCostFun implements Breeze's DiffFunction[T] for a multinomial logistic loss function,
 * as used in multi-class classification (it is also used in binary logistic regression).
 * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
 * It's used in Breeze's convex optimization routines.
 */
private class LogisticCostFun(
    instances: RDD[Instance],
    numClasses: Int,
    fitIntercept: Boolean,
    standardization: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    regParamL2: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val numFeatures = featuresStd.length
    val coeffs = Vectors.fromBreeze(coefficients)

    val logisticAggregator = {
      val seqOp = (c: LogisticAggregator, instance: Instance) => c.add(instance)
      val combOp = (c1: LogisticAggregator, c2: LogisticAggregator) => c1.merge(c2)

      instances.treeAggregate(
        new LogisticAggregator(coeffs, numClasses, fitIntercept, featuresStd, featuresMean)
      )(seqOp, combOp)
    }

    val totalGradientArray = logisticAggregator.gradient.toArray

    // regVal is the sum of coefficients squares excluding intercept for L2 regularization.
    val regVal = if (regParamL2 == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.foreachActive { (index, value) =>
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
