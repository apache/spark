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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.regression.{FactorizationMachines, FactorizationMachinesParams}
import org.apache.spark.ml.regression.FactorizationMachines._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

/**
 * Params for FMClassifier.
 */
private[classification] trait FMClassifierParams extends ProbabilisticClassifierParams
  with FactorizationMachinesParams {
}

/**
 * Factorization Machines learning algorithm for classification.
 * It supports normal gradient descent and AdamW solver.
 *
 * The implementation is based upon:
 * <a href="https://www.csie.ntu.edu.tw/~b97053/paper/Rendle2010FM.pdf">
 * S. Rendle. "Factorization machines" 2010</a>.
 *
 * FM is able to estimate interactions even in problems with huge sparsity
 * (like advertising and recommendation system).
 * FM formula is:
 * <blockquote>
 *   $$
 *   \begin{align}
 *   y = \sigma\left( w_0 + \sum\limits^n_{i-1} w_i x_i +
 *     \sum\limits^n_{i=1} \sum\limits^n_{j=i+1} \langle v_i, v_j \rangle x_i x_j \right)
 *   \end{align}
 *   $$
 * </blockquote>
 * First two terms denote global bias and linear term (as same as linear regression),
 * and last term denotes pairwise interactions term. v_i describes the i-th variable
 * with k factors.
 *
 * FM classification model uses logistic loss which can be solved by gradient descent method, and
 * regularization terms like L2 are usually added to the loss function to prevent overfitting.
 *
 * @note Multiclass labels are not currently supported.
 */
@Since("3.0.0")
class FMClassifier @Since("3.0.0") (
    @Since("3.0.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, FMClassifier, FMClassificationModel]
  with FactorizationMachines with FMClassifierParams with DefaultParamsWritable with Logging {

  @Since("3.0.0")
  def this() = this(Identifiable.randomUID("fmc"))

  /**
   * Set the dimensionality of the factors.
   * Default is 8.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFactorSize(value: Int): this.type = set(factorSize, value)

  /**
   * Set whether to fit intercept term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)

  /**
   * Set whether to fit linear term.
   * Default is true.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setFitLinear(value: Boolean): this.type = set(fitLinear, value)

  /**
   * Set the L2 regularization parameter.
   * Default is 0.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setRegParam(value: Double): this.type = set(regParam, value)

  /**
   * Set the mini-batch fraction parameter.
   * Default is 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setMiniBatchFraction(value: Double): this.type = set(miniBatchFraction, value)

  /**
   * Set the standard deviation of initial coefficients.
   * Default is 0.01.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setInitStd(value: Double): this.type = set(initStd, value)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the initial step size for the first step (like learning rate).
   * Default is 1.0.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setStepSize(value: Double): this.type = set(stepSize, value)

  /**
   * Set the convergence tolerance of iterations.
   * Default is 1E-6.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Set the solver algorithm used for optimization.
   * Supported options: "gd", "adamW".
   * Default: "adamW"
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setSolver(value: String): this.type = set(solver, value)

  /**
   * Set the random seed for weight initialization.
   *
   * @group setParam
   */
  @Since("3.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  override protected def train(
      dataset: Dataset[_]): FMClassificationModel = instrumented { instr =>
    val numClasses = 2
    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, factorSize, fitIntercept, fitLinear, regParam,
      miniBatchFraction, initStd, maxIter, stepSize, tol, solver, thresholds)
    instr.logNumClasses(numClasses)

    val numFeatures = getNumFeatures(dataset, $(featuresCol))
    instr.logNumFeatures(numFeatures)

    val handlePersistence = dataset.storageLevel == StorageLevel.NONE

    val data = dataset.select(
      checkClassificationLabels($(labelCol), Some(2)),
      checkNonNanVectors($(featuresCol))
    ).rdd.map { case Row(l: Double, v: Vector) => (l, OldVectors.fromML(v))
    }.setName("training instances")

    if (handlePersistence) data.persist(StorageLevel.MEMORY_AND_DISK)

    val (coefficients, objectiveHistory) = trainImpl(data, numFeatures, LogisticLoss)

    val (intercept, linear, factors) = splitCoefficients(
      coefficients, numFeatures, $(factorSize), $(fitIntercept), $(fitLinear))

    if (handlePersistence) data.unpersist()

    createModel(dataset, intercept, linear, factors, objectiveHistory)
  }

  private def createModel(
    dataset: Dataset[_],
    intercept: Double,
    linear: Vector,
    factors: Matrix,
    objectiveHistory: Array[Double]): FMClassificationModel = {
    val model = copyValues(new FMClassificationModel(uid, intercept, linear, factors))
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)

    val (summaryModel, probabilityColName, predictionColName) = model.findSummaryModel()
    val summary = new FMClassificationTrainingSummaryImpl(
      summaryModel.transform(dataset),
      probabilityColName,
      predictionColName,
      $(labelCol),
      weightColName,
      objectiveHistory)
    model.setSummary(Some(summary))
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): FMClassifier = defaultCopy(extra)
}

@Since("3.0.0")
object FMClassifier extends DefaultParamsReadable[FMClassifier] {

  @Since("3.0.0")
  override def load(path: String): FMClassifier = super.load(path)
}

/**
 * Model produced by [[FMClassifier]]
 */
@Since("3.0.0")
class FMClassificationModel private[classification] (
  @Since("3.0.0") override val uid: String,
  @Since("3.0.0") val intercept: Double,
  @Since("3.0.0") val linear: Vector,
  @Since("3.0.0") val factors: Matrix)
  extends ProbabilisticClassificationModel[Vector, FMClassificationModel]
    with FMClassifierParams with MLWritable
    with HasTrainingSummary[FMClassificationTrainingSummary]{

  @Since("3.0.0")
  override val numClasses: Int = 2

  @Since("3.0.0")
  override val numFeatures: Int = linear.size

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false.
   */
  @Since("3.1.0")
  override def summary: FMClassificationTrainingSummary = super.summary

  /**
   * Evaluates the model on a test dataset.
   *
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("3.1.0")
  def evaluate(dataset: Dataset[_]): FMClassificationSummary = {
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)
    // Handle possible missing or invalid probability or prediction columns
    val (summaryModel, probability, predictionColName) = findSummaryModel()
    new FMClassificationSummaryImpl(summaryModel.transform(dataset),
      probability, predictionColName, $(labelCol), weightColName)
  }

  @Since("3.0.0")
  override def predictRaw(features: Vector): Vector = {
    val rawPrediction = getRawPrediction(features, intercept, linear, factors)
    Vectors.dense(Array(-rawPrediction, rawPrediction))
  }

  override protected def raw2probabilityInPlace(rawPrediction: Vector): Vector = {
    rawPrediction match {
      case dv: DenseVector =>
        dv.values(1) = 1.0 / (1.0 + math.exp(-dv.values(1)))
        dv.values(0) = 1.0 - dv.values(1)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in FMClassificationModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }
  }

  @Since("3.0.0")
  override def copy(extra: ParamMap): FMClassificationModel = {
    copyValues(new FMClassificationModel(uid, intercept, linear, factors), extra)
  }

  @Since("3.0.0")
  override def write: MLWriter =
    new FMClassificationModel.FMClassificationModelWriter(this)

  override def toString: String = {
    s"FMClassificationModel: " +
      s"uid=${super.toString}, numClasses=$numClasses, numFeatures=$numFeatures, " +
      s"factorSize=${$(factorSize)}, fitLinear=${$(fitLinear)}, fitIntercept=${$(fitIntercept)}"
  }
}

@Since("3.0.0")
object FMClassificationModel extends MLReadable[FMClassificationModel] {

  @Since("3.0.0")
  override def read: MLReader[FMClassificationModel] = new FMClassificationModelReader

  @Since("3.0.0")
  override def load(path: String): FMClassificationModel = super.load(path)

  /** [[MLWriter]] instance for [[FMClassificationModel]] */
  private[FMClassificationModel] class FMClassificationModelWriter(
    instance: FMClassificationModel) extends MLWriter with Logging {

    private case class Data(
      intercept: Double,
      linear: Vector,
      factors: Matrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.intercept, instance.linear, instance.factors)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FMClassificationModelReader extends MLReader[FMClassificationModel] {

    private val className = classOf[FMClassificationModel].getName

    override def load(path: String): FMClassificationModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.format("parquet").load(dataPath)

      val Row(intercept: Double, linear: Vector, factors: Matrix) =
        data.select("intercept", "linear", "factors").head()
      val model = new FMClassificationModel(metadata.uid, intercept, linear, factors)
      metadata.getAndSetParams(model)
      model
    }
  }
}

/**
 * Abstraction for FMClassifier results for a given model.
 */
sealed trait FMClassificationSummary extends BinaryClassificationSummary

/**
 * Abstraction for FMClassifier training results.
 */
sealed trait FMClassificationTrainingSummary extends FMClassificationSummary with TrainingSummary

/**
 * FMClassifier results for a given model.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the probability of each instance.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 */
private class FMClassificationSummaryImpl(
    @transient override val predictions: DataFrame,
    override val scoreCol: String,
    override val predictionCol: String,
    override val labelCol: String,
    override val weightCol: String)
  extends FMClassificationSummary

/**
 * FMClassifier training results.
 *
 * @param predictions dataframe output by the model's `transform` method.
 * @param scoreCol field in "predictions" which gives the probability of each instance.
 * @param predictionCol field in "predictions" which gives the prediction for a data instance as a
 *                      double.
 * @param labelCol field in "predictions" which gives the true label of each instance.
 * @param weightCol field in "predictions" which gives the weight of each instance.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
private class FMClassificationTrainingSummaryImpl(
    predictions: DataFrame,
    scoreCol: String,
    predictionCol: String,
    labelCol: String,
    weightCol: String,
    override val objectiveHistory: Array[Double])
  extends FMClassificationSummaryImpl(
    predictions, scoreCol, predictionCol, labelCol, weightCol)
    with FMClassificationTrainingSummary
