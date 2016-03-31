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

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}
import breeze.stats.distributions.StudentsT
import org.apache.hadoop.fs.Path

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.optim.WeightedLeastSquares
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.BLAS._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * Params for linear regression.
 */
private[regression] trait LinearRegressionParams extends PredictorParams
    with HasRegParam with HasElasticNetParam with HasMaxIter with HasTol
    with HasFitIntercept with HasStandardization with HasWeightCol with HasSolver

/**
 * :: Experimental ::
 * Linear regression.
 *
 * The learning objective is to minimize the squared error, with regularization.
 * The specific squared error loss function used is:
 *   L = 1/2n ||A coefficients - y||^2^
 *
 * This support multiple types of regularization:
 *  - none (a.k.a. ordinary least squares)
 *  - L2 (ridge regression)
 *  - L1 (Lasso)
 *  - L2 + L1 (elastic net)
 */
@Since("1.3.0")
@Experimental
class LinearRegression @Since("1.3.0") (@Since("1.3.0") override val uid: String)
  extends Regressor[Vector, LinearRegression, LinearRegressionModel]
  with LinearRegressionParams with DefaultParamsWritable with Logging {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("linReg"))

  /**
   * Set the regularization parameter.
   * Default is 0.0.
   * @group setParam
   */
  @Since("1.3.0")
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam -> 0.0)

  /**
   * Set if we should fit the intercept
   * Default is true.
   * @group setParam
   */
  @Since("1.5.0")
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
  @Since("1.3.0")
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
   * Whether to over-/under-sample training instances according to the given weights in weightCol.
   * If empty, all instances are treated equally (weight 1.0).
   * Default is empty, so all instances have weight one.
   * @group setParam
   */
  @Since("1.6.0")
  def setWeightCol(value: String): this.type = set(weightCol, value)
  setDefault(weightCol -> "")

  /**
   * Set the solver algorithm used for optimization.
   * In case of linear regression, this can be "l-bfgs", "normal" and "auto".
   * "l-bfgs" denotes Limited-memory BFGS which is a limited-memory quasi-Newton
   * optimization method. "normal" denotes using Normal Equation as an analytical
   * solution to the linear regression problem.
   * The default value is "auto" which means that the solver algorithm is
   * selected automatically.
   * @group setParam
   */
  @Since("1.6.0")
  def setSolver(value: String): this.type = set(solver, value)
  setDefault(solver -> "auto")

  override protected def train(dataset: DataFrame): LinearRegressionModel = {
    // Extract the number of features before deciding optimization solver.
    val numFeatures = dataset.select(col($(featuresCol))).limit(1).map {
      case Row(features: Vector) => features.size
    }.first()
    val w = if ($(weightCol).isEmpty) lit(1.0) else col($(weightCol))

    if (($(solver) == "auto" && $(elasticNetParam) == 0.0 && numFeatures <= 4096) ||
      $(solver) == "normal") {
      require($(elasticNetParam) == 0.0, "Only L2 regularization can be used when normal " +
        "solver is used.'")
      // For low dimensional data, WeightedLeastSquares is more efficiently since the
      // training algorithm only requires one pass through the data. (SPARK-10668)
      val instances: RDD[Instance] = dataset.select(
        col($(labelCol)), w, col($(featuresCol))).map {
          case Row(label: Double, weight: Double, features: Vector) =>
            Instance(label, weight, features)
      }

      val optimizer = new WeightedLeastSquares($(fitIntercept), $(regParam),
        $(standardization), true)
      val model = optimizer.fit(instances)
      // When it is trained by WeightedLeastSquares, training summary does not
      // attached returned model.
      val lrModel = copyValues(new LinearRegressionModel(uid, model.coefficients, model.intercept))
      // WeightedLeastSquares does not run through iterations. So it does not generate
      // an objective history.
      val (summaryModel, predictionColName) = lrModel.findSummaryModelAndPredictionCol()
      val trainingSummary = new LinearRegressionTrainingSummary(
        summaryModel.transform(dataset),
        predictionColName,
        $(labelCol),
        summaryModel,
        model.diagInvAtWA.toArray,
        $(featuresCol),
        Array(0D))

      return lrModel.setSummary(trainingSummary)
    }

    val instances: RDD[Instance] = dataset.select(col($(labelCol)), w, col($(featuresCol))).map {
      case Row(label: Double, weight: Double, features: Vector) =>
        Instance(label, weight, features)
    }

    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val (featuresSummarizer, ySummarizer) = {
      val seqOp = (c: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
        instance: Instance) =>
          (c._1.add(instance.features, instance.weight),
            c._2.add(Vectors.dense(instance.label), instance.weight))

      val combOp = (c1: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer),
        c2: (MultivariateOnlineSummarizer, MultivariateOnlineSummarizer)) =>
          (c1._1.merge(c2._1), c1._2.merge(c2._2))

      instances.treeAggregate(
        new MultivariateOnlineSummarizer, new MultivariateOnlineSummarizer)(seqOp, combOp)
    }

    val yMean = ySummarizer.mean(0)
    val yStd = math.sqrt(ySummarizer.variance(0))

    // If the yStd is zero, then the intercept is yMean with zero coefficient;
    // as a result, training is not needed.
    if (yStd == 0.0) {
      logWarning(s"The standard deviation of the label is zero, so the coefficients will be " +
        s"zeros and the intercept will be the mean of the label; as a result, " +
        s"training is not needed.")
      if (handlePersistence) instances.unpersist()
      val coefficients = Vectors.sparse(numFeatures, Seq())
      val intercept = yMean

      val model = new LinearRegressionModel(uid, coefficients, intercept)
      // Handle possible missing or invalid prediction columns
      val (summaryModel, predictionColName) = model.findSummaryModelAndPredictionCol()

      val trainingSummary = new LinearRegressionTrainingSummary(
        summaryModel.transform(dataset),
        predictionColName,
        $(labelCol),
        model,
        Array(0D),
        $(featuresCol),
        Array(0D))
      return copyValues(model.setSummary(trainingSummary))
    }

    val featuresMean = featuresSummarizer.mean.toArray
    val featuresStd = featuresSummarizer.variance.toArray.map(math.sqrt)

    // Since we implicitly do the feature scaling when we compute the cost function
    // to improve the convergence, the effective regParam will be changed.
    val effectiveRegParam = $(regParam) / yStd
    val effectiveL1RegParam = $(elasticNetParam) * effectiveRegParam
    val effectiveL2RegParam = (1.0 - $(elasticNetParam)) * effectiveRegParam

    val costFun = new LeastSquaresCostFun(instances, yStd, yMean, $(fitIntercept),
      $(standardization), featuresStd, featuresMean, effectiveL2RegParam)

    val optimizer = if ($(elasticNetParam) == 0.0 || effectiveRegParam == 0.0) {
      new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    } else {
      def effectiveL1RegFun = (index: Int) => {
        if ($(standardization)) {
          effectiveL1RegParam
        } else {
          // If `standardization` is false, we still standardize the data
          // to improve the rate of convergence; as a result, we have to
          // perform this reverse standardization by penalizing each component
          // differently to get effectively the same objective function when
          // the training dataset is not standardized.
          if (featuresStd(index) != 0.0) effectiveL1RegParam / featuresStd(index) else 0.0
        }
      }
      new BreezeOWLQN[Int, BDV[Double]]($(maxIter), 10, effectiveL1RegFun, $(tol))
    }

    val initialCoefficients = Vectors.zeros(numFeatures)
    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialCoefficients.toBreeze.toDenseVector)

    val (coefficients, objectiveHistory) = {
      /*
         Note that in Linear Regression, the objective history (loss + regularization) returned
         from optimizer is computed in the scaled space given by the following formula.
         {{{
         L = 1/2n||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2 + regTerms
         }}}
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
       */
      val rawCoefficients = state.x.toArray.clone()
      var i = 0
      val len = rawCoefficients.length
      while (i < len) {
        rawCoefficients(i) *= { if (featuresStd(i) != 0.0) yStd / featuresStd(i) else 0.0 }
        i += 1
      }

      (Vectors.dense(rawCoefficients).compressed, arrayBuilder.result())
    }

    /*
       The intercept in R's GLMNET is computed using closed form after the coefficients are
       converged. See the following discussion for detail.
       http://stats.stackexchange.com/questions/13617/how-is-the-intercept-computed-in-glmnet
     */
    val intercept = if ($(fitIntercept)) {
      yMean - dot(coefficients, Vectors.dense(featuresMean))
    } else {
      0.0
    }

    if (handlePersistence) instances.unpersist()

    val model = copyValues(new LinearRegressionModel(uid, coefficients, intercept))
    // Handle possible missing or invalid prediction columns
    val (summaryModel, predictionColName) = model.findSummaryModelAndPredictionCol()

    val trainingSummary = new LinearRegressionTrainingSummary(
      summaryModel.transform(dataset),
      predictionColName,
      $(labelCol),
      model,
      Array(0D),
      $(featuresCol),
      objectiveHistory)
    model.setSummary(trainingSummary)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LinearRegression = defaultCopy(extra)
}

@Since("1.6.0")
object LinearRegression extends DefaultParamsReadable[LinearRegression] {

  @Since("1.6.0")
  override def load(path: String): LinearRegression = super.load(path)
}

/**
 * :: Experimental ::
 * Model produced by [[LinearRegression]].
 */
@Since("1.3.0")
@Experimental
class LinearRegressionModel private[ml] (
    override val uid: String,
    val coefficients: Vector,
    val intercept: Double)
  extends RegressionModel[Vector, LinearRegressionModel]
  with LinearRegressionParams with MLWritable {

  private var trainingSummary: Option[LinearRegressionTrainingSummary] = None

  @deprecated("Use coefficients instead.", "1.6.0")
  def weights: Vector = coefficients

  override val numFeatures: Int = coefficients.size

  /**
   * Gets summary (e.g. residuals, mse, r-squared ) of model on training set. An exception is
   * thrown if `trainingSummary == None`.
   */
  @Since("1.5.0")
  def summary: LinearRegressionTrainingSummary = trainingSummary match {
    case Some(summ) => summ
    case None =>
      throw new SparkException(
        "No training summary available for this LinearRegressionModel",
        new NullPointerException())
  }

  private[regression] def setSummary(summary: LinearRegressionTrainingSummary): this.type = {
    this.trainingSummary = Some(summary)
    this
  }

  /** Indicates whether a training summary exists for this model instance. */
  @Since("1.5.0")
  def hasSummary: Boolean = trainingSummary.isDefined

  /**
   * Evaluates the model on a testset.
   * @param dataset Test dataset to evaluate model on.
   */
  // TODO: decide on a good name before exposing to public API
  private[regression] def evaluate(dataset: DataFrame): LinearRegressionSummary = {
    // Handle possible missing or invalid prediction columns
    val (summaryModel, predictionColName) = findSummaryModelAndPredictionCol()
    new LinearRegressionSummary(summaryModel.transform(dataset), predictionColName,
      $(labelCol), this, Array(0D))
  }

  /**
   * If the prediction column is set returns the current model and prediction column,
   * otherwise generates a new column and sets it as the prediction column on a new copy
   * of the current model.
   */
  private[regression] def findSummaryModelAndPredictionCol(): (LinearRegressionModel, String) = {
    $(predictionCol) match {
      case "" =>
        val predictionColName = "prediction_" + java.util.UUID.randomUUID.toString()
        (copy(ParamMap.empty).setPredictionCol(predictionColName), predictionColName)
      case p => (this, p)
    }
  }


  override protected def predict(features: Vector): Double = {
    dot(features, coefficients) + intercept
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): LinearRegressionModel = {
    val newModel = copyValues(new LinearRegressionModel(uid, coefficients, intercept), extra)
    if (trainingSummary.isDefined) newModel.setSummary(trainingSummary.get)
    newModel.setParent(parent)
  }

  /**
   * Returns a [[MLWriter]] instance for this ML instance.
   *
   * For [[LinearRegressionModel]], this does NOT currently save the training [[summary]].
   * An option to save [[summary]] may be added in the future.
   *
   * This also does not save the [[parent]] currently.
   */
  @Since("1.6.0")
  override def write: MLWriter = new LinearRegressionModel.LinearRegressionModelWriter(this)
}

@Since("1.6.0")
object LinearRegressionModel extends MLReadable[LinearRegressionModel] {

  @Since("1.6.0")
  override def read: MLReader[LinearRegressionModel] = new LinearRegressionModelReader

  @Since("1.6.0")
  override def load(path: String): LinearRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[LinearRegressionModel]] */
  private[LinearRegressionModel] class LinearRegressionModelWriter(instance: LinearRegressionModel)
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

  private class LinearRegressionModelReader extends MLReader[LinearRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[LinearRegressionModel].getName

    override def load(path: String): LinearRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.format("parquet").load(dataPath)
        .select("intercept", "coefficients").head()
      val intercept = data.getDouble(0)
      val coefficients = data.getAs[Vector](1)
      val model = new LinearRegressionModel(metadata.uid, coefficients, intercept)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * :: Experimental ::
 * Linear regression training results. Currently, the training summary ignores the
 * training coefficients except for the objective trace.
 * @param predictions predictions outputted by the model's `transform` method.
 * @param objectiveHistory objective function (scaled loss + regularization) at each iteration.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionTrainingSummary private[regression] (
    predictions: DataFrame,
    predictionCol: String,
    labelCol: String,
    model: LinearRegressionModel,
    diagInvAtWA: Array[Double],
    val featuresCol: String,
    val objectiveHistory: Array[Double])
  extends LinearRegressionSummary(predictions, predictionCol, labelCol, model, diagInvAtWA) {

  /** Number of training iterations until termination */
  @Since("1.5.0")
  val totalIterations = objectiveHistory.length

}

/**
 * :: Experimental ::
 * Linear regression results evaluated on a dataset.
 * @param predictions predictions outputted by the model's `transform` method.
 */
@Since("1.5.0")
@Experimental
class LinearRegressionSummary private[regression] (
    @transient val predictions: DataFrame,
    val predictionCol: String,
    val labelCol: String,
    val model: LinearRegressionModel,
    private val diagInvAtWA: Array[Double]) extends Serializable {

  @transient private val metrics = new RegressionMetrics(
    predictions
      .select(predictionCol, labelCol)
      .map { case Row(pred: Double, label: Double) => (pred, label) } )

  /**
   * Returns the explained variance regression score.
   * explainedVariance = 1 - variance(y - \hat{y}) / variance(y)
   * Reference: [[http://en.wikipedia.org/wiki/Explained_variation]]
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LinearRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  val explainedVariance: Double = metrics.explainedVariance

  /**
   * Returns the mean absolute error, which is a risk function corresponding to the
   * expected value of the absolute error loss or l1-norm loss.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LinearRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  val meanAbsoluteError: Double = metrics.meanAbsoluteError

  /**
   * Returns the mean squared error, which is a risk function corresponding to the
   * expected value of the squared error loss or quadratic loss.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LinearRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  val meanSquaredError: Double = metrics.meanSquaredError

  /**
   * Returns the root mean squared error, which is defined as the square root of
   * the mean squared error.
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LinearRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  val rootMeanSquaredError: Double = metrics.rootMeanSquaredError

  /**
   * Returns R^2^, the coefficient of determination.
   * Reference: [[http://en.wikipedia.org/wiki/Coefficient_of_determination]]
   *
   * Note: This ignores instance weights (setting all to 1.0) from [[LinearRegression.weightCol]].
   *       This will change in later Spark versions.
   */
  @Since("1.5.0")
  val r2: Double = metrics.r2

  /** Residuals (label - predicted value) */
  @Since("1.5.0")
  @transient lazy val residuals: DataFrame = {
    val t = udf { (pred: Double, label: Double) => label - pred }
    predictions.select(t(col(predictionCol), col(labelCol)).as("residuals"))
  }

  /** Number of instances in DataFrame predictions */
  lazy val numInstances: Long = predictions.count()

  /** Degrees of freedom */
  private val degreesOfFreedom: Long = if (model.getFitIntercept) {
    numInstances - model.coefficients.size - 1
  } else {
    numInstances - model.coefficients.size
  }

  /**
   * The weighted residuals, the usual residuals rescaled by
   * the square root of the instance weights.
   */
  lazy val devianceResiduals: Array[Double] = {
    val weighted = if (model.getWeightCol.isEmpty) lit(1.0) else sqrt(col(model.getWeightCol))
    val dr = predictions.select(col(model.getLabelCol).minus(col(model.getPredictionCol))
      .multiply(weighted).as("weightedResiduals"))
      .select(min(col("weightedResiduals")).as("min"), max(col("weightedResiduals")).as("max"))
      .first()
    Array(dr.getDouble(0), dr.getDouble(1))
  }

  /**
   * Standard error of estimated coefficients and intercept.
   */
  lazy val coefficientStandardErrors: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No Std. Error of coefficients available for this LinearRegressionModel")
    } else {
      val rss = if (model.getWeightCol.isEmpty) {
        meanSquaredError * numInstances
      } else {
        val t = udf { (pred: Double, label: Double, weight: Double) =>
          math.pow(label - pred, 2.0) * weight }
        predictions.select(t(col(model.getPredictionCol), col(model.getLabelCol),
          col(model.getWeightCol)).as("wse")).agg(sum(col("wse"))).first().getDouble(0)
      }
      val sigma2 = rss / degreesOfFreedom
      diagInvAtWA.map(_ * sigma2).map(math.sqrt(_))
    }
  }

  /**
   * T-statistic of estimated coefficients and intercept.
   */
  lazy val tValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No t-statistic available for this LinearRegressionModel")
    } else {
      val estimate = if (model.getFitIntercept) {
        Array.concat(model.coefficients.toArray, Array(model.intercept))
      } else {
        model.coefficients.toArray
      }
      estimate.zip(coefficientStandardErrors).map { x => x._1 / x._2 }
    }
  }

  /**
   * Two-sided p-value of estimated coefficients and intercept.
   */
  lazy val pValues: Array[Double] = {
    if (diagInvAtWA.length == 1 && diagInvAtWA(0) == 0) {
      throw new UnsupportedOperationException(
        "No p-value available for this LinearRegressionModel")
    } else {
      tValues.map { x => 2.0 * (1.0 - StudentsT(degreesOfFreedom.toDouble).cdf(math.abs(x))) }
    }
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
 * the condition number, and then trains the model in scaled space but returns the coefficients in
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
 * When training with intercept enabled,
 * The objective function in the scaled space is given by
 * {{{
 * L = 1/2n ||\sum_i w_i(x_i - \bar{x_i}) / \hat{x_i} - (y - \bar{y}) / \hat{y}||^2,
 * }}}
 * where \bar{x_i} is the mean of x_i, \hat{x_i} is the standard deviation of x_i,
 * \bar{y} is the mean of label, and \hat{y} is the standard deviation of label.
 *
 * If we fitting the intercept disabled (that is forced through 0.0),
 * we can use the same equation except we set \bar{y} and \bar{x_i} to 0 instead
 * of the respective means.
 *
 * This can be rewritten as
 * {{{
 * L = 1/2n ||\sum_i (w_i/\hat{x_i})x_i - \sum_i (w_i/\hat{x_i})\bar{x_i} - y / \hat{y}
 *     + \bar{y} / \hat{y}||^2
 *   = 1/2n ||\sum_i w_i^\prime x_i - y / \hat{y} + offset||^2 = 1/2n diff^2
 * }}}
 * where w_i^\prime^ is the effective coefficients defined by w_i/\hat{x_i}, offset is
 * {{{
 * - \sum_i (w_i/\hat{x_i})\bar{x_i} + \bar{y} / \hat{y}.
 * }}}, and diff is
 * {{{
 * \sum_i w_i^\prime x_i - y / \hat{y} + offset
 * }}}
 *
 *
 * Note that the effective coefficients and offset don't depend on training dataset,
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
 * @param coefficients The coefficients corresponding to the features.
 * @param labelStd The standard deviation value of the label.
 * @param labelMean The mean value of the label.
 * @param fitIntercept Whether to fit an intercept term.
 * @param featuresStd The standard deviation values of the features.
 * @param featuresMean The mean values of the features.
 */
private class LeastSquaresAggregator(
    coefficients: Vector,
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double]) extends Serializable {

  private var totalCnt: Long = 0L
  private var weightSum: Double = 0.0
  private var lossSum = 0.0

  private val (effectiveCoefficientsArray: Array[Double], offset: Double, dim: Int) = {
    val coefficientsArray = coefficients.toArray.clone()
    var sum = 0.0
    var i = 0
    val len = coefficientsArray.length
    while (i < len) {
      if (featuresStd(i) != 0.0) {
        coefficientsArray(i) /=  featuresStd(i)
        sum += coefficientsArray(i) * featuresMean(i)
      } else {
        coefficientsArray(i) = 0.0
      }
      i += 1
    }
    val offset = if (fitIntercept) labelMean / labelStd - sum else 0.0
    (coefficientsArray, offset, coefficientsArray.length)
  }

  private val effectiveCoefficientsVector = Vectors.dense(effectiveCoefficientsArray)

  private val gradientSumArray = Array.ofDim[Double](dim)

  /**
   * Add a new training instance to this LeastSquaresAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param instance The instance of data point to be added.
   * @return This LeastSquaresAggregator object.
   */
  def add(instance: Instance): this.type = {
    instance match { case Instance(label, weight, features) =>
      require(dim == features.size, s"Dimensions mismatch when adding new sample." +
        s" Expecting $dim but got ${features.size}.")
      require(weight >= 0.0, s"instance weight, ${weight} has to be >= 0.0")

      if (weight == 0.0) return this

      val diff = dot(features, effectiveCoefficientsVector) - label / labelStd + offset

      if (diff != 0) {
        val localGradientSumArray = gradientSumArray
        features.foreachActive { (index, value) =>
          if (featuresStd(index) != 0.0 && value != 0.0) {
            localGradientSumArray(index) += weight * diff * value / featuresStd(index)
          }
        }
        lossSum += weight * diff * diff / 2.0
      }

      totalCnt += 1
      weightSum += weight
      this
    }
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

    if (other.weightSum != 0) {
      totalCnt += other.totalCnt
      weightSum += other.weightSum
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
 * LeastSquaresCostFun implements Breeze's DiffFunction[T] for Least Squares cost.
 * It returns the loss and gradient with L2 regularization at a particular point (coefficients).
 * It's used in Breeze's convex optimization routines.
 */
private class LeastSquaresCostFun(
    instances: RDD[Instance],
    labelStd: Double,
    labelMean: Double,
    fitIntercept: Boolean,
    standardization: Boolean,
    featuresStd: Array[Double],
    featuresMean: Array[Double],
    effectiveL2regParam: Double) extends DiffFunction[BDV[Double]] {

  override def calculate(coefficients: BDV[Double]): (Double, BDV[Double]) = {
    val coeffs = Vectors.fromBreeze(coefficients)

    val leastSquaresAggregator = {
      val seqOp = (c: LeastSquaresAggregator, instance: Instance) => c.add(instance)
      val combOp = (c1: LeastSquaresAggregator, c2: LeastSquaresAggregator) => c1.merge(c2)

      instances.treeAggregate(
        new LeastSquaresAggregator(coeffs, labelStd, labelMean, fitIntercept, featuresStd,
          featuresMean))(seqOp, combOp)
    }

    val totalGradientArray = leastSquaresAggregator.gradient.toArray

    val regVal = if (effectiveL2regParam == 0.0) {
      0.0
    } else {
      var sum = 0.0
      coeffs.foreachActive { (index, value) =>
        // The following code will compute the loss of the regularization; also
        // the gradient of the regularization, and add back to totalGradientArray.
        sum += {
          if (standardization) {
            totalGradientArray(index) += effectiveL2regParam * value
            value * value
          } else {
            if (featuresStd(index) != 0.0) {
              // If `standardization` is false, we still standardize the data
              // to improve the rate of convergence; as a result, we have to
              // perform this reverse standardization by penalizing each component
              // differently to get effectively the same objective function when
              // the training dataset is not standardized.
              val temp = value / (featuresStd(index) * featuresStd(index))
              totalGradientArray(index) += effectiveL2regParam * temp
              value * temp
            } else {
              0.0
            }
          }
        }
      }
      0.5 * effectiveL2regParam * sum
    }

    (leastSquaresAggregator.loss + regVal, new BDV(totalGradientArray))
  }
}
