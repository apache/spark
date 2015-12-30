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
import breeze.optimize.{CachedDiffFunction, DiffFunction, LBFGS => BreezeLBFGS}
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{BLAS, Vector, VectorUDT, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Logging, SparkException}

/**
 * Params for accelerated failure time (AFT) regression.
 */
private[regression] trait AFTSurvivalRegressionParams extends Params
  with HasFeaturesCol with HasLabelCol with HasPredictionCol with HasMaxIter
  with HasTol with HasFitIntercept with Logging {

  /**
   * Param for censor column name.
   * The value of this column could be 0 or 1.
   * If the value is 1, it means the event has occurred i.e. uncensored; otherwise censored.
   * @group param
   */
  @Since("1.6.0")
  final val censorCol: Param[String] = new Param(this, "censorCol", "censor column name")

  /** @group getParam */
  @Since("1.6.0")
  def getCensorCol: String = $(censorCol)
  setDefault(censorCol -> "censor")

  /**
   * Param for quantile probabilities array.
   * Values of the quantile probabilities array should be in the range (0, 1)
   * and the array should be non-empty.
   * @group param
   */
  @Since("1.6.0")
  final val quantileProbabilities: DoubleArrayParam = new DoubleArrayParam(this,
    "quantileProbabilities", "quantile probabilities array",
    (t: Array[Double]) => t.forall(ParamValidators.inRange(0, 1, false, false)) && t.length > 0)

  /** @group getParam */
  @Since("1.6.0")
  def getQuantileProbabilities: Array[Double] = $(quantileProbabilities)
  setDefault(quantileProbabilities -> Array(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99))

  /**
   * Param for quantiles column name.
   * This column will output quantiles of corresponding quantileProbabilities if it is set.
   * @group param
   */
  @Since("1.6.0")
  final val quantilesCol: Param[String] = new Param(this, "quantilesCol", "quantiles column name")

  /** @group getParam */
  @Since("1.6.0")
  def getQuantilesCol: String = $(quantilesCol)

  /** Checks whether the input has quantiles column name. */
  protected[regression] def hasQuantilesCol: Boolean = {
    isDefined(quantilesCol) && $(quantilesCol) != ""
  }

  /**
   * Validates and transforms the input schema with the provided param map.
   * @param schema input schema
   * @param fitting whether this is in fitting or prediction
   * @return output schema
   */
  protected def validateAndTransformSchema(
      schema: StructType,
      fitting: Boolean): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    if (fitting) {
      SchemaUtils.checkColumnType(schema, $(censorCol), DoubleType)
      SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)
    }
    if (hasQuantilesCol) {
      SchemaUtils.appendColumn(schema, $(quantilesCol), new VectorUDT)
    }
    SchemaUtils.appendColumn(schema, $(predictionCol), DoubleType)
  }
}

/**
 * :: Experimental ::
 * Fit a parametric survival regression model named accelerated failure time (AFT) model
 * ([[https://en.wikipedia.org/wiki/Accelerated_failure_time_model]])
 * based on the Weibull distribution of the survival time.
 */
@Experimental
@Since("1.6.0")
class AFTSurvivalRegression @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Estimator[AFTSurvivalRegressionModel] with AFTSurvivalRegressionParams
  with DefaultParamsWritable with Logging {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("aftSurvReg"))

  /** @group setParam */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setCensorCol(value: String): this.type = set(censorCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantileProbabilities(value: Array[Double]): this.type = set(quantileProbabilities, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantilesCol(value: String): this.type = set(quantilesCol, value)

  /**
   * Set if we should fit the intercept
   * Default is true.
   * @group setParam
   */
  @Since("1.6.0")
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept -> true)

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)
  setDefault(maxIter -> 100)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("1.6.0")
  def setTol(value: Double): this.type = set(tol, value)
  setDefault(tol -> 1E-6)

  /**
   * Extract [[featuresCol]], [[labelCol]] and [[censorCol]] from input dataset,
   * and put it in an RDD with strong types.
   */
  protected[ml] def extractAFTPoints(dataset: DataFrame): RDD[AFTPoint] = {
    dataset.select($(featuresCol), $(labelCol), $(censorCol)).map {
      case Row(features: Vector, label: Double, censor: Double) =>
        AFTPoint(features, label, censor)
    }
  }

  @Since("1.6.0")
  override def fit(dataset: DataFrame): AFTSurvivalRegressionModel = {
    validateAndTransformSchema(dataset.schema, fitting = true)
    val instances = extractAFTPoints(dataset)
    val handlePersistence = dataset.rdd.getStorageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val costFun = new AFTCostFun(instances, $(fitIntercept))
    val optimizer = new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))

    val numFeatures = dataset.select($(featuresCol)).take(1)(0).getAs[Vector](0).size
    /*
       The parameters vector has three parts:
       the first element: Double, log(sigma), the log of scale parameter
       the second element: Double, intercept of the beta parameter
       the third to the end elements: Doubles, regression coefficients vector of the beta parameter
     */
    val initialParameters = Vectors.zeros(numFeatures + 2)

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialParameters.toBreeze.toDenseVector)

    val parameters = {
      val arrayBuilder = mutable.ArrayBuilder.make[Double]
      var state: optimizer.State = null
      while (states.hasNext) {
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

    val coefficients = Vectors.dense(parameters.slice(2, parameters.length))
    val intercept = parameters(1)
    val scale = math.exp(parameters(0))
    val model = new AFTSurvivalRegressionModel(uid, coefficients, intercept, scale)
    copyValues(model.setParent(this))
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): AFTSurvivalRegression = defaultCopy(extra)
}

@Since("1.6.0")
object AFTSurvivalRegression extends DefaultParamsReadable[AFTSurvivalRegression] {

  @Since("1.6.0")
  override def load(path: String): AFTSurvivalRegression = super.load(path)
}

/**
 * :: Experimental ::
 * Model produced by [[AFTSurvivalRegression]].
 */
@Experimental
@Since("1.6.0")
class AFTSurvivalRegressionModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("1.6.0") val coefficients: Vector,
    @Since("1.6.0") val intercept: Double,
    @Since("1.6.0") val scale: Double)
  extends Model[AFTSurvivalRegressionModel] with AFTSurvivalRegressionParams with MLWritable {

  /** @group setParam */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantileProbabilities(value: Array[Double]): this.type = set(quantileProbabilities, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantilesCol(value: String): this.type = set(quantilesCol, value)

  @Since("1.6.0")
  def predictQuantiles(features: Vector): Vector = {
    // scale parameter for the Weibull distribution of lifetime
    val lambda = math.exp(BLAS.dot(coefficients, features) + intercept)
    // shape parameter for the Weibull distribution of lifetime
    val k = 1 / scale
    val quantiles = $(quantileProbabilities).map {
      q => lambda * math.exp(math.log(-math.log(1 - q)) / k)
    }
    Vectors.dense(quantiles)
  }

  @Since("1.6.0")
  def predict(features: Vector): Double = {
    math.exp(BLAS.dot(coefficients, features) + intercept)
  }

  @Since("1.6.0")
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val predictUDF = udf { features: Vector => predict(features) }
    val predictQuantilesUDF = udf { features: Vector => predictQuantiles(features)}
    if (hasQuantilesCol) {
      dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
        .withColumn($(quantilesCol), predictQuantilesUDF(col($(featuresCol))))
    } else {
      dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
    }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = false)
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): AFTSurvivalRegressionModel = {
    copyValues(new AFTSurvivalRegressionModel(uid, coefficients, intercept, scale), extra)
      .setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter =
    new AFTSurvivalRegressionModel.AFTSurvivalRegressionModelWriter(this)
}

@Since("1.6.0")
object AFTSurvivalRegressionModel extends MLReadable[AFTSurvivalRegressionModel] {

  @Since("1.6.0")
  override def read: MLReader[AFTSurvivalRegressionModel] = new AFTSurvivalRegressionModelReader

  @Since("1.6.0")
  override def load(path: String): AFTSurvivalRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[AFTSurvivalRegressionModel]] */
  private[AFTSurvivalRegressionModel] class AFTSurvivalRegressionModelWriter (
      instance: AFTSurvivalRegressionModel
    ) extends MLWriter with Logging {

    private case class Data(coefficients: Vector, intercept: Double, scale: Double)

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      // Save model data: coefficients, intercept, scale
      val data = Data(instance.coefficients, instance.intercept, instance.scale)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class AFTSurvivalRegressionModelReader extends MLReader[AFTSurvivalRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AFTSurvivalRegressionModel].getName

    override def load(path: String): AFTSurvivalRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sqlContext.read.parquet(dataPath)
        .select("coefficients", "intercept", "scale").head()
      val coefficients = data.getAs[Vector](0)
      val intercept = data.getDouble(1)
      val scale = data.getDouble(2)
      val model = new AFTSurvivalRegressionModel(metadata.uid, coefficients, intercept, scale)

      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }
}

/**
 * AFTAggregator computes the gradient and loss for a AFT loss function,
 * as used in AFT survival regression for samples in sparse or dense vector in a online fashion.
 *
 * The loss function and likelihood function under the AFT model based on:
 * Lawless, J. F., Statistical Models and Methods for Lifetime Data,
 * New York: John Wiley & Sons, Inc. 2003.
 *
 * Two AFTAggregator can be merged together to have a summary of loss and gradient of
 * the corresponding joint dataset.
 *
 * Given the values of the covariates x^{'}, for random lifetime t_{i} of subjects i = 1, ..., n,
 * with possible right-censoring, the likelihood function under the AFT model is given as
 * {{{
 *   L(\beta,\sigma)=\prod_{i=1}^n[\frac{1}{\sigma}f_{0}
 *   (\frac{\log{t_{i}}-x^{'}\beta}{\sigma})]^{\delta_{i}}S_{0}
 *   (\frac{\log{t_{i}}-x^{'}\beta}{\sigma})^{1-\delta_{i}}
 * }}}
 * Where \delta_{i} is the indicator of the event has occurred i.e. uncensored or not.
 * Using \epsilon_{i}=\frac{\log{t_{i}}-x^{'}\beta}{\sigma}, the log-likelihood function
 * assumes the form
 * {{{
 *   \iota(\beta,\sigma)=\sum_{i=1}^{n}[-\delta_{i}\log\sigma+
 *   \delta_{i}\log{f_{0}}(\epsilon_{i})+(1-\delta_{i})\log{S_{0}(\epsilon_{i})}]
 * }}}
 * Where S_{0}(\epsilon_{i}) is the baseline survivor function,
 * and f_{0}(\epsilon_{i}) is corresponding density function.
 *
 * The most commonly used log-linear survival regression method is based on the Weibull
 * distribution of the survival time. The Weibull distribution for lifetime corresponding
 * to extreme value distribution for log of the lifetime,
 * and the S_{0}(\epsilon) function is
 * {{{
 *   S_{0}(\epsilon_{i})=\exp(-e^{\epsilon_{i}})
 * }}}
 * the f_{0}(\epsilon_{i}) function is
 * {{{
 *   f_{0}(\epsilon_{i})=e^{\epsilon_{i}}\exp(-e^{\epsilon_{i}})
 * }}}
 * The log-likelihood function for Weibull distribution of lifetime is
 * {{{
 *   \iota(\beta,\sigma)=
 *   -\sum_{i=1}^n[\delta_{i}\log\sigma-\delta_{i}\epsilon_{i}+e^{\epsilon_{i}}]
 * }}}
 * Due to minimizing the negative log-likelihood equivalent to maximum a posteriori probability,
 * the loss function we use to optimize is -\iota(\beta,\sigma).
 * The gradient functions for \beta and \log\sigma respectively are
 * {{{
 *   \frac{\partial (-\iota)}{\partial \beta}=
 *   \sum_{1=1}^{n}[\delta_{i}-e^{\epsilon_{i}}]\frac{x_{i}}{\sigma}
 * }}}
 * {{{
 *   \frac{\partial (-\iota)}{\partial (\log\sigma)}=
 *   \sum_{i=1}^{n}[\delta_{i}+(\delta_{i}-e^{\epsilon_{i}})\epsilon_{i}]
 * }}}
 * @param parameters including three part: The log of scale parameter, the intercept and
 *                regression coefficients corresponding to the features.
 * @param fitIntercept Whether to fit an intercept term.
 */
private class AFTAggregator(parameters: BDV[Double], fitIntercept: Boolean)
  extends Serializable {

  // beta is the intercept and regression coefficients to the covariates
  private val beta = parameters.slice(1, parameters.length)
  // sigma is the scale parameter of the AFT model
  private val sigma = math.exp(parameters(0))

  private var totalCnt: Long = 0L
  private var lossSum = 0.0
  private var gradientBetaSum = BDV.zeros[Double](beta.length)
  private var gradientLogSigmaSum = 0.0

  def count: Long = totalCnt

  def loss: Double = if (totalCnt == 0) 1.0 else lossSum / totalCnt

  // Here we optimize loss function over beta and log(sigma)
  def gradient: BDV[Double] = BDV.vertcat(BDV(Array(gradientLogSigmaSum / totalCnt.toDouble)),
    gradientBetaSum/totalCnt.toDouble)

  /**
   * Add a new training data to this AFTAggregator, and update the loss and gradient
   * of the objective function.
   *
   * @param data The AFTPoint representation for one data point to be added into this aggregator.
   * @return This AFTAggregator object.
   */
  def add(data: AFTPoint): this.type = {

    // TODO: Don't create a new xi vector each time.
    val xi = if (fitIntercept) {
      Vectors.dense(Array(1.0) ++ data.features.toArray).toBreeze
    } else {
      Vectors.dense(Array(0.0) ++ data.features.toArray).toBreeze
    }
    val ti = data.label
    val delta = data.censor
    val epsilon = (math.log(ti) - beta.dot(xi)) / sigma

    lossSum += math.log(sigma) * delta
    lossSum += (math.exp(epsilon) - delta * epsilon)

    // Sanity check (should never occur):
    assert(!lossSum.isInfinity,
      s"AFTAggregator loss sum is infinity. Error for unknown reason.")

    gradientBetaSum += xi * (delta - math.exp(epsilon)) / sigma
    gradientLogSigmaSum += delta + (delta - math.exp(epsilon)) * epsilon

    totalCnt += 1
    this
  }

  /**
   * Merge another AFTAggregator, and update the loss and gradient
   * of the objective function.
   * (Note that it's in place merging; as a result, `this` object will be modified.)
   *
   * @param other The other AFTAggregator to be merged.
   * @return This AFTAggregator object.
   */
  def merge(other: AFTAggregator): this.type = {
    if (totalCnt != 0) {
      totalCnt += other.totalCnt
      lossSum += other.lossSum

      gradientBetaSum += other.gradientBetaSum
      gradientLogSigmaSum += other.gradientLogSigmaSum
    }
    this
  }
}

/**
 * AFTCostFun implements Breeze's DiffFunction[T] for AFT cost.
 * It returns the loss and gradient at a particular point (parameters).
 * It's used in Breeze's convex optimization routines.
 */
private class AFTCostFun(data: RDD[AFTPoint], fitIntercept: Boolean)
  extends DiffFunction[BDV[Double]] {

  override def calculate(parameters: BDV[Double]): (Double, BDV[Double]) = {

    val aftAggregator = data.treeAggregate(new AFTAggregator(parameters, fitIntercept))(
      seqOp = (c, v) => (c, v) match {
        case (aggregator, instance) => aggregator.add(instance)
      },
      combOp = (c1, c2) => (c1, c2) match {
        case (aggregator1, aggregator2) => aggregator1.merge(aggregator2)
      })

    (aftAggregator.loss, aftAggregator.gradient)
  }
}

/**
 * Class that represents the (features, label, censor) of a data point.
 *
 * @param features List of features for this data point.
 * @param label Label for this data point.
 * @param censor Indicator of the event has occurred or not. If the value is 1, it means
 *                 the event has occurred i.e. uncensored; otherwise censored.
 */
private[regression] case class AFTPoint(features: Vector, label: Double, censor: Double) {
  require(censor == 1.0 || censor == 0.0, "censor of class AFTPoint must be 1.0 or 0.0")
}
