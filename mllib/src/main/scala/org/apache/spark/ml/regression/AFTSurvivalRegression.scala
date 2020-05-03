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
import breeze.optimize.{CachedDiffFunction, LBFGS => BreezeLBFGS}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.linalg.{BLAS, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.optim.aggregator.AFTAggregator
import org.apache.spark.ml.optim.loss.RDDLossFunction
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.Instrumentation.instrumented
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}
import org.apache.spark.storage.StorageLevel

/**
 * Params for accelerated failure time (AFT) regression.
 */
private[regression] trait AFTSurvivalRegressionParams extends PredictorParams
  with HasMaxIter with HasTol with HasFitIntercept with HasAggregationDepth with Logging {

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
  private[regression] def hasQuantilesCol: Boolean = {
    isDefined(quantilesCol) && $(quantilesCol).nonEmpty
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
      SchemaUtils.checkNumericType(schema, $(censorCol))
      SchemaUtils.checkNumericType(schema, $(labelCol))
    }

    val schemaWithQuantilesCol = if (hasQuantilesCol) {
      SchemaUtils.appendColumn(schema, $(quantilesCol), new VectorUDT)
    } else schema

    SchemaUtils.appendColumn(schemaWithQuantilesCol, $(predictionCol), DoubleType)
  }
}

/**
 * Fit a parametric survival regression model named accelerated failure time (AFT) model
 * (see <a href="https://en.wikipedia.org/wiki/Accelerated_failure_time_model">
 * Accelerated failure time model (Wikipedia)</a>)
 * based on the Weibull distribution of the survival time.
 */
@Since("1.6.0")
class AFTSurvivalRegression @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Regressor[Vector, AFTSurvivalRegression, AFTSurvivalRegressionModel]
  with AFTSurvivalRegressionParams with DefaultParamsWritable with Logging {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("aftSurvReg"))

  /** @group setParam */
  @Since("1.6.0")
  def setCensorCol(value: String): this.type = set(censorCol, value)

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
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   * @group expertSetParam
   */
  @Since("2.1.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)
  setDefault(aggregationDepth -> 2)

  /**
   * Extract [[featuresCol]], [[labelCol]] and [[censorCol]] from input dataset,
   * and put it in an RDD with strong types.
   */
  protected[ml] def extractAFTPoints(dataset: Dataset[_]): RDD[AFTPoint] = {
    dataset.select(col($(featuresCol)), col($(labelCol)).cast(DoubleType),
      col($(censorCol)).cast(DoubleType)).rdd.map {
        case Row(features: Vector, label: Double, censor: Double) =>
          AFTPoint(features, label, censor)
      }
  }

  override protected def train(
      dataset: Dataset[_]): AFTSurvivalRegressionModel = instrumented { instr =>
    val instances = extractAFTPoints(dataset)
    val handlePersistence = dataset.storageLevel == StorageLevel.NONE
    if (handlePersistence) instances.persist(StorageLevel.MEMORY_AND_DISK)

    val featuresSummarizer = instances.treeAggregate(
      Summarizer.createSummarizerBuffer("mean", "std", "count"))(
      seqOp = (c: SummarizerBuffer, v: AFTPoint) => c.add(v.features),
      combOp = (c1: SummarizerBuffer, c2: SummarizerBuffer) => c1.merge(c2),
      depth = $(aggregationDepth)
    )

    val featuresStd = featuresSummarizer.std.toArray
    val numFeatures = featuresStd.length

    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, censorCol, predictionCol, quantilesCol,
      fitIntercept, maxIter, tol, aggregationDepth)
    instr.logNamedValue("quantileProbabilities.size", $(quantileProbabilities).length)
    instr.logNumFeatures(numFeatures)
    instr.logNumExamples(featuresSummarizer.count)

    if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
        featuresStd(i) == 0.0 && featuresSummarizer.mean(i) != 0.0 }) {
      instr.logWarning("Fitting AFTSurvivalRegressionModel without intercept on dataset with " +
        "constant nonzero column, Spark MLlib outputs zero coefficients for constant nonzero " +
        "columns. This behavior is different from R survival::survreg.")
    }

    val bcFeaturesStd = instances.context.broadcast(featuresStd)
    val getAggregatorFunc = new AFTAggregator(bcFeaturesStd, $(fitIntercept))(_)
    val optimizer = new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))
    val costFun = new RDDLossFunction(instances, getAggregatorFunc, None, $(aggregationDepth))

    /*
       The parameters vector has three parts:
       the first element: Double, log(sigma), the log of scale parameter
       the second element: Double, intercept of the beta parameter
       the third to the end elements: Doubles, regression coefficients vector of the beta parameter
     */
    val initialParameters = Vectors.zeros(numFeatures + 2)

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      initialParameters.asBreeze.toDenseVector)

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

    bcFeaturesStd.destroy()
    if (handlePersistence) instances.unpersist()

    val rawCoefficients = parameters.take(numFeatures)
    var i = 0
    while (i < numFeatures) {
      rawCoefficients(i) *= { if (featuresStd(i) != 0.0) 1.0 / featuresStd(i) else 0.0 }
      i += 1
    }
    val coefficients = Vectors.dense(rawCoefficients)
    val intercept = parameters(numFeatures)
    val scale = math.exp(parameters(numFeatures + 1))
    new AFTSurvivalRegressionModel(uid, coefficients, intercept, scale)
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
 * Model produced by [[AFTSurvivalRegression]].
 */
@Since("1.6.0")
class AFTSurvivalRegressionModel private[ml] (
    @Since("1.6.0") override val uid: String,
    @Since("2.0.0") val coefficients: Vector,
    @Since("1.6.0") val intercept: Double,
    @Since("1.6.0") val scale: Double)
  extends RegressionModel[Vector, AFTSurvivalRegressionModel] with AFTSurvivalRegressionParams
  with MLWritable {

  @Since("3.0.0")
  override def numFeatures: Int = coefficients.size

  /** @group setParam */
  @Since("1.6.0")
  def setQuantileProbabilities(value: Array[Double]): this.type = set(quantileProbabilities, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantilesCol(value: String): this.type = set(quantilesCol, value)

  @Since("2.0.0")
  def predictQuantiles(features: Vector): Vector = {
    // scale parameter for the Weibull distribution of lifetime
    val lambda = math.exp(BLAS.dot(coefficients, features) + intercept)
    // shape parameter for the Weibull distribution of lifetime
    val k = 1 / scale
    val quantiles = $(quantileProbabilities).map {
      q => lambda * math.exp(math.log(-math.log1p(-q)) / k)
    }
    Vectors.dense(quantiles)
  }

  @Since("2.0.0")
  def predict(features: Vector): Double = {
    math.exp(BLAS.dot(coefficients, features) + intercept)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    var predictionColNames = Seq.empty[String]
    var predictionColumns = Seq.empty[Column]

    if ($(predictionCol).nonEmpty) {
      val predictUDF = udf { features: Vector => predict(features) }
      predictionColNames :+= $(predictionCol)
      predictionColumns :+= predictUDF(col($(featuresCol)))
        .as($(predictionCol), outputSchema($(predictionCol)).metadata)
    }

    if (hasQuantilesCol) {
      val predictQuantilesUDF = udf { features: Vector => predictQuantiles(features)}
      predictionColNames :+= $(quantilesCol)
      predictionColumns :+= predictQuantilesUDF(col($(featuresCol)))
        .as($(quantilesCol), outputSchema($(quantilesCol)).metadata)
    }

    if (predictionColNames.nonEmpty) {
      dataset.withColumns(predictionColNames, predictionColumns)
    } else {
      this.logWarning(s"$uid: AFTSurvivalRegressionModel.transform() does nothing" +
        " because no output columns were set.")
      dataset.toDF()
    }
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema, fitting = false)
    if ($(predictionCol).nonEmpty) {
      outputSchema = SchemaUtils.updateNumeric(outputSchema, $(predictionCol))
    }
    if (isDefined(quantilesCol) && $(quantilesCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(quantilesCol), $(quantileProbabilities).length)
    }
    outputSchema
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): AFTSurvivalRegressionModel = {
    copyValues(new AFTSurvivalRegressionModel(uid, coefficients, intercept, scale), extra)
      .setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter =
    new AFTSurvivalRegressionModel.AFTSurvivalRegressionModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"AFTSurvivalRegressionModel: uid=$uid, numFeatures=$numFeatures"
  }
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
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class AFTSurvivalRegressionModelReader extends MLReader[AFTSurvivalRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AFTSurvivalRegressionModel].getName

    override def load(path: String): AFTSurvivalRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(coefficients: Vector, intercept: Double, scale: Double) =
        MLUtils.convertVectorColumnsToML(data, "coefficients")
          .select("coefficients", "intercept", "scale")
          .head()
      val model = new AFTSurvivalRegressionModel(metadata.uid, coefficients, intercept, scale)

      metadata.getAndSetParams(model)
      model
    }
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
private[ml] case class AFTPoint(features: Vector, label: Double, censor: Double) {
  require(censor == 1.0 || censor == 0.0, "censor of class AFTPoint must be 1.0 or 0.0")
}
