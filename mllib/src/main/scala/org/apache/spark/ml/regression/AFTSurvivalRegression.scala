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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.ml.PredictorParams
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.optim.aggregator._
import org.apache.spark.ml.optim.loss.RDDLossFunction
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat._
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DatasetUtils._
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
  with HasMaxIter with HasTol with HasFitIntercept with HasAggregationDepth
  with HasMaxBlockSizeInMB with Logging {

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

  setDefault(censorCol -> "censor",
    quantileProbabilities -> Array(0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99),
    fitIntercept -> true, maxIter -> 100, tol -> 1E-6, aggregationDepth -> 2,
    maxBlockSizeInMB -> 0.0)

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
 *
 * Since 3.1.0, it supports stacking instances into blocks and using GEMV for
 * better performance.
 * The block size will be 1.0 MB, if param maxBlockSizeInMB is set 0.0 by default.
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

  /**
   * Set the maximum number of iterations.
   * Default is 100.
   * @group setParam
   */
  @Since("1.6.0")
  def setMaxIter(value: Int): this.type = set(maxIter, value)

  /**
   * Set the convergence tolerance of iterations.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   * Default is 1E-6.
   * @group setParam
   */
  @Since("1.6.0")
  def setTol(value: Double): this.type = set(tol, value)

  /**
   * Suggested depth for treeAggregate (greater than or equal to 2).
   * If the dimensions of features or the number of partitions are large,
   * this param could be adjusted to a larger size.
   * Default is 2.
   * @group expertSetParam
   */
  @Since("2.1.0")
  def setAggregationDepth(value: Int): this.type = set(aggregationDepth, value)

  /**
   * Sets the value of param [[maxBlockSizeInMB]].
   * Default is 0.0, then 1.0 MB will be chosen.
   *
   * @group expertSetParam
   */
  @Since("3.1.0")
  def setMaxBlockSizeInMB(value: Double): this.type = set(maxBlockSizeInMB, value)

  override protected def train(
      dataset: Dataset[_]): AFTSurvivalRegressionModel = instrumented { instr =>
    instr.logPipelineStage(this)
    instr.logDataset(dataset)
    instr.logParams(this, labelCol, featuresCol, censorCol, predictionCol, quantilesCol,
      fitIntercept, maxIter, tol, aggregationDepth, maxBlockSizeInMB)
    instr.logNamedValue("quantileProbabilities.size", $(quantileProbabilities).length)

    if (dataset.storageLevel != StorageLevel.NONE) {
      instr.logWarning("Input instances will be standardized, blockified to blocks, and " +
        "then cached during training. Be careful of double caching!")
    }

    val validatedCensorCol = {
      val casted = col($(censorCol)).cast(DoubleType)
      when(casted.isNull || casted.isNaN, raise_error(lit("Censors MUST NOT be Null or NaN")))
        .when(casted =!= 0 && casted =!= 1,
          raise_error(concat(lit("Censors MUST be in {0, 1}, but got "), casted)))
        .otherwise(casted)
    }

    val instances = dataset.select(
      checkRegressionLabels($(labelCol)),
      validatedCensorCol,
      checkNonNanVectors($(featuresCol))
    ).rdd.map { case Row(l: Double, c: Double, v: Vector) =>
      // AFT does not support instance weighting,
      // here use Instance.weight to store censor for convenience
      Instance(l, c, v)
    }.setName("training instances")

    val summarizer = instances.treeAggregate(
      Summarizer.createSummarizerBuffer("mean", "std", "count"))(
      seqOp = (c: SummarizerBuffer, i: Instance) => c.add(i.features),
      combOp = (c1: SummarizerBuffer, c2: SummarizerBuffer) => c1.merge(c2),
      depth = $(aggregationDepth)
    )

    val featuresMean = summarizer.mean.toArray
    val featuresStd = summarizer.std.toArray
    val numFeatures = featuresStd.length
    instr.logNumFeatures(numFeatures)
    instr.logNumExamples(summarizer.count)

    var actualBlockSizeInMB = $(maxBlockSizeInMB)
    if (actualBlockSizeInMB == 0) {
      actualBlockSizeInMB = InstanceBlock.DefaultBlockSizeInMB
      require(actualBlockSizeInMB > 0, "inferred actual BlockSizeInMB must > 0")
      instr.logNamedValue("actualBlockSizeInMB", actualBlockSizeInMB.toString)
    }

    if (!$(fitIntercept) && (0 until numFeatures).exists { i =>
        featuresStd(i) == 0.0 && summarizer.mean(i) != 0.0 }) {
      instr.logWarning("Fitting AFTSurvivalRegressionModel without intercept on dataset with " +
        "constant nonzero column, Spark MLlib outputs zero coefficients for constant nonzero " +
        "columns. This behavior is different from R survival::survreg.")
    }

    val optimizer = new BreezeLBFGS[BDV[Double]]($(maxIter), 10, $(tol))

    /*
       The parameters vector has three parts:
       the first element: Double, log(sigma), the log of scale parameter
       the second element: Double, intercept of the beta parameter
       the third to the end elements: Doubles, regression coefficients vector of the beta parameter
     */
    val initialSolution = Array.ofDim[Double](numFeatures + 2)

    val (rawCoefficients, objectiveHistory) =
      trainImpl(instances, actualBlockSizeInMB, featuresStd, featuresMean,
        optimizer, initialSolution)

    if (rawCoefficients == null) {
      MLUtils.optimizerFailed(instr, optimizer.getClass)
    }

    val coefficientArray = Array.tabulate(numFeatures) { i =>
      if (featuresStd(i) != 0) rawCoefficients(i) / featuresStd(i) else 0.0
    }
    val coefficients = Vectors.dense(coefficientArray)
    val intercept = rawCoefficients(numFeatures)
    val scale = math.exp(rawCoefficients(numFeatures + 1))
    new AFTSurvivalRegressionModel(uid, coefficients, intercept, scale)
  }

  private def trainImpl(
      instances: RDD[Instance],
      actualBlockSizeInMB: Double,
      featuresStd: Array[Double],
      featuresMean: Array[Double],
      optimizer: BreezeLBFGS[BDV[Double]],
      initialSolution: Array[Double]): (Array[Double], Array[Double]) = {
    val numFeatures = featuresStd.length
    val inverseStd = featuresStd.map(std => if (std != 0) 1.0 / std else 0.0)
    val scaledMean = Array.tabulate(numFeatures)(i => inverseStd(i) * featuresMean(i))
    val bcInverseStd = instances.context.broadcast(inverseStd)
    val bcScaledMean = instances.context.broadcast(scaledMean)

    val scaled = instances.mapPartitions { iter =>
      val func = StandardScalerModel.getTransformFunc(Array.empty, bcInverseStd.value, false, true)
      iter.map { case Instance(label, weight, vec) => Instance(label, weight, func(vec)) }
    }

    val maxMemUsage = (actualBlockSizeInMB * 1024L * 1024L).ceil.toLong
    val blocks = InstanceBlock.blokifyWithMaxMemUsage(scaled, maxMemUsage)
      .persist(StorageLevel.MEMORY_AND_DISK)
      .setName(s"training blocks (blockSizeInMB=$actualBlockSizeInMB)")

    val getAggregatorFunc = new AFTBlockAggregator(bcScaledMean, $(fitIntercept))(_)
    val costFun = new RDDLossFunction(blocks, getAggregatorFunc, None, $(aggregationDepth))

    if ($(fitIntercept)) {
      // original `initialSolution` is for problem:
      // y = f(w1 * x1 / std_x1, w2 * x2 / std_x2, ..., intercept)
      // we should adjust it to the initial solution for problem:
      // y = f(w1 * (x1 - avg_x1) / std_x1, w2 * (x2 - avg_x2) / std_x2, ..., intercept)
      // NOTE: this is NOOP before we finally support model initialization
      val adapt = BLAS.javaBLAS.ddot(numFeatures, initialSolution, 1, scaledMean, 1)
      initialSolution(numFeatures) += adapt
    }

    val states = optimizer.iterations(new CachedDiffFunction(costFun),
      new BDV[Double](initialSolution))

    val arrayBuilder = mutable.ArrayBuilder.make[Double]
    var state: optimizer.State = null
    while (states.hasNext) {
      state = states.next()
      arrayBuilder += state.adjustedValue
    }
    blocks.unpersist()
    bcInverseStd.destroy()
    bcScaledMean.destroy()

    val solution = if (state == null) null else state.x.toArray
    if ($(fitIntercept) && solution != null) {
      // the final solution is for problem:
      // y = f(w1 * (x1 - avg_x1) / std_x1, w2 * (x2 - avg_x2) / std_x2, ..., intercept)
      // we should adjust it back for original problem:
      // y = f(w1 * x1 / std_x1, w2 * x2 / std_x2, ..., intercept)
      val adapt = BLAS.getBLAS(numFeatures).ddot(numFeatures, solution, 1, scaledMean, 1)
      solution(numFeatures) -= adapt
    }
    (solution, arrayBuilder.result())
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema, fitting = true)
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): AFTSurvivalRegression = defaultCopy(extra)

  private[spark] override def estimateModelSize(dataset: Dataset[_]): Long = {
    val numFeatures = DatasetUtils.getNumFeatures(dataset, $(featuresCol))

    var size = this.estimateMatadataSize
    size += Vectors.getDenseSize(numFeatures) // coefficients
    size
  }
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

  // For ml connect only
  private[ml] def this() = this("", Vectors.empty, Double.NaN, Double.NaN)

  @Since("3.0.0")
  override def numFeatures: Int = coefficients.size

  /** @group setParam */
  @Since("1.6.0")
  def setQuantileProbabilities(value: Array[Double]): this.type = set(quantileProbabilities, value)

  /** @group setParam */
  @Since("1.6.0")
  def setQuantilesCol(value: String): this.type = set(quantilesCol, value)

  private var _quantiles: Vector = _

  private[ml] override def onParamChange(param: Param[_]): Unit = {
    if (param.name == "quantileProbabilities") {
      if (isDefined(quantileProbabilities)) {
        _quantiles = Vectors.dense(
          $(quantileProbabilities).map(q => math.exp(math.log(-math.log1p(-q)) * scale)))
      } else {
        _quantiles = null
      }
    }
  }

  private def lambda2Quantiles(lambda: Double): Vector = {
    val quantiles = _quantiles.copy
    BLAS.scal(lambda, quantiles)
    quantiles
  }

  @Since("2.0.0")
  def predictQuantiles(features: Vector): Vector = {
    // scale parameter for the Weibull distribution of lifetime
    val lambda = predict(features)
    lambda2Quantiles(lambda)
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
      val predCol = udf(predict _).apply(col($(featuresCol)))
      predictionColNames :+= $(predictionCol)
      predictionColumns :+= predCol
        .as($(predictionCol), outputSchema($(predictionCol)).metadata)
    }

    if (hasQuantilesCol) {
      val quanCol = if ($(predictionCol).nonEmpty) {
        udf(lambda2Quantiles _).apply(predictionColumns.head)
      } else {
        udf(predictQuantiles _).apply(col($(featuresCol)))
      }
      predictionColNames :+= $(quantilesCol)
      predictionColumns :+= quanCol
        .as($(quantilesCol), outputSchema($(quantilesCol)).metadata)
    }

    if (predictionColNames.nonEmpty) {
      dataset.withColumns(predictionColNames, predictionColumns)
    } else {
      this.logWarning(log"${MDC(LogKeys.UUID, uid)}: AFTSurvivalRegressionModel.transform() " +
        log"does nothing because no output columns were set.")
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

  private[spark] override def estimatedSize: Long = {
    var size = this.estimateMatadataSize
    if (this.coefficients != null) {
      size += this.coefficients.getSizeInBytes
    }
    size
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
  private[ml] case class Data(coefficients: Vector, intercept: Double, scale: Double)

  @Since("1.6.0")
  override def read: MLReader[AFTSurvivalRegressionModel] = new AFTSurvivalRegressionModelReader

  @Since("1.6.0")
  override def load(path: String): AFTSurvivalRegressionModel = super.load(path)

  /** [[MLWriter]] instance for [[AFTSurvivalRegressionModel]] */
  private[AFTSurvivalRegressionModel] class AFTSurvivalRegressionModelWriter (
      instance: AFTSurvivalRegressionModel
    ) extends MLWriter with Logging {

    override protected def saveImpl(path: String): Unit = {
      // Save metadata and Params
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      // Save model data: coefficients, intercept, scale
      val data = Data(instance.coefficients, instance.intercept, instance.scale)
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveObject[Data](dataPath, data, sparkSession)
    }
  }

  private class AFTSurvivalRegressionModelReader extends MLReader[AFTSurvivalRegressionModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[AFTSurvivalRegressionModel].getName

    override def load(path: String): AFTSurvivalRegressionModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)

      val dataPath = new Path(path, "data").toString
      val data = ReadWriteUtils.loadObject[Data](dataPath, sparkSession)
      val model = new AFTSurvivalRegressionModel(
        metadata.uid, data.coefficients, data.intercept, data.scale
      )

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
