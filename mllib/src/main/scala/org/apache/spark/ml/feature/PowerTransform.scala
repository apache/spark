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

package org.apache.spark.ml.feature

import org.apache.commons.math3.analysis._
import org.apache.commons.math3.optim._
import org.apache.commons.math3.optim.nonlinear.scalar._
import org.apache.commons.math3.optim.univariate._
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
 * Params for [[PowerTransform]] and [[PowerTransformModel]].
 */
private[feature] trait PowerTransformParams extends Params with HasInputCol with HasOutputCol {

  /**
   * The model type which is a string (case-sensitive).
   * Supported options: "yeo-johnson", "box-cox".
   * (default = yeo-johnson)
   *
   * @group param
   */
  final val modelType: Param[String] = new Param[String](this, "modelType", "The model type " +
    "which is a string (case-sensitive). Supported options: yeo-johnson (default), and box-cox.",
    ParamValidators.inArray[String](PowerTransform.supportedModelTypes))

  /** @group getParam */
  final def getModelType: String = $(modelType)

  setDefault(modelType -> PowerTransform.YeoJohnson)

  /**
   * param for number of bins to down-sample the curves in statistics computation.
   * If 0, no down-sampling will occur.
   * Default: 100,000.
   * @group expertParam
   */
  val numBins: IntParam = new IntParam(this, "numBins", "Number of bins to down-sample " +
    "the curves in statistics computation. If 0, no down-sampling will occur. Must be >= 0.",
    ParamValidators.gtEq(0))

  /** @group expertGetParam */
  def getNumBins: Int = $(numBins)

  setDefault(numBins -> 100000)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }
}


/**
 * Apply a power transform to make data more Gaussian-like.
 * Currently, PowerTransform supports the Box-Cox transform and the Yeo-Johnson transform.
 * The optimal parameter for stabilizing variance and minimizing skewness is estimated through
 * maximum likelihood.
 * Box-Cox requires input data to be strictly positive, while Yeo-Johnson supports both
 * positive or negative data.
 */
@Since("3.1.0")
class PowerTransform @Since("3.1.0")(@Since("3.1.0") override val uid: String)
  extends Estimator[PowerTransformModel] with PowerTransformParams with DefaultParamsWritable {

  import PowerTransform._

  def this() = this(Identifiable.randomUID("power_trans"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setModelType(value: String): this.type = set(modelType, value)

  /** @group expertSetParam */
  def setNumBins(value: Int): this.type = set(numBins, value)

  override def fit(dataset: Dataset[_]): PowerTransformModel = {
    transformSchema(dataset.schema, logging = true)

    val spark = dataset.sparkSession
    import spark.implicits._

    val numFeatures = MetadataUtils.getNumFeatures(dataset, $(inputCol))
    lazy val numRows = dataset.count()

    val validateFunc = $(modelType) match {
      case BoxCox => vec: Vector => requirePositiveValues(vec)
      case YeoJohnson => vec: Vector => requireNonNaNValues(vec)
    }

    var pairCounts = dataset
      .select($(inputCol))
      .flatMap { case Row(vector: Vector) =>
        require(vector.size == numFeatures,
          s"Number of features must be $numFeatures but got ${vector.size}")
        validateFunc(vector)
        vector.iterator
      }.toDF("column", "value")
      .groupBy("column", "value")
      .agg(count(lit(0)).as("count"))
      .sort("column", "value")

    val groupSizes = if (0 < $(numBins) && $(numBins) < numRows) {
      val localNumBins = $(numBins)
      pairCounts
        .groupBy("column")
        .count()
        .as[(Int, Long)]
        .flatMap { case (col, numDistinctValues) =>
          val groupSize = numDistinctValues / localNumBins
          if (groupSize > 1) {
            Iterator.single((col, groupSize))
          } else Iterator.empty
        }.collect().toMap
    } else Map.empty[Int, Long]

    if (groupSizes.nonEmpty) {
      // make bins:
      // group pairs by key with a bounded size, using weighted average
      // as the output value, and sum of counts as the output count.
      pairCounts = pairCounts.as[(Int, Double, Long)]
        .mapPartitions { iter =>
          MLUtils.combineWithinGroups[Int, (Double, Long), (Double, Long)](
            input = iter.map(t => (t._1, (t._2, t._3))),
            initOp = { case (v, c) => (v * c, c) },
            seqOp = { case ((sum, count), (v, c)) => (sum + v * c, count + c) },
            getSize = (k: Int) => groupSizes.getOrElse(k, 1)
          ).map { case (key, (sum, count)) => (key, sum / count, count) }
        }.toDF("column", "value", "count")
    }

    val solutions = pairCounts
      .groupBy("column")
      .agg(collect_list(struct("value", "count")))
      .as[(Int, Seq[(Double, Long)])]
      .map { case (col, seq) =>
        val computeIter = () => seq.iterator
        val (solution, _) = $(modelType) match {
          case BoxCox =>
            solveBoxCox(computeIter)
          case YeoJohnson =>
            solveYeoJohnson(computeIter)
        }
        (col, solution)
      }.collect().sortBy(_._1).map(_._2)

    val lambda = Vectors.dense(solutions)
    copyValues(new PowerTransformModel(uid, lambda.compressed).setParent(this))
  }

  override def copy(extra: ParamMap): PowerTransform = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}


@Since("3.1.0")
object PowerTransform extends DefaultParamsReadable[PowerTransform] {

  override def load(path: String): PowerTransform = super.load(path)

  /** String name for Box-Cox transform model type. */
  private[feature] val BoxCox: String = "box-cox"

  /** String name for Yeo-Johnson transform model type. */
  private[feature] val YeoJohnson: String = "yeo-johnson"

  /* Set of modelTypes that PowerTransform supports */
  private[feature] val supportedModelTypes = Array(BoxCox, YeoJohnson)

  private[feature] def brentSolve(obj: UnivariateFunction): (Double, Double) = {
    val BrentLowerBound = -10.0
    val BrentUpperBound = 10.0
    val BrentRel = 1E-8
    val BrentAbs = 1.48E-8
    val BrentMaxIter = 1000
    val brent = new BrentOptimizer(BrentRel, BrentAbs)
    val result = brent.optimize(
      new UnivariateObjectiveFunction(obj),
      GoalType.MINIMIZE,
      new SearchInterval(BrentLowerBound, BrentUpperBound, 0.0),
      new MaxIter(BrentMaxIter),
      new MaxEval(BrentMaxIter))
    (result.getPoint, result.getValue)
  }

  private[feature] def solveBoxCox(
      computeIter: () => Iterator[(Double, Long)]): (Double, Double) = {
    val (logxSum, count) = computeIter().fold((0.0, 0L)) {
      case ((sum, cnt), (x, c)) =>
        (sum + math.log(x) * c, cnt + c)
    }

    val obj = new UnivariateFunction() {
      override def value(lambda: Double): Double = {
        val lambda0 = math.abs(lambda) < MLUtils.EPSILON

        val iter = computeIter()
        var ySum = 0.0
        var ySumL2 = 0.0

        if (lambda0) {
          while (iter.hasNext) {
            val (x, c) = iter.next
            val y = math.log(x)
            ySumL2 += y * y * c
          }
          ySum = logxSum
        } else {
          while (iter.hasNext) {
            val (x, w) = iter.next
            val y = (math.pow(x, lambda) - 1) / lambda
            ySum += y * w
            ySumL2 += y * y * w
          }
        }

        val yAvg = ySum / count
        val yVar = (ySumL2 - yAvg * ySum) / count
        0.5 * count * math.log(yVar) - (lambda - 1) * logxSum
      }
    }

    brentSolve(obj)
  }

  private[feature] def solveYeoJohnson(
      computeIter: () => Iterator[(Double, Long)]): (Double, Double) = {
    val (log1pxSum, count) = computeIter().fold((0.0, 0L)) {
      case ((sum, cnt), (x, c)) =>
        (sum + math.signum(x) * math.log1p(math.abs(x)) * c, cnt + c)
    }

    val obj = new UnivariateFunction() {
      override def value(lambda: Double): Double = {
        val lambda0 = math.abs(lambda) < MLUtils.EPSILON
        val lambda2 = math.abs(lambda - 2) < MLUtils.EPSILON

        val iter = computeIter()
        var ySum = 0.0
        var ySumL2 = 0.0

        while (iter.hasNext) {
          val (x, c) = iter.next
          val y = if (x >= 0) {
            if (lambda0) {
              math.log(x + 1)
            } else {
              (math.pow(x + 1, lambda) - 1) / lambda
            }
          } else {
            if (lambda2) {
              -math.log(1 - x)
            } else {
              (math.pow(1 - x, 2 - lambda) - 1) / (lambda - 2)
            }
          }
          ySum += y * c
          ySumL2 += y * y * c
        }

        val yAvg = ySum / count
        val yVar = (ySumL2 - yAvg * ySum) / count
        0.5 * count * math.log(yVar) - (lambda - 1) * log1pxSum
      }
    }

    brentSolve(obj)
  }

  private[feature] def requirePositiveValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector =>
        require(sv.size == sv.numActives,
          s"PowerTransform by Box-Cox method requires positive feature values but got $sv")
        sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(_ > 0.0),
      s"PowerTransform by Box-Cox method requires positive feature values but got $v.")
  }

  private[feature] def requireNonNaNValues(v: Vector): Unit = {
    val values = v match {
      case sv: SparseVector => sv.values
      case dv: DenseVector => dv.values
    }

    require(values.forall(v => !v.isNaN),
      s"PowerTransform by Yeo-Johnson method requires NonNaN values but got $v.")
  }
}


/**
 * Model fitted by [[PowerTransform]].
 *
 * @param lambda parameters of the power transformation for the features
 */
@Since("3.1.0")
class PowerTransformModel private[ml](
    override val uid: String,
    val lambda: Vector)
  extends Model[PowerTransformModel] with PowerTransformParams with MLWritable {

  import PowerTransform._
  import PowerTransformModel._

  val numFeatures: Int = lambda.size

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def copy(extra: ParamMap): PowerTransformModel = {
    val copied = new PowerTransformModel(uid, lambda)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new PowerTransformModelWriter(this)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val transformer = $(modelType) match {
      case BoxCox =>
        udf { vector: Vector =>
          require(vector.size == numFeatures)
          requirePositiveValues(vector)
          val localLambda = lambda
          val transformed = Array.tabulate(numFeatures) { i =>
            boxCoxTransform(vector(i), localLambda(i))
          }
          Vectors.dense(transformed)
        }

      case YeoJohnson =>
        udf { vector: Vector =>
          require(vector.size == numFeatures)
          requireNonNaNValues(vector)
          val localLambda = lambda
          val transformed = Array.tabulate(numFeatures) { i =>
            yeoJohnsonTransform(vector(i), localLambda(i))
          }
          Vectors.dense(transformed)
        }
    }

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  private def boxCoxTransform(x: Double, l: Double): Double = {
    if (math.abs(l) < MLUtils.EPSILON) {
      math.log(x)
    } else {
      (math.pow(x, l) - 1) / l
    }
  }

  private def yeoJohnsonTransform(x: Double, l: Double): Double = {
    if (x >= 0) {
      if (math.abs(l) < MLUtils.EPSILON) {
        math.log(x + 1)
      } else {
        (math.pow(x + 1, l) - 1) / l
      }
    } else {
      if (math.abs(l - 2) < MLUtils.EPSILON) {
        -math.log(1 - x)
      } else {
        (math.pow(1 - x, 2 - l) - 1) / (l - 2)
      }
    }
  }

  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), lambda.size)
    }
    outputSchema
  }

  @Since("3.1.0")
  override def toString: String = {
    s"PowerTransformModel: uid=$uid, modelType=${$(modelType)}, numFeatures=$numFeatures"
  }
}


@Since("3.1.0")
object PowerTransformModel extends MLReadable[PowerTransformModel] {

  private[PowerTransformModel]
  class PowerTransformModelWriter(instance: PowerTransformModel) extends MLWriter {

    private case class Data(lambda: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.lambda)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class PowerTransformModelReader extends MLReader[PowerTransformModel] {

    private val className = classOf[PowerTransformModel].getName

    override def load(path: String): PowerTransformModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(lambda: Vector) = MLUtils
        .convertVectorColumnsToML(data, "lambda")
        .select("lambda")
        .head()
      val model = new PowerTransformModel(metadata.uid, lambda)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[PowerTransformModel] = new PowerTransformModelReader

  override def load(path: String): PowerTransformModel = super.load(path)
}
