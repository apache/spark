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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol, HasRelativeError}
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.QuantileSummaries
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

/**
 * Params for [[RobustScaler]] and [[RobustScalerModel]].
 */
private[feature] trait RobustScalerParams extends Params with HasInputCol with HasOutputCol
  with HasRelativeError {

  /**
   * Lower quantile to calculate quantile range, shared by all features
   * Default: 0.25
   * @group param
   */
  val lower: DoubleParam = new DoubleParam(this, "lower",
    "Lower quantile to calculate quantile range",
    ParamValidators.inRange(0, 1, false, false))

  /** @group getParam */
  def getLower: Double = $(lower)

  setDefault(lower -> 0.25)

  /**
   * Upper quantile to calculate quantile range, shared by all features
   * Default: 0.75
   * @group param
   */
  val upper: DoubleParam = new DoubleParam(this, "upper",
    "Upper quantile to calculate quantile range",
    ParamValidators.inRange(0, 1, false, false))

  /** @group getParam */
  def getUpper: Double = $(upper)

  setDefault(upper -> 0.75)

  /**
   * Whether to center the data with median before scaling.
   * It will build a dense output, so take care when applying to sparse input.
   * Default: false
   * @group param
   */
  val withCentering: BooleanParam = new BooleanParam(this, "withCentering",
    "Whether to center data with median")

  /** @group getParam */
  def getWithCentering: Boolean = $(withCentering)

  setDefault(withCentering -> false)

  /**
   * Whether to scale the data to quantile range.
   * Default: true
   * @group param
   */
  val withScaling: BooleanParam = new BooleanParam(this, "withScaling",
    "Whether to scale the data to quantile range")

  /** @group getParam */
  def getWithScaling: Boolean = $(withScaling)

  setDefault(withScaling -> true)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(lower) < $(upper), s"The specified lower quantile(${$(lower)}) is " +
      s"larger or equal to upper quantile(${$(upper)})")
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT, false)
  }
}


/**
 * Scale features using statistics that are robust to outliers.
 * RobustScaler removes the median and scales the data according to the quantile range.
 * The quantile range is by default IQR (Interquartile Range, quantile range between the
 * 1st quartile = 25th quantile and the 3rd quartile = 75th quantile) but can be configured.
 * Centering and scaling happen independently on each feature by computing the relevant
 * statistics on the samples in the training set. Median and quantile range are then
 * stored to be used on later data using the transform method.
 * Standardization of a dataset is a common requirement for many machine learning estimators.
 * Typically this is done by removing the mean and scaling to unit variance. However,
 * outliers can often influence the sample mean / variance in a negative way.
 * In such cases, the median and the quantile range often give better results.
 */
@Since("3.0.0")
class RobustScaler (override val uid: String)
  extends Estimator[RobustScalerModel] with RobustScalerParams with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("robustScal"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLower(value: Double): this.type = set(lower, value)

  /** @group setParam */
  def setUpper(value: Double): this.type = set(upper, value)

  /** @group setParam */
  def setWithCentering(value: Boolean): this.type = set(withCentering, value)

  /** @group setParam */
  def setWithScaling(value: Boolean): this.type = set(withScaling, value)

  /** @group expertSetParam */
  def setRelativeError(value: Double): this.type = set(relativeError, value)

  override def fit(dataset: Dataset[_]): RobustScalerModel = {
    transformSchema(dataset.schema, logging = true)

    val vectors = dataset.select($(inputCol)).rdd.map { case Row(vec: Vector) => vec }
    val numFeatures = MetadataUtils.getNumFeatures(dataset.schema($(inputCol)))
      .getOrElse(vectors.first().size)

    val localRelativeError = $(relativeError)
    val localUpper = $(upper)
    val localLower = $(lower)

    val collected = vectors.flatMap { vec =>
      require(vec.size == numFeatures,
        s"Number of dimensions must be $numFeatures but got ${vec.size}")
      Iterator.range(0, numFeatures).map { i => (i, vec(i)) }
    }.aggregateByKey(
      new QuantileSummaries(QuantileSummaries.defaultCompressThreshold, localRelativeError))(
      seqOp = (s, v) => s.insert(v),
      combOp = (s1, s2) => s1.compress.merge(s2.compress)
    ).mapValues { s =>
      // confirm compression before query
      val s2 = s.compress
      val range = s2.query(localUpper).get - s2.query(localLower).get
      val median = s2.query(0.5).get
      (range, median)
    }.collectAsMap()

    val ranges = Array.ofDim[Double](numFeatures)
    val medians = Array.ofDim[Double](numFeatures)
    var i = 0
    while (i < numFeatures) {
      val (range, median) = collected(i)
      ranges(i) = range
      medians(i) = median
      i += 1
    }

    copyValues(new RobustScalerModel(uid, Vectors.dense(ranges).compressed,
      Vectors.dense(medians).compressed).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): RobustScaler = defaultCopy(extra)
}

@Since("3.0.0")
object RobustScaler extends DefaultParamsReadable[RobustScaler] {

  override def load(path: String): RobustScaler = super.load(path)
}

/**
 * Model fitted by [[RobustScaler]].
 *
 * @param range quantile range for each original column during fitting
 * @param median median value for each original column during fitting
 */
@Since("3.0.0")
class RobustScalerModel private[ml] (
    override val uid: String,
    val range: Vector,
    val median: Vector)
  extends Model[RobustScalerModel] with RobustScalerParams with MLWritable {

  import RobustScalerModel._

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val shift = if ($(withCentering)) median.toArray else Array.emptyDoubleArray
    val scale = if ($(withScaling)) {
      range.toArray.map { v => if (v == 0) 0.0 else 1.0 / v }
    } else Array.emptyDoubleArray

    val func = StandardScalerModel.getTransformFunc(
      shift, scale, $(withCentering), $(withScaling))
    val transformer = udf(func)

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), median.size)
    }
    outputSchema
  }

  override def copy(extra: ParamMap): RobustScalerModel = {
    val copied = new RobustScalerModel(uid, range, median)
    copyValues(copied, extra).setParent(parent)
  }

  override def write: MLWriter = new RobustScalerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"RobustScalerModel: uid=$uid, numFeatures=${median.size}, " +
      s"withCentering=${$(withCentering)}, withScaling=${$(withScaling)}"
  }
}

@Since("3.0.0")
object RobustScalerModel extends MLReadable[RobustScalerModel] {

  private[RobustScalerModel]
  class RobustScalerModelWriter(instance: RobustScalerModel) extends MLWriter {

    private case class Data(range: Vector, median: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = new Data(instance.range, instance.median)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class RobustScalerModelReader extends MLReader[RobustScalerModel] {

    private val className = classOf[RobustScalerModel].getName

    override def load(path: String): RobustScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(range: Vector, median: Vector) = MLUtils
        .convertVectorColumnsToML(data, "range", "median")
        .select("range", "median")
        .head()
      val model = new RobustScalerModel(metadata.uid, range, median)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[RobustScalerModel] = new RobustScalerModelReader

  override def load(path: String): RobustScalerModel = super.load(path)
}
