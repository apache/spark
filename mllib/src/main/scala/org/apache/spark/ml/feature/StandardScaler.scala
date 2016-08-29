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
import org.apache.spark.ml._
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.mllib.linalg.VectorImplicits._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 */
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Whether to center the data with mean before scaling.
   * It will build a dense output, so take care when applying to sparse input.
   * Default: false
   * @group param
   */
  val withMean: BooleanParam = new BooleanParam(this, "withMean",
    "Whether to center data with mean")

  /** @group getParam */
  def getWithMean: Boolean = $(withMean)

  /**
   * Whether to scale the data to unit standard deviation.
   * Default: true
   * @group param
   */
  val withStd: BooleanParam = new BooleanParam(this, "withStd",
    "Whether to scale the data to unit standard deviation")

  /** @group getParam */
  def getWithStd: Boolean = $(withStd)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  setDefault(withMean -> false, withStd -> true)
}

/**
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 *
 * The "unit std" is computed using the
 * [[https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation
 *   corrected sample standard deviation]],
 * which is computed as the square root of the unbiased sample variance.
 */
@Since("1.2.0")
class StandardScaler @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Estimator[StandardScalerModel] with StandardScalerParams with DefaultParamsWritable {

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("stdScal"))

  /** @group setParam */
  @Since("1.2.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.2.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.4.0")
  def setWithMean(value: Boolean): this.type = set(withMean, value)

  /** @group setParam */
  @Since("1.4.0")
  def setWithStd(value: Boolean): this.type = set(withStd, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): StandardScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[OldVector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val scaler = new feature.StandardScaler(withMean = $(withMean), withStd = $(withStd))
    val scalerModel = scaler.fit(input)
    copyValues(new StandardScalerModel(uid, scalerModel.std, scalerModel.mean).setParent(this))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StandardScaler = defaultCopy(extra)
}

@Since("1.6.0")
object StandardScaler extends DefaultParamsReadable[StandardScaler] {

  @Since("1.6.0")
  override def load(path: String): StandardScaler = super.load(path)
}

/**
 * Model fitted by [[StandardScaler]].
 *
 * @param std Standard deviation of the StandardScalerModel
 * @param mean Mean of the StandardScalerModel
 */
@Since("1.2.0")
class StandardScalerModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("2.0.0") val std: Vector,
    @Since("2.0.0") val mean: Vector)
  extends Model[StandardScalerModel] with StandardScalerParams with MLWritable {

  import StandardScalerModel._

  /** @group setParam */
  @Since("1.2.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.2.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val scaler = new feature.StandardScalerModel(std, mean, $(withStd), $(withMean))

    // TODO: Make the transformer natively in ml framework to avoid extra conversion.
    val transformer: Vector => Vector = v => scaler.transform(OldVectors.fromML(v)).asML

    val scale = udf(transformer)
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(uid, std, mean)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new StandardScalerModelWriter(this)
}

@Since("1.6.0")
object StandardScalerModel extends MLReadable[StandardScalerModel] {

  private[StandardScalerModel]
  class StandardScalerModelWriter(instance: StandardScalerModel) extends MLWriter {

    private case class Data(std: Vector, mean: Vector)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.std, instance.mean)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class StandardScalerModelReader extends MLReader[StandardScalerModel] {

    private val className = classOf[StandardScalerModel].getName

    override def load(path: String): StandardScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
      val Row(std: Vector, mean: Vector) = MLUtils.convertVectorColumnsToML(data, "std", "mean")
        .select("std", "mean")
        .head()
      val model = new StandardScalerModel(metadata.uid, std, mean)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[StandardScalerModel] = new StandardScalerModelReader

  @Since("1.6.0")
  override def load(path: String): StandardScalerModel = super.load(path)
}
