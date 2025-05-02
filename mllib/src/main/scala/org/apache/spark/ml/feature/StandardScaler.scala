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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
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
 * <a href="https://en.wikipedia.org/wiki/Standard_deviation#Corrected_sample_standard_deviation">
 * corrected sample standard deviation</a>,
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

    val Row(mean: Vector, std: Vector) = dataset
      .select(Summarizer.metrics("mean", "std").summary(col($(inputCol))).as("summary"))
      .select("summary.mean", "summary.std")
      .first()

    copyValues(new StandardScalerModel(uid, std.compressed, mean.compressed).setParent(this))
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

  // For ml connect only
  private[ml] def this() = this("", Vectors.empty, Vectors.empty)

  /** @group setParam */
  @Since("1.2.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.2.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)
    val shift = if ($(withMean)) mean.toArray else Array.emptyDoubleArray
    val scale = if ($(withStd)) {
      std.toArray.map { v => if (v == 0) 0.0 else 1.0 / v }
    } else Array.emptyDoubleArray

    val func = getTransformFunc(shift, scale, $(withMean), $(withStd))
    val transformer = udf(func)

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), mean.size)
    }
    outputSchema
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): StandardScalerModel = {
    val copied = new StandardScalerModel(uid, std, mean)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new StandardScalerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"StandardScalerModel: uid=$uid, numFeatures=${mean.size}, withMean=${$(withMean)}, " +
      s"withStd=${$(withStd)}"
  }
}

@Since("1.6.0")
object StandardScalerModel extends MLReadable[StandardScalerModel] {
  private[ml] case class Data(std: Vector, mean: Vector)

  private[StandardScalerModel]
  class StandardScalerModelWriter(instance: StandardScalerModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.std, instance.mean)
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveObject[Data](dataPath, data, sparkSession)
    }
  }

  private class StandardScalerModelReader extends MLReader[StandardScalerModel] {

    private val className = classOf[StandardScalerModel].getName

    override def load(path: String): StandardScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = ReadWriteUtils.loadObject[Data](dataPath, sparkSession)
      val model = new StandardScalerModel(metadata.uid, data.std, data.mean)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[StandardScalerModel] = new StandardScalerModelReader

  @Since("1.6.0")
  override def load(path: String): StandardScalerModel = super.load(path)

  private[spark] def transformWithBoth(
      shift: Array[Double],
      scale: Array[Double],
      values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) = (values(i) - shift(i)) * scale(i)
      i += 1
    }
    values
  }

  private[spark] def transformWithShift(
      shift: Array[Double],
      values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) -= shift(i)
      i += 1
    }
    values
  }

  private[spark] def transformDenseWithScale(
      scale: Array[Double],
      values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) *= scale(i)
      i += 1
    }
    values
  }

  private[spark] def transformSparseWithScale(
      scale: Array[Double],
      indices: Array[Int],
      values: Array[Double]): Array[Double] = {
    var i = 0
    while (i < values.length) {
      values(i) *= scale(indices(i))
      i += 1
    }
    values
  }

  private[spark] def getTransformFunc(
      shift: Array[Double],
      scale: Array[Double],
      withShift: Boolean,
      withScale: Boolean): Vector => Vector = {
    (withShift, withScale) match {
      case (true, true) =>
        vector: Vector =>
          val values = vector match {
            case d: DenseVector => d.values.clone()
            case v: Vector => v.toArray
          }
          val newValues = transformWithBoth(shift, scale, values)
          Vectors.dense(newValues)

      case (true, false) =>
        vector: Vector =>
          val values = vector match {
            case d: DenseVector => d.values.clone()
            case v: Vector => v.toArray
          }
          val newValues = transformWithShift(shift, values)
          Vectors.dense(newValues)

      case (false, true) =>
        vector: Vector =>
          vector match {
            case DenseVector(values) =>
              val newValues = transformDenseWithScale(scale, values.clone())
              Vectors.dense(newValues)
            case SparseVector(size, indices, values) =>
              val newValues = transformSparseWithScale(scale, indices, values.clone())
              Vectors.sparse(size, indices, newValues)
            case v =>
              throw new IllegalArgumentException(s"Unknown vector type ${v.getClass}.")
          }

      case (false, false) =>
        vector: Vector => vector
    }
  }
}
