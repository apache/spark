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
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.{DoubleParam, ParamMap, Params}
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[MinMaxScaler]] and [[MinMaxScalerModel]].
 */
private[feature] trait MinMaxScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * lower bound after transformation, shared by all features
   * Default: 0.0
   * @group param
   */
  val min: DoubleParam = new DoubleParam(this, "min",
    "lower bound of the output feature range")

  /** @group getParam */
  def getMin: Double = $(min)

  /**
   * upper bound after transformation, shared by all features
   * Default: 1.0
   * @group param
   */
  val max: DoubleParam = new DoubleParam(this, "max",
    "upper bound of the output feature range")

  /** @group getParam */
  def getMax: Double = $(max)

  setDefault(min -> 0.0, max -> 1.0)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    require($(min) < $(max), s"The specified min(${$(min)}) is larger or equal to max(${$(max)})")
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

}

/**
 * Rescale each feature individually to a common range [min, max] linearly using column summary
 * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
 * feature E is calculated as:
 *
 * <blockquote>
 *    $$
 *    Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 *    $$
 * </blockquote>
 *
 * For the case \(E_{max} == E_{min}\), \(Rescaled(e_i) = 0.5 * (max + min)\).
 *
 * @note Since zero values will probably be transformed to non-zero values, output of the
 * transformer will be DenseVector even for sparse input.
 */
@Since("1.5.0")
class MinMaxScaler @Since("1.5.0") (@Since("1.5.0") override val uid: String)
  extends Estimator[MinMaxScalerModel] with MinMaxScalerParams with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("minMaxScal"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): MinMaxScalerModel = {
    transformSchema(dataset.schema, logging = true)

    val Row(max: Vector, min: Vector) = dataset
      .select(Summarizer.metrics("max", "min").summary(col($(inputCol))).as("summary"))
      .select("summary.max", "summary.min")
      .first()

    copyValues(new MinMaxScalerModel(uid, min.compressed, max.compressed).setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScaler = defaultCopy(extra)
}

@Since("1.6.0")
object MinMaxScaler extends DefaultParamsReadable[MinMaxScaler] {

  @Since("1.6.0")
  override def load(path: String): MinMaxScaler = super.load(path)
}

/**
 * Model fitted by [[MinMaxScaler]].
 *
 * @param originalMin min value for each original column during fitting
 * @param originalMax max value for each original column during fitting
 */
@Since("1.5.0")
class MinMaxScalerModel private[ml] (
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val originalMin: Vector,
    @Since("2.0.0") val originalMax: Vector)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams with MLWritable {

  import MinMaxScalerModel._

  // For ml connect only
  private[ml] def this() = this("", Vectors.empty, Vectors.empty)

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  @Since("1.5.0")
  def setMax(value: Double): this.type = set(max, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val numFeatures = originalMax.size
    val scale = $(max) - $(min)
    val minValue = $(min)

    // transformed value for constant cols
    val constantOutput = ($(min) + $(max)) / 2
    val minArray = originalMin.toArray

    val scaleArray = Array.tabulate(numFeatures) { i =>
      val range = originalMax(i) - originalMin(i)
      // scaleArray(i) == 0 iff i-th col is constant (range == 0)
      if (range != 0) scale / range else 0.0
    }

    val transformer = udf { vector: Vector =>
      require(vector.size == numFeatures,
        s"Number of features must be $numFeatures but got ${vector.size}")
      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      var i = 0
      while (i < numFeatures) {
        if (!values(i).isNaN) {
          if (scaleArray(i) != 0) {
            values(i) = (values(i) - minArray(i)) * scaleArray(i) + minValue
          } else {
            // scaleArray(i) == 0 means i-th col is constant
            values(i) = constantOutput
          }
        }
        i += 1
      }
      Vectors.dense(values).compressed
    }

    dataset.withColumn($(outputCol), transformer(col($(inputCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    var outputSchema = validateAndTransformSchema(schema)
    if ($(outputCol).nonEmpty) {
      outputSchema = SchemaUtils.updateAttributeGroupSize(outputSchema,
        $(outputCol), originalMin.size)
    }
    outputSchema
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): MinMaxScalerModel = {
    val copied = new MinMaxScalerModel(uid, originalMin, originalMax)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new MinMaxScalerModelWriter(this)

  @Since("3.0.0")
  override def toString: String = {
    s"MinMaxScalerModel: uid=$uid, numFeatures=${originalMin.size}, min=${$(min)}, " +
      s"max=${$(max)}"
  }
}

@Since("1.6.0")
object MinMaxScalerModel extends MLReadable[MinMaxScalerModel] {
  private[ml] case class Data(originalMin: Vector, originalMax: Vector)

  private[MinMaxScalerModel]
  class MinMaxScalerModelWriter(instance: MinMaxScalerModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.originalMin, instance.originalMax)
      val dataPath = new Path(path, "data").toString
      ReadWriteUtils.saveObject[Data](dataPath, data, sparkSession)
    }
  }

  private class MinMaxScalerModelReader extends MLReader[MinMaxScalerModel] {

    private val className = classOf[MinMaxScalerModel].getName

    override def load(path: String): MinMaxScalerModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = ReadWriteUtils.loadObject[Data](dataPath, sparkSession)
      val model = new MinMaxScalerModel(metadata.uid, data.originalMin, data.originalMax)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[MinMaxScalerModel] = new MinMaxScalerModelReader

  @Since("1.6.0")
  override def load(path: String): MinMaxScalerModel = super.load(path)
}
