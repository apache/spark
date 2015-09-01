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

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.param.{ParamMap, DoubleParam, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.mllib.linalg.{Vector, VectorUDT, Vectors}
import org.apache.spark.mllib.stat.Statistics
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

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

  override def validateParams(): Unit = {
    require($(min) < $(max), s"The specified min(${$(min)}) is larger or equal to max(${$(max)})")
  }
}

/**
 * :: Experimental ::
 * Rescale each feature individually to a common range [min, max] linearly using column summary
 * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
 * feature E is calculated as,
 *
 * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 *
 * For the case E_{max} == E_{min}, Rescaled(e_i) = 0.5 * (max + min)
 * Note that since zero values will probably be transformed to non-zero values, output of the
 * transformer will be DenseVector even for sparse input.
 */
@Experimental
class MinMaxScaler(override val uid: String)
  extends Estimator[MinMaxScalerModel] with MinMaxScalerParams {

  def this() = this(Identifiable.randomUID("minMaxScal"))

  setDefault(min -> 0.0, max -> 1.0)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  def setMax(value: Double): this.type = set(max, value)

  override def fit(dataset: DataFrame): MinMaxScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val summary = Statistics.colStats(input)
    copyValues(new MinMaxScalerModel(uid, summary.min, summary.max).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MinMaxScaler = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Model fitted by [[MinMaxScaler]].
 *
 * @param originalMin min value for each original column during fitting
 * @param originalMax max value for each original column during fitting
 *
 * TODO: The transformer does not yet set the metadata in the output column (SPARK-8529).
 */
@Experimental
class MinMaxScalerModel private[ml] (
    override val uid: String,
    val originalMin: Vector,
    val originalMax: Vector)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setMin(value: Double): this.type = set(min, value)

  /** @group setParam */
  def setMax(value: Double): this.type = set(max, value)

  override def transform(dataset: DataFrame): DataFrame = {
    val originalRange = (originalMax.toBreeze - originalMin.toBreeze).toArray
    val minArray = originalMin.toArray

    val reScale = udf { (vector: Vector) =>
      val scale = $(max) - $(min)

      // 0 in sparse vector will probably be rescaled to non-zero
      val values = vector.toArray
      val size = values.size
      var i = 0
      while (i < size) {
        val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
        values(i) = raw * scale + $(min)
        i += 1
      }
      Vectors.dense(values)
    }

    dataset.withColumn($(outputCol), reScale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): MinMaxScalerModel = {
    val copied = new MinMaxScalerModel(uid, originalMin, originalMax)
    copyValues(copied, extra).setParent(parent)
  }
}
