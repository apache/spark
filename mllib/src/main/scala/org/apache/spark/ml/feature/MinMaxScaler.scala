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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[MinMaxScaler]] and [[MinMaxScalerModel]].
 */
private[feature] trait MinMaxScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * new minimum value after transformation, shared by all features
   * Default: 0.0
   * @group param
   */
  val lowerBound: DoubleParam = new DoubleParam(this, "lowerBound",
    "lower bound of the expected feature range")

  /**
   * new maximum value after transformation, shared by all features
   * Default: 1.0
   * @group param
   */
  val upperBound: DoubleParam = new DoubleParam(this, "upperBound",
    "upper bound of the expected feature range")
}

/**
 * :: AlphaComponent ::
 * Rescale original data values to a new range [lowerBound, upperBound] linearly using column
 * summary statistics (minimum and maximum), which is also known as min-max normalization or
 * Rescaling. The rescaled value for feature E is calculated as,
 *
 * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (upperBound - lowerBound) + lowerBound
 */
@AlphaComponent
class MinMaxScaler extends Estimator[MinMaxScalerModel] with MinMaxScalerParams {

  setDefault(lowerBound -> 0.0, upperBound -> 1.0)

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLowerBound(value: Double): this.type = set(lowerBound, value)

  /** @group setParam */
  def setUpperBound(value: Double): this.type = set(upperBound, value)

  override def fit(dataset: DataFrame): MinMaxScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.MinMaxScaler(lowerBound = $(lowerBound), upperBound = $(upperBound))
    val scalerModel = scaler.fit(input)
    copyValues(new MinMaxScalerModel(this, scalerModel))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[MinMaxScaler]].
 */
@AlphaComponent
class MinMaxScalerModel private[ml] (
    override val parent: MinMaxScaler,
    scaler: feature.MinMaxScalerModel)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val scale = udf { scaler.transform _ }
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}
