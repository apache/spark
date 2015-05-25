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
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.MultivariateOnlineSummarizer
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
    "lower bound of the expected feature range")

  /**
   * upper bound after transformation, shared by all features
   * Default: 1.0
   * @group param
   */
  val max: DoubleParam = new DoubleParam(this, "max",
    "upper bound of the expected feature range")
}

/**
 * :: AlphaComponent ::
 * Rescale each feature individually to a common range [min, max] linearly using column summary
 * statistics, which is also known as min-max normalization or Rescaling. The rescaled value for
 * feature E is calculated as, *
 *
 * Rescaled(e_i) = \frac{e_i - E_{min}}{E_{max} - E_{min}} * (max - min) + min
 */
@AlphaComponent
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
    val summary = input.treeAggregate(new MultivariateOnlineSummarizer)(
      (aggregator, data) => aggregator.add(data),
      (aggregator1, aggregator2) => aggregator1.merge(aggregator2))

    copyValues(new MinMaxScalerModel(uid, summary.min, summary.max).setParent(this))
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

  override def validateParams(): Unit = {
    require($(min) <= $(max))
  }

}

/**
 * :: AlphaComponent ::
 * Model fitted by [[MinMaxScaler]].
 */
@AlphaComponent
class MinMaxScalerModel private[ml] (
    override val uid: String,
    val originMin: Vector,
    val originMax: Vector)
  extends Model[MinMaxScalerModel] with MinMaxScalerParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)

    val scale = udf { (vector: Vector) => {
      val scale = $(max) - $(min)
      val result = vector match {
        case DenseVector(vs) =>
          val values = vs.clone()
          val size = values.size
          var i = 0
          while(i < size) {
            val originalRange = originMax(i) - originMin(i)
            val raw = if(originalRange != 0) (values(i) - originMin(i)) / originalRange else 0.5
            values(i) =  raw * scale + $(min)
            i += 1
          }
          Vectors.dense(values)
        case SparseVector(size, indices, vs) =>
          // For sparse vector, the `index` array inside sparse vector object will not be changed,
          // so we can re-use it to save memory.
          val values = vs.clone()
          val nnz = values.size
          var i = 0
          while (i < nnz) {
            val index = indices(i)
            val originRange = originMax(index) - originMin(index)
            val raw = if(originRange != 0) (values(i) - originMin(index)) / originRange else 0.5
            values(i) = raw  * scale + $(min)
            i += 1
          }
          Vectors.sparse(size, indices, values)
        case v => throw new IllegalArgumentException("Do not support vector type " + v.getClass)
      }
      result
    }}
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
