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
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.Dsl._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 */
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol

/**
 * :: AlphaComponent ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 */
@AlphaComponent
class StandardScaler extends Estimator[StandardScalerModel] with StandardScalerParams {

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): StandardScalerModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val input = dataset.select(map(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.StandardScaler().fit(input)
    val model = new StandardScalerModel(this, map, scaler)
    Params.inheritValues(map, this, model)
    model
  }

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${map(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains(map(outputCol)),
      s"Output column ${map(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField(map(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[StandardScaler]].
 */
@AlphaComponent
class StandardScalerModel private[ml] (
    override val parent: StandardScaler,
    override val fittingParamMap: ParamMap,
    scaler: feature.StandardScalerModel)
  extends Model[StandardScalerModel] with StandardScalerParams {

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val scale: (Vector) => Vector = (v) => {
      scaler.transform(v)
    }
    dataset.select($"*", callUDF(scale, col(map(inputCol))).as(map(outputCol)))
  }

  private[ml] override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${map(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains(map(outputCol)),
      s"Output column ${map(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField(map(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }
}
