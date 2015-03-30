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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[IDF]] and [[IDFModel]].
 */
private[feature] trait IDFParams extends Params with HasInputCol with HasOutputCol {
  val minDocFreq = new IntParam(
    this, "minDocFreq", "minimum of documents in which a term should appear for filtering", Some(0))

  def getMinDocFreq: Int = {
    get(minDocFreq)
  }

  def setMinDocFreq(value: Int): this.type = {
    set(minDocFreq, value)
  }
}

/**
 * :: AlphaComponent ::
 * Compute the Inverse Document Frequency (IDF) given a collection of documents.
 */
@AlphaComponent
class IDF extends Estimator[IDFModel] with IDFParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): IDFModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val input = dataset.select(map(inputCol)).map { case Row(v: Vector) => v }
    val idf = new feature.IDF(getMinDocFreq).fit(input)
    val model = new IDFModel(this, map, idf)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
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
 * Model fitted by [[IDF]].
 */
@AlphaComponent
class IDFModel private[ml] (
    override val parent: IDF,
    override val fittingParamMap: ParamMap,
    idfModel: feature.IDFModel)
  extends Model[IDFModel] with IDFParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = this.paramMap ++ paramMap
    val idf = udf((v: Vector) => { idfModel.transform(v) } : Vector)
    dataset.withColumn(map(outputCol), idf(col(map(inputCol))))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
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
