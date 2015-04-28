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
import org.apache.spark.ml.util.SchemaUtils
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructType}

/**
 * Params for [[ChiSqSelector]] and [[ChiSqSelectorModel]].
 */
private[feature] trait ChiSqSelectorBase extends Params
  with HasInputCol with HasOutputCol with HasLabelCol {

  /**
   * Number of features that selector will select (ordered by statistic value descending).
   * @group param
   */
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by statistics value descending.")
  setDefault(numTopFeatures -> 1)

  /** @group getParam */
  def getNumTopFeatures: Int = getOrDefault(numTopFeatures)

  /** @group setParam */
  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)
}

/**
 * :: AlphaComponent ::
 * Compute the Chi-Square selector model given an `RDD` of `LabeledPoint` data.
 */
@AlphaComponent
final class ChiSqSelector extends Estimator[ChiSqSelectorModel] with ChiSqSelectorBase {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def fit(dataset: DataFrame, paramMap: ParamMap): ChiSqSelectorModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val input = dataset.select(map(labelCol), map(inputCol)).map {
      case Row(label: Double, features: Vector) =>
        LabeledPoint(label, features)
    }
    val chiSqSelector = new feature.ChiSqSelector(map(numTopFeatures)).fit(input)
    val model = new ChiSqSelectorModel(this, paramMap, chiSqSelector)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    SchemaUtils.checkColumnType(schema, map(inputCol), new VectorUDT)
    SchemaUtils.checkColumnType(schema, map(labelCol), DoubleType)
    SchemaUtils.appendColumn(schema, map(outputCol), new VectorUDT)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[ChiSqSelector]].
 */
@AlphaComponent
class ChiSqSelectorModel private[ml] (
    override val parent: ChiSqSelector,
    override val fittingParamMap: ParamMap,
    chiSqSelector: feature.ChiSqSelectorModel)
  extends Model[ChiSqSelectorModel] with ChiSqSelectorBase {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val idf = udf { vec: Vector => chiSqSelector.transform(vec) }
    dataset.withColumn(map(outputCol), idf(col(map(inputCol))))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    SchemaUtils.checkColumnType(schema, map(inputCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, map(outputCol), new VectorUDT)
  }
}
