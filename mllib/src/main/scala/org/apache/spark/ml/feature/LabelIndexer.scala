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

import org.apache.spark.SparkException
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.param._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[LabelIndexer]] and [[LabelIndexerModel]].
 */
private[feature] trait LabelIndexerBase extends Params with HasLabelCol with HasOutputCol {

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = this.paramMap ++ paramMap
    val labelType = schema(map(labelCol)).dataType
    require(labelType == StringType, s"The label column must be string-typed but got $labelType.")
    val inputFields = schema.fields
    val outputColName = map(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName(map(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

/**
 * :: AlphaComponent ::
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * The indices are in [0, numLabels), ordered by label frequencies.
 * So the most frequent label gets index 0.
 */
@AlphaComponent
class LabelIndexer extends Estimator[LabelIndexerModel] with LabelIndexerBase {

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // TODO: handle unseen labels

  override def fit(dataset: DataFrame, paramMap: ParamMap): LabelIndexerModel = {
    val map = this.paramMap ++ paramMap
    val counts = dataset.select(map(labelCol)).map(_.getString(0)).countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    val model = new LabelIndexerModel(this, map, labels)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[LabelIndexer]].
 */
@AlphaComponent
class LabelIndexerModel private[ml] (
    override val parent: LabelIndexer,
    override val fittingParamMap: ParamMap,
    labels: Array[String]) extends Model[LabelIndexerModel] with LabelIndexerBase {

  private val labelToIndex: OpenHashMap[String, Double] = {
    val n = labels.length
    val map = new OpenHashMap[String, Double](n)
    var i = 0
    while (i < n) {
      map.update(labels(i), i)
      i += 1
    }
    map
  }

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    val map = this.paramMap ++ paramMap
    val indexer = udf { label: String =>
      if (labelToIndex.contains(label)) {
        labelToIndex(label)
      } else {
        throw new SparkException(s"Unseen label: $label.")
      }
    }
    val outputColName = map(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues(labels).toStructField().metadata
    dataset.select(col("*"), indexer(dataset(map(labelCol))).as(outputColName, metadata))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    validateAndTransformSchema(schema, paramMap)
  }
}
