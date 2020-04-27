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

import scala.collection.mutable.ArrayBuilder

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.{AttributeGroup, _}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * Params for [[Selector]] and [[SelectorModel]].
 */
private[feature] trait SelectorParams extends Params
  with HasFeaturesCol with HasOutputCol {
}

/**
 * Super class for all the feature selectors. The following selectors are supported:
 * 1. Chi-Square Selector
 * This feature selector is for categorical features and categorical labels.
 * 2. ANOVA F-value Classification Selector
 * This feature selector is for continuous features and categorical labels.
 * 3. Regression F-value Selector
 * This feature selector is for continuous features and continuous labels.
 * 4. Variance Threshold Selector
 * This feature selector removes all low-variance features. Features with a
 * variance not greater than the threshold will be removed.
 */
@Since("3.1.0")
private[ml] abstract class Selector[T <: SelectorModel[T]]
  extends Estimator[T] with SelectorParams with DefaultParamsWritable {

  /** @group setParam */
  @Since("3.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * get the indices of the selected features
   */
  protected[this] def getSelectionIndices(dataset: Dataset[_]): Array[Int]

  /**
   * Create a new instance of concrete SelectorModel.
   * @param indices The indices of the selected features
   * @return A new SelectorModel instance
   */
  protected[this] def createSelectorModel(
      uid: String,
      indices: Array[Int]): T

  @Since("3.1.0")
  override def fit(dataset: Dataset[_]): T = {
    transformSchema(dataset.schema, logging = true)
    val indices = getSelectionIndices(dataset).sorted

    copyValues(createSelectorModel(uid, indices)
      .setParent(this))
  }
}

/**
 * Model fitted by [[Selector]].
 */
@Since("3.1.0")
private[ml] abstract class SelectorModel[T <: SelectorModel[T]] (
    @Since("3.1.0") val uid: String,
    @Since("3.1.0") val selectedFeatures: Array[Int])
  extends Model[T] with SelectorParams with MLWritable {
  self: T =>

  if (selectedFeatures.length >= 2) {
    require(selectedFeatures.sliding(2).forall(l => l(0) < l(1)),
      "Index should be strictly increasing.")
  }

  /** @group setParam */
  @Since("3.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("3.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    val newSize = selectedFeatures.length
    val func = { vector: Vector =>
      vector match {
        case SparseVector(_, indices, values) =>
          val (newIndices, newValues) = compressSparse(indices, values)
          Vectors.sparse(newSize, newIndices, newValues)
        case DenseVector(values) =>
          Vectors.dense(selectedFeatures.map(values))
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }

    val transformer = udf(func)
    dataset.withColumn($(outputCol), transformer(col($(featuresCol))),
      outputSchema($(outputCol)).metadata)
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField = prepOutputField(schema)
    SchemaUtils.appendColumn(schema, newField)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   */
  private def prepOutputField(schema: StructType): StructField = {
    val selector = selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema($(featuresCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  private[ml] def compressSparse(
      indices: Array[Int],
      values: Array[Double]): (Array[Int], Array[Double]) = {
    val newValues = new ArrayBuilder.ofDouble
    val newIndices = new ArrayBuilder.ofInt
    var i = 0
    var j = 0
    while (i < indices.length && j < selectedFeatures.length) {
      val indicesIdx = indices(i)
      val filterIndicesIdx = selectedFeatures(j)
      if (indicesIdx == filterIndicesIdx) {
        newIndices += j
        newValues += values(i)
        j += 1
        i += 1
      } else {
        if (indicesIdx > filterIndicesIdx) {
          j += 1
        } else {
          i += 1
        }
      }
    }
    // TODO: Sparse representation might be ineffective if (newSize ~= newValues.size)
    (newIndices.result(), newValues.result())
  }
}
