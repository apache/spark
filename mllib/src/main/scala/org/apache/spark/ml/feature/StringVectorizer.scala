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
import org.apache.spark.ml.attribute.{Attribute, BinaryAttribute, NominalAttribute}
import org.apache.spark.mllib.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, NumericType, StringType, StructType}
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[StringVectorizer]] and [[StringVectorizerModel]].
 */
private[feature] trait StringVectorizerBase extends Params with HasInputCol with HasOutputCol {

  /**
   * Whether to include a component in the encoded vectors for the first category, defaults to true.
   * @group param
   */
  final val includeFirst: BooleanParam =
    new BooleanParam(this, "includeFirst", "include first category")
  setDefault(includeFirst -> true)

  /** @group setParam */
  def setIncludeFirst(value: Boolean): this.type = set(includeFirst, value)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType

    require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be either string type or numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields

    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
 
    val attr = NominalAttribute.defaultAttr.withName(outputColName)
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

/**
 * :: AlphaComponent ::
 * A label vectorizer that maps a string column of labels to a vector column with binary values.
 * If the input column is numeric, we cast it to string and index the string values.
 * The output should be the same as chaining [[StringIndexer]] and [[OneHotEncoder]].
 */
@AlphaComponent
class StringVectorizer extends Estimator[StringVectorizerModel] with StringVectorizerBase {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def fit(dataset: DataFrame): StringVectorizerModel = {
    val counts = dataset.select(col($(inputCol)).cast(StringType))
      .map(_.getString(0))
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    copyValues(new StringVectorizerModel(this, labels))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}

/**
 * :: AlphaComponent ::
 * Model fitted by [[StringVectorizer]].
 */
@AlphaComponent
class StringVectorizerModel private[ml] (
    override val parent: StringVectorizer,
    labels: Array[String]) extends Model[StringVectorizerModel] with StringVectorizerBase {

  val categories: Array[String] = labels

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
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  private def vectorize(label: Double): Vector = {
    val first = $(includeFirst)
    val vecLen = if (first) categories.length else categories.length - 1
    val oneValue = Array(1.0)
    val emptyValues = Array[Double]()
    val emptyIndices = Array[Int]()

    val values = if (first || label != 0.0) oneValue else emptyValues
    val indices = if (first) {
      Array(label.toInt)
    } else if (label != 0.0) {
      Array(label.toInt - 1)
    } else {
      emptyIndices
    }
    Vectors.sparse(vecLen, indices, values)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema)
    val vectorizer = udf { label: String =>
      if (labelToIndex.contains(label)) {
        vectorize(labelToIndex(label))
      } else {
        throw new SparkException(s"Unseen label: $label.")
      }
    }
    val outputColName = $(outputCol)
    val attrValues = (if ($(includeFirst)) categories else categories.drop(1)).toArray
    val metadata = NominalAttribute.defaultAttr.withName(outputColName).withValues(attrValues)
      .toMetadata()
    dataset.select(col("*"),
      vectorizer(dataset($(inputCol)).cast(StringType)).as(outputColName, metadata))
  }

  /**
   * Returns the data type of the output column.
   */
  protected def outputDataType: DataType = new VectorUDT

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }
}
