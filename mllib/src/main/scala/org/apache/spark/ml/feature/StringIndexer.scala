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
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, NumericType, StringType, StructType}
import org.apache.spark.util.collection.OpenHashMap

/**
 * Base trait for [[StringIndexer]] and [[StringIndexerModel]].
 */
private[feature] trait StringIndexerBase extends Params with HasInputCol with HasOutputCol {

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
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }
}

/**
 * :: Experimental ::
 * A label indexer that maps a string column of labels to an ML column of label indices.
 * If the input column is numeric, we cast it to string and index the string values.
 * The indices are in [0, numLabels), ordered by label frequencies.
 * So the most frequent label gets index 0.
 *
 * @see [[IndexToString]] for the inverse transformation
 */
@Experimental
class StringIndexer(override val uid: String) extends Estimator[StringIndexerModel]
  with StringIndexerBase {

  def this() = this(Identifiable.randomUID("strIdx"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  // TODO: handle unseen labels

  override def fit(dataset: DataFrame): StringIndexerModel = {
    val counts = dataset.select(col($(inputCol)).cast(StringType))
      .map(_.getString(0))
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    copyValues(new StringIndexerModel(uid, labels).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  override def copy(extra: ParamMap): StringIndexer = defaultCopy(extra)
}

/**
 * :: Experimental ::
 * Model fitted by [[StringIndexer]].
 *
 * NOTE: During transformation, if the input column does not exist,
 * [[StringIndexerModel.transform]] would return the input dataset unmodified.
 * This is a temporary fix for the case when target labels do not exist during prediction.
 *
 * @param labels  Ordered list of labels, corresponding to indices to be assigned
 */
@Experimental
class StringIndexerModel (
    override val uid: String,
    val labels: Array[String]) extends Model[StringIndexerModel] with StringIndexerBase {

  def this(labels: Array[String]) = this(Identifiable.randomUID("strIdx"), labels)

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

  override def transform(dataset: DataFrame): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip StringIndexerModel.")
      return dataset
    }

    val indexer = udf { label: String =>
      if (labelToIndex.contains(label)) {
        labelToIndex(label)
      } else {
        // TODO: handle unseen labels
        throw new SparkException(s"Unseen label: $label.")
      }
    }
    val outputColName = $(outputCol)
    val metadata = NominalAttribute.defaultAttr
      .withName(outputColName).withValues(labels).toMetadata()
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as(outputColName, metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      validateAndTransformSchema(schema)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexerModel.
      schema
    }
  }

  override def copy(extra: ParamMap): StringIndexerModel = {
    val copied = new StringIndexerModel(uid, labels)
    copyValues(copied, extra).setParent(parent)
  }
}

/**
 * :: Experimental ::
 * A [[Transformer]] that maps a column of string indices back to a new column of corresponding
 * string values using either the ML attributes of the input column, or if provided using the labels
 * supplied by the user.
 * All original columns are kept during transformation.
 *
 * @see [[StringIndexer]] for converting strings into indices
 */
@Experimental
class IndexToString private[ml] (
  override val uid: String) extends Transformer
    with HasInputCol with HasOutputCol {

  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Optional labels to be provided by the user, if not supplied column
   * metadata is read for labels. The default value is an empty array,
   * but the empty array is ignored and column metadata used instead.
   * @group setParam
   */
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Param for array of labels.
   * Optional labels to be provided by the user, if not supplied column
   * metadata is read for labels.
   * @group param
   */
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "array of labels, if not provided metadata from inputCol is used instead.")
  setDefault(labels, Array.empty[String])

  /**
   * Optional labels to be provided by the user, if not supplied column
   * metadata is read for labels.
   * @group getParam
   */
  final def getLabels: Array[String] = $(labels)

  /** Transform the schema for the inverse transformation */
  override def transformSchema(schema: StructType): StructType = {
    val inputColName = $(inputCol)
    val inputDataType = schema(inputColName).dataType
    require(inputDataType.isInstanceOf[NumericType],
      s"The input column $inputColName must be a numeric type, " +
        s"but got $inputDataType.")
    val inputFields = schema.fields
    val outputColName = $(outputCol)
    require(inputFields.forall(_.name != outputColName),
      s"Output column $outputColName already exists.")
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    val outputFields = inputFields :+ attr.toStructField()
    StructType(outputFields)
  }

  override def transform(dataset: DataFrame): DataFrame = {
    val inputColSchema = dataset.schema($(inputCol))
    // If the labels array is empty use column metadata
    val values = if ($(labels).isEmpty) {
      Attribute.fromStructField(inputColSchema)
        .asInstanceOf[NominalAttribute].values.get
    } else {
      $(labels)
    }
    val indexer = udf { index: Double =>
      val idx = index.toInt
      if (0 <= idx && idx < values.length) {
        values(idx)
      } else {
        throw new SparkException(s"Unseen index: $index ??")
      }
    }
    val outputColName = $(outputCol)
    dataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(DoubleType)).as(outputColName))
  }

  override def copy(extra: ParamMap): IndexToString = {
    defaultCopy(extra)
  }
}
