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
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{MutableEstimator, Transformer}
import org.apache.spark.ml.attribute.{Attribute, NominalAttribute}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.util.collection.OpenHashMap


/**
 * :: Experimental ::
 *
 * A label indexer that maps a String column of labels (categories) to a column of label indices.
 * If the input column is numeric, we cast values to strings and index the string values.
 *
 * When this class is used as an Estimator, fitting computes a label index.
 * The user may also specify a pre-computed index by setting `labels`.
 *
 * @see [[IndexToString]] for the inverse transformation
 */
@Experimental
class StringIndexer(override val uid: String) extends MutableEstimator[StringIndexer]
  with HasInputCol with HasOutputCol with HasHandleInvalid with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("strIdx"))

  ////////////////////////////////////////////////////////////////////////////
  // Params
  ////////////////////////////////////////////////////////////////////////////

  private[ml] val labelsParam: StringArrayParam = new StringArrayParam(this, "labelsParam",
    "Array of labels specifying index-string mapping to use during transform()." +
      " If not provided, then this is computed during fit().")

  /**
   * Array of labels specifying the index-string mapping to use during [[transform()]].
   *
   * If the user does not specify [[labels]], then [[labels]] will be computed during [[fit()]].
   * The computed index will be accessible via this param.
   *
   * If the user specifies [[labels]], then the given [[labels]] will be used for [[transform()]],
   * and nothing will happen during [[fit()]].
   *
   * Default: Not set.  [[labels]] are computed during [[fit()]].
   *
   * @group param
   */
  final def labels: Array[String] = {
    if (!isSet(labelsParam)) {
      throw new RuntimeException("StringIndexer.labels was called, but labels are not yet" +
        " defined.  Either set labels manually, or call fit() to compute label index.")
    }
    $(labelsParam)
  }

  /** @group setParam */
  def setLabels(value: Array[String]): this.type = set(labelsParam, value)

  /** @group setParam */
  def setHandleInvalid(value: String): this.type = set(handleInvalid, value)
  setDefault(handleInvalid, "error")

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  ////////////////////////////////////////////////////////////////////////////
  // Estimator API
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Computes a label index for [[inputCol]], setting [[labels]] to store the computes index.
   *
   * The indices are in [0, numLabels), ordered by decreasing label frequencies so that
   * the most frequent label gets index 0.
   */
  override def fit(dataset: DataFrame): Unit = {
    val counts = dataset.select(col($(inputCol)).cast(StringType))
      .rdd
      .map(_.getString(0))
      .countByValue()
    val labels = counts.toSeq.sortBy(-_._2).map(_._1).toArray
    setLabels(labels)
  }

  /**
   * Maps the [[inputCol]] column of labels (categories) to a new [[outputCol]] column of label
   * indices.
   */
  override def transform(dataset: DataFrame): DataFrame = {
    if (!dataset.schema.fieldNames.contains($(inputCol))) {
      logInfo(s"Input column ${$(inputCol)} does not exist during transformation. " +
        "Skip StringIndexer.")
      return dataset
    }
    require(isSet(labelsParam), s"StringIndexer.transform() was called without labels Param ever" +
      s" being set.  Either set labels manually, or call fit() to compute the labels index.")
    transformSchema(dataset.schema)

    val l2i = labelToIndex

    val indexer = udf { label: String =>
      if (l2i.contains(label)) {
        l2i(label)
      } else {
        throw new SparkException(s"Unseen label: $label.")
      }
    }

    val metadata = getOutputAttr.toMetadata()

    // If we are skipping invalid records, filter them out.
    val filteredDataset = getHandleInvalid match {
      case "skip" =>
        val filterer = udf { label: String =>
          labelToIndex.contains(label)
        }
        dataset.where(filterer(dataset($(inputCol))))
      case _ => dataset
    }
    filteredDataset.select(col("*"),
      indexer(dataset($(inputCol)).cast(StringType)).as($(outputCol), metadata))
  }

  override def transformSchema(schema: StructType): StructType = {
    if (schema.fieldNames.contains($(inputCol))) {
      val inputColName = $(inputCol)
      val inputDataType = schema(inputColName).dataType
      require(inputDataType == StringType || inputDataType.isInstanceOf[NumericType],
        s"The input column $inputColName must be either string type or numeric type, " +
          s"but got $inputDataType.")
      val inputFields = schema.fields
      val outputColName = $(outputCol)
      require(inputFields.forall(_.name != outputColName),
        s"Output column $outputColName already exists.")
      val outputFields = inputFields :+ getOutputAttr.toStructField()
      StructType(outputFields)
    } else {
      // If the input column does not exist during transformation, we skip StringIndexer.
      schema
    }
  }

  private def getOutputAttr: NominalAttribute = {
    val attr = NominalAttribute.defaultAttr.withName($(outputCol))
    if (isSet(labelsParam)) attr.withValues(labels) else attr
  }

  override def copy(extra: ParamMap): StringIndexer = {
    val copied = new StringIndexer(uid)
    copyValues(copied, extra)
  }

  ////////////////////////////////////////////////////////////////////////////
  // Internals
  ////////////////////////////////////////////////////////////////////////////

  // A reference to the array used to compute the labels.
  private[this] var cachedArray: Array[String] = null

  // A cache of the indexes.
  private[this] var cachedLabelToIndex: OpenHashMap[String, Double] = null

  private def labelToIndex: OpenHashMap[String, Double] = this.synchronized {
    // Do reference equality to check if the label has been changed.
    val currentArray = labels
    if (cachedArray == null || !cachedArray.eq(currentArray)) {
      cachedArray = currentArray
      val n = currentArray.length
      cachedLabelToIndex = new OpenHashMap[String, Double](n)
      var i = 0
      while (i < n) {
        cachedLabelToIndex.update(currentArray(i), i)
        i += 1
      }
    }
    cachedLabelToIndex
  }
}

@Since("1.6.0")
object StringIndexer extends DefaultParamsReadable[StringIndexer] {

  def apply(uid: String, labels: Array[String]): StringIndexer = {
    new StringIndexer(uid).setLabels(labels)
  }

  @Since("1.6.0")
  override def load(path: String): StringIndexer = super.load(path)
}

/**
 * :: Experimental ::
 * A [[Transformer]] that maps a column of indices back to a new column of corresponding
 * string values.
 * The index-string mapping is either from the ML attributes of the input column,
 * or from user-supplied labels (which take precedence over ML attributes).
 *
 * @see [[StringIndexer]] for converting strings into indices
 */
@Experimental
class IndexToString private[ml] (override val uid: String)
  extends Transformer with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() =
    this(Identifiable.randomUID("idxToStr"))

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  def setLabels(value: Array[String]): this.type = set(labels, value)

  /**
   * Optional param for array of labels specifying index-string mapping.
   *
   * Default: Empty array, in which case [[inputCol]] metadata is used for labels.
    *
    * @group param
   */
  final val labels: StringArrayParam = new StringArrayParam(this, "labels",
    "Optional array of labels specifying index-string mapping." +
      " If not provided or if empty, then metadata from inputCol is used instead.")
  setDefault(labels, Array.empty[String])

  /** @group getParam */
  final def getLabels: Array[String] = $(labels)

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
    val outputFields = inputFields :+ StructField($(outputCol), StringType)
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

@Since("1.6.0")
object IndexToString extends DefaultParamsReadable[IndexToString] {

  @Since("1.6.0")
  override def load(path: String): IndexToString = super.load(path)
}
