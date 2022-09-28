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
  with HasFeaturesCol with HasLabelCol with HasOutputCol {

  /**
   * Number of features that selector will select, ordered by ascending p-value. If the
   * number of features is less than numTopFeatures, then this will select all features.
   * Only applicable when selectorType = "numTopFeatures".
   * The default value of numTopFeatures is 50.
   *
   * @group param
   */
  @Since("3.1.0")
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by ascending p-value. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))

  /** @group getParam */
  @Since("3.1.0")
  def getNumTopFeatures: Int = $(numTopFeatures)

  /**
   * Percentile of features that selector will select, ordered by ascending p-value.
   * Only applicable when selectorType = "percentile".
   * Default value is 0.1.
   * @group param
   */
  @Since("3.1.0")
  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by ascending p-value.",
    ParamValidators.inRange(0, 1))

  /** @group getParam */
  @Since("3.1.0")
  def getPercentile: Double = $(percentile)

  /**
   * The highest p-value for features to be kept.
   * Only applicable when selectorType = "fpr".
   * Default value is 0.05.
   * @group param
   */
  @Since("3.1.0")
  final val fpr = new DoubleParam(this, "fpr", "The highest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))

  /** @group getParam */
  @Since("3.1.0")
  def getFpr: Double = $(fpr)

  /**
   * The upper bound of the expected false discovery rate.
   * Only applicable when selectorType = "fdr".
   * Default value is 0.05.
   * @group param
   */
  @Since("3.1.0")
  final val fdr = new DoubleParam(this, "fdr",
    "The upper bound of the expected false discovery rate.", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getFdr: Double = $(fdr)

  /**
   * The upper bound of the expected family-wise error rate.
   * Only applicable when selectorType = "fwe".
   * Default value is 0.05.
   * @group param
   */
  @Since("3.1.0")
  final val fwe = new DoubleParam(this, "fwe",
    "The upper bound of the expected family-wise error rate.", ParamValidators.inRange(0, 1))

  /** @group getParam */
  def getFwe: Double = $(fwe)

  /**
   * The selector type.
   * Supported options: "numTopFeatures" (default), "percentile", "fpr", "fdr", "fwe"
   * @group param
   */
  @Since("3.1.0")
  final val selectorType = new Param[String](this, "selectorType",
    "The selector type. Supported options: numTopFeatures, percentile, fpr, fdr, fwe",
    ParamValidators.inArray(Array("numTopFeatures", "percentile", "fpr", "fdr",
      "fwe")))

  /** @group getParam */
  @Since("3.1.0")
  def getSelectorType: String = $(selectorType)

  setDefault(numTopFeatures -> 50, percentile -> 0.1, fpr -> 0.05, fdr -> 0.05, fwe -> 0.05,
    selectorType -> "numTopFeatures")
}

/**
 * Super class for feature selectors.
 * 1. Chi-Square Selector
 * This feature selector is for categorical features and categorical labels.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
 * `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a hypothesis.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-value are below a threshold, thus controlling the false
 *    positive rate of selection.
 *  - `fdr` uses the [Benjamini-Hochberg procedure]
 *    (https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure)
 *    to choose all features whose false discovery rate is below a threshold.
 *  - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
 *    1/numFeatures, thus controlling the family-wise error rate of selection.
 * By default, the selection method is `numTopFeatures`, with the default number of top features
 * set to 50.
 */
private[ml] abstract class Selector[T <: SelectorModel[T]]
  extends Estimator[T] with SelectorParams with DefaultParamsWritable {

  /** @group setParam */
  @Since("3.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  /** @group setParam */
  @Since("3.1.0")
  def setPercentile(value: Double): this.type = set(percentile, value)

  /** @group setParam */
  @Since("3.1.0")
  def setFpr(value: Double): this.type = set(fpr, value)

  /** @group setParam */
  @Since("3.1.0")
  def setFdr(value: Double): this.type = set(fdr, value)

  /** @group setParam */
  @Since("3.1.0")
  def setFwe(value: Double): this.type = set(fwe, value)

  /** @group setParam */
  @Since("3.1.0")
  def setSelectorType(value: String): this.type = set(selectorType, value)

  /** @group setParam */
  @Since("3.1.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /**
   * get the SelectionTestResult for every feature against the label
   */
  protected[this] def getSelectionTestResult(df: DataFrame): DataFrame

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
    val spark = dataset.sparkSession
    import spark.implicits._

    val numFeatures = DatasetUtils.getNumFeatures(dataset, $(featuresCol))
    val resultDF = getSelectionTestResult(dataset.toDF)

    def getTopIndices(k: Int): Array[Int] = {
      resultDF.sort("pValue", "featureIndex")
        .select("featureIndex")
        .limit(k)
        .as[Int]
        .collect()
    }

    val indices = $(selectorType) match {
      case "numTopFeatures" =>
        getTopIndices($(numTopFeatures))
      case "percentile" =>
        getTopIndices((numFeatures * getPercentile).toInt)
      case "fpr" =>
        resultDF.select("featureIndex")
          .where(col("pValue") < $(fpr))
          .as[Int].collect()
      case "fdr" =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val f = $(fdr) / numFeatures
        val maxIndex = resultDF.sort("pValue", "featureIndex")
          .select("pValue")
          .as[Double].rdd
          .zipWithIndex
          .flatMap { case (pValue, index) =>
            if (pValue <= f * (index + 1)) {
              Iterator.single(index.toInt)
            } else Iterator.empty
          }.fold(-1)(math.max)
        if (maxIndex >= 0) {
          getTopIndices(maxIndex + 1)
        } else Array.emptyIntArray
      case "fwe" =>
        resultDF.select("featureIndex")
          .where(col("pValue") < $(fwe) / numFeatures)
          .as[Int].collect()
      case errorType =>
        throw new IllegalArgumentException(s"Unknown Selector Type: $errorType")
    }

    copyValues(createSelectorModel(uid, indices.sorted)
      .setParent(this))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): Selector[T] = defaultCopy(extra)
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

  /** @group setParam */
  @Since("3.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  protected def isNumericAttribute = true

  @Since("3.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    SelectorModel.transform(dataset, selectedFeatures.sorted, outputSchema,
      $(outputCol), $(featuresCol))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField =
      SelectorModel.prepOutputField(schema, selectedFeatures, $(outputCol), $(featuresCol),
        isNumericAttribute)
    SchemaUtils.appendColumn(schema, newField)
  }
}

private[feature] object SelectorModel {

  def transform(
      dataset: Dataset[_],
      selectedFeatures: Array[Int],
      outputSchema: StructType,
      outputCol: String,
      featuresCol: String): DataFrame = {
    val newSize = selectedFeatures.length
    val func = { vector: Vector =>
      vector match {
        case SparseVector(_, indices, values) =>
          val (newIndices, newValues) =
            compressSparse(indices, values, selectedFeatures)
          Vectors.sparse(newSize, newIndices, newValues)
        case DenseVector(values) =>
          Vectors.dense(selectedFeatures.map(values))
        case other =>
          throw new UnsupportedOperationException(
            s"Only sparse and dense vectors are supported but got ${other.getClass}.")
      }
    }

    val transformer = udf(func)
    dataset.withColumn(outputCol, transformer(col(featuresCol)),
      outputSchema(outputCol).metadata)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   */
  def prepOutputField(
      schema: StructType,
      selectedFeatures: Array[Int],
      outputCol: String,
      featuresCol: String,
      isNumericAttribute: Boolean): StructField = {
    val selector = selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema(featuresCol))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      if (isNumericAttribute) {
        Array.fill[Attribute](selector.size)(NumericAttribute.defaultAttr)
      } else {
        Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
      }
    }
    val newAttributeGroup = new AttributeGroup(outputCol, featureAttributes)
    newAttributeGroup.toStructField()
  }

  def compressSparse(
      indices: Array[Int],
      values: Array[Double],
      selectedFeatures: Array[Int]): (Array[Int], Array[Double]) = {
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

