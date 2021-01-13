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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute, NumericAttribute}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared.{HasFeaturesCol, HasLabelCol, HasOutputCol}
import org.apache.spark.ml.stat.{ANOVATest, ChiSquareTest, FValueTest}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * Params for [[UnivariateFeatureSelector]] and [[UnivariateFeatureSelectorModel]].
 */
private[feature] trait UnivariateFeatureSelectorParams extends Params
  with HasFeaturesCol with HasLabelCol with HasOutputCol {

  /**
   * The feature type.
   * Supported options: "categorical", "continuous"
   * @group param
   */
  @Since("3.1.0")
  final val featureType = new Param[String](this, "featureType",
    "Feature type. Supported options: categorical, continuous.",
    ParamValidators.inArray(Array("categorical", "continuous")))

  /** @group getParam */
  @Since("3.1.0")
  def getFeatureType: String = $(featureType)

  /**
   * The label type.
   * Supported options: "categorical", "continuous"
   * @group param
   */
  @Since("3.1.0")
  final val labelType = new Param[String](this, "labelType",
    "Label type. Supported options: categorical, continuous.",
    ParamValidators.inArray(Array("categorical", "continuous")))

  /** @group getParam */
  @Since("3.1.0")
  def getLabelType: String = $(labelType)

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

  setDefault(numTopFeatures -> 50, percentile -> 0.1, fpr -> 0.05, fdr -> 0.05, fwe -> 0.05,
    selectorType -> "numTopFeatures")
}

/**
 * UnivariateFeatureSelector
 * User can set featureType and labelType, and Spark will pick the score function based on
 * the specified featureType and labelType.
 * The following combination of featureType and labelType are supported"
 * 1. featureType categorical and labelType categorical, Spark uses chi2
 * 2. featureType continuous and labelType categorical, Spark uses f_classif
 * 3. featureType continuous and labelType continuous, Spark uses f_regression
 *
 * The UnivariateFeatureSelector supports different selection methods: `numTopFeatures`,
 * `percentile`, `fpr`, `fdr`, `fwe`.
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
@Since("3.1.0")
final class UnivariateFeatureSelector @Since("3.1.0")(@Since("3.1.0") override val uid: String)
  extends Estimator[UnivariateFeatureSelectorModel] with UnivariateFeatureSelectorParams
    with DefaultParamsWritable {

  @Since("3.1.0")
  def this() = this(Identifiable.randomUID("UnivariateFeatureSelector"))

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
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setFeatureType(value: String): this.type = set(featureType, value)

  /** @group setParam */
  @Since("3.1.0")
  def setLabelType(value: String): this.type = set(labelType, value)

  @Since("3.1.0")
  override def fit(dataset: Dataset[_]): UnivariateFeatureSelectorModel = {
    transformSchema(dataset.schema, logging = true)
    val spark = dataset.sparkSession
    import spark.implicits._

    val numFeatures = MetadataUtils.getNumFeatures(dataset, $(featuresCol))

    val resultDF = if (isSet(featureType) && isSet(labelType)) {
      $(featureType) match {
        case "categorical" =>
          $(labelType) match {
            case "categorical" =>
              ChiSquareTest.test(dataset.toDF, getFeaturesCol, getLabelCol, true)
            case errorType =>
              throw new IllegalStateException(s"Unknown Label Type: $errorType")
          }
        case "continuous" =>
          $(labelType) match {
            case "categorical" =>
              ANOVATest.test(dataset.toDF, getFeaturesCol, getLabelCol, true)
            case "continuous" =>
              FValueTest.test(dataset.toDF, getFeaturesCol, getLabelCol, true)
            case errorType =>
              throw new IllegalStateException(s"Unknown Label Type: $errorType")
          }
        case errorType =>
          throw new IllegalStateException(s"Unknown Feature Type: $errorType")
      }
    } else {
      throw new IllegalStateException("featureType and labelType need to be set")
    }

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
        throw new IllegalStateException(s"Unknown Selector Type: $errorType")
    }

    copyValues(new UnivariateFeatureSelectorModel(uid, indices)
      .setParent(this))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): UnivariateFeatureSelector = defaultCopy(extra)
}

@Since("3.1.0")
object UnivariateFeatureSelector extends DefaultParamsReadable[UnivariateFeatureSelector] {

  @Since("3.1.0")
  override def load(path: String): UnivariateFeatureSelector = super.load(path)
}

/**
 * Model fitted by [[UnivariateFeatureSelectorModel]].
 */
@Since("3.1.0")
class UnivariateFeatureSelectorModel private[ml](
    @Since("3.1.0") override val uid: String,
    @Since("3.1.0") val selectedFeatures: Array[Int])
  extends Model[UnivariateFeatureSelectorModel] with UnivariateFeatureSelectorParams
    with MLWritable {

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

  protected def isNumericAttribute = true

  @Since("3.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    UnivariateFeatureSelectorModel
      .transform(dataset, selectedFeatures, outputSchema, $(outputCol), $(featuresCol))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField =
      UnivariateFeatureSelectorModel
        .prepOutputField(schema, selectedFeatures, $(outputCol), $(featuresCol), isNumericAttribute)
    SchemaUtils.appendColumn(schema, newField)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): UnivariateFeatureSelectorModel = {
    val copied = new UnivariateFeatureSelectorModel(uid, selectedFeatures)
      .setParent(parent)
    copyValues(copied, extra)
  }

  @Since("3.1.0")
  override def write: MLWriter =
    new UnivariateFeatureSelectorModel.UnivariateFeatureSelectorModelWriter(this)

  @Since("3.1.0")
  override def toString: String = {
    s"UnivariateFeatureSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }
}

@Since("3.1.0")
object UnivariateFeatureSelectorModel extends MLReadable[UnivariateFeatureSelectorModel] {

  @Since("3.1.0")
  override def read: MLReader[UnivariateFeatureSelectorModel] =
    new UnivariateFeatureSelectorModelReader

  @Since("3.1.0")
  override def load(path: String): UnivariateFeatureSelectorModel = super.load(path)

  private[UnivariateFeatureSelectorModel] class UnivariateFeatureSelectorModelWriter(
      instance: UnivariateFeatureSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class UnivariateFeatureSelectorModelReader
    extends MLReader[UnivariateFeatureSelectorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[UnivariateFeatureSelectorModel].getName

    override def load(path: String): UnivariateFeatureSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new UnivariateFeatureSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }

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
