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
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.ArrayImplicits._


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
  @Since("3.1.1")
  final val featureType = new Param[String](this, "featureType",
    "Feature type. Supported options: categorical, continuous.",
    ParamValidators.inArray(Array("categorical", "continuous")))

  /** @group getParam */
  @Since("3.1.1")
  def getFeatureType: String = $(featureType)

  /**
   * The label type.
   * Supported options: "categorical", "continuous"
   * @group param
   */
  @Since("3.1.1")
  final val labelType = new Param[String](this, "labelType",
    "Label type. Supported options: categorical, continuous.",
    ParamValidators.inArray(Array("categorical", "continuous")))

  /** @group getParam */
  @Since("3.1.1")
  def getLabelType: String = $(labelType)

  /**
   * The selection mode.
   * Supported options: "numTopFeatures" (default), "percentile", "fpr", "fdr", "fwe"
   * @group param
   */
  @Since("3.1.1")
  final val selectionMode = new Param[String](this, "selectionMode",
    "The selection mode. Supported options: numTopFeatures, percentile, fpr, fdr, fwe",
    ParamValidators.inArray(Array("numTopFeatures", "percentile", "fpr", "fdr", "fwe")))

  /** @group getParam */
  @Since("3.1.1")
  def getSelectionMode: String = $(selectionMode)

  /**
   * The upper bound of the features that selector will select.
   * @group param
   */
  @Since("3.1.1")
  final val selectionThreshold = new DoubleParam(this, "selectionThreshold",
    "The upper bound of the features that selector will select.")

  /** @group getParam */
  def getSelectionThreshold: Double = $(selectionThreshold)

  setDefault(selectionMode -> "numTopFeatures")
}

/**
 * Feature selector based on univariate statistical tests against labels. Currently, Spark
 * supports three Univariate Feature Selectors: chi-squared, ANOVA F-test and F-value.
 * User can choose Univariate Feature Selector by setting `featureType` and `labelType`,
 * and Spark will pick the score function based on the specified `featureType` and `labelType`.
 *
 * The following combination of `featureType` and `labelType` are supported:
 *  - `featureType` `categorical` and `labelType` `categorical`: Spark uses chi-squared,
 *    i.e. chi2 in sklearn.
 *  - `featureType` `continuous` and `labelType` `categorical`: Spark uses ANOVA F-test,
 *    i.e. f_classif in sklearn.
 *  - `featureType` `continuous` and `labelType` `continuous`: Spark uses F-value,
 *    i.e. f_regression in sklearn.
 *
 * The `UnivariateFeatureSelector` supports different selection modes: `numTopFeatures`,
 * `percentile`, `fpr`, `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a hypothesis.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-value are below a threshold, thus controlling the false
 *    positive rate of selection.
 *  - `fdr` uses the <a href=
 *  "https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure">
 *  Benjamini-Hochberg procedure</a>
 *    to choose all features whose false discovery rate is below a threshold.
 *  - `fwe` chooses all features whose p-values are below a threshold. The threshold is scaled by
 *    1/numFeatures, thus controlling the family-wise error rate of selection.
 *
 * By default, the selection mode is `numTopFeatures`.
 */
@Since("3.1.1")
final class UnivariateFeatureSelector @Since("3.1.1")(@Since("3.1.1") override val uid: String)
  extends Estimator[UnivariateFeatureSelectorModel] with UnivariateFeatureSelectorParams
    with DefaultParamsWritable {

  @Since("3.1.1")
  def this() = this(Identifiable.randomUID("UnivariateFeatureSelector"))

  /** @group setParam */
  @Since("3.1.1")
  def setSelectionMode(value: String): this.type = set(selectionMode, value)

  /** @group setParam */
  @Since("3.1.1")
  def setSelectionThreshold(value: Double): this.type = set(selectionThreshold, value)

  /** @group setParam */
  @Since("3.1.1")
  def setFeatureType(value: String): this.type = set(featureType, value)

  /** @group setParam */
  @Since("3.1.1")
  def setLabelType(value: String): this.type = set(labelType, value)

  /** @group setParam */
  @Since("3.1.1")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.1")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("3.1.1")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  @Since("3.1.1")
  override def fit(dataset: Dataset[_]): UnivariateFeatureSelectorModel = {
    transformSchema(dataset.schema, logging = true)
    val numFeatures = DatasetUtils.getNumFeatures(dataset, $(featuresCol))

    var threshold = Double.NaN
    if (isSet(selectionThreshold)) {
      threshold = $(selectionThreshold)
    } else {
      $(selectionMode) match {
        case "numTopFeatures" => threshold = 50
        case "percentile" => threshold = 0.1
        case "fpr" | "fdr" | "fwe" => threshold = 0.05
      }
    }

    val resultDF = ($(featureType), $(labelType)) match {
      case ("categorical", "categorical") =>
        ChiSquareTest.test(dataset.toDF(), getFeaturesCol, getLabelCol, true)
      case ("continuous", "categorical") =>
        ANOVATest.test(dataset.toDF(), getFeaturesCol, getLabelCol, true)
      case ("continuous", "continuous") =>
        FValueTest.test(dataset.toDF(), getFeaturesCol, getLabelCol, true)
      case _ =>
        throw new IllegalArgumentException(s"Unsupported combination:" +
          s" featureType=${$(featureType)}, labelType=${$(labelType)}")
    }

    val indices = selectIndicesFromPValues(numFeatures, resultDF, $(selectionMode), threshold)
    copyValues(new UnivariateFeatureSelectorModel(uid, indices)
      .setParent(this))
  }

  private def getTopIndices(df: DataFrame, k: Int): Array[Int] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    df.sort("pValue", "featureIndex")
      .select("featureIndex")
      .limit(k)
      .as[Int]
      .collect()
  }

  private[feature] def selectIndicesFromPValues(
      numFeatures: Int,
      resultDF: DataFrame,
      selectionMode: String,
      selectionThreshold: Double): Array[Int] = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val indices = selectionMode match {
      case "numTopFeatures" =>
        getTopIndices(resultDF, selectionThreshold.toInt)
      case "percentile" =>
        getTopIndices(resultDF, (numFeatures * selectionThreshold).toInt)
      case "fpr" =>
        resultDF.select("featureIndex")
          .where(col("pValue") < selectionThreshold)
          .as[Int].collect()
      case "fdr" =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val f = selectionThreshold / numFeatures
        val maxIndex = resultDF.sort("pValue", "featureIndex")
          .select("pValue")
          .as[Double].rdd
          .zipWithIndex()
          .flatMap { case (pValue, index) =>
            if (pValue <= f * (index + 1)) {
              Iterator.single(index.toInt)
            } else Iterator.empty
          }.fold(-1)(math.max)
        if (maxIndex >= 0) {
          getTopIndices(resultDF, maxIndex + 1)
        } else Array.emptyIntArray
      case "fwe" =>
        resultDF.select("featureIndex")
          .where(col("pValue") < selectionThreshold / numFeatures)
          .as[Int].collect()
      case errorType =>
        throw new IllegalArgumentException(s"Unknown Selector Type: $errorType")
    }
    indices
  }

  @Since("3.1.1")
  override def transformSchema(schema: StructType): StructType = {
    if (isSet(selectionThreshold)) {
      val threshold = $(selectionThreshold)
      $(selectionMode) match {
        case "numTopFeatures" =>
          require(threshold >= 1 && threshold.toInt == threshold,
            s"selectionThreshold needs to be a positive Integer for selection mode " +
              s"numTopFeatures, but got $threshold")
        case "percentile" | "fpr" | "fdr" | "fwe" =>
          require(0 <= threshold && threshold <= 1,
            s"selectionThreshold needs to be in the range [0, 1] for selection mode " +
              s"${$(selectionMode)}, but got $threshold")
      }
    }
    require(isSet(featureType) && isSet(labelType), "featureType and labelType need to be set")
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.1")
  override def copy(extra: ParamMap): UnivariateFeatureSelector = defaultCopy(extra)
}

@Since("3.1.1")
object UnivariateFeatureSelector extends DefaultParamsReadable[UnivariateFeatureSelector] {

  @Since("3.1.1")
  override def load(path: String): UnivariateFeatureSelector = super.load(path)
}

/**
 * Model fitted by [[UnivariateFeatureSelectorModel]].
 */
@Since("3.1.1")
class UnivariateFeatureSelectorModel private[ml](
    @Since("3.1.1") override val uid: String,
    @Since("3.1.1") val selectedFeatures: Array[Int])
  extends Model[UnivariateFeatureSelectorModel] with UnivariateFeatureSelectorParams
    with MLWritable {

  /** @group setParam */
  @Since("3.1.1")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.1")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  protected def isNumericAttribute = true

  @Since("3.1.1")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    UnivariateFeatureSelectorModel
      .transform(dataset, selectedFeatures.sorted, outputSchema, $(outputCol), $(featuresCol))
  }

  @Since("3.1.1")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField =
      UnivariateFeatureSelectorModel
        .prepOutputField(schema, selectedFeatures, $(outputCol), $(featuresCol), isNumericAttribute)
    SchemaUtils.appendColumn(schema, newField)
  }

  @Since("3.1.1")
  override def copy(extra: ParamMap): UnivariateFeatureSelectorModel = {
    val copied = new UnivariateFeatureSelectorModel(uid, selectedFeatures)
      .setParent(parent)
    copyValues(copied, extra)
  }

  @Since("3.1.1")
  override def write: MLWriter =
    new UnivariateFeatureSelectorModel.UnivariateFeatureSelectorModelWriter(this)

  @Since("3.1.1")
  override def toString: String = {
    s"UnivariateFeatureSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }
}

@Since("3.1.1")
object UnivariateFeatureSelectorModel extends MLReadable[UnivariateFeatureSelectorModel] {

  @Since("3.1.1")
  override def read: MLReader[UnivariateFeatureSelectorModel] =
    new UnivariateFeatureSelectorModelReader

  @Since("3.1.1")
  override def load(path: String): UnivariateFeatureSelectorModel = super.load(path)

  private[UnivariateFeatureSelectorModel] class UnivariateFeatureSelectorModelWriter(
      instance: UnivariateFeatureSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.selectedFeatures.toImmutableArraySeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class UnivariateFeatureSelectorModelReader
    extends MLReader[UnivariateFeatureSelectorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[UnivariateFeatureSelectorModel].getName

    override def load(path: String): UnivariateFeatureSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new UnivariateFeatureSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }

  private def transform(
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
  private def prepOutputField(
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

  private def compressSparse(
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
