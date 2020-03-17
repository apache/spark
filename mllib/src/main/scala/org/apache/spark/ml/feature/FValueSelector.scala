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
import org.apache.spark.ml._
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.{FValueTest, SelectionTestResult}
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * Params for [[FValueSelector]] and [[FValueSelectorModel]].
 */
private[feature] trait FValueSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {

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
  setDefault(numTopFeatures -> 50)

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
  setDefault(percentile -> 0.1)

  /** @group getParam */
  @Since("3.1.0")
  def getPercentile: Double = $(percentile)

  /**
   * The lowest p-value for features to be kept.
   * Only applicable when selectorType = "fpr".
   * Default value is 0.05.
   * @group param
   */
  @Since("3.1.0")
  final val fpr = new DoubleParam(this, "fpr", "The lowest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))
  setDefault(fpr -> 0.05)

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
  setDefault(fdr -> 0.05)

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
  setDefault(fwe -> 0.05)

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
  setDefault(selectorType -> "numTopFeatures")

  /** @group getParam */
  @Since("3.1.0")
  def getSelectorType: String = $(selectorType)
}

/**
 * F Value Regression feature selector, which selects continuous features to use for predicting a
 * continuous label.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`,
 * `fdr`, `fwe`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a F value regression
 *  test.
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
final class FValueSelector @Since("3.1.0") (override val uid: String)
  extends Estimator[FValueSelectorModel] with FValueSelectorParams
    with DefaultParamsWritable {

  @Since("3.1.0")
  def this() = this(Identifiable.randomUID("FValueSelector"))

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

  @Since("3.1.0")
  override def fit(dataset: Dataset[_]): FValueSelectorModel = {
    transformSchema(dataset.schema, logging = true)

    val testResult = FValueTest.testRegression(dataset, getFeaturesCol, getLabelCol)
      .zipWithIndex
    val features = $(selectorType) match {
      case "numTopFeatures" =>
        testResult
          .sortBy { case (res, _) => res.pValue }
          .take(getNumTopFeatures)
      case "percentile" =>
        testResult
          .sortBy { case (res, _) => res.pValue }
          .take((testResult.length * getPercentile).toInt)
      case "fpr" =>
        testResult
          .filter { case (res, _) => res.pValue < getFpr }
      case "fdr" =>
        // This uses the Benjamini-Hochberg procedure.
        // https://en.wikipedia.org/wiki/False_discovery_rate#Benjamini.E2.80.93Hochberg_procedure
        val tempRes = testResult
          .sortBy { case (res, _) => res.pValue }
        val selected = tempRes
          .zipWithIndex
          .filter { case ((res, _), index) =>
            res.pValue <= getFdr * (index + 1) / testResult.length
          }
        if (selected.isEmpty) {
          Array.empty[(SelectionTestResult, Int)]
        } else {
          val maxIndex = selected.map(_._2).max
          tempRes.take(maxIndex + 1)
        }
      case "fwe" =>
        testResult
          .filter { case (res, _) => res.pValue < getFwe / testResult.length }
      case errorType =>
        throw new IllegalStateException(s"Unknown Selector Type: $errorType")
    }
    val indices = features.map { case (_, index) => index }
    copyValues(new FValueSelectorModel(uid, indices.sorted)
      .setParent(this))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): FValueSelector = defaultCopy(extra)
}

@Since("3.1.0")
object FValueSelector extends DefaultParamsReadable[FValueSelector] {

  @Since("3.1.0")
  override def load(path: String): FValueSelector = super.load(path)
}

/**
 * Model fitted by [[FValueSelector]].
 */
@Since("3.1.0")
class FValueSelectorModel private[ml](
    override val uid: String,
    val selectedFeatures: Array[Int])
  extends Model[FValueSelectorModel] with FValueSelectorParams with MLWritable {

  var prev = -1
  selectedFeatures.foreach { i =>
    require(prev < i, s"Index $i follows $prev and is not strictly increasing")
    prev = i
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
          Vectors.dense(selectedFeatures.map(i => values(i)))
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

  @Since("3.1.0")
  override def copy(extra: ParamMap): FValueSelectorModel = {
    val copied = new FValueSelectorModel(uid, selectedFeatures)
      .setParent(parent)
    copyValues(copied, extra)
  }

  @Since("3.1.0")
  override def write: MLWriter = new FValueSelectorModel.
  FValueSelectorModelWriter(this)

  @Since("3.1.0")
  override def toString: String = {
    s"FValueSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }

  private[spark] def compressSparse(
      indices: Array[Int],
      values: Array[Double]): (Array[Int], Array[Double]) = {
    val newValues = new ArrayBuilder.ofDouble
    val newIndices = new ArrayBuilder.ofInt
    var i = 0
    var j = 0
    var indicesIdx = 0
    var filterIndicesIdx = 0
    while (i < indices.length && j < selectedFeatures.length) {
      indicesIdx = indices(i)
      filterIndicesIdx = selectedFeatures(j)
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

@Since("3.1.0")
object FValueSelectorModel extends MLReadable[FValueSelectorModel] {

  @Since("3.1.0")
  override def read: MLReader[FValueSelectorModel] =
    new FValueSelectorModelReader

  @Since("3.1.0")
  override def load(path: String): FValueSelectorModel = super.load(path)

  private[FValueSelectorModel] class FValueSelectorModelWriter(
      instance: FValueSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FValueSelectorModelReader extends
    MLReader[FValueSelectorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[FValueSelectorModel].getName

    override def load(path: String): FValueSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new FValueSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }
}
