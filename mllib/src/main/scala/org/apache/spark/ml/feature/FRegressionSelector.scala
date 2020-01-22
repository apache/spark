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
import org.apache.spark.ml.attribute.{AttributeGroup, _}
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.FRegressionTest
import org.apache.spark.ml.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}


/**
 * Params for [[FRegressionSelector]] and [[FRegressionSelectorModel]].
 * TODO: put all these params in shared.scala
 * TODO: Not include fdr and fwe for now. Need to check if these two are applicable!!!
 */
private[feature] trait FRegressionSelectorParams extends Params
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
   * Percentile of features that selector will select, ordered by statistics value descending.
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
   * The highest p-value for features to be kept.
   * Only applicable when selectorType = "fpr".
   * Default value is 0.05.
   * @group param
   */
  @Since("3.1.0")
  final val fpr = new DoubleParam(this, "fpr", "The highest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))
  setDefault(fpr -> 0.05)

  /** @group getParam */
  @Since("3.1.0")
  def getFpr: Double = $(fpr)

  /**
   * The selector type of the FRegressionSelector.
   * Supported options: "numTopFeatures" (default), "percentile", "fpr".
   * @group param
   */
  @Since("3.1.0")
  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the FRegressionSelector. " +
      "Supported options: numTopFeatures, percentile, fpr")

  /** @group getParam */
  @Since("3.1.0")
  def getSelectorType: String = $(selectorType)
}

/**
 * Regression F-value Selector
 * This feature selector is for regressions where features are continuous and labels are continuous.
 * ANOVA F-value Classification Selector is for when features are continuous and labels are
 * categorical.
 * Currently, Chi-Squared is for categorical features and categorical labels
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`
 *  - `numTopFeatures` chooses a fixed number of top features according to a fRegression test.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-value are below a threshold, thus controlling the false
 *    positive rate of selection.
 *
 * By default, the selection method is `numTopFeatures`, with the default number of top features
 * set to 50.
 */
@Since("3.1.0")
final class FRegressionSelector @Since("3.1.0") (@Since("3.1.0") override val uid: String)
  extends Estimator[FRegressionSelectorModel] with FRegressionSelectorParams
  with DefaultParamsWritable {

  @Since("3.1.0")
  def this() = this(Identifiable.randomUID("FRegressionSelector"))

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
  override def fit(dataset: Dataset[_]): FRegressionSelectorModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[LabeledPoint] =
      dataset.select(col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map {
        case Row(label: Double, features: Vector) =>
          LabeledPoint(label, features)
      }
    val FTestResult = FRegressionTest.test_regression(dataset, getFeaturesCol, getLabelCol)
      .zipWithIndex
    val features = $(selectorType) match {
      case "numTopFeatures" =>
        FTestResult
          .sortBy { case (res, _) => res.pValue }
          .take(getNumTopFeatures)
      case "percentile" =>
        FTestResult
          .sortBy { case (res, _) => res.pValue }
          .take((FTestResult.length * getPercentile).toInt)
      case "fpr" =>
        FTestResult
          .filter { case (res, _) => res.pValue < getFpr }
      case errorType =>
        throw new IllegalStateException(s"Unknown FRegressionSelector Type: $errorType")
    }
    val indices = features.map { case (_, index) => index }
    copyValues(new FRegressionSelectorModel(uid, indices).setParent(this))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): FRegressionSelector = defaultCopy(extra)
}

@Since("3.1.0")
object FRegressionSelector extends DefaultParamsReadable[FRegressionSelector] {

  @Since("3.1.0")
  override def load(path: String): FRegressionSelector = super.load(path)
}

/**
 * Model fitted by [[FRegressionSelector]].
 */
@Since("3.1.0")
final class FRegressionSelectorModel private[ml] (
    @Since("3.1.0") override val uid: String,
    val selectedFeatures: Array[Int])
  extends Model[FRegressionSelectorModel] with FRegressionSelectorParams with MLWritable {

  private val filterIndices = selectedFeatures.sorted
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
          Vectors.dense(compressDense(values))
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
  override def copy(extra: ParamMap): FRegressionSelectorModel = {
    val copied = new FRegressionSelectorModel(uid, selectedFeatures)
    copyValues(copied, extra).setParent(parent)
  }

   @Since("3.1.0")
   override def write: MLWriter = null // new FRegressionSelectorModelWriter(this)

  @Since("3.1.0")
  override def toString: String = {
    s"FRegressionSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }

  private[spark] def compressSparse(indices: Array[Int],
                                    values: Array[Double]): (Array[Int], Array[Double]) = {
    val newValues = new ArrayBuilder.ofDouble
    val newIndices = new ArrayBuilder.ofInt
    var i = 0
    var j = 0
    var indicesIdx = 0
    var filterIndicesIdx = 0
    while (i < indices.length && j < filterIndices.length) {
      indicesIdx = indices(i)
      filterIndicesIdx = filterIndices(j)
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

  private[spark] def compressDense(values: Array[Double]): Array[Double] = {
    filterIndices.map(i => values(i))
  }
}

@Since("3.1.0")
object FRegressionSelectorModel extends MLReadable[FRegressionSelectorModel] {

  private[FRegressionSelectorModel]
  class FRegressionSelectorModelWriter(instance: FRegressionSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class FRegressionSelectorModelReader extends
    MLReader[FRegressionSelectorModel] {

    private val className = classOf[FRegressionSelectorModel].getName

    override def load(path: String): FRegressionSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new FRegressionSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }

  @Since("3.1.0")
  override def read: MLReader[FRegressionSelectorModel] =
    new FRegressionSelectorModelReader

  @Since("3.1.0")
  override def load(path: String): FRegressionSelectorModel = super.load(path)
 }
