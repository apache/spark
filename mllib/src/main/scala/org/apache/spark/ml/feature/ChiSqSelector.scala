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

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.Since
import org.apache.spark.ml._
import org.apache.spark.ml.attribute.{AttributeGroup, _}
import org.apache.spark.ml.linalg.{Vector, VectorUDT}
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.feature.{ChiSqSelector => OldChiSqSelector}
import org.apache.spark.mllib.linalg.{Vectors => OldVectors}
import org.apache.spark.mllib.regression.{LabeledPoint => OldLabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

/**
 * Params for [[ChiSqSelector]] and [[ChiSqSelectorModel]].
 */
private[feature] trait ChiSqSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol with HasLabelCol {

  /**
   * Number of features that selector will select, ordered by ascending p-value. If the
   * number of features is less than numTopFeatures, then this will select all features.
   * Only applicable when selectorType = "numTopFeatures".
   * The default value of numTopFeatures is 50.
   *
   * @group param
   */
  @Since("1.6.0")
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by ascending p-value. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))
  setDefault(numTopFeatures -> 50)

  /** @group getParam */
  @Since("1.6.0")
  def getNumTopFeatures: Int = $(numTopFeatures)

  /**
   * Percentile of features that selector will select, ordered by statistics value descending.
   * Only applicable when selectorType = "percentile".
   * Default value is 0.1.
   * @group param
   */
  @Since("2.1.0")
  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by ascending p-value.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  /** @group getParam */
  @Since("2.1.0")
  def getPercentile: Double = $(percentile)

  /**
   * The highest p-value for features to be kept.
   * Only applicable when selectorType = "fpr".
   * Default value is 0.05.
   * @group param
   */
  @Since("2.1.0")
  final val fpr = new DoubleParam(this, "fpr", "The highest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))
  setDefault(fpr -> 0.05)

  /** @group getParam */
  @Since("2.1.0")
  def getFpr: Double = $(fpr)

  /**
   * The selector type of the ChisqSelector.
   * Supported options: "numTopFeatures" (default), "percentile", "fpr".
   * @group param
   */
  @Since("2.1.0")
  final val selectorType = new Param[String](this, "selectorType",
    "The selector type of the ChisqSelector. " +
      "Supported options: " + OldChiSqSelector.supportedSelectorTypes.mkString(", "),
    ParamValidators.inArray[String](OldChiSqSelector.supportedSelectorTypes))
  setDefault(selectorType -> OldChiSqSelector.NumTopFeatures)

  /** @group getParam */
  @Since("2.1.0")
  def getSelectorType: String = $(selectorType)
}

/**
 * Chi-Squared feature selection, which selects categorical features to use for predicting a
 * categorical label.
 * The selector supports different selection methods: `numTopFeatures`, `percentile`, `fpr`.
 *  - `numTopFeatures` chooses a fixed number of top features according to a chi-squared test.
 *  - `percentile` is similar but chooses a fraction of all features instead of a fixed number.
 *  - `fpr` chooses all features whose p-value is below a threshold, thus controlling the false
 *    positive rate of selection.
 * By default, the selection method is `numTopFeatures`, with the default number of top features
 * set to 50.
 */
@Since("1.6.0")
final class ChiSqSelector @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Estimator[ChiSqSelectorModel] with ChiSqSelectorParams with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("chiSqSelector"))

  /** @group setParam */
  @Since("1.6.0")
  def setNumTopFeatures(value: Int): this.type = set(numTopFeatures, value)

  /** @group setParam */
  @Since("2.1.0")
  def setPercentile(value: Double): this.type = set(percentile, value)

  /** @group setParam */
  @Since("2.1.0")
  def setFpr(value: Double): this.type = set(fpr, value)

  /** @group setParam */
  @Since("2.1.0")
  def setSelectorType(value: String): this.type = set(selectorType, value)

  /** @group setParam */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): ChiSqSelectorModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[OldLabeledPoint] =
      dataset.select(col($(labelCol)).cast(DoubleType), col($(featuresCol))).rdd.map {
        case Row(label: Double, features: Vector) =>
          OldLabeledPoint(label, OldVectors.fromML(features))
      }
    val selector = new feature.ChiSqSelector()
      .setSelectorType($(selectorType))
      .setNumTopFeatures($(numTopFeatures))
      .setPercentile($(percentile))
      .setFpr($(fpr))
    val model = selector.fit(input)
    copyValues(new ChiSqSelectorModel(uid, model).setParent(this))
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    val otherPairs = OldChiSqSelector.supportedSelectorTypes.filter(_ != $(selectorType))
    otherPairs.foreach { paramName: String =>
      if (isSet(getParam(paramName))) {
        logWarning(s"Param $paramName will take no effect when selector type = ${$(selectorType)}.")
      }
    }
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.checkNumericType(schema, $(labelCol))
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): ChiSqSelector = defaultCopy(extra)
}

@Since("1.6.0")
object ChiSqSelector extends DefaultParamsReadable[ChiSqSelector] {

  @Since("1.6.0")
  override def load(path: String): ChiSqSelector = super.load(path)
}

/**
 * Model fitted by [[ChiSqSelector]].
 */
@Since("1.6.0")
final class ChiSqSelectorModel private[ml] (
    @Since("1.6.0") override val uid: String,
    private val chiSqSelector: feature.ChiSqSelectorModel)
  extends Model[ChiSqSelectorModel] with ChiSqSelectorParams with MLWritable {

  import ChiSqSelectorModel._

  /** list of indices to select (filter). */
  @Since("1.6.0")
  val selectedFeatures: Array[Int] = chiSqSelector.selectedFeatures

  /** @group setParam */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformedSchema = transformSchema(dataset.schema, logging = true)
    val newField = transformedSchema.last

    // TODO: Make the transformer natively in ml framework to avoid extra conversion.
    val transformer: Vector => Vector = v => chiSqSelector.transform(OldVectors.fromML(v)).asML

    val selector = udf(transformer)
    dataset.withColumn($(outputCol), selector(col($(featuresCol))), newField.metadata)
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField = prepOutputField(schema)
    val outputFields = schema.fields :+ newField
    StructType(outputFields)
  }

  /**
   * Prepare the output column field, including per-feature metadata.
   */
  private def prepOutputField(schema: StructType): StructField = {
    val selector = chiSqSelector.selectedFeatures.toSet
    val origAttrGroup = AttributeGroup.fromStructField(schema($(featuresCol)))
    val featureAttributes: Array[Attribute] = if (origAttrGroup.attributes.nonEmpty) {
      origAttrGroup.attributes.get.zipWithIndex.filter(x => selector.contains(x._2)).map(_._1)
    } else {
      Array.fill[Attribute](selector.size)(NominalAttribute.defaultAttr)
    }
    val newAttributeGroup = new AttributeGroup($(outputCol), featureAttributes)
    newAttributeGroup.toStructField()
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): ChiSqSelectorModel = {
    val copied = new ChiSqSelectorModel(uid, chiSqSelector)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new ChiSqSelectorModelWriter(this)
}

@Since("1.6.0")
object ChiSqSelectorModel extends MLReadable[ChiSqSelectorModel] {

  private[ChiSqSelectorModel]
  class ChiSqSelectorModelWriter(instance: ChiSqSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.selectedFeatures.toSeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class ChiSqSelectorModelReader extends MLReader[ChiSqSelectorModel] {

    private val className = classOf[ChiSqSelectorModel].getName

    override def load(path: String): ChiSqSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath).select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val oldModel = new feature.ChiSqSelectorModel(selectedFeatures)
      val model = new ChiSqSelectorModel(metadata.uid, oldModel)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[ChiSqSelectorModel] = new ChiSqSelectorModelReader

  @Since("1.6.0")
  override def load(path: String): ChiSqSelectorModel = super.load(path)
}
