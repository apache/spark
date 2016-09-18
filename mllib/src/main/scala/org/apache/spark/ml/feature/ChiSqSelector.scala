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
import org.apache.spark.mllib.feature.ChiSqSelectorType
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
   * Number of features that selector will select (ordered by statistic value descending). If the
   * number of features is less than numTopFeatures, then this will select all features.
   * The default value of numTopFeatures is 50.
   * @group param
   */
  final val numTopFeatures = new IntParam(this, "numTopFeatures",
    "Number of features that selector will select, ordered by statistics value descending. If the" +
      " number of features is < numTopFeatures, then this will select all features.",
    ParamValidators.gtEq(1))
  setDefault(numTopFeatures -> 50)

  /** @group getParam */
  def getNumTopFeatures: Int = $(numTopFeatures)

  final val percentile = new DoubleParam(this, "percentile",
    "Percentile of features that selector will select, ordered by statistics value descending.",
    ParamValidators.inRange(0, 1))
  setDefault(percentile -> 0.1)

  /** @group getParam */
  def getPercentile: Double = $(percentile)

  final val alpha = new DoubleParam(this, "alpha",
    "The highest p-value for features to be kept.",
    ParamValidators.inRange(0, 1))
  setDefault(alpha -> 0.05)

  /** @group getParam */
  def getAlpha: Double = $(alpha)

  /**
   * The ChiSqSelector supports KBest, Percentile, FPR selection,
   * which is the same as ChiSqSelectorType defined in MLLIB.
   * when call setNumTopFeatures, the selectorType is set to KBest
   * when call setPercentile, the selectorType is set to Percentile
   * when call setAlpha, the selectorType is set to FPR
   */
  final val selectorType = new Param[String](this, "selectorType",
    "ChiSqSelector Type: KBest, Percentile, FPR")
  setDefault(selectorType -> ChiSqSelectorType.KBest.toString)

  /** @group getParam */
  def getChiSqSelectorType: String = $(selectorType)
}

/**
 * Chi-Squared feature selection, which selects categorical features to use for predicting a
 * categorical label.
 * The selector supports three selection methods: KBest, Percentile and FPR.
 * By default, the selection method is KBest, the default number of top features is 50.
 * User can use setNumTopFeatures, setPercentile and setAlpha to set different selection methods.
 */
@Since("1.6.0")
final class ChiSqSelector @Since("1.6.0") (@Since("1.6.0") override val uid: String)
  extends Estimator[ChiSqSelectorModel] with ChiSqSelectorParams with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("chiSqSelector"))

  /** @group setParam */
  @Since("1.6.0")
  def setNumTopFeatures(value: Int): this.type = {
    set(selectorType, ChiSqSelectorType.KBest.toString)
    set(numTopFeatures, value)
  }

  @Since("2.1.0")
  def setPercentile(value: Double): this.type = {
    set(selectorType, ChiSqSelectorType.Percentile.toString)
    set(percentile, value)
  }

  @Since("2.1.0")
  def setAlpha(value: Double): this.type = {
    set(selectorType, ChiSqSelectorType.FPR.toString)
    set(alpha, value)
  }

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
    var selector = new feature.ChiSqSelector()
    ChiSqSelectorType.withName($(selectorType)) match {
      case ChiSqSelectorType.KBest =>
        selector.setNumTopFeatures($(numTopFeatures))
      case ChiSqSelectorType.Percentile =>
        selector.setPercentile($(percentile))
      case ChiSqSelectorType.FPR =>
        selector.setAlpha($(alpha))
      case errorType =>
        throw new IllegalStateException(s"Unknown ChiSqSelector Type: $errorType")
    }
    val model = selector.fit(input)
    copyValues(new ChiSqSelectorModel(uid, model).setParent(this))
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
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

  /** list of indices to select (filter). Must be ordered asc */
  @Since("1.6.0")
  val selectedFeatures: Array[Int] = chiSqSelector.selectedFeatures

  /** @group setParam */
  @Since("1.6.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("1.6.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * @group setParam
   */
  @Since("1.6.0")
  @deprecated("labelCol is not used by ChiSqSelectorModel.", "2.0.0")
  def setLabelCol(value: String): this.type = set(labelCol, value)

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
