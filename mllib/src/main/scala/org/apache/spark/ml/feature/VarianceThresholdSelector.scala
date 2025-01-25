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
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.stat.Summarizer
import org.apache.spark.ml.util._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ArrayImplicits._

/**
 * Params for [[VarianceThresholdSelector]] and [[VarianceThresholdSelectorModel]].
 */
private[feature] trait VarianceThresholdSelectorParams extends Params
  with HasFeaturesCol with HasOutputCol {

  /**
   * Param for variance threshold. Features with a variance not greater than this threshold
   * will be removed. The default value is 0.0.
   *
   * @group param
   */
  @Since("3.1.0")
  final val varianceThreshold = new DoubleParam(this, "varianceThreshold",
    "Param for variance threshold. Features with a variance not greater than this threshold" +
      " will be removed. The default value is 0.0.", ParamValidators.gtEq(0))
  setDefault(varianceThreshold -> 0.0)

  /** @group getParam */
  @Since("3.1.0")
  def getVarianceThreshold: Double = $(varianceThreshold)
}

/**
 * Feature selector that removes all low-variance features. Features with a
 * (sample) variance not greater than the threshold will be removed. The default is to keep
 * all features with non-zero variance, i.e. remove the features that have the
 * same value in all samples.
 */
@Since("3.1.0")
final class VarianceThresholdSelector @Since("3.1.0")(@Since("3.1.0") override val uid: String)
  extends Estimator[VarianceThresholdSelectorModel] with VarianceThresholdSelectorParams
with DefaultParamsWritable {

  @Since("3.1.0")
  def this() = this(Identifiable.randomUID("VarianceThresholdSelector"))

  /** @group setParam */
  @Since("3.1.0")
  def setVarianceThreshold(value: Double): this.type = set(varianceThreshold, value)

  /** @group setParam */
  @Since("3.1.0")
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)

  /** @group setParam */
  @Since("3.1.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  @Since("3.1.0")
  override def fit(dataset: Dataset[_]): VarianceThresholdSelectorModel = {
    transformSchema(dataset.schema, logging = true)

    val Row(maxs: Vector, mins: Vector, variances: Vector) = dataset
      .select(Summarizer.metrics("max", "min", "variance").summary(col($(featuresCol)))
        .as("summary"))
      .select("summary.max", "summary.min", "summary.variance")
      .first()

    val numFeatures = maxs.size
    val indices = Array.tabulate(numFeatures) { i =>
      // Use peak-to-peak to avoid numeric precision issues for constant features
      (i, if (maxs(i) == mins(i)) 0.0 else variances(i))
    }.filter(_._2 > getVarianceThreshold).map(_._1)
    copyValues(new VarianceThresholdSelectorModel(uid, indices.sorted)
      .setParent(this))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    SchemaUtils.appendColumn(schema, $(outputCol), new VectorUDT)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): VarianceThresholdSelector = defaultCopy(extra)
}

@Since("3.1.0")
object VarianceThresholdSelector extends DefaultParamsReadable[VarianceThresholdSelector] {

  @Since("3.1.0")
  override def load(path: String): VarianceThresholdSelector = super.load(path)
}

/**
 * Model fitted by [[VarianceThresholdSelector]].
 */
@Since("3.1.0")
class VarianceThresholdSelectorModel private[ml](
    @Since("3.1.0") override val uid: String,
    @Since("3.1.0") val selectedFeatures: Array[Int])
  extends Model[VarianceThresholdSelectorModel] with VarianceThresholdSelectorParams
    with MLWritable {

  private[ml] def this() = this(
    Identifiable.randomUID("VarianceThresholdSelector"), Array.emptyIntArray)

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

  @Since("3.1.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputSchema = transformSchema(dataset.schema, logging = true)

    SelectorModel.transform(dataset, selectedFeatures, outputSchema, $(outputCol), $(featuresCol))
  }

  @Since("3.1.0")
  override def transformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(featuresCol), new VectorUDT)
    val newField =
      SelectorModel.prepOutputField(schema, selectedFeatures, $(outputCol), $(featuresCol), true)
    SchemaUtils.appendColumn(schema, newField)
  }

  @Since("3.1.0")
  override def copy(extra: ParamMap): VarianceThresholdSelectorModel = {
    val copied = new VarianceThresholdSelectorModel(uid, selectedFeatures)
      .setParent(parent)
    copyValues(copied, extra)
  }

  @Since("3.1.0")
  override def write: MLWriter =
    new VarianceThresholdSelectorModel.VarianceThresholdSelectorWriter(this)

  @Since("3.1.0")
  override def toString: String = {
    s"VarianceThresholdSelectorModel: uid=$uid, numSelectedFeatures=${selectedFeatures.length}"
  }
}

@Since("3.1.0")
object VarianceThresholdSelectorModel extends MLReadable[VarianceThresholdSelectorModel] {

  @Since("3.1.0")
  override def read: MLReader[VarianceThresholdSelectorModel] =
    new VarianceThresholdSelectorModelReader

  @Since("3.1.0")
  override def load(path: String): VarianceThresholdSelectorModel = super.load(path)

  private[VarianceThresholdSelectorModel] class VarianceThresholdSelectorWriter(
      instance: VarianceThresholdSelectorModel) extends MLWriter {

    private case class Data(selectedFeatures: Seq[Int])

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sparkSession)
      val data = Data(instance.selectedFeatures.toImmutableArraySeq)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).write.parquet(dataPath)
    }
  }

  private class VarianceThresholdSelectorModelReader extends
    MLReader[VarianceThresholdSelectorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[VarianceThresholdSelectorModel].getName

    override def load(path: String): VarianceThresholdSelectorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sparkSession, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("selectedFeatures").head()
      val selectedFeatures = data.getAs[Seq[Int]](0).toArray
      val model = new VarianceThresholdSelectorModel(metadata.uid, selectedFeatures)
      metadata.getAndSetParams(model)
      model
    }
  }
}
