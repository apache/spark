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

package org.apache.spark.ml.r

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.{AttributeGroup}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class IsotonicRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String]) extends MLWritable {

  private val isotonicRegressionModel: IsotonicRegressionModel =
    pipeline.stages(1).asInstanceOf[IsotonicRegressionModel]

  lazy val boundaries: Array[Double] = isotonicRegressionModel.boundaries.toArray

  lazy val predictions: Array[Double] = isotonicRegressionModel.predictions.toArray

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(isotonicRegressionModel.getFeaturesCol)
  }

  override def write: MLWriter = new IsotonicRegressionWrapper.IsotonicRegressionWrapperWriter(this)
}

private[r] object IsotonicRegressionWrapper
    extends MLReadable[IsotonicRegressionWrapper] {

  def fit(
      data: DataFrame,
      formula: String,
      isotonic: Boolean,
      featureIndex: Int,
      weightCol: String): IsotonicRegressionWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("features")
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    require(features.size == 1)

    // assemble and fit the pipeline
    val isotonicRegression = new IsotonicRegression()
      .setIsotonic(isotonic)
      .setFeatureIndex(featureIndex)
      .setWeightCol(weightCol)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, isotonicRegression))
      .fit(data)

    new IsotonicRegressionWrapper(pipeline, features)
  }

  override def read: MLReader[IsotonicRegressionWrapper] = new IsotonicRegressionWrapperReader

  override def load(path: String): IsotonicRegressionWrapper = super.load(path)

  class IsotonicRegressionWrapperWriter(instance: IsotonicRegressionWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class IsotonicRegressionWrapperReader extends MLReader[IsotonicRegressionWrapper] {

    override def load(path: String): IsotonicRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new IsotonicRegressionWrapper(pipeline, features)
    }
  }
}
