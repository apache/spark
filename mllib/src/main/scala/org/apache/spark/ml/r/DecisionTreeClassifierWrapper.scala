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
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class DecisionTreeClassifierWrapper private (
  val pipeline: PipelineModel,
  val features: Array[String],
  val maxDepth: Int,
  val maxBins: Int) extends MLWritable {

  private val DTModel: DecisionTreeClassificationModel =
    pipeline.stages(1).asInstanceOf[DecisionTreeClassificationModel]

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(DTModel.getFeaturesCol)
  }

  override def write: MLWriter = new
      DecisionTreeClassifierWrapper.DecisionTreeClassifierWrapperWriter(this)
}

private[r] object DecisionTreeClassifierWrapper extends MLReadable[DecisionTreeClassifierWrapper] {
  def fit(data: DataFrame,
          formula: String,
          maxDepth: Int,
          maxBins: Int): DecisionTreeClassifierWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setFeaturesCol("features")
      .setLabelCol("label")

    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    // assemble and fit the pipeline
    val decisionTreeClassification = new DecisionTreeClassifier()
      .setMaxDepth(maxDepth)
      .setMaxBins(maxBins)
      .setFeaturesCol(rFormula.getFeaturesCol)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, decisionTreeClassification))
      .fit(data)

    new DecisionTreeClassifierWrapper(pipeline, features, maxDepth, maxBins)
  }

  override def read: MLReader[DecisionTreeClassifierWrapper] =
    new DecisionTreeClassifierWrapperReader

  override def load(path: String): DecisionTreeClassifierWrapper = super.load(path)

  class DecisionTreeClassifierWrapperWriter(instance: DecisionTreeClassifierWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toSeq) ~
        ("maxDepth" -> instance.maxDepth) ~
        ("maxBins" -> instance.maxBins)
      val rMetadataJson: String = compact(render(rMetadata))

      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)
      instance.pipeline.save(pipelinePath)
    }
  }

  class DecisionTreeClassifierWrapperReader extends MLReader[DecisionTreeClassifierWrapper] {

    override def load(path: String): DecisionTreeClassifierWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString
      val pipeline = PipelineModel.load(pipelinePath)

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val maxDepth = (rMetadata \ "maxDepth").extract[Int]
      val maxBins = (rMetadata \ "maxBins").extract[Int]

      new DecisionTreeClassifierWrapper(pipeline, features, maxDepth, maxBins)
    }
  }
}