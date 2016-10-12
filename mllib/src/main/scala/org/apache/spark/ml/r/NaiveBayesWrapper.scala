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
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class NaiveBayesWrapper private (
    val pipeline: PipelineModel,
    val labels: Array[String],
    val features: Array[String]) extends MLWritable {

  import NaiveBayesWrapper._

  private val naiveBayesModel: NaiveBayesModel = pipeline.stages(1).asInstanceOf[NaiveBayesModel]

  lazy val apriori: Array[Double] = naiveBayesModel.pi.toArray.map(math.exp)

  lazy val tables: Array[Double] = naiveBayesModel.theta.toArray.map(math.exp)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(naiveBayesModel.getFeaturesCol)
  }

  override def write: MLWriter = new NaiveBayesWrapper.NaiveBayesWrapperWriter(this)
}

private[r] object NaiveBayesWrapper extends MLReadable[NaiveBayesWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit(formula: String, data: DataFrame, smoothing: Double): NaiveBayesWrapper = {
    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)
    // get labels and feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val labelAttr = Attribute.fromStructField(schema(rFormulaModel.getLabelCol))
      .asInstanceOf[NominalAttribute]
    val labels = labelAttr.values.get
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    // assemble and fit the pipeline
    val naiveBayes = new NaiveBayes()
      .setSmoothing(smoothing)
      .setModelType("bernoulli")
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)
    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, naiveBayes, idxToStr))
      .fit(data)
    new NaiveBayesWrapper(pipeline, labels, features)
  }

  override def read: MLReader[NaiveBayesWrapper] = new NaiveBayesWrapperReader

  override def load(path: String): NaiveBayesWrapper = super.load(path)

  class NaiveBayesWrapperWriter(instance: NaiveBayesWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("labels" -> instance.labels.toSeq) ~
        ("features" -> instance.features.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class NaiveBayesWrapperReader extends MLReader[NaiveBayesWrapper] {

    override def load(path: String): NaiveBayesWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val labels = (rMetadata \ "labels").extract[Array[String]]
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new NaiveBayesWrapper(pipeline, labels, features)
    }
  }
}
