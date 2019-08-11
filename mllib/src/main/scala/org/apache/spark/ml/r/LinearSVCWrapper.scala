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
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LinearSVCWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val labels: Array[String]) extends MLWritable {
  import LinearSVCWrapper._

  private val svcModel: LinearSVCModel =
    pipeline.stages(1).asInstanceOf[LinearSVCModel]

  lazy val rFeatures: Array[String] = if (svcModel.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  lazy val rCoefficients: Array[Double] = if (svcModel.getFitIntercept) {
    Array(svcModel.intercept) ++ svcModel.coefficients.toArray
  } else {
    svcModel.coefficients.toArray
  }

  lazy val numClasses: Int = svcModel.numClasses

  lazy val numFeatures: Int = svcModel.numFeatures

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(svcModel.getFeaturesCol)
      .drop(svcModel.getLabelCol)
  }

  override def write: MLWriter = new LinearSVCWrapper.LinearSVCWrapperWriter(this)
}

private[r] object LinearSVCWrapper
  extends MLReadable[LinearSVCWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit(
      data: DataFrame,
      formula: String,
      regParam: Double,
      maxIter: Int,
      tol: Double,
      standardization: Boolean,
      threshold: Double,
      weightCol: String,
      aggregationDepth: Int,
      handleInvalid: String
      ): LinearSVCWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setForceIndexLabel(true)
      .setHandleInvalid(handleInvalid)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    val fitIntercept = rFormula.hasIntercept

    // get labels and feature names from output schema
    val (features, labels) = getFeaturesAndLabels(rFormulaModel, data)

    // assemble and fit the pipeline
    val svc = new LinearSVC()
      .setRegParam(regParam)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFitIntercept(fitIntercept)
      .setStandardization(standardization)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
      .setThreshold(threshold)
      .setAggregationDepth(aggregationDepth)

    if (weightCol != null) svc.setWeightCol(weightCol)

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, svc, idxToStr))
      .fit(data)

    new LinearSVCWrapper(pipeline, features, labels)
  }

  override def read: MLReader[LinearSVCWrapper] = new LinearSVCWrapperReader

  override def load(path: String): LinearSVCWrapper = super.load(path)

  class LinearSVCWrapperWriter(instance: LinearSVCWrapper) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("features" -> instance.features.toSeq) ~
        ("labels" -> instance.labels.toSeq)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class LinearSVCWrapperReader extends MLReader[LinearSVCWrapper] {

    override def load(path: String): LinearSVCWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val labels = (rMetadata \ "labels").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new LinearSVCWrapper(pipeline, features, labels)
    }
  }
}

