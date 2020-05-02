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
import org.apache.spark.ml.classification.{FMClassificationModel, FMClassifier}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class FMClassifierWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val labels: Array[String]) extends MLWritable {
  import FMClassifierWrapper._

  private val fmClassificationModel: FMClassificationModel =
    pipeline.stages(1).asInstanceOf[FMClassificationModel]

  lazy val rFeatures: Array[String] = if (fmClassificationModel.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  lazy val rCoefficients: Array[Double] = if (fmClassificationModel.getFitIntercept) {
    Array(fmClassificationModel.intercept) ++ fmClassificationModel.linear.toArray
  } else {
    fmClassificationModel.linear.toArray
  }

  lazy val rFactors = fmClassificationModel.factors.toArray

  lazy val numClasses: Int = fmClassificationModel.numClasses

  lazy val numFeatures: Int = fmClassificationModel.numFeatures

  lazy val factorSize: Int = fmClassificationModel.getFactorSize

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(fmClassificationModel.getFeaturesCol)
      .drop(fmClassificationModel.getLabelCol)
  }

  override def write: MLWriter = new FMClassifierWrapper.FMClassifierWrapperWriter(this)
}

private[r] object FMClassifierWrapper
  extends MLReadable[FMClassifierWrapper] {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit(  // scalastyle:ignore
      data: DataFrame,
      formula: String,
      factorSize: Int,
      fitLinear: Boolean,
      regParam: Double,
      miniBatchFraction: Double,
      initStd: Double,
      maxIter: Int,
      stepSize: Double,
      tol: Double,
      solver: String,
      seed: String,
      thresholds: Array[Double],
      handleInvalid: String): FMClassifierWrapper = {

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
    val fmc = new FMClassifier()
      .setFactorSize(factorSize)
      .setFitIntercept(fitIntercept)
      .setFitLinear(fitLinear)
      .setRegParam(regParam)
      .setMiniBatchFraction(miniBatchFraction)
      .setInitStd(initStd)
      .setMaxIter(maxIter)
      .setStepSize(stepSize)
      .setTol(tol)
      .setSolver(solver)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setLabelCol(rFormula.getLabelCol)
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)

    if (seed != null && seed.length > 0) {
      fmc.setSeed(seed.toLong)
    }

    if (thresholds != null) {
      fmc.setThresholds(thresholds)
    }

    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, fmc, idxToStr))
      .fit(data)

    new FMClassifierWrapper(pipeline, features, labels)
  }

  override def read: MLReader[FMClassifierWrapper] = new FMClassifierWrapperReader

  class FMClassifierWrapperWriter(instance: FMClassifierWrapper) extends MLWriter {

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

  class FMClassifierWrapperReader extends MLReader[FMClassifierWrapper] {

    override def load(path: String): FMClassifierWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]
      val labels = (rMetadata \ "labels").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new FMClassifierWrapper(pipeline, features, labels)
    }
  }
}
