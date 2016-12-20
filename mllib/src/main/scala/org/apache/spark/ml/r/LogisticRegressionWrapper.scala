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
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LogisticRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String],
    val isLoaded: Boolean = false) extends MLWritable {

  private val logisticRegressionModel: LogisticRegressionModel =
    pipeline.stages(1).asInstanceOf[LogisticRegressionModel]

  lazy val totalIterations: Int = logisticRegressionModel.summary.totalIterations

  lazy val objectiveHistory: Array[Double] = logisticRegressionModel.summary.objectiveHistory

  lazy val blrSummary =
    logisticRegressionModel.summary.asInstanceOf[BinaryLogisticRegressionSummary]

  lazy val roc: DataFrame = blrSummary.roc

  lazy val areaUnderROC: Double = blrSummary.areaUnderROC

  lazy val pr: DataFrame = blrSummary.pr

  lazy val fMeasureByThreshold: DataFrame = blrSummary.fMeasureByThreshold

  lazy val precisionByThreshold: DataFrame = blrSummary.precisionByThreshold

  lazy val recallByThreshold: DataFrame = blrSummary.recallByThreshold

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(logisticRegressionModel.getFeaturesCol)
  }

  override def write: MLWriter = new LogisticRegressionWrapper.LogisticRegressionWrapperWriter(this)
}

private[r] object LogisticRegressionWrapper
    extends MLReadable[LogisticRegressionWrapper] {

  def fit( // scalastyle:ignore
      data: DataFrame,
      formula: String,
      regParam: Double,
      elasticNetParam: Double,
      maxIter: Int,
      tol: Double,
      fitIntercept: Boolean,
      family: String,
      standardization: Boolean,
      thresholds: Array[Double],
      weightCol: String,
      aggregationDepth: Int,
      probability: String
      ): LogisticRegressionWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
    RWrapperUtils.checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    // assemble and fit the pipeline
    val logisticRegression = new LogisticRegression()
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setMaxIter(maxIter)
      .setTol(tol)
      .setFitIntercept(fitIntercept)
      .setFamily(family)
      .setStandardization(standardization)
      .setWeightCol(weightCol)
      .setAggregationDepth(aggregationDepth)
      .setFeaturesCol(rFormula.getFeaturesCol)
      .setProbabilityCol(probability)

    if (thresholds.length > 1) {
      logisticRegression.setThresholds(thresholds)
    } else {
      logisticRegression.setThreshold(thresholds(0))
    }

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, logisticRegression))
      .fit(data)

    new LogisticRegressionWrapper(pipeline, features)
  }

  override def read: MLReader[LogisticRegressionWrapper] = new LogisticRegressionWrapperReader

  override def load(path: String): LogisticRegressionWrapper = super.load(path)

  class LogisticRegressionWrapperWriter(instance: LogisticRegressionWrapper) extends MLWriter {

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

  class LogisticRegressionWrapperReader extends MLReader[LogisticRegressionWrapper] {

    override def load(path: String): LogisticRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new LogisticRegressionWrapper(pipeline, features, isLoaded = true)
    }
  }
}