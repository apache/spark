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
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class LinearRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String]) extends MLWritable {

  private val linearRegressionModel: LinearRegressionModel =
    pipeline.stages(1).asInstanceOf[LinearRegressionModel]

  lazy val rFeatures: Array[String] = if (linearRegressionModel.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  lazy val rCoefficients: Array[Double] = if (linearRegressionModel.getFitIntercept) {
    Array(linearRegressionModel.intercept) ++ linearRegressionModel.coefficients.toArray
  } else {
    linearRegressionModel.coefficients.toArray
  }

  lazy val numFeatures: Int = linearRegressionModel.numFeatures

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(linearRegressionModel.getFeaturesCol)
  }

  override def write: MLWriter = new LinearRegressionWrapper.LinearRegressionWrapperWriter(this)
}

private[r] object LinearRegressionWrapper
  extends MLReadable[LinearRegressionWrapper] {

  def fit(  // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxIter: Int,
      regParam: Double,
      elasticNetParam: Double,
      tol: Double,
      standardization: Boolean,
      solver: String,
      weightCol: String,
      aggregationDepth: Int,
      loss: String,
      epsilon: Double,
      stringIndexerOrderType: String): LinearRegressionWrapper = {

    val rFormula = new RFormula()
      .setFormula(formula)
      .setStringIndexerOrderType(stringIndexerOrderType)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    val fitIntercept = rFormula.hasIntercept

    // get feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormulaModel.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)

    // assemble and fit the pipeline
    val lm = new LinearRegression()
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setElasticNetParam(elasticNetParam)
      .setTol(tol)
      .setFitIntercept(fitIntercept)
      .setStandardization(standardization)
      .setSolver(solver)
      .setAggregationDepth(aggregationDepth)
      .setLoss(loss)
      .setEpsilon(epsilon)
      .setFeaturesCol(rFormula.getFeaturesCol)

    if (weightCol != null) {
      lm.setWeightCol(weightCol)
    }

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, lm))
      .fit(data)

    new LinearRegressionWrapper(pipeline, features)
  }

  override def read: MLReader[LinearRegressionWrapper] = new LinearRegressionWrapperReader

  class LinearRegressionWrapperWriter(instance: LinearRegressionWrapper) extends MLWriter {
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

  class LinearRegressionWrapperReader extends MLReader[LinearRegressionWrapper] {

    override def load(path: String): LinearRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val features = (rMetadata \ "features").extract[Array[String]]

      val pipeline = PipelineModel.load(pipelinePath)
      new LinearRegressionWrapper(pipeline, features)
    }
  }
}
