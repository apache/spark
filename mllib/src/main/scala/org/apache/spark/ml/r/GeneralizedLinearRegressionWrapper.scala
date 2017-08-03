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

import java.util.Locale

import org.apache.hadoop.fs.Path
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.r.RWrapperUtils._
import org.apache.spark.ml.regression._
import org.apache.spark.ml.util._
import org.apache.spark.sql._

private[r] class GeneralizedLinearRegressionWrapper private (
    val pipeline: PipelineModel,
    val rFeatures: Array[String],
    val rCoefficients: Array[Double],
    val rDispersion: Double,
    val rNullDeviance: Double,
    val rDeviance: Double,
    val rResidualDegreeOfFreedomNull: Long,
    val rResidualDegreeOfFreedom: Long,
    val rAic: Double,
    val rNumIterations: Int,
    val isLoaded: Boolean = false) extends MLWritable {

  private val glm: GeneralizedLinearRegressionModel =
    pipeline.stages(1).asInstanceOf[GeneralizedLinearRegressionModel]

  lazy val rDevianceResiduals: DataFrame = glm.summary.residuals()

  lazy val rFamily: String = glm.getFamily

  def residuals(residualsType: String): DataFrame = glm.summary.residuals(residualsType)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(glm.getFeaturesCol)
  }

  override def write: MLWriter =
    new GeneralizedLinearRegressionWrapper.GeneralizedLinearRegressionWrapperWriter(this)
}

private[r] object GeneralizedLinearRegressionWrapper
  extends MLReadable[GeneralizedLinearRegressionWrapper] {

  // scalastyle:off
  def fit(
      formula: String,
      data: DataFrame,
      family: String,
      link: String,
      tol: Double,
      maxIter: Int,
      weightCol: String,
      regParam: Double,
      variancePower: Double,
      linkPower: Double,
      stringIndexerOrderType: String,
      offsetCol: String): GeneralizedLinearRegressionWrapper = {
  // scalastyle:on
    val rFormula = new RFormula().setFormula(formula)
      .setStringIndexerOrderType(stringIndexerOrderType)
    checkDataColumns(rFormula, data)
    val rFormulaModel = rFormula.fit(data)

    // assemble and fit the pipeline
    val glr = new GeneralizedLinearRegression()
      .setFamily(family)
      .setFitIntercept(rFormula.hasIntercept)
      .setTol(tol)
      .setMaxIter(maxIter)
      .setRegParam(regParam)
      .setFeaturesCol(rFormula.getFeaturesCol)
    // set variancePower and linkPower if family is tweedie; otherwise, set link function
    if (family.toLowerCase(Locale.ROOT) == "tweedie") {
      glr.setVariancePower(variancePower).setLinkPower(linkPower)
    } else {
      glr.setLink(link)
    }
    if (weightCol != null) glr.setWeightCol(weightCol)
    if (offsetCol != null) glr.setOffsetCol(offsetCol)

    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, glr))
      .fit(data)

    val glm: GeneralizedLinearRegressionModel =
      pipeline.stages(1).asInstanceOf[GeneralizedLinearRegressionModel]
    val summary = glm.summary

    val rFeatures: Array[String] = if (glm.getFitIntercept) {
      Array("(Intercept)") ++ summary.featureNames
    } else {
      summary.featureNames
    }

    val rCoefficients: Array[Double] = if (summary.isNormalSolver) {
      summary.coefficientsWithStatistics.map(_._2) ++
        summary.coefficientsWithStatistics.map(_._3) ++
        summary.coefficientsWithStatistics.map(_._4) ++
        summary.coefficientsWithStatistics.map(_._5)
    } else {
      if (glm.getFitIntercept) {
        Array(glm.intercept) ++ glm.coefficients.toArray
      } else {
        glm.coefficients.toArray
      }
    }

    val rDispersion: Double = summary.dispersion
    val rNullDeviance: Double = summary.nullDeviance
    val rDeviance: Double = summary.deviance
    val rResidualDegreeOfFreedomNull: Long = summary.residualDegreeOfFreedomNull
    val rResidualDegreeOfFreedom: Long = summary.residualDegreeOfFreedom
    val rAic: Double = if (family.toLowerCase(Locale.ROOT) == "tweedie" &&
      !Array(0.0, 1.0, 2.0).exists(x => math.abs(x - variancePower) < 1e-8)) {
      0.0
    } else {
      summary.aic
    }
    val rNumIterations: Int = summary.numIterations

    new GeneralizedLinearRegressionWrapper(pipeline, rFeatures, rCoefficients, rDispersion,
      rNullDeviance, rDeviance, rResidualDegreeOfFreedomNull, rResidualDegreeOfFreedom,
      rAic, rNumIterations)
  }

  override def read: MLReader[GeneralizedLinearRegressionWrapper] =
    new GeneralizedLinearRegressionWrapperReader

  override def load(path: String): GeneralizedLinearRegressionWrapper = super.load(path)

  class GeneralizedLinearRegressionWrapperWriter(instance: GeneralizedLinearRegressionWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = {
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadata = ("class" -> instance.getClass.getName) ~
        ("rFeatures" -> instance.rFeatures.toSeq) ~
        ("rCoefficients" -> instance.rCoefficients.toSeq) ~
        ("rDispersion" -> instance.rDispersion) ~
        ("rNullDeviance" -> instance.rNullDeviance) ~
        ("rDeviance" -> instance.rDeviance) ~
        ("rResidualDegreeOfFreedomNull" -> instance.rResidualDegreeOfFreedomNull) ~
        ("rResidualDegreeOfFreedom" -> instance.rResidualDegreeOfFreedom) ~
        ("rAic" -> instance.rAic) ~
        ("rNumIterations" -> instance.rNumIterations)
      val rMetadataJson: String = compact(render(rMetadata))
      sc.parallelize(Seq(rMetadataJson), 1).saveAsTextFile(rMetadataPath)

      instance.pipeline.save(pipelinePath)
    }
  }

  class GeneralizedLinearRegressionWrapperReader
    extends MLReader[GeneralizedLinearRegressionWrapper] {

    override def load(path: String): GeneralizedLinearRegressionWrapper = {
      implicit val format = DefaultFormats
      val rMetadataPath = new Path(path, "rMetadata").toString
      val pipelinePath = new Path(path, "pipeline").toString

      val rMetadataStr = sc.textFile(rMetadataPath, 1).first()
      val rMetadata = parse(rMetadataStr)
      val rFeatures = (rMetadata \ "rFeatures").extract[Array[String]]
      val rCoefficients = (rMetadata \ "rCoefficients").extract[Array[Double]]
      val rDispersion = (rMetadata \ "rDispersion").extract[Double]
      val rNullDeviance = (rMetadata \ "rNullDeviance").extract[Double]
      val rDeviance = (rMetadata \ "rDeviance").extract[Double]
      val rResidualDegreeOfFreedomNull = (rMetadata \ "rResidualDegreeOfFreedomNull").extract[Long]
      val rResidualDegreeOfFreedom = (rMetadata \ "rResidualDegreeOfFreedom").extract[Long]
      val rAic = (rMetadata \ "rAic").extract[Double]
      val rNumIterations = (rMetadata \ "rNumIterations").extract[Int]

      val pipeline = PipelineModel.load(pipelinePath)

      new GeneralizedLinearRegressionWrapper(pipeline, rFeatures, rCoefficients, rDispersion,
        rNullDeviance, rDeviance, rResidualDegreeOfFreedomNull, rResidualDegreeOfFreedom,
        rAic, rNumIterations, isLoaded = true)
    }
  }
}
