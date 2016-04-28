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

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression._
import org.apache.spark.sql._

private[r] class GeneralizedLinearRegressionWrapper private (
    pipeline: PipelineModel,
    val features: Array[String]) {

  private val glm: GeneralizedLinearRegressionModel =
    pipeline.stages(1).asInstanceOf[GeneralizedLinearRegressionModel]

  lazy val rFeatures: Array[String] = if (glm.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  lazy val rCoefficients: Array[Double] = if (glm.getFitIntercept) {
    Array(glm.intercept) ++ glm.coefficients.toArray ++
      rCoefficientStandardErrors ++ rTValues ++ rPValues
  } else {
    glm.coefficients.toArray ++ rCoefficientStandardErrors ++ rTValues ++ rPValues
  }

  private lazy val rCoefficientStandardErrors = if (glm.getFitIntercept) {
    Array(glm.summary.coefficientStandardErrors.last) ++
      glm.summary.coefficientStandardErrors.dropRight(1)
  } else {
    glm.summary.coefficientStandardErrors
  }

  private lazy val rTValues = if (glm.getFitIntercept) {
    Array(glm.summary.tValues.last) ++ glm.summary.tValues.dropRight(1)
  } else {
    glm.summary.tValues
  }

  private lazy val rPValues = if (glm.getFitIntercept) {
    Array(glm.summary.pValues.last) ++ glm.summary.pValues.dropRight(1)
  } else {
    glm.summary.pValues
  }

  lazy val rDispersion: Double = glm.summary.dispersion

  lazy val rNullDeviance: Double = glm.summary.nullDeviance

  lazy val rDeviance: Double = glm.summary.deviance

  lazy val rResidualDegreeOfFreedomNull: Long = glm.summary.residualDegreeOfFreedomNull

  lazy val rResidualDegreeOfFreedom: Long = glm.summary.residualDegreeOfFreedom

  lazy val rAic: Double = glm.summary.aic

  lazy val rNumIterations: Int = glm.summary.numIterations

  lazy val rDevianceResiduals: DataFrame = glm.summary.residuals()

  lazy val rFamily: String = glm.getFamily

  def residuals(residualsType: String): DataFrame = glm.summary.residuals(residualsType)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset).drop(glm.getFeaturesCol)
  }
}

private[r] object GeneralizedLinearRegressionWrapper {

  def fit(
      formula: String,
      data: DataFrame,
      family: String,
      link: String,
      epsilon: Double,
      maxit: Int): GeneralizedLinearRegressionWrapper = {
    val rFormula = new RFormula()
      .setFormula(formula)
    val rFormulaModel = rFormula.fit(data)
    // get labels and feature names from output schema
    val schema = rFormulaModel.transform(data).schema
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormula.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    // assemble and fit the pipeline
    val glm = new GeneralizedLinearRegression()
      .setFamily(family)
      .setLink(link)
      .setFitIntercept(rFormula.hasIntercept)
      .setTol(epsilon)
      .setMaxIter(maxit)
    val pipeline = new Pipeline()
      .setStages(Array(rFormulaModel, glm))
      .fit(data)
    new GeneralizedLinearRegressionWrapper(pipeline, features)
  }
}
