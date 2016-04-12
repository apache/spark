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

  lazy val rCoefficients: Array[Double] = if (glm.getFitIntercept) {
    Array(glm.intercept) ++ glm.coefficients.toArray
  } else {
    glm.coefficients.toArray
  }

  lazy val rFeatures: Array[String] = if (glm.getFitIntercept) {
    Array("(Intercept)") ++ features
  } else {
    features
  }

  def transform(dataset: DataFrame): DataFrame = {
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
