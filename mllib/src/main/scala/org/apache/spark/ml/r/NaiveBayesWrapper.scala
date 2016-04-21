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
import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NominalAttribute}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.feature.{IndexToString, RFormula}
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class NaiveBayesWrapper private (
    pipeline: PipelineModel,
    val labels: Array[String],
    val features: Array[String]) {

  import NaiveBayesWrapper._

  private val naiveBayesModel: NaiveBayesModel = pipeline.stages(1).asInstanceOf[NaiveBayesModel]

  lazy val apriori: Array[Double] = naiveBayesModel.pi.toArray.map(math.exp)

  lazy val tables: Array[Double] = naiveBayesModel.theta.toArray.map(math.exp)

  def transform(dataset: Dataset[_]): DataFrame = {
    pipeline.transform(dataset)
      .drop(PREDICTED_LABEL_INDEX_COL)
      .drop(naiveBayesModel.getFeaturesCol)
  }
}

private[r] object NaiveBayesWrapper {

  val PREDICTED_LABEL_INDEX_COL = "pred_label_idx"
  val PREDICTED_LABEL_COL = "prediction"

  def fit(formula: String, data: DataFrame, laplace: Double): NaiveBayesWrapper = {
    val rFormula = new RFormula()
      .setFormula(formula)
      .fit(data)
    // get labels and feature names from output schema
    val schema = rFormula.transform(data).schema
    val labelAttr = Attribute.fromStructField(schema(rFormula.getLabelCol))
      .asInstanceOf[NominalAttribute]
    val labels = labelAttr.values.get
    val featureAttrs = AttributeGroup.fromStructField(schema(rFormula.getFeaturesCol))
      .attributes.get
    val features = featureAttrs.map(_.name.get)
    // assemble and fit the pipeline
    val naiveBayes = new NaiveBayes()
      .setSmoothing(laplace)
      .setModelType("bernoulli")
      .setPredictionCol(PREDICTED_LABEL_INDEX_COL)
    val idxToStr = new IndexToString()
      .setInputCol(PREDICTED_LABEL_INDEX_COL)
      .setOutputCol(PREDICTED_LABEL_COL)
      .setLabels(labels)
    val pipeline = new Pipeline()
      .setStages(Array(rFormula, naiveBayes, idxToStr))
      .fit(data)
    new NaiveBayesWrapper(pipeline, labels, features)
  }
}
