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
import org.apache.spark.ml.classification.{MultinomialLogisticRegression, MultinomialLogisticRegressionModel}
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset}

private[r] class MultinomialLogisticRegressionWrapper private (
    val pipeline: PipelineModel,
    val features: Array[String]) extends MLWritable {

  private  val multinomialLogisticRegressionModel: MultinomialLogisticRegressionModel =
    pipeline.stages(1).asInstanceOf[MultinomialLogisticRegressionModel]

  lazy val coefficients: Matrix = multinomialLogisticRegressionModel.coefficients

  lazy val intercepts: Vector = multinomialLogisticRegressionModel.intercepts

  lazy val numClasses: Int = multinomialLogisticRegressionModel.numClasses

  lazy val numFeatures: Int = multinomialLogisticRegressionModel.numFeatures

  override def write: MLWriter =
    new MultinomialLogisticRegressionWrapper.MultinomialLogisticRegressionWrapperWriter(this)
}

private[r] object MultinomialLogisticRegressionWrapper
  extends MLReadable[MultinomialLogisticRegressionWrapper] {
  def fit(): MultinomialLogisticRegressionWrapper = {

  }

  override def read: MLReader[MultinomialLogisticRegressionWrapper] =
    new MultinomialLogisticRegressionWrapperReader

  override def load(path: String): MultinomialLogisticRegressionWrapper = super.load(path)

  class MultinomialLogisticRegressionWrapperWriter(instance: MultinomialLogisticRegressionWrapper)
    extends MLWriter {

    override protected def saveImpl(path: String): Unit = ???
  }

  class MultinomialLogisticRegressionWrapperReader
    extends MLReader[MultinomialLogisticRegressionWrapper] {

    override def load(path: String): MultinomialLogisticRegressionWrapper = ???
  }
}
