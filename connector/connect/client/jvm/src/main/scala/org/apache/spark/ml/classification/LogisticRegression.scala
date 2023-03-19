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

package org.apache.spark.ml.classification

import org.apache.spark.annotation.Since
import org.apache.spark.ml.{Model, ModelRef}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{HasTrainingSummary, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}


class LogisticRegression @Since("3.5.0") (
    @Since("1.4.0") override val uid: String)
  extends ProbabilisticClassifier[Vector, LogisticRegression, LogisticRegressionModel]
    with LogisticRegressionEstimatorParams {

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("logreg"))

  override def copy(extra: ParamMap): LogisticRegression = defaultCopy(extra)

  override def createModel(modelRef: ModelRef): LogisticRegressionModel = {
    val model = new LogisticRegressionModel(uid)
    model.modelRef = modelRef
    model
  }
}

@Since("1.4.0")
class LogisticRegressionModel private[spark] (@Since("1.4.0") override val uid: String)
  extends ProbabilisticClassificationModel[Vector, LogisticRegressionModel]
    with LogisticRegressionParams with HasTrainingSummary[LogisticRegressionTrainingSummary] {

  def coefficientMatrix: Matrix = getModelAttr("coefficientMatrix").asInstanceOf[Matrix]

  def interceptVector: Vector = getModelAttr("coefficientMatrix").asInstanceOf[Vector]

  def isMultinomial: Boolean = getModelAttr("coefficientMatrix").asInstanceOf[Boolean]

  @Since("1.5.0")
  override def setThreshold(value: Double): this.type = super.setThreshold(value)

  @Since("1.5.0")
  override def getThreshold: Double = super.getThreshold

  @Since("1.5.0")
  override def setThresholds(value: Array[Double]): this.type = super.setThresholds(value)

  @Since("1.5.0")
  override def getThresholds: Array[Double] = super.getThresholds

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false.
   */
  @Since("1.5.0")
  override def summary: LogisticRegressionTrainingSummary = super.summary

  /**
   * Gets summary of model on training set. An exception is thrown
   * if `hasSummary` is false or it is a multiclass model.
   */
  @Since("2.3.0")
  def binarySummary: BinaryLogisticRegressionTrainingSummary = summary match {
    case b: BinaryLogisticRegressionTrainingSummary => b
    case _ =>
      throw new RuntimeException("Cannot create a binary summary for a non-binary model" +
        s"(numClasses=${numClasses}), use summary instead.")
  }

  /**
   * Evaluates the model on a test dataset.
   *
   * @param dataset Test dataset to evaluate model on.
   */
  @Since("2.0.0")
  def evaluate(dataset: Dataset[_]): LogisticRegressionSummary = {
    val weightColName = if (!isDefined(weightCol)) "weightCol" else $(weightCol)
    // Handle possible missing or invalid prediction columns
    val (summaryModel, probabilityColName, predictionColName) = findSummaryModel()
    val model = this
    val sumamryDatasetOpt = Some(summaryModel.transform(dataset))
    if (numClasses > 2) {
      new LogisticRegressionSummary{
        override def model: Model[_] = model
        override def datasetOpt: Option[Dataset[_]] = sumamryDatasetOpt
      }
    } else {
      new BinaryLogisticRegressionSummary{
        override def model: Model[_] = model
        override def datasetOpt: Option[Dataset[_]] = sumamryDatasetOpt
      }
    }
  }

}

/**
 * Abstraction for logistic regression results for a given model.
 */
trait LogisticRegressionSummary extends ClassificationSummary {

  /** Field in "predictions" which gives the probability of each class as a vector. */
  @Since("1.5.0")
  def probabilityCol: String = {
    getModelSummaryAttr("probabilityCol").asInstanceOf[String]
  }

  /** Field in "predictions" which gives the features of each instance as a vector. */
  @Since("1.6.0")
  def featuresCol: String = {
    getModelSummaryAttr("featuresCol").asInstanceOf[String]
  }

  /**
   * Convenient method for casting to binary logistic regression summary.
   * This method will throw an Exception if the summary is not a binary summary.
   */
  @Since("2.3.0")
  def asBinary: BinaryLogisticRegressionSummary = this match {
    case b: BinaryLogisticRegressionSummary => b
    case _ =>
      throw new RuntimeException("Cannot cast to a binary summary.")
  }
}

/**
 * Abstraction for multiclass logistic regression training results.
 */
trait LogisticRegressionTrainingSummary extends LogisticRegressionSummary
  with TrainingSummary {
}

/**
 * Abstraction for binary logistic regression results for a given model.
 */
trait BinaryLogisticRegressionSummary extends LogisticRegressionSummary
  with BinaryClassificationSummary

/**
 * Abstraction for binary logistic regression training results.
 */
trait BinaryLogisticRegressionTrainingSummary extends BinaryLogisticRegressionSummary
  with LogisticRegressionTrainingSummary
