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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.evaluation.ClassificationEvaluator
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml._
import org.apache.spark.ml.impl.estimator.{HasDefaultEvaluator, PredictionModel, Predictor,
  PredictorParams}
import org.apache.spark.rdd.RDD

@AlphaComponent
private[classification] trait ClassifierParams extends PredictorParams

/**
 * Single-label binary or multiclass classification
 */
abstract class Classifier[Learner <: Classifier[Learner, M], M <: ClassificationModel[M]]
  extends Predictor[Learner, M]
  with ClassifierParams
  with HasDefaultEvaluator {

  override def defaultEvaluator: Evaluator = new ClassificationEvaluator
}


private[ml] abstract class ClassificationModel[M <: ClassificationModel[M]]
  extends PredictionModel[M] with ClassifierParams {

  def numClasses: Int

  /**
   * Predict label for the given features.  Labels are indexed {0, 1, ..., numClasses - 1}.
   * This default implementation for classification predicts the index of the maximum value
   * from [[predictRaw()]].
   */
  override def predict(features: Vector): Double = {
    predictRaw(features).toArray.zipWithIndex.maxBy(_._1)._2
  }

  /**
   * Raw prediction for each possible label
   * @return  vector where element i is the raw score for label i
   */
  def predictRaw(features: Vector): Vector

  /**
   * Compute this model's accuracy on the given dataset.
   */
  def accuracy(dataset: RDD[LabeledPoint]): Double = {
    // TODO: Handle instance weights.
    val predictionsAndLabels = dataset.map(lp => predict(lp.features))
      .zip(dataset.map(_.label))
    ClassificationEvaluator.computeMetric(predictionsAndLabels, "accuracy")
  }

}
