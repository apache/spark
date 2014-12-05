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
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.mllib.linalg.Vector

/**
 * Params for classification.
 * Currently empty, but may add functionality later.
 */
private[classification] trait ClassifierParams extends PredictorParams

/**
 * Single-label binary or multiclass classification
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 */
@AlphaComponent
abstract class Classifier[Learner <: Classifier[Learner, M], M <: ClassificationModel[M]]
  extends Predictor[Learner, M]
  with ClassifierParams {

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: AlphaComponent ::
 * Model produced by a [[Classifier]].
 * Classes are indexed {0, 1, ..., numClasses - 1}.
 *
 * @tparam M  Model type.
 */
@AlphaComponent
abstract class ClassificationModel[M <: ClassificationModel[M]]
  extends PredictionModel[M] with ClassifierParams {

  /** Number of classes (values which the label can take). */
  def numClasses: Int

  /**
   * Predict label for the given features.
   * This default implementation for classification predicts the index of the maximum value
   * from [[predictRaw()]].
   */
  override def predict(features: Vector): Double = {
    predictRaw(features).toArray.zipWithIndex.maxBy(_._1)._2
  }

  /**
   * Raw prediction for each possible label.
   * The meaning of a "raw" prediction may vary between algorithms, but it intuitively gives
   * a magnitude of confidence in each possible label.
   * @return  vector where element i is the raw prediction for label i.
   *          This raw prediction may be any real number, where a larger value indicates greater
   *          confidence for that label.
   */
  def predictRaw(features: Vector): Vector

  // TODO: accuracy(dataset: RDD[LabeledPoint]): Double (follow-up PR)

}
