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

package org.apache.spark.ml.regression

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor, PredictorParams}
import org.apache.spark.mllib.linalg.Vector

/**
 * Params for regression.
 * Currently empty, but may add functionality later.
 */
private[regression] trait RegressorParams extends PredictorParams

/**
 * :: AlphaComponent ::
 * Single-label regression
 */
@AlphaComponent
abstract class Regressor[Learner <: Regressor[Learner, M], M <: RegressionModel[M]]
  extends Predictor[Learner, M]
  with RegressorParams {

  // TODO: defaultEvaluator (follow-up PR)
}

/**
 * :: AlphaComponent ::
 * Model produced by a [[Regressor]].
 * @tparam M  Model type.
 */
@AlphaComponent
abstract class RegressionModel[M <: RegressionModel[M]]
  extends PredictionModel[M] with RegressorParams {

  /**
   * Predict real-valued label for the given features.
   */
  def predict(features: Vector): Double

}
