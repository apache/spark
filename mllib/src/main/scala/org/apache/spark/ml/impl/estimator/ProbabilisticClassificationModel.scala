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

package org.apache.spark.ml.impl.estimator

import org.apache.spark.mllib.linalg.Vector

/**
 * Trait for a [[org.apache.spark.ml.classification.ClassificationModel]] which can output
 * class conditional probabilities.
 */
private[ml] trait ProbabilisticClassificationModel {

  /**
   * Predict the probability of each class given the features.
   * These predictions are also called class conditional probabilities.
   *
   * WARNING: Not all models output well-calibrated probability estimates!  These probabilities
   *          should be treated as confidences, not precise probabilities.
   */
  def predictProbabilities(features: Vector): Vector

}
