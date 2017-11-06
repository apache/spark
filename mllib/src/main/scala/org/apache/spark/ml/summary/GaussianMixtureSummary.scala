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
package org.apache.spark.ml.summary

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.sql.DataFrame

/**
 * :: Experimental ::
 * Summary of GaussianMixture.
 *
 * @param predictions  `DataFrame` produced by `GaussianMixtureModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param probabilityCol  Name for column of predicted probability of each cluster
 *                        in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 * @param logLikelihood  Total log-likelihood for this model on the given data.
 */
@Since("2.0.0")
@Experimental
class GaussianMixtureSummary private[ml] (
    predictions: DataFrame,
    predictionCol: String,
    @Since("2.0.0") val probabilityCol: String,
    featuresCol: String,
    k: Int,
    @Since("2.2.0") val logLikelihood: Double)
  extends ClusteringSummary(predictions, predictionCol, featuresCol, k) {

  /**
   * Probability of each cluster.
   */
  @Since("2.0.0")
  @transient lazy val probability: DataFrame = predictions.select(probabilityCol)
}
