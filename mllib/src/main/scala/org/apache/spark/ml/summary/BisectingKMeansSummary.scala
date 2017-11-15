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
 * Summary of BisectingKMeans.
 *
 * @param predictions  `DataFrame` produced by `BisectingKMeansModel.transform()`.
 * @param predictionCol  Name for column of predicted clusters in `predictions`.
 * @param featuresCol  Name for column of features in `predictions`.
 * @param k  Number of clusters.
 */
@Since("2.1.0")
@Experimental
class BisectingKMeansSummary private[ml] (
    predictions: DataFrame,
    predictionCol: String,
    featuresCol: String,
    k: Int) extends ClusteringSummary(predictions, predictionCol, featuresCol, k)
