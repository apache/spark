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

import org.apache.spark.sql.DataFrame

private[spark] trait PredictionSummary extends Serializable {

  /** Dataframe output by the model's `transform` method. */
  def predictions: DataFrame

  /** Field in "predictions" which gives the prediction of each class. */
  def predictionCol: String

  /** Field in "predictions" which gives the features of each instance as a vector. */
  def featuresCol: String

}

private[spark] trait SupervisedPredictionSummary extends PredictionSummary {

  /** Field in "predictions" which gives the true label of each instance (if available). */
  def labelCol: String

}
