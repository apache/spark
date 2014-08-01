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

package org.apache.spark.mllib.regression

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

/**
 * :: DeveloperApi ::
 * StreamingRegression implements methods for training
 * a linear regression model on streaming data, and using it
 * for prediction on streaming data.
 *
 * This class takes as type parameters a GeneralizedLinearModel,
 * and a GeneralizedLinearAlgorithm, making it easy to extend to construct
 * streaming versions of arbitrary regression analyses. For example usage,
 * see StreamingLinearRegressionWithSGD.
 *
 */
@DeveloperApi
abstract class StreamingRegression[
    M <: GeneralizedLinearModel,
    A <: GeneralizedLinearAlgorithm[M]] extends Logging {

  /** The model to be updated and used for prediction. */
  var model: M

  /** The algorithm to use for updating. */
  val algorithm: A

  /** Return the latest model. */
  def latest(): M = {
    model
  }

  /**
   * Update the model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * and updates the model based on every subsequent non-empty
   * batch of data from the stream.
   *
   * @param data DStream containing labeled data
   */
  def trainOn(data: DStream[LabeledPoint]) {
    data.foreachRDD{
      rdd =>
        model = algorithm.run(rdd, model.weights)
        logInfo("Model updated")
        logInfo("Current model: weights, %s".format(model.weights.toString))
        logInfo("Current model: intercept, %s".format(model.intercept.toString))
    }
  }

  /**
   * Use the model to make predictions on batches of data from a DStream
   *
   * @param data DStream containing labeled data
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[LabeledPoint]): DStream[Double] = {
    data.map(x => model.predict(x.features))
  }

}
