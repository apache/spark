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
 * StreamingLinearAlgorithm implements methods for continuously
 * training a generalized linear model model on streaming data,
 * and using it for prediction on streaming data.
 *
 * This class takes as type parameters a GeneralizedLinearModel,
 * and a GeneralizedLinearAlgorithm, making it easy to extend to construct
 * streaming versions of any analyses using GLMs. For example usage,
 * see StreamingLinearRegressionWithSGD.
 *
 * NOTE: Only weights will be updated, not an intercept.
 * If the model needs an intercept, it should be manually appended
 * to the input data.
 *
 */
@DeveloperApi
abstract class StreamingLinearAlgorithm[
    M <: GeneralizedLinearModel,
    A <: GeneralizedLinearAlgorithm[M]] extends Logging {

  /** The model to be updated and used for prediction. */
  protected var model: M

  /** The algorithm to use for updating. */
  protected val algorithm: A

  /** Return the latest model. */
  def latestModel(): M = {
    model
  }

  /**
   * Update the model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * and updates the model based on every subsequent
   * batch of data from the stream.
   *
   * @param data DStream containing labeled data
   */
  def trainOn(data: DStream[LabeledPoint]) {
    data.foreachRDD { (rdd, time) =>
        model = algorithm.run(rdd, model.weights)
        logInfo("Model updated at time %s".format(time.toString))
        logInfo("Current model: weights, %s".format(
          model.weights.toArray.take(100).mkString("[", ",", "]")))
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
