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

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.dstream.DStream

/**
 * :: DeveloperApi ::
 * StreamingLinearAlgorithm implements methods for continuously
 * training a generalized linear model model on streaming data,
 * and using it for prediction on (possibly different) streaming data.
 *
 * This class takes as type parameters a GeneralizedLinearModel,
 * and a GeneralizedLinearAlgorithm, making it easy to extend to construct
 * streaming versions of any analyses using GLMs.
 * Initial weights must be set before calling trainOn or predictOn.
 * Only weights will be updated, not an intercept. If the model needs
 * an intercept, it should be manually appended to the input data.
 *
 * For example usage, see `StreamingLinearRegressionWithSGD`.
 *
 * NOTE(Freeman): In some use cases, the order in which trainOn and predictOn
 * are called in an application will affect the results. When called on
 * the same DStream, if trainOn is called before predictOn, when new data
 * arrive the model will update and the prediction will be based on the new
 * model. Whereas if predictOn is called first, the prediction will use the model
 * from the previous update.
 *
 * NOTE(Freeman): It is ok to call predictOn repeatedly on multiple streams; this
 * will generate predictions for each one all using the current model.
 * It is also ok to call trainOn on different streams; this will update
 * the model using each of the different sources, in sequence.
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
    if (Option(model.weights) == None) {
      logError("Initial weights must be set before starting training")
      throw new IllegalArgumentException
    }
    data.foreachRDD { (rdd, time) =>
        model = algorithm.run(rdd, model.weights)
        logInfo("Model updated at time %s".format(time.toString))
        val display = model.weights.size match {
          case x if x > 100 => model.weights.toArray.take(100).mkString("[", ",", "...")
          case _ => model.weights.toArray.mkString("[", ",", "]")
        }
        logInfo("Current model: weights, %s".format (display))
    }
  }

  /**
   * Use the model to make predictions on batches of data from a DStream
   *
   * @param data DStream containing feature vectors
   * @return DStream containing predictions
   */
  def predictOn(data: DStream[Vector]): DStream[Double] = {
    if (Option(model.weights) == None) {
      val msg = "Initial weights must be set before starting prediction"
      logError(msg)
      throw new IllegalArgumentException(msg)
    }
    data.map(model.predict)
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   * @param data DStream containing feature vectors
   * @tparam K key type
   * @return DStream containing the input keys and the predictions as values
   */
  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Double)] = {
    if (Option(model.weights) == None) {
      val msg = "Initial weights must be set before starting prediction"
      logError(msg)
      throw new IllegalArgumentException(msg)
    }
    data.mapValues(model.predict)
  }
}
