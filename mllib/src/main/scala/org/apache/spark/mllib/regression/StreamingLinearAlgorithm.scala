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

import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.api.java.{JavaDStream, JavaPairDStream}
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
 * NOTE: In some use cases, the order in which trainOn and predictOn
 * are called in an application will affect the results. When called on
 * the same DStream, if trainOn is called before predictOn, when new data
 * arrive the model will update and the prediction will be based on the new
 * model. Whereas if predictOn is called first, the prediction will use the model
 * from the previous update.
 *
 * NOTE: It is ok to call predictOn repeatedly on multiple streams; this
 * will generate predictions for each one all using the current model.
 * It is also ok to call trainOn on different streams; this will update
 * the model using each of the different sources, in sequence.
 *
 *
 */
@Since("1.1.0")
@DeveloperApi
abstract class StreamingLinearAlgorithm[
    M <: GeneralizedLinearModel,
    A <: GeneralizedLinearAlgorithm[M]] extends Logging {

  /** The model to be updated and used for prediction. */
  protected var model: Option[M]

  /** The algorithm to use for updating. */
  protected val algorithm: A

  /**
   * Return the latest model.
   *
   */
  @Since("1.1.0")
  def latestModel(): M = {
    model.get
  }

  /**
   * Update the model by training on batches of data from a DStream.
   * This operation registers a DStream for training the model,
   * and updates the model based on every subsequent
   * batch of data from the stream.
   *
   * @param data DStream containing labeled data
   */
  @Since("1.1.0")
  def trainOn(data: DStream[LabeledPoint]): Unit = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting training.")
    }
    data.foreachRDD { (rdd, time) =>
      if (!rdd.isEmpty) {
        model = Some(algorithm.run(rdd, model.get.weights))
        logInfo(s"Model updated at time ${time.toString}")
        val display = model.get.weights.size match {
          case x if x > 100 => model.get.weights.toArray.take(100).mkString("[", ",", "...")
          case _ => model.get.weights.toArray.mkString("[", ",", "]")
        }
        logInfo(s"Current model: weights, ${display}")
      }
    }
  }

  /**
   * Java-friendly version of `trainOn`.
   */
  @Since("1.3.0")
  def trainOn(data: JavaDStream[LabeledPoint]): Unit = trainOn(data.dstream)

  /**
   * Use the model to make predictions on batches of data from a DStream
   *
   * @param data DStream containing feature vectors
   * @return DStream containing predictions
   *
   */
  @Since("1.1.0")
  def predictOn(data: DStream[Vector]): DStream[Double] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction.")
    }
    data.map{x => model.get.predict(x)}
  }

  /**
   * Java-friendly version of `predictOn`.
   *
   */
  @Since("1.3.0")
  def predictOn(data: JavaDStream[Vector]): JavaDStream[java.lang.Double] = {
    JavaDStream.fromDStream(predictOn(data.dstream).asInstanceOf[DStream[java.lang.Double]])
  }

  /**
   * Use the model to make predictions on the values of a DStream and carry over its keys.
   * @param data DStream containing feature vectors
   * @tparam K key type
   * @return DStream containing the input keys and the predictions as values
   *
   */
  @Since("1.1.0")
  def predictOnValues[K: ClassTag](data: DStream[(K, Vector)]): DStream[(K, Double)] = {
    if (model.isEmpty) {
      throw new IllegalArgumentException("Model must be initialized before starting prediction")
    }
    data.mapValues{x => model.get.predict(x)}
  }


  /**
   * Java-friendly version of `predictOnValues`.
   *
   */
  @Since("1.3.0")
  def predictOnValues[K](data: JavaPairDStream[K, Vector]): JavaPairDStream[K, java.lang.Double] = {
    implicit val tag = fakeClassTag[K]
    JavaPairDStream.fromPairDStream(
      predictOnValues(data.dstream).asInstanceOf[DStream[(K, java.lang.Double)]])
  }
}
