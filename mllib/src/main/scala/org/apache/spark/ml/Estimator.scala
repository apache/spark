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

package org.apache.spark.ml

import scala.annotation.varargs
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.annotation.Since
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql.Dataset

/**
 * Abstract class for estimators that fit models to data.
 */
abstract class Estimator[M <: Model[M]] extends PipelineStage {

  /**
   * Fits a single model to the input data with optional parameters.
   *
   * @param dataset input dataset
   * @param firstParamPair the first param pair, overrides embedded params
   * @param otherParamPairs other param pairs.  These values override any specified in this
   *                        Estimator's embedded ParamMap.
   * @return fitted model
   */
  @Since("2.0.0")
  @varargs
  def fit(dataset: Dataset[_], firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]*): M = {
    val map = new ParamMap()
      .put(firstParamPair)
      .put(otherParamPairs: _*)
    fit(dataset, map)
  }

  /**
   * Fits a single model to the input data with provided parameter map.
   *
   * @param dataset input dataset
   * @param paramMap Parameter map.
   *                 These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted model
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_], paramMap: ParamMap): M = {
    copy(paramMap).fit(dataset)
  }

  /**
   * Fits a model to the input data.
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_]): M

  /**
   * Fits multiple models to the input data with multiple sets of parameters.
   * The default implementation uses a for loop on each parameter map.
   * Subclasses could override this to optimize multi-model training.
   *
   * @param dataset input dataset
   * @param paramMaps An array of parameter maps.
   *                  These values override any specified in this Estimator's embedded ParamMap.
   * @return fitted models, matching the input parameter maps
   */
  @Since("2.0.0")
  def fit(dataset: Dataset[_], paramMaps: Seq[ParamMap]): Seq[M] = {
    paramMaps.map(fit(dataset, _))
  }

  override def copy(extra: ParamMap): Estimator[M]

  /**
   * For ml connect only.
   * Estimate an upper-bound size of the model to be fitted in bytes, based on the
   * parameters and the dataset, e.g., using $(k) and numFeatures to estimate a
   * k-means model size.
   * 1, Both driver side memory usage and distributed objects size (like DataFrame,
   * RDD, Graph, Summary) are counted.
   * 2, Lazy vals are not counted, e.g., an auxiliary object used in prediction.
   * 3, If there is no enough information to get an accurate size, try to estimate the
   * upper-bound size, e.g.
   *    - Given a LogisticRegression estimator, assume the coefficients are dense, even
   *      though the actual fitted model might be sparse (by L1 penalty).
   *    - Given a tree model, assume all underlying trees are complete binary trees, even
   *      though some branches might be pruned or truncated.
   * 4, For some model such as tree model, estimating model size before training is hard,
   *    the `estimateModelSize` method is not supported.
   */
  private[spark] def estimateModelSize(dataset: Dataset[_]): Long = {
    throw new UnsupportedOperationException
  }
}


object EstimatorUtils {
  // This warningMessagesBuffer is for collecting warning messages during `estimator.fit`
  // execution in Spark Connect server.
  private[spark] val warningMessagesBuffer = new java.lang.ThreadLocal[ArrayBuffer[String]]() {
    override def initialValue: ArrayBuffer[String] = null
  }
}
