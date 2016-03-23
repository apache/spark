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

import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.sql._

import scala.annotation.varargs

/**
  * An estimator that also acts as a transformer and that updates itself when being fit.
  * @tparam T
  */
abstract class MutableEstimator[T <: MutableEstimator[T]] extends PipelineStage {

  def setParams(paramMap: ParamMap): Unit = {
    paramMap.toSeq.foreach(set)
  }

  def setParams(first: ParamPair[_], other: ParamPair[_]*): Unit = {
    set(first)
    other.foreach(set)
  }

  /**
    * Fits a single model to the input data with optional parameters.
    *
    * @param dataset input dataset
    * @param firstParamPair the first param pair, overrides embedded params
    * @param otherParamPairs other param pairs.  These values override any specified in this
    *                        Estimator's embedded ParamMap.
    * @return fitted model
    */
  @deprecated("2.0.0", "You should use setParams() followed by fit() instead")
  @varargs
  def fit(
      dataset: DataFrame,
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): Unit = {
    setParams(firstParamPair, otherParamPairs: _*)
    fit(dataset)
  }

  /**
    * Fits a single model to the input data with provided parameter map.
    *
    * @param dataset input dataset
    * @param paramMap Parameter map.
    *                 These values override any specified in this Estimator's embedded ParamMap.
    * @return fitted model
    */
  @deprecated("2.0.0", "You should use setParams() followed by fit() instead")
  def fit(
      dataset: DataFrame,
      paramMap: ParamMap): Unit = {
    setParams(paramMap)
    fit(dataset)
  }

  /**
    * Fits a model to the input data.
    */
  def fit(dataset: DataFrame): Unit

  /**
    * Transforms the input dataset.
    */
  def transform(dataset: DataFrame): DataFrame

  // TODO: check that this is not changing the signature in the documentation.
  override def copy(extra: ParamMap): T

}
