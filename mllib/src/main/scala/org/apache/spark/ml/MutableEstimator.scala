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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param.{ParamMap, ParamPair}
import org.apache.spark.sql._

import scala.annotation.varargs


/**
 * :: DeveloperApi ::
 * An estimator that also acts as a transformer and that updates itself when being fit.
 *
 * @tparam T  Type of Estimator implementing this abstraction
 */
// NOTE: This class temporarily overrides Transformer during the process of merging Estimator
//       and Model.  This helps with handling meta-algorithms.  We will later extend PipelineStage
//       instead of Transformer.
@DeveloperApi
abstract class MutableEstimator[T <: MutableEstimator[T]] extends Transformer {

  // TODO: Decide on multi-model fitting API, or hide for 2.0.

  /**
   * Fits this model to the input data, modifying this instance in-place.
   */
  def fit(dataset: DataFrame): Unit

  /**
   * Transforms the given dataset.
   */
  def transform(dataset: DataFrame): DataFrame

  // TODO: check that this is not changing the signature in the documentation.
  /**
   * Copy this instance, returning the copy.
   *
   * @param extra Any parameters specified in this map will be set to the given values
   *              in the copied instance.
   */
  override def copy(extra: ParamMap): T

  // TEMP METHODS from Transformer, to be removed after merge is complete
  @varargs
  override def transform(
      dataset: DataFrame,
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): DataFrame = throw new RuntimeException

  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame =
    throw new RuntimeException
}
