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

import org.apache.spark.ml.param.{ParamMap, Params, ParamPair}
import org.apache.spark.sql.SchemaRDD

/**
 * Abstract class for estimators that fits models to data.
 */
abstract class Estimator[M <: Model] extends Identifiable with Params with PipelineStage {

  /**
   * Fits a single model to the input data with default parameters.
   *
   * @param dataset input dataset
   * @return fitted model
   */
  def fit(dataset: SchemaRDD): M = {
    fit(dataset, ParamMap.empty)
  }

  /**
   * Fits a single model to the input data with provided parameter map.
   *
   * @param dataset input dataset
   * @param paramMap parameters
   * @return fitted model
   */
  def fit(dataset: SchemaRDD, paramMap: ParamMap): M

  /**
   * Fits a single model to the input data with provided parameters.
   *
   * @param dataset input dataset
   * @param firstParamPair first parameter
   * @param otherParamPairs other parameters
   * @return fitted model
   */
  def fit(
      dataset: SchemaRDD,
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): M = {
    val map = new ParamMap()
    map.put(firstParamPair)
    otherParamPairs.foreach(map.put(_))
    fit(dataset, map)
  }

  /**
   * Fits multiple models to the input data with multiple sets of parameters.
   * The default implementation uses a for loop on each parameter map.
   * Subclasses could overwrite this to optimize multi-model training.
   *
   * @param dataset input dataset
   * @param paramMaps an array of parameter maps
   * @return fitted models, matching the input parameter maps
   */
  def fit(dataset: SchemaRDD, paramMaps: Array[ParamMap]): Seq[M] = {
    paramMaps.map(fit(dataset, _))
  }

  /**
   * Parameter for the output model.
   */
  def model: Params = Params.empty
}
