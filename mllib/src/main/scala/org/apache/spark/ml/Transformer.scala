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

import org.apache.spark.sql.SchemaRDD

/**
 * Abstract class for transformers that transform one dataset into another.
 */
abstract class Transformer extends Identifiable with Params with PipelineStage {

  /**
   * Transforms the dataset with the default parameters.
   * @param dataset input dataset
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD): SchemaRDD = {
    transform(dataset, ParamMap.empty)
  }

  /**
   * Transforms the dataset with provided parameter map.
   * @param dataset input dataset
   * @param paramMap parameters
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD

  /**
   * Transforms the dataset with provided parameter pairs.
   * @param dataset input dataset
   * @param firstParamPair first parameter pair
   * @param otherParamPairs second parameter pair
   * @return transformed dataset
   */
  def transform(
      dataset: SchemaRDD,
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]*): SchemaRDD = {
    val map = new ParamMap()
    map.put(firstParamPair)
    otherParamPairs.foreach(map.put(_))
    transform(dataset, map)
  }

  /**
   * Transforms the dataset with multiple sets of parameters.
   * @param dataset input dataset
   * @param paramMaps an array of parameter maps
   * @return transformed dataset
   */
  def transform(dataset: SchemaRDD, paramMaps: Array[ParamMap]): Array[SchemaRDD] = {
    paramMaps.map(transform(dataset, _))
  }
}
