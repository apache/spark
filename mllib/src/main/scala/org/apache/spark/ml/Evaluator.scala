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

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame

/**
 * :: AlphaComponent ::
 * Abstract class for evaluators that compute metrics from predictions.
 */
@AlphaComponent
abstract class Evaluator extends Identifiable {

  /**
   * Evaluates the output.
   *
   * @param dataset a dataset that contains labels/observations and predictions.
   * @param paramMap parameter map that specifies the input columns and output metrics
   * @return metric
   */
  def evaluate(dataset: DataFrame, paramMap: ParamMap): Double
}
