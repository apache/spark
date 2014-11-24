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

package org.apache.spark.ml.evaluation

import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.evaluation.impl.PredictionEvaluator
import org.apache.spark.ml.param._
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.rdd.RDD

/**
 * :: AlphaComponent ::
 * Evaluator for single-label regression,
 * which expects two input columns: prediction and label.
 */
@AlphaComponent
class RegressionEvaluator extends PredictionEvaluator {

  /** param for metric name in evaluation */
  val metricName: Param[String] = new Param(this, "metricName",
    "metric name in evaluation (RMSE)", Some("RMSE"))
  def getMetricName: String = get(metricName)
  def setMetricName(value: String): this.type = set(metricName, value)

  protected override def evaluateImpl(predictionsAndLabels: RDD[(Double, Double)]): Double = {
    val map = this.paramMap ++ paramMap
    RegressionEvaluator.computeMetric(predictionsAndLabels, map(metricName))
  }
}

private[ml] object RegressionEvaluator {

  def computeMetric(predictionsAndLabels: RDD[(Double, Double)], metricName: String): Double = {
    val metrics = new RegressionMetrics(predictionsAndLabels)
    metricName match {
      case "RMSE" =>
        metrics.rootMeanSquaredError
      case other =>
        throw new IllegalArgumentException(s"Does not support metric $other.")
    }
  }
}
