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
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.rdd.RDD

/**
 * :: AlphaComponent ::
 * Evaluator for single-label multiclass classification,
 * which expects two input columns: prediction and label.
 */
@AlphaComponent
class ClassificationEvaluator extends PredictionEvaluator {

  /** param for metric name in evaluation */
  val metricName: Param[String] = new Param(this, "metricName",
    "metric name in evaluation (accuracy)", Some("accuracy"))
  def getMetricName: String = get(metricName)
  def setMetricName(value: String): this.type = set(metricName, value)

  protected override def evaluateImpl(predictionsAndLabels: RDD[(Double, Double)]): Double = {
    val map = this.paramMap ++ paramMap
    ClassificationEvaluator.computeMetric(predictionsAndLabels, map(metricName))
  }
}

private[ml] object ClassificationEvaluator {

  def computeMetric(predictionsAndLabels: RDD[(Double, Double)], metricName: String): Double = {
    val metrics = new MulticlassMetrics(predictionsAndLabels)
    metricName match {
      case "accuracy" =>
        metrics.precision
      case other =>
        throw new IllegalArgumentException(s"Does not support metric $other.")
    }
  }
}
