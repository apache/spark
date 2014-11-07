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

package org.apache.spark.ml.example

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions.Row

class BinaryClassificationEvaluator extends Evaluator with Params
    with HasScoreCol with HasLabelCol with HasMetricName {

  setMetricName("areaUnderROC")

  def setMetricName(value: String): this.type = { set(metricName, value); this }
  def setScoreCol(value: String): this.type = { set(scoreCol, value); this }
  def setLabelCol(value: String): this.type = { set(labelCol, value); this }

  override def evaluate(dataset: SchemaRDD, paramMap: ParamMap): Double = {
    import dataset.sqlContext._
    val map = this.paramMap ++ paramMap
    val scoreAndLabels = dataset.select(map(scoreCol).attr, map(labelCol).attr)
      .map { case Row(score: Double, label: Double) =>
        (score, label)
      }.cache()
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    map(metricName) match {
      case "areaUnderROC" =>
        metrics.areaUnderROC()
      case "areaUnderPR" =>
        metrics.areaUnderPR()
      case other =>
        throw new IllegalArgumentException(s"Do not support metric $other.")
    }
  }
}
