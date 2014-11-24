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

package org.apache.spark.ml.evaluation.impl

import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DoubleType, Row, SchemaRDD}

/**
 * Evaluator for single-label prediction problems,
 * which expects two input columns: prediction and label.
 */
private[ml] abstract class PredictionEvaluator extends Evaluator with Params
  with HasPredictionCol with HasLabelCol {

  def setPredictionCol(value: String): this.type = set(predictionCol, value)
  def setLabelCol(value: String): this.type = set(labelCol, value)

  override def evaluate(dataset: SchemaRDD, paramMap: ParamMap): Double = {
    val map = this.paramMap ++ paramMap

    val schema = dataset.schema
    val predictionType = schema(map(predictionCol)).dataType
    require(predictionType == DoubleType,
      s"Prediction column ${map(predictionCol)} must be double type but found $predictionType")
    val labelType = schema(map(labelCol)).dataType
    require(labelType == DoubleType,
      s"Label column ${map(labelCol)} must be double type but found $labelType")

    import dataset.sqlContext._
    val predictionsAndLabels = dataset.select(map(predictionCol).attr, map(labelCol).attr)
      .map { case Row(prediction: Double, label: Double) =>
      (prediction, label)
    }

    evaluateImpl(predictionsAndLabels)
  }

  /**
   * Developers can implement this method for evaluators taking (prediction, label) tuples
   */
  protected def evaluateImpl(predictionsAndLabels: RDD[(Double, Double)]): Double
}
