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

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.sql.{Dataset, Row}

/**
 * :: Experimental ::
 * Metrics for multiclass classification, which expects two input columns: prediction and label.
 */
@Since("2.3.0")
@Experimental
class MultiClassClassificationMetrics private[spark](dataset: Dataset[_]) {

  private val mllibMetrics = {
    val rdd = dataset.toDF().rdd.map {
      case Row(prediction: Double, label: Double) => (prediction, label)
    }
    new org.apache.spark.mllib.evaluation.MulticlassMetrics(rdd)
  }

  /**
   * Returns accuracy
   * (equals to the total number of correctly classified instances
   * out of the total number of instances.)
   */
  @Since("2.3.0")
  lazy val accuracy: Double = mllibMetrics.accuracy

  /**
   * Returns weighted averaged f1-measure.
   */
  @Since("2.3.0")
  lazy val weightedFMeasure: Double = mllibMetrics.weightedFMeasure

  /**
   * Returns weighted averaged precision.
   */
  @Since("2.3.0")
  lazy val weightedPrecision: Double = mllibMetrics.weightedPrecision

  /**
   * Returns weighted averaged recall.
   */
  @Since("2.3.0")
  lazy val weightedRecall: Double = mllibMetrics.weightedRecall

}
