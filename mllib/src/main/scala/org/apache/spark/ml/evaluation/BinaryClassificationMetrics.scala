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
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Dataset, Row}

/**
 * :: Experimental ::
 * Metrics for binary classification[0, 1], which expects two input columns: rawPrediction
 * and label. The rawPrediction column can be of type double (binary 0/1 prediction, or
 * probability of label 1) or of type vector (length-2 vector of raw predictions, scores,
 * or label probabilities).
 */
@Since("2.3.0")
@Experimental
class BinaryClassificationMetrics private[spark] (dataset: Dataset[_]) {

  private val mllibMetrics = {
    val rdd = dataset.rdd.map {
      case Row(rawPrediction: Vector, label: Double) => (rawPrediction(1), label)
      case Row(rawPrediction: Double, label: Double) => (rawPrediction, label)
    }
    new org.apache.spark.mllib.evaluation.BinaryClassificationMetrics(rdd)
  }

  /**
   * Computes the area under the receiver operating characteristic (ROC) curve.
   */
  @Since("2.3.0")
  lazy val areaUnderROC: Double = mllibMetrics.areaUnderROC()

  /**
   * Computes the area under the precision-recall curve.
   */
  @Since("2.3.0")
  lazy val areaUnderPR: Double = mllibMetrics.areaUnderPR()

  /**
   * Unpersist intermediate RDDs used in the computation.
   */
  @Since("2.3.0")
  def unpersist() {
    mllibMetrics.unpersist()
  }

}
