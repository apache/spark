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
package org.apache.spark.mllib.api.python

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * A Wrapper of BinaryClassificationMetrics to provide helper method for Python
 */
private[python] class BinaryClassificationMetricsWrapper(
    scoreAndLabels: DataFrame
  ) extends BinaryClassificationMetrics(scoreAndLabels) {

  def wrappedRoc(): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(roc().asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedPr(): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(pr().asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedFMeasureByThreshold(beta: Double): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(fMeasureByThreshold(beta).asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedPrecisionByThreshold(): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(precisionByThreshold().asInstanceOf[RDD[(Any, Any)]])
  }

  def wrappedRecallByThreshold(): RDD[Array[Any]] = {
    SerDe.fromTuple2RDD(recallByThreshold().asInstanceOf[RDD[(Any, Any)]])
  }

}
