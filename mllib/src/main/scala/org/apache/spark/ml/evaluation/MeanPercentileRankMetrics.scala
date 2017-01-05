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

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DoubleType

@Since("2.2.0")
class MeanPercentileRankMetrics (
  predictionAndObservations: DataFrame, predictionCol: String, labelCol: String)
  extends Logging {

  def meanPercentileRank: Double = {

    def rank_ui = udf((recs: Seq[Long], item: Long) => {
      val l_i = recs.indexOf(item)

      if (l_i == -1) {
        1
      } else {
        l_i.toDouble / recs.size
      }
    }, DoubleType)

    val R_prime = predictionAndObservations.count()
    val predictionColumn: Column = predictionAndObservations.col(predictionCol)
    val labelColumn: Column = predictionAndObservations.col(labelCol)

    val rankSum: Double = predictionAndObservations
      .withColumn("rank_ui", rank_ui(predictionColumn, labelColumn))
      .agg(sum("rank_ui")).first().getDouble(0)

    rankSum / R_prime
  }
}
