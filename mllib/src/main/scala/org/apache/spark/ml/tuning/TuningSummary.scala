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
package org.apache.spark.ml.tuning

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * :: Experimental ::
 * Summary of grid search tuning.
 *
 * @param params  estimator param maps
 * @param metrics  Corresponding evaluation metrics for the param maps
 */
@Since("2.3.0")
@Experimental
private[tuning] class TuningSummary private[tuning](
    val params: Array[ParamMap],
    val metrics: Array[Double],
    val bestIndex: Int) {

  /**
   * Summary of grid search tuning in the format of DataFrame. Each row contains one candidate
   * paramMap and its corresponding metrics.
   */
  def trainingMetrics: DataFrame = {
    require(params.nonEmpty, "estimator param maps should not be empty")
    require(params.length == metrics.length, "estimator param maps number should match metrics")
    val spark = SparkSession.builder().getOrCreate()
    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    val fields = params(0).toSeq.sortBy(_.param.name).map(_.param.name) ++ Seq("metrics")
    val schema = new StructType(fields.map(name => StructField(name, StringType)).toArray)
    val rows = sc.parallelize(params.zip(metrics)).map { case (param, metric) =>
      val values = param.toSeq.sortBy(_.param.name).map(_.value.toString) ++ Seq(metric.toString)
      Row.fromSeq(values)
    }
    sqlContext.createDataFrame(rows, schema)
  }
}

