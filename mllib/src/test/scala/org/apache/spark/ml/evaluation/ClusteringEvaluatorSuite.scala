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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}


class ClusteringEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  val dataset = Seq(Row(Vectors.dense(5.1, 3.5, 1.4, 0.2), 0),
      Row(Vectors.dense(4.9, 3.0, 1.4, 0.2), 0),
      Row(Vectors.dense(4.7, 3.2, 1.3, 0.2), 0),
      Row(Vectors.dense(4.6, 3.1, 1.5, 0.2), 0),
      Row(Vectors.dense(5.0, 3.6, 1.4, 0.2), 0),
      Row(Vectors.dense(5.4, 3.9, 1.7, 0.4), 0),
      Row(Vectors.dense(4.6, 3.4, 1.4, 0.3), 0),
      Row(Vectors.dense(5.0, 3.4, 1.5, 0.2), 0),
      Row(Vectors.dense(4.4, 2.9, 1.4, 0.2), 0),
      Row(Vectors.dense(4.9, 3.1, 1.5, 0.1), 0),
      Row(Vectors.dense(5.4, 3.7, 1.5, 0.2), 0),
      Row(Vectors.dense(4.8, 3.4, 1.6, 0.2), 0),
      Row(Vectors.dense(4.8, 3.0, 1.4, 0.1), 0),
      Row(Vectors.dense(4.3, 3.0, 1.1, 0.1), 0),
      Row(Vectors.dense(5.8, 4.0, 1.2, 0.2), 0),
      Row(Vectors.dense(5.7, 4.4, 1.5, 0.4), 0),
      Row(Vectors.dense(5.4, 3.9, 1.3, 0.4), 0),
      Row(Vectors.dense(5.1, 3.5, 1.4, 0.3), 0),
      Row(Vectors.dense(5.7, 3.8, 1.7, 0.3), 0),
      Row(Vectors.dense(5.1, 3.8, 1.5, 0.3), 0),
      Row(Vectors.dense(5.4, 3.4, 1.7, 0.2), 0),
      Row(Vectors.dense(5.1, 3.7, 1.5, 0.4), 0),
      Row(Vectors.dense(4.6, 3.6, 1.0, 0.2), 0),
      Row(Vectors.dense(5.1, 3.3, 1.7, 0.5), 0),
      Row(Vectors.dense(4.8, 3.4, 1.9, 0.2), 0),
      Row(Vectors.dense(5.0, 3.0, 1.6, 0.2), 0),
      Row(Vectors.dense(5.0, 3.4, 1.6, 0.4), 0),
      Row(Vectors.dense(5.2, 3.5, 1.5, 0.2), 0),
      Row(Vectors.dense(5.2, 3.4, 1.4, 0.2), 0),
      Row(Vectors.dense(4.7, 3.2, 1.6, 0.2), 0),
      Row(Vectors.dense(4.8, 3.1, 1.6, 0.2), 0),
      Row(Vectors.dense(5.4, 3.4, 1.5, 0.4), 0),
      Row(Vectors.dense(5.2, 4.1, 1.5, 0.1), 0),
      Row(Vectors.dense(5.5, 4.2, 1.4, 0.2), 0),
      Row(Vectors.dense(4.9, 3.1, 1.5, 0.1), 0),
      Row(Vectors.dense(5.0, 3.2, 1.2, 0.2), 0),
      Row(Vectors.dense(5.5, 3.5, 1.3, 0.2), 0),
      Row(Vectors.dense(4.9, 3.1, 1.5, 0.1), 0),
      Row(Vectors.dense(4.4, 3.0, 1.3, 0.2), 0),
      Row(Vectors.dense(5.1, 3.4, 1.5, 0.2), 0),
      Row(Vectors.dense(5.0, 3.5, 1.3, 0.3), 0),
      Row(Vectors.dense(4.5, 2.3, 1.3, 0.3), 0),
      Row(Vectors.dense(4.4, 3.2, 1.3, 0.2), 0),
      Row(Vectors.dense(5.0, 3.5, 1.6, 0.6), 0),
      Row(Vectors.dense(5.1, 3.8, 1.9, 0.4), 0),
      Row(Vectors.dense(4.8, 3.0, 1.4, 0.3), 0),
      Row(Vectors.dense(5.1, 3.8, 1.6, 0.2), 0),
      Row(Vectors.dense(4.6, 3.2, 1.4, 0.2), 0),
      Row(Vectors.dense(5.3, 3.7, 1.5, 0.2), 0),
      Row(Vectors.dense(5.0, 3.3, 1.4, 0.2), 0),
      Row(Vectors.dense(7.0, 3.2, 4.7, 1.4), 1),
      Row(Vectors.dense(6.4, 3.2, 4.5, 1.5), 1),
      Row(Vectors.dense(6.9, 3.1, 4.9, 1.5), 1),
      Row(Vectors.dense(5.5, 2.3, 4.0, 1.3), 1),
      Row(Vectors.dense(6.5, 2.8, 4.6, 1.5), 1),
      Row(Vectors.dense(5.7, 2.8, 4.5, 1.3), 1),
      Row(Vectors.dense(6.3, 3.3, 4.7, 1.6), 1),
      Row(Vectors.dense(4.9, 2.4, 3.3, 1.0), 1),
      Row(Vectors.dense(6.6, 2.9, 4.6, 1.3), 1),
      Row(Vectors.dense(5.2, 2.7, 3.9, 1.4), 1),
      Row(Vectors.dense(5.0, 2.0, 3.5, 1.0), 1),
      Row(Vectors.dense(5.9, 3.0, 4.2, 1.5), 1),
      Row(Vectors.dense(6.0, 2.2, 4.0, 1.0), 1),
      Row(Vectors.dense(6.1, 2.9, 4.7, 1.4), 1),
      Row(Vectors.dense(5.6, 2.9, 3.6, 1.3), 1),
      Row(Vectors.dense(6.7, 3.1, 4.4, 1.4), 1),
      Row(Vectors.dense(5.6, 3.0, 4.5, 1.5), 1),
      Row(Vectors.dense(5.8, 2.7, 4.1, 1.0), 1),
      Row(Vectors.dense(6.2, 2.2, 4.5, 1.5), 1),
      Row(Vectors.dense(5.6, 2.5, 3.9, 1.1), 1),
      Row(Vectors.dense(5.9, 3.2, 4.8, 1.8), 1),
      Row(Vectors.dense(6.1, 2.8, 4.0, 1.3), 1),
      Row(Vectors.dense(6.3, 2.5, 4.9, 1.5), 1),
      Row(Vectors.dense(6.1, 2.8, 4.7, 1.2), 1),
      Row(Vectors.dense(6.4, 2.9, 4.3, 1.3), 1),
      Row(Vectors.dense(6.6, 3.0, 4.4, 1.4), 1),
      Row(Vectors.dense(6.8, 2.8, 4.8, 1.4), 1),
      Row(Vectors.dense(6.7, 3.0, 5.0, 1.7), 1),
      Row(Vectors.dense(6.0, 2.9, 4.5, 1.5), 1),
      Row(Vectors.dense(5.7, 2.6, 3.5, 1.0), 1),
      Row(Vectors.dense(5.5, 2.4, 3.8, 1.1), 1),
      Row(Vectors.dense(5.5, 2.4, 3.7, 1.0), 1),
      Row(Vectors.dense(5.8, 2.7, 3.9, 1.2), 1),
      Row(Vectors.dense(6.0, 2.7, 5.1, 1.6), 1),
      Row(Vectors.dense(5.4, 3.0, 4.5, 1.5), 1),
      Row(Vectors.dense(6.0, 3.4, 4.5, 1.6), 1),
      Row(Vectors.dense(6.7, 3.1, 4.7, 1.5), 1),
      Row(Vectors.dense(6.3, 2.3, 4.4, 1.3), 1),
      Row(Vectors.dense(5.6, 3.0, 4.1, 1.3), 1),
      Row(Vectors.dense(5.5, 2.5, 4.0, 1.3), 1),
      Row(Vectors.dense(5.5, 2.6, 4.4, 1.2), 1),
      Row(Vectors.dense(6.1, 3.0, 4.6, 1.4), 1),
      Row(Vectors.dense(5.8, 2.6, 4.0, 1.2), 1),
      Row(Vectors.dense(5.0, 2.3, 3.3, 1.0), 1),
      Row(Vectors.dense(5.6, 2.7, 4.2, 1.3), 1),
      Row(Vectors.dense(5.7, 3.0, 4.2, 1.2), 1),
      Row(Vectors.dense(5.7, 2.9, 4.2, 1.3), 1),
      Row(Vectors.dense(6.2, 2.9, 4.3, 1.3), 1),
      Row(Vectors.dense(5.1, 2.5, 3.0, 1.1), 1),
      Row(Vectors.dense(5.7, 2.8, 4.1, 1.3), 1),
      Row(Vectors.dense(6.3, 3.3, 6.0, 2.5), 2),
      Row(Vectors.dense(5.8, 2.7, 5.1, 1.9), 2),
      Row(Vectors.dense(7.1, 3.0, 5.9, 2.1), 2),
      Row(Vectors.dense(6.3, 2.9, 5.6, 1.8), 2),
      Row(Vectors.dense(6.5, 3.0, 5.8, 2.2), 2),
      Row(Vectors.dense(7.6, 3.0, 6.6, 2.1), 2),
      Row(Vectors.dense(4.9, 2.5, 4.5, 1.7), 2),
      Row(Vectors.dense(7.3, 2.9, 6.3, 1.8), 2),
      Row(Vectors.dense(6.7, 2.5, 5.8, 1.8), 2),
      Row(Vectors.dense(7.2, 3.6, 6.1, 2.5), 2),
      Row(Vectors.dense(6.5, 3.2, 5.1, 2.0), 2),
      Row(Vectors.dense(6.4, 2.7, 5.3, 1.9), 2),
      Row(Vectors.dense(6.8, 3.0, 5.5, 2.1), 2),
      Row(Vectors.dense(5.7, 2.5, 5.0, 2.0), 2),
      Row(Vectors.dense(5.8, 2.8, 5.1, 2.4), 2),
      Row(Vectors.dense(6.4, 3.2, 5.3, 2.3), 2),
      Row(Vectors.dense(6.5, 3.0, 5.5, 1.8), 2),
      Row(Vectors.dense(7.7, 3.8, 6.7, 2.2), 2),
      Row(Vectors.dense(7.7, 2.6, 6.9, 2.3), 2),
      Row(Vectors.dense(6.0, 2.2, 5.0, 1.5), 2),
      Row(Vectors.dense(6.9, 3.2, 5.7, 2.3), 2),
      Row(Vectors.dense(5.6, 2.8, 4.9, 2.0), 2),
      Row(Vectors.dense(7.7, 2.8, 6.7, 2.0), 2),
      Row(Vectors.dense(6.3, 2.7, 4.9, 1.8), 2),
      Row(Vectors.dense(6.7, 3.3, 5.7, 2.1), 2),
      Row(Vectors.dense(7.2, 3.2, 6.0, 1.8), 2),
      Row(Vectors.dense(6.2, 2.8, 4.8, 1.8), 2),
      Row(Vectors.dense(6.1, 3.0, 4.9, 1.8), 2),
      Row(Vectors.dense(6.4, 2.8, 5.6, 2.1), 2),
      Row(Vectors.dense(7.2, 3.0, 5.8, 1.6), 2),
      Row(Vectors.dense(7.4, 2.8, 6.1, 1.9), 2),
      Row(Vectors.dense(7.9, 3.8, 6.4, 2.0), 2),
      Row(Vectors.dense(6.4, 2.8, 5.6, 2.2), 2),
      Row(Vectors.dense(6.3, 2.8, 5.1, 1.5), 2),
      Row(Vectors.dense(6.1, 2.6, 5.6, 1.4), 2),
      Row(Vectors.dense(7.7, 3.0, 6.1, 2.3), 2),
      Row(Vectors.dense(6.3, 3.4, 5.6, 2.4), 2),
      Row(Vectors.dense(6.4, 3.1, 5.5, 1.8), 2),
      Row(Vectors.dense(6.0, 3.0, 4.8, 1.8), 2),
      Row(Vectors.dense(6.9, 3.1, 5.4, 2.1), 2),
      Row(Vectors.dense(6.7, 3.1, 5.6, 2.4), 2),
      Row(Vectors.dense(6.9, 3.1, 5.1, 2.3), 2),
      Row(Vectors.dense(5.8, 2.7, 5.1, 1.9), 2),
      Row(Vectors.dense(6.8, 3.2, 5.9, 2.3), 2),
      Row(Vectors.dense(6.7, 3.3, 5.7, 2.5), 2),
      Row(Vectors.dense(6.7, 3.0, 5.2, 2.3), 2),
      Row(Vectors.dense(6.3, 2.5, 5.0, 1.9), 2),
      Row(Vectors.dense(6.5, 3.0, 5.2, 2.0), 2),
      Row(Vectors.dense(6.2, 3.4, 5.4, 2.3), 2),
      Row(Vectors.dense(5.9, 3.0, 5.1, 1.8), 2))

  val dsStruct = StructType( Seq(
    StructField("point", new VectorUDT, nullable = false),
    StructField("label", IntegerType, nullable = false)
  ))

  test("params") {
    ParamsSuite.checkParams(new RegressionEvaluator)
  }

  test("read/write") {
    val evaluator = new ClusteringEvaluator()
      .setPredictionCol("myPrediction")
      .setFeaturesCol("myLabel")
      .setMetricName("cosineSilhouette")
    testDefaultReadWrite(evaluator)
  }

  test("squared euclidean Silhouette") {
    val result = BigDecimal(0.6564679231)
    val dsRDD = spark.sparkContext.parallelize(dataset)
    val df = spark.createDataFrame(dsRDD, dsStruct)

    val evaluator = new ClusteringEvaluator()
        .setFeaturesCol("point")
        .setPredictionCol("label")
        .setMetricName("squaredSilhouette")
    val actual = BigDecimal(evaluator.evaluate(df))
      .setScale(10, BigDecimal.RoundingMode.HALF_UP)
    assertResult(result)(actual)

  }

  test("cosine Silhouette") {
    val result = BigDecimal(0.7222369298)
    val dsRDD = spark.sparkContext.parallelize(dataset)
    val df = spark.createDataFrame(dsRDD, dsStruct)

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("point")
      .setPredictionCol("label")
      .setMetricName("cosineSilhouette")
    val actual = BigDecimal(evaluator.evaluate(df))
      .setScale(10, BigDecimal.RoundingMode.HALF_UP)
    assertResult(result)(actual)

  }




}
