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
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._

class RankingEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  test("params") {
    ParamsSuite.checkParams(new RankingEvaluator)
  }

  test("Ranking Evaluator: default params") {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val predictionAndLabels = sqlContext.createDataFrame(sc.parallelize(
      Seq(
        (Array[Int](1, 6, 2, 7, 8, 3, 9, 10, 4, 5), Array[Int](1, 2, 3, 4, 5)),
        (Array[Int](4, 1, 5, 6, 2, 7, 3, 8, 9, 10), Array[Int](1, 2, 3)),
        (Array[Int](1, 2, 3, 4, 5), Array[Int]())
      ), 2)).toDF(Seq("prediction", "label"): _*)

    // default = map, k = 1
    val evaluator = new RankingEvaluator()
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.355026 absTol 0.01)

    // mapk, k = 5
    evaluator.setMetricName("mapk").setK(5)
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.8/3 absTol 0.01)

    // ndcg, k = 5
    evaluator.setMetricName("ndcg")
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.328788 absTol 0.01)

    // mrr
    evaluator.setMetricName("mrr")
    assert(evaluator.evaluate(predictionAndLabels) ~== 0.5 absTol 0.01)
  }
}
