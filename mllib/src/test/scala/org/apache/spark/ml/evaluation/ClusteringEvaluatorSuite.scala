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
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}


private[ml] case class ClusteringEvaluationTestData(features: Vector, label: Int)

class ClusteringEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  test("params") {
    ParamsSuite.checkParams(new ClusteringEvaluator)
  }

  test("read/write") {
    val evaluator = new ClusteringEvaluator()
      .setPredictionCol("myPrediction")
      .setFeaturesCol("myLabel")
    testDefaultReadWrite(evaluator)
  }

  /*
    Use the following python code to load the data and evaluate it using scikit-learn package.

    from sklearn import datasets
    from sklearn.metrics import silhouette_score
    iris = datasets.load_iris()
    round(silhouette_score(iris.data, iris.target, metric='sqeuclidean'), 10)

    0.6564679231
  */
  test("squared euclidean Silhouette") {
    val result = BigDecimal(0.6564679231)
    val iris = ClusteringEvaluatorSuite.irisDataset(spark)
    val evaluator = new ClusteringEvaluator()
        .setFeaturesCol("features")
        .setPredictionCol("label")
    val actual = BigDecimal(evaluator.evaluate(iris))
      .setScale(10, BigDecimal.RoundingMode.HALF_UP)

    assertResult(result)(actual)
  }

}

object ClusteringEvaluatorSuite {
  def irisDataset(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val irisCsvPath = Thread.currentThread()
      .getContextClassLoader
      .getResource("test-data/iris.csv")
      .toString

    spark.sparkContext
      .textFile(irisCsvPath)
      .map {
        line =>
          val splits = line.split(",")
          ClusteringEvaluationTestData(
            Vectors.dense(splits.take(splits.length-1).map(_.toDouble)),
            splits(splits.length-1).toInt
          )
      }
      .toDF()
  }
}
