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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame


class ClusteringEvaluatorSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  @transient var irisDataset: DataFrame = _
  @transient var newIrisDataset: DataFrame = _
  @transient var newIrisDatasetD: DataFrame = _
  @transient var newIrisDatasetF: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    irisDataset = spark.read.format("libsvm").load("../data/mllib/iris_libsvm.txt")
    val datasets = MLTestingUtils.generateArrayFeatureDataset(irisDataset)
    newIrisDataset = datasets._1
    newIrisDatasetD = datasets._2
    newIrisDatasetF = datasets._3
  }

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
    val evaluator = new ClusteringEvaluator()
        .setFeaturesCol("features")
        .setPredictionCol("label")

    assert(evaluator.evaluate(irisDataset) ~== 0.6564679231 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDataset) ~== 0.6564679231 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDatasetD) ~== 0.6564679231 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDatasetF) ~== 0.6564679231 relTol 1e-5)
  }

  /*
    Use the following python code to load the data and evaluate it using scikit-learn package.

    from sklearn import datasets
    from sklearn.metrics import silhouette_score
    iris = datasets.load_iris()
    round(silhouette_score(iris.data, iris.target, metric='cosine'), 10)

    0.7222369298
  */
  test("cosine Silhouette") {
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("features")
      .setPredictionCol("label")
      .setDistanceMeasure("cosine")

    assert(evaluator.evaluate(irisDataset) ~== 0.7222369298 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDataset) ~== 0.7222369298 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDatasetD) ~== 0.7222369298 relTol 1e-5)
    assert(evaluator.evaluate(newIrisDatasetF) ~== 0.7222369298 relTol 1e-5)
  }

  test("number of clusters must be greater than one") {
    val singleClusterDataset = irisDataset.where($"label" === 0.0)
    Seq("squaredEuclidean", "cosine").foreach { distanceMeasure =>
      val evaluator = new ClusteringEvaluator()
        .setFeaturesCol("features")
        .setPredictionCol("label")
        .setDistanceMeasure(distanceMeasure)

      val e = intercept[AssertionError] {
        evaluator.evaluate(singleClusterDataset)
      }
      assert(e.getMessage.contains("Number of clusters must be greater than one"))
    }
  }

  test("SPARK-23568: we should use metadata to determine features number") {
    val attributesNum = irisDataset.select("features").rdd.first().getAs[Vector](0).size
    val attrGroup = new AttributeGroup("features", attributesNum)
    val df = irisDataset.select($"features".as("features", attrGroup.toMetadata()), $"label")
    require(AttributeGroup.fromStructField(df.schema("features"))
      .numAttributes.isDefined, "numAttributes metadata should be defined")
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("features")
      .setPredictionCol("label")

    // with the proper metadata we compute correctly the result
    assert(evaluator.evaluate(df) ~== 0.6564679231 relTol 1e-5)

    val wrongAttrGroup = new AttributeGroup("features", attributesNum + 1)
    val dfWrong = irisDataset.select($"features".as("features", wrongAttrGroup.toMetadata()),
      $"label")
    // with wrong metadata the evaluator throws an Exception
    intercept[SparkException](evaluator.evaluate(dfWrong))
  }
}
