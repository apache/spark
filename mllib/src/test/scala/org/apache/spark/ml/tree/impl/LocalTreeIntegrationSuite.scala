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

package org.apache.spark.ml.tree.impl

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.mllib.tree.DecisionTreeSuite
import org.apache.spark.mllib.util.{LogisticRegressionDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.DataFrame

/** Tests checking equivalence of trees produced by local and distributed tree training. */
class LocalTreeIntegrationSuite extends SparkFunSuite with MLlibTestSparkContext {

  val medDepthTreeSettings = TreeTests.allParamSettings ++ Map[String, Any]("maxDepth" -> 4)

  /**
   * For each (paramName, paramVal) pair in the passed-in map, set the corresponding
   * parameter of the passed-in estimator & return the estimator.
   */
  private def setParams[E <: Estimator[_]](estimator: E, params: Map[String, Any]): E = {
    params.foreach { case (p, v) =>
      estimator.set(estimator.getParam(p), v)
    }
    estimator
  }

  /**
   * Verifies that local tree training & distributed training produce the same tree
   * when fit on the same dataset with the same set of params.
   */
  private def testEquivalence(train: DataFrame, testParams: Map[String, Any]): Unit = {
    val distribTree = setParams(new DecisionTreeRegressor(), testParams)
    val localTree = setParams(new LocalDecisionTreeRegressor(), testParams)
    val model = distribTree.fit(train)
    val localModel = localTree.fit(train)
    TreeTests.checkEqual(localModel, model)
  }

  test("Local & distributed training produce the same tree on a toy dataset") {
    val data = sc.parallelize(Range(0, 8).map(x => LabeledPoint(x, Vectors.dense(x))))
    val df = spark.createDataFrame(data)
    testEquivalence(df, TreeTests.allParamSettings)
  }

  test("Local & distributed training produce the same tree on a larger toy dataset") {
    val data = sc.parallelize(Range(0, 64).map(x => LabeledPoint(x, Vectors.dense(x))))
    val df = spark.createDataFrame(data)
    testEquivalence(df, medDepthTreeSettings)
  }

  test("Local & distributed training produce same tree on a dataset of categorical features") {
    val data = sc.parallelize(DecisionTreeSuite.generateCategoricalDataPoints().map(_.asML))
    // Create a map of categorical feature index to arity; each feature has arity nclasses
    val featuresMap: Map[Int, Int] = Map(0 -> 3, 1 -> 3)
    // Convert the data RDD to a DataFrame with metadata indicating the arity of each of its
    // categorical features
    val df = TreeTests.setMetadata(data, featuresMap, numClasses = 2)
    testEquivalence(df, TreeTests.allParamSettings)
  }

  test("Local & distributed training produce the same tree on a dataset of continuous features") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    // Use maxDepth = 5 and default params
    val params = medDepthTreeSettings
    val data = LogisticRegressionDataGenerator.generateLogisticRDD(spark.sparkContext,
      nexamples = 1000, nfeatures = 5, eps = 2.0, nparts = 1, probOne = 0.2)
      .map(_.asML).toDF().cache()
    testEquivalence(data, params)
  }

  test("Local & distributed training produce the same tree on a dataset of constant features") {
    // Generate constant, continuous data
    val data = sc.parallelize(Range(0, 8).map(_ => LabeledPoint(1, Vectors.dense(1))))
    val df = spark.createDataFrame(data)
    testEquivalence(df, TreeTests.allParamSettings)
  }

  // TODO(smurching): Probably remove this (since it depends on user env). Currently fails, partly
  // because collecting data for local training is slow but also because local training is
  // slightly slower than distributed training.
//  test("Local tree training is faster than distributed training on a medium-sized dataset") {
//    val sqlContext = spark.sqlContext
//    import sqlContext.implicits._
//    val df = LogisticRegressionDataGenerator.generateLogisticRDD(spark.sparkContext,
//      nexamples = 100000, nfeatures = 5, eps = 2.0, nparts = 1, probOne = 0.2)
//      .map(_.asML).toDF().cache()
//
//    val timer = new TimeTracker()
//
//    timer.start("local")
//    val localTree = setParams(new LocalDecisionTreeRegressor(), TreeTests.allParamSettings)
//    localTree.fit(df)
//    val localTrainTime = timer.stop("local")
//
//    timer.start("distributed")
//    val distribTree = setParams(new DecisionTreeRegressor(), TreeTests.allParamSettings)
//    distribTree.fit(df)
//    val distribTrainTime = timer.stop("distributed")
//
//    assert(localTrainTime < distribTrainTime, s"Local tree training time ($localTrainTime) " +
//      s"should be less than distributed tree training time ($distribTrainTime).")
//  }


}
