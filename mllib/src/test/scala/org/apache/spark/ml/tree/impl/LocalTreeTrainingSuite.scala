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
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.tree.InternalNode
import org.apache.spark.ml.util.DefaultReadWriteTest
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.DataFrame

/** Tests for equivalence/performance of local tree training vs distributed training. */
class LocalTreeTrainingSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  /**
   * For each (paramName, paramVal) pair in the passed-in map, set the corresponding
   * parameter of the passed-in estimator.
   * @return The passed-in estimator
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
    val localTree = setParams(new LocalDecisionTreeRegressor(), testParams)
    val sparkTree = setParams(new DecisionTreeRegressor(), testParams)

    val localModel = localTree.fit(train)
    val model = sparkTree.fit(train)
    TreeTests.checkEqual(localModel, model)
  }

  test("Fit a single decision tree regressor on some continuous features") {
    val data = sc.parallelize(Range(0, 8).map(x => LabeledPoint(x, Vectors.dense(x))))
    val df = spark.sqlContext.createDataFrame(data)

    val dt = new LocalDecisionTreeRegressor()
      .setFeaturesCol("features") // indexedFeatures
      .setLabelCol("label")
      .setMaxDepth(10)
    val model = dt.fit(df)
    assert(model.rootNode.isInstanceOf[InternalNode])
    val root = model.rootNode.asInstanceOf[InternalNode]
    assert(root.leftChild.isInstanceOf[InternalNode] && root.rightChild.isInstanceOf[InternalNode])
    val left = root.leftChild.asInstanceOf[InternalNode]
    val right = root.rightChild.asInstanceOf[InternalNode]
    val grandkids = Array(left.leftChild, left.rightChild, right.leftChild, right.rightChild)
    assert(grandkids.forall(_.isInstanceOf[InternalNode]))
  }

  test("Local & distributed training produce the same tree on a toy dataset") {
    val data = sc.parallelize(Range(0, 8).map(x => LabeledPoint(x, Vectors.dense(x))))
    val df = spark.sqlContext.createDataFrame(data)
    testEquivalence(df, TreeTests.allParamSettings)
  }

  test("Local & distributed training produce the same tree on a dataset of categorical features") {
    val (train, test) = TreeTests.buildCategoricalData(seed = 42, sqlContext = spark.sqlContext,
      nexamples = 1000, nfeatures = 5, nclasses = 4, trainFraction = 1.0)
    train.cache()
    testEquivalence(train, TreeTests.allParamSettings)
  }

  test("Local & distributed training produce the same tree on a dataset of continuous features") {
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    // Use maxDepth = 5 and default params
    val params = Map[String, Any]("maxDepth" -> 5)
    testEquivalence(TreeTests.varianceData(sc).toDF(), params)
  }

}
