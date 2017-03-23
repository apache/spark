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

import org.apache.spark.SparkFunSuite

import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, Evaluator}
import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SQLContext}
import org.apache.spark.sql.types.StructType
import org.scalatest.Matchers._

class SlidingWindowCrossValidatorSuite
  extends SparkFunSuite with MLlibTestSparkContext {

  test("sliding window split") {
    val dataset = sqlContext
      .createDataFrame(Array[(Int, Long)](
        (1, 1L), (2, 3L), (3, 3L), (4, 4L), (5, 5L), (6, 10L), (7, 6L), (8, 5L)))
      .toDF("data", "index")

    val splits = SlidingWindowCrossValidator.split(
      dataset, firstCutoffIndex = 2, windowSize = 2, numFolds = 6, indexCol = "index")

    assert(splits.size == 6)

    val training0 = Row(1, 1L) :: Nil
    val validation0 = Row(2, 3L) :: Row(3, 3L) :: Nil
    QueryTest.checkAnswer(splits(0)._1, training0) shouldBe None
    QueryTest.checkAnswer(splits(0)._2, validation0) shouldBe None

    val training1 = training0 ++ validation0
    val validation1 = Row(4, 4L) :: Row(5, 5L) :: Row(8, 5L) :: Nil
    QueryTest.checkAnswer(splits(1)._1, training1) shouldBe None
    QueryTest.checkAnswer(splits(1)._2, validation1) shouldBe None

    val training2 = training1 ++ validation1
    val validation2 = Row(7, 6L) :: Nil
    QueryTest.checkAnswer(splits(2)._1, training2) shouldBe None
    QueryTest.checkAnswer(splits(2)._2, validation2) shouldBe None

    val training3 = training2 ++ validation2
    val validation3 = Nil
    QueryTest.checkAnswer(splits(3)._1, training3) shouldBe None
    QueryTest.checkAnswer(splits(3)._2, validation3) shouldBe None

    val training4 = training3 ++ validation3
    val validation4 = Row(6, 10L) :: Nil
    QueryTest.checkAnswer(splits(4)._1, training4) shouldBe None
    QueryTest.checkAnswer(splits(4)._2, validation4) shouldBe None

    val training5 = training4 ++ validation4
    val validation5 = Nil
    QueryTest.checkAnswer(splits(5)._1, training5) shouldBe None
    QueryTest.checkAnswer(splits(5)._2, validation5) shouldBe None
  }

  test("sliding cross validation with logistic regression") {
    // This test is copied from CrossValidator. The model should expose the same information.
    val rawData = generateLogisticInput(1.0, 1.0, 100, 42)
      .zipWithIndex
      .map { case (lp, idx) => (lp.label, lp.features, idx) }

    val dataset = sqlContext.createDataFrame(rawData).toDF("label", "features", "index")

    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.001, 1000.0))
      .addGrid(lr.maxIter, Array(0, 10))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new SlidingWindowCrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setFirstCutoffIndex(10)
      .setWindowSize(5)
      .setNumFolds(3)
    val cvModel = cv.fit(dataset)
    val parent = cvModel.bestModel.parent.asInstanceOf[LogisticRegression]
    assert(parent.getRegParam === 0.001)
    assert(parent.getMaxIter === 10)
  }
}
