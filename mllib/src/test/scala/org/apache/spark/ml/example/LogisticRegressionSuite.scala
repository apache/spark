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

package org.apache.spark.ml.example

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.param.ParamGridBuilder
import org.scalatest.FunSuite

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.test.TestSQLContext._

class LogisticRegressionSuite extends FunSuite {

  val dataset = MLUtils
    .loadLibSVMFile(sparkContext, "../data/mllib/sample_binary_classification_data.txt")
    .cache()

  test("logistic regression alone") {
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
    val model = lr.fit(dataset)
    model.transform(dataset, lr.model.threshold -> 0.8) // overwrite threshold
      .select('label, 'score, 'prediction).collect()
      .foreach(println)
  }

  test("logistic regression fit with varargs") {
    val lr = new LogisticRegression
    lr.fit(dataset, lr.maxIter -> 10, lr.regParam -> 1.0)
  }

  test("logistic regression with cross validation") {
    val lr = new LogisticRegression
    val lrParamMaps = new ParamGridBuilder()
      .addMulti(lr.regParam, Array(0.1, 100.0))
      .addMulti(lr.maxIter, Array(0, 5))
      .build()
    val eval = new BinaryClassificationEvaluator
    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(lrParamMaps)
      .setEvaluator(eval)
      .setNumFolds(3)
    val bestModel = cv.fit(dataset)
  }

  test("logistic regression with pipeline") {
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
    val lr = new LogisticRegression()
      .setFeaturesCol("scaledFeatures")
    val pipeline = new Pipeline()
      .setStages(Array(scaler, lr))
    val model = pipeline.fit(dataset)
    val predictions = model.transform(dataset)
      .select('label, 'score, 'prediction)
      .collect()
      .foreach(println)
  }
}
