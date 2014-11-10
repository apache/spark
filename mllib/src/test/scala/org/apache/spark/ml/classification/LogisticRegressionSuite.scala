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

package org.apache.spark.ml.classification

import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.test.TestSQLContext._

class LogisticRegressionSuite extends FunSuite with BeforeAndAfterAll {

  var dataset: SchemaRDD = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = MLUtils.loadLibSVMFile(
      sparkContext, "../data/mllib/sample_binary_classification_data.txt")
    dataset.cache()
  }

  override def afterAll(): Unit = {
    dataset.unpersist()
    dataset = null
    super.afterAll()
  }

  test("logistic regression") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset)
    model.transform(dataset)
      .select('label, 'prediction)
      .collect()
      .foreach(println)
  }

  test("logistic regression with setters") {
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
    val model = lr.fit(dataset)
    model.transform(dataset, model.threshold -> 0.8) // overwrite threshold
      .select('label, 'score, 'prediction).collect()
      .foreach(println)
  }

  test("logistic regression fit and transform with varargs") {
    val lr = new LogisticRegression
    val model = lr.fit(dataset, lr.maxIter -> 10, lr.regParam -> 1.0)
    model.transform(dataset, model.threshold -> 0.8, model.scoreCol -> "probability")
      .select('label, 'probability, 'prediction)
      .foreach(println)
  }
}
