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

package org.apache.spark.ml.regression

import org.scalatest.FunSuite

import org.apache.spark.mllib.classification.LogisticRegressionSuite.generateLogisticInput
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{Row, SQLContext, SchemaRDD}

class LinearRegressionSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: SchemaRDD = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataset = sqlContext.createSchemaRDD(
      sc.parallelize(generateLogisticInput(1.0, 1.0, nPoints = 100, seed = 42), 2))
  }

  test("linear regression: default params") {
    val sqlContext = this.sqlContext
    import sqlContext._
    val lr = new LinearRegression
    assert(lr.getLabelCol == "label")
    val model = lr.fit(dataset)
    model.transform(dataset)
      .select('label, 'prediction)
      .collect()
    // Check defaults
    assert(model.getFeaturesCol == "features")
    assert(model.getPredictionCol == "prediction")
  }

  test("linear regression with setters") {
    // Set params, train, and check as many as we can.
    val sqlContext = this.sqlContext
    import sqlContext._
    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
    val model = lr.fit(dataset)
    assert(model.fittingParamMap.get(lr.maxIter).get === 10)
    assert(model.fittingParamMap.get(lr.regParam).get === 1.0)

    // Call fit() with new params, and check as many as we can.
    val model2 = lr.fit(dataset, lr.maxIter -> 5, lr.regParam -> 0.1, lr.predictionCol -> "thePred")
    assert(model2.fittingParamMap.get(lr.maxIter).get === 5)
    assert(model2.fittingParamMap.get(lr.regParam).get === 0.1)
    assert(model2.getPredictionCol == "thePred")
  }

  test("linear regression: Predictor, Regressor methods") {
    val sqlContext = this.sqlContext
    import sqlContext._
    val lr = new LinearRegression

    // fit() vs. train()
    val model1 = lr.fit(dataset)
    val rdd = dataset.select('label, 'features).map { case Row(label: Double, features: Vector) =>
      LabeledPoint(label, features)
    }
    val features = rdd.map(_.features)
    val model2 = lr.train(rdd)
    assert(model1.intercept == model2.intercept)
    assert(model1.weights.equals(model2.weights))

    // transform() vs. predict()
    val trans = model1.transform(dataset).select('prediction)
    val preds = model1.predict(rdd.map(_.features))
    trans.zip(preds).collect().foreach { case (Row(pred1: Double), pred2: Double) =>
      assert(pred1 == pred2)
    }
  }
}
