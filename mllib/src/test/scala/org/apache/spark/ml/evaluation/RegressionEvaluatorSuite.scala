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

import org.scalatest.FunSuite

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.DataFrame

class RegressionEvaluatorSuite extends FunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    /**
     * Here is the instruction describing how to export the test data into CSV format
     * so we can validate the metrics compared with scikit learns regression metrics package.
     *
     * import org.apache.spark.mllib.util.LinearDataGenerator
     * val data = sc.parallelize(LinearDataGenerator.generateLinearInput(6.3,
     *   Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1))
     * data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1))
     *   .saveAsTextFile("path")
     */
    dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 100, 42, 0.1), 2))
  }

  test("Regression Evaluator: default params") {
    /**
     * Using the following python code to load the data and train the model using scikit learn.
     *
     * > from sklearn.linear_model import LinearRegression
     * > from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
     * > import pandas as pd
     * > from patsy import dmatrices
     * > df = pd.read_csv("path")
     * > y, X = dmatrices('label ~ x + y',df, return_type="dataframe")
     * > regr = LinearRegression()
     * > regr.fit(X, y)
     * > print('Mean Squared Error: %.2f' % mean_squared_error(y, regr.predict(X)))
     * > print('Mean Absolute Error: %.2f' % mean_absolute_error(y, regr.predict(X)))
     * > print('R2 score: %.2f' % r2_score(y, regr.predict(X)))
     * > Mean Squared Error: 0.01
     * > Mean Absolute Error: 0.08
     * > R2 score: 1.00
     */
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)
    val predictions = model.transform(dataset)

    // default = rmse
    val evaluator = new RegressionEvaluator()
    assert(evaluator.evaluate(predictions) ~== 0.1 relTol 0.02)

    // r2 score
    evaluator.setMetricName("r2")
    assert(evaluator.evaluate(predictions) ~== 0.01 relTol 0.002)

    // mae
    evaluator.setMetricName("mae")
    assert(evaluator.evaluate(predictions) ~== 0.08 relTol 0.01)
  }
}
