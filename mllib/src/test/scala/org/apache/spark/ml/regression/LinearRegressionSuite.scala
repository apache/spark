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

import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

class LinearRegressionSuite extends FunSuite with MLlibTestSparkContext {

  @transient var sqlContext: SQLContext = _
  @transient var dataset: DataFrame = _

  /**
   * In `LinearRegressionSuite`, we will make sure that the model trained by SparkML
   * is the same as the one trained by R's glmnet package. The following instruction
   * describes how to reproduce the data in R.
   *
   * import org.apache.spark.mllib.util.LinearDataGenerator
   * val data =
   *   sc.parallelize(LinearDataGenerator.generateLinearInput(6.3, Array(4.7, 7.2), 10000, 42), 2)
   * data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1)).saveAsTextFile("path")
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    sqlContext = new SQLContext(sc)
    dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 10000, 42, 0.1), 2))
  }

  test("linear regression with intercept without regularization") {
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)

    /**
     * Using the following R code to load the data and train the model using glmnet package.
     *
     * library("glmnet")
     * data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
     * features <- as.matrix(data.frame(as.numeric(data$V2), as.numeric(data$V3)))
     * label <- as.numeric(data$V1)
     * weights <- coef(glmnet(features, label, family="gaussian", alpha = 0, lambda = 0))
     * > weights
     *  3 x 1 sparse Matrix of class "dgCMatrix"
     *                           s0
     * (Intercept)         6.300528
     * as.numeric.data.V2. 4.701024
     * as.numeric.data.V3. 7.198257
     */
    val interceptR = 6.298698
    val weightsR = Array(4.700706, 7.199082)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights(0) ~== weightsR(0) relTol 1E-3)
    assert(model.weights(1) ~== weightsR(1) relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression with intercept with L1 regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
    val model = trainer.fit(dataset)

    /**
     * weights <- coef(glmnet(features, label, family="gaussian", alpha = 1.0, lambda = 0.57))
     * > weights
     *  3 x 1 sparse Matrix of class "dgCMatrix"
     *                           s0
     * (Intercept)         6.311546
     * as.numeric.data.V2. 2.123522
     * as.numeric.data.V3. 4.605651
     */
    val interceptR = 6.243000
    val weightsR = Array(4.024821, 6.679841)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights(0) ~== weightsR(0) relTol 1E-3)
    assert(model.weights(1) ~== weightsR(1) relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression with intercept with L2 regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
    val model = trainer.fit(dataset)

    /**
     * weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3))
     * > weights
     *  3 x 1 sparse Matrix of class "dgCMatrix"
     *                           s0
     * (Intercept)         6.328062
     * as.numeric.data.V2. 3.222034
     * as.numeric.data.V3. 4.926260
     */
    val interceptR = 5.269376
    val weightsR = Array(3.736216, 5.712356)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights(0) ~== weightsR(0) relTol 1E-3)
    assert(model.weights(1) ~== weightsR(1) relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression with intercept with ElasticNet regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
    val model = trainer.fit(dataset)

    /**
     * weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.3, lambda = 1.6))
     * > weights
     * 3 x 1 sparse Matrix of class "dgCMatrix"
     * s0
     * (Intercept)         6.324108
     * as.numeric.data.V2. 3.168435
     * as.numeric.data.V3. 5.200403
     */
    val interceptR = 5.696056
    val weightsR = Array(3.670489, 6.001122)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights(0) ~== weightsR(0) relTol 1E-3)
    assert(model.weights(1) ~== weightsR(1) relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }
}
