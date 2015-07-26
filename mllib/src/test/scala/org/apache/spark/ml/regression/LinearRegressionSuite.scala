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

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.mllib.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}

class LinearRegressionSuite extends SparkFunSuite with MLlibTestSparkContext {

  @transient var dataset: DataFrame = _
  @transient var datasetWithoutIntercept: DataFrame = _

  /*
     In `LinearRegressionSuite`, we will make sure that the model trained by SparkML
     is the same as the one trained by R's glmnet package. The following instruction
     describes how to reproduce the data in R.

     import org.apache.spark.mllib.util.LinearDataGenerator
     val data =
       sc.parallelize(LinearDataGenerator.generateLinearInput(6.3, Array(4.7, 7.2),
         Array(0.9, -1.3), Array(0.7, 1.2), 10000, 42, 0.1), 2)
     data.map(x=> x.label + ", " + x.features(0) + ", " + x.features(1)).coalesce(1)
       .saveAsTextFile("path")
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 10000, 42, 0.1), 2))
    /*
       datasetWithoutIntercept is not needed for correctness testing but is useful for illustrating
       training model without intercept
     */
    datasetWithoutIntercept = sqlContext.createDataFrame(
      sc.parallelize(LinearDataGenerator.generateLinearInput(
        0.0, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 10000, 42, 0.1), 2))

  }

  test("params") {
    ParamsSuite.checkParams(new LinearRegression)
    val model = new LinearRegressionModel("linearReg", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("linear regression: default params") {
    val lir = new LinearRegression
    assert(lir.getLabelCol === "label")
    assert(lir.getFeaturesCol === "features")
    assert(lir.getPredictionCol === "prediction")
    assert(lir.getRegParam === 0.0)
    assert(lir.getElasticNetParam === 0.0)
    assert(lir.getFitIntercept)
    val model = lir.fit(dataset)
    model.transform(dataset)
      .select("label", "prediction")
      .collect()
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
  }

  test("linear regression with intercept without regularization") {
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
       features <- as.matrix(data.frame(as.numeric(data$V2), as.numeric(data$V3)))
       label <- as.numeric(data$V1)
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0, lambda = 0))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)         6.300528
       as.numeric.data.V2. 4.701024
       as.numeric.data.V3. 7.198257
     */
    val interceptR = 6.298698
    val weightsR = Vectors.dense(4.700706, 7.199082)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression without intercept without regularization") {
    val trainer = (new LinearRegression).setFitIntercept(false)
    val model = trainer.fit(dataset)
    val modelWithoutIntercept = trainer.fit(datasetWithoutIntercept)

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0, lambda = 0,
         intercept = FALSE))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)         .
       as.numeric.data.V2. 6.995908
       as.numeric.data.V3. 5.275131
     */
    val weightsR = Vectors.dense(6.995908, 5.275131)

    assert(model.intercept ~== 0 absTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)
    /*
       Then again with the data with no intercept:
       > weightsWithoutIntercept
       3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
       (Intercept)           .
       as.numeric.data3.V2. 4.70011
       as.numeric.data3.V3. 7.19943
     */
    val weightsWithoutInterceptR = Vectors.dense(4.70011, 7.19943)

    assert(modelWithoutIntercept.intercept ~== 0 absTol 1E-3)
    assert(modelWithoutIntercept.weights ~= weightsWithoutInterceptR relTol 1E-3)
  }

  test("linear regression with intercept with L1 regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
    val model = trainer.fit(dataset)

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 1.0, lambda = 0.57))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)         6.24300
       as.numeric.data.V2. 4.024821
       as.numeric.data.V3. 6.679841
     */
    val interceptR = 6.24300
    val weightsR = Vectors.dense(4.024821, 6.679841)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression without intercept with L1 regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
      .setFitIntercept(false)
    val model = trainer.fit(dataset)

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 1.0, lambda = 0.57,
         intercept=FALSE))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)          .
       as.numeric.data.V2. 6.299752
       as.numeric.data.V3. 4.772913
     */
    val interceptR = 0.0
    val weightsR = Vectors.dense(6.299752, 4.772913)

    assert(model.intercept ~== interceptR absTol 1E-5)
    assert(model.weights ~= weightsR relTol 1E-3)

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

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)         6.328062
       as.numeric.data.V2. 3.222034
       as.numeric.data.V3. 4.926260
     */
    val interceptR = 5.269376
    val weightsR = Vectors.dense(3.736216, 5.712356)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression without intercept with L2 regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
      .setFitIntercept(false)
    val model = trainer.fit(dataset)

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3,
         intercept = FALSE))
       > weights
        3 x 1 sparse Matrix of class "dgCMatrix"
                                 s0
       (Intercept)         .
       as.numeric.data.V2. 5.522875
       as.numeric.data.V3. 4.214502
     */
    val interceptR = 0.0
    val weightsR = Vectors.dense(5.522875, 4.214502)

    assert(model.intercept ~== interceptR absTol 1E-3)
    assert(model.weights ~== weightsR relTol 1E-3)

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

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.3, lambda = 1.6))
       > weights
       3 x 1 sparse Matrix of class "dgCMatrix"
       s0
       (Intercept)         6.324108
       as.numeric.data.V2. 3.168435
       as.numeric.data.V3. 5.200403
     */
    val interceptR = 5.696056
    val weightsR = Vectors.dense(3.670489, 6.001122)

    assert(model.intercept ~== interceptR relTol 1E-3)
    assert(model.weights ~== weightsR relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression without intercept with ElasticNet regularization") {
    val trainer = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
      .setFitIntercept(false)
    val model = trainer.fit(dataset)

    /*
       weights <- coef(glmnet(features, label, family="gaussian", alpha = 0.3, lambda = 1.6,
         intercept=FALSE))
       > weights
       3 x 1 sparse Matrix of class "dgCMatrix"
       s0
       (Intercept)         .
       as.numeric.dataM.V2. 5.673348
       as.numeric.dataM.V3. 4.322251
     */
    val interceptR = 0.0
    val weightsR = Vectors.dense(5.673348, 4.322251)

    assert(model.intercept ~== interceptR absTol 1E-3)
    assert(model.weights ~= weightsR relTol 1E-3)

    model.transform(dataset).select("features", "prediction").collect().foreach {
      case Row(features: DenseVector, prediction1: Double) =>
        val prediction2 =
          features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
        assert(prediction1 ~== prediction2 relTol 1E-5)
    }
  }

  test("linear regression model training summary") {
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)

    // Training results for the model should be available
    assert(model.hasSummary)

    // Residuals in [[LinearRegressionResults]] should equal those manually computed
    val expectedResiduals = dataset.select("features", "label")
      .map { case Row(features: DenseVector, label: Double) =>
      val prediction =
        features(0) * model.weights(0) + features(1) * model.weights(1) + model.intercept
      label - prediction
    }
      .zip(model.summary.residuals.map(_.getDouble(0)))
      .collect()
      .foreach { case (manualResidual: Double, resultResidual: Double) =>
      assert(manualResidual ~== resultResidual relTol 1E-5)
    }

    /*
       Use the following R code to generate model training results.

       predictions <- predict(fit, newx=features)
       residuals <- label - predictions
       > mean(residuals^2) # MSE
       [1] 0.009720325
       > mean(abs(residuals)) # MAD
       [1] 0.07863206
       > cor(predictions, label)^2# r^2
               [,1]
       s0 0.9998749
     */
    assert(model.summary.meanSquaredError ~== 0.00972035 relTol 1E-5)
    assert(model.summary.meanAbsoluteError ~== 0.07863206  relTol 1E-5)
    assert(model.summary.r2 ~== 0.9998749 relTol 1E-5)

    // Objective function should be monotonically decreasing for linear regression
    assert(
      model.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))
  }

  test("linear regression model testset evaluation summary") {
    val trainer = new LinearRegression
    val model = trainer.fit(dataset)

    // Evaluating on training dataset should yield results summary equal to training summary
    val testSummary = model.evaluate(dataset)
    assert(model.summary.meanSquaredError ~== testSummary.meanSquaredError relTol 1E-5)
    assert(model.summary.r2 ~== testSummary.r2 relTol 1E-5)
    model.summary.residuals.select("residuals").collect()
      .zip(testSummary.residuals.select("residuals").collect())
      .forall { case (Row(r1: Double), Row(r2: Double)) => r1 ~== r2 relTol 1E-5 }
  }

}
