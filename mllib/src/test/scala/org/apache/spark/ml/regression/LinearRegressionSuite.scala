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

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.{LinearDataGenerator, MLlibTestSparkContext}
import org.apache.spark.sql.{DataFrame, Row}

class LinearRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  private val seed: Int = 42
  @transient var datasetWithDenseFeature: DataFrame = _
  @transient var datasetWithDenseFeatureWithoutIntercept: DataFrame = _
  @transient var datasetWithSparseFeature: DataFrame = _
  @transient var datasetWithWeight: DataFrame = _
  @transient var datasetWithWeightConstantLabel: DataFrame = _
  @transient var datasetWithWeightZeroLabel: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    datasetWithDenseFeature = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, eps = 0.1), 2).map(_.asML).toDF()
    /*
       datasetWithDenseFeatureWithoutIntercept is not needed for correctness testing
       but is useful for illustrating training model without intercept
     */
    datasetWithDenseFeatureWithoutIntercept = sc.parallelize(
      LinearDataGenerator.generateLinearInput(
        intercept = 0.0, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
        xVariance = Array(0.7, 1.2), nPoints = 10000, seed, eps = 0.1), 2).map(_.asML).toDF()

    val r = new Random(seed)
    // When feature size is larger than 4096, normal optimizer is chosen
    // as the solver of linear regression in the case of "auto" mode.
    val featureSize = 4100
    datasetWithSparseFeature = sc.parallelize(LinearDataGenerator.generateLinearInput(
        intercept = 0.0, weights = Seq.fill(featureSize)(r.nextDouble()).toArray,
        xMean = Seq.fill(featureSize)(r.nextDouble()).toArray,
        xVariance = Seq.fill(featureSize)(r.nextDouble()).toArray, nPoints = 200,
        seed, eps = 0.1, sparsity = 0.7), 2).map(_.asML).toDF()

    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
       b <- c(17, 19, 23, 29)
       w <- c(1, 2, 3, 4)
       df <- as.data.frame(cbind(A, b))
     */
    datasetWithWeight = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(19.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(23.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(29.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2).toDF()

    /*
       R code:

       A <- matrix(c(0, 1, 2, 3, 5, 7, 11, 13), 4, 2)
       b.const <- c(17, 17, 17, 17)
       w <- c(1, 2, 3, 4)
       df.const.label <- as.data.frame(cbind(A, b.const))
     */
    datasetWithWeightConstantLabel = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(17.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(17.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(17.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2).toDF()
    datasetWithWeightZeroLabel = sc.parallelize(Seq(
      Instance(0.0, 1.0, Vectors.dense(0.0, 5.0).toSparse),
      Instance(0.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(0.0, 3.0, Vectors.dense(2.0, 11.0)),
      Instance(0.0, 4.0, Vectors.dense(3.0, 13.0))
    ), 2).toDF()
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glmnet package.
   */
  ignore("export test data into CSV format") {
    datasetWithDenseFeature.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LinearRegressionSuite/datasetWithDenseFeature")

    datasetWithDenseFeatureWithoutIntercept.rdd.map {
      case Row(label: Double, features: Vector) =>
        label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile(
      "target/tmp/LinearRegressionSuite/datasetWithDenseFeatureWithoutIntercept")

    datasetWithSparseFeature.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LinearRegressionSuite/datasetWithSparseFeature")
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
    assert(lir.getStandardization)
    assert(lir.getSolver == "auto")
    val model = lir.fit(datasetWithDenseFeature)

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)

    model.transform(datasetWithDenseFeature)
      .select("label", "prediction")
      .collect()
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
    val numFeatures = datasetWithDenseFeature.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)
  }

  test("linear regression handles singular matrices") {
    // check for both constant columns with intercept (zero std) and collinear
    val singularDataConstantColumn = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(1.0, 5.0).toSparse),
      Instance(19.0, 2.0, Vectors.dense(1.0, 7.0)),
      Instance(23.0, 3.0, Vectors.dense(1.0, 11.0)),
      Instance(29.0, 4.0, Vectors.dense(1.0, 13.0))
    ), 2).toDF()

    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer = new LinearRegression().setSolver(solver).setFitIntercept(true)
      val model = trainer.fit(singularDataConstantColumn)
      // to make it clear that WLS did not solve analytically
      intercept[UnsupportedOperationException] {
        model.summary.coefficientStandardErrors
      }
      assert(model.summary.objectiveHistory !== Array(0.0))
    }

    val singularDataCollinearFeatures = sc.parallelize(Seq(
      Instance(17.0, 1.0, Vectors.dense(10.0, 5.0).toSparse),
      Instance(19.0, 2.0, Vectors.dense(14.0, 7.0)),
      Instance(23.0, 3.0, Vectors.dense(22.0, 11.0)),
      Instance(29.0, 4.0, Vectors.dense(26.0, 13.0))
    ), 2).toDF()

    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer = new LinearRegression().setSolver(solver).setFitIntercept(true)
      val model = trainer.fit(singularDataCollinearFeatures)
      intercept[UnsupportedOperationException] {
        model.summary.coefficientStandardErrors
      }
      assert(model.summary.objectiveHistory !== Array(0.0))
    }
  }

  test("linear regression with intercept without regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = new LinearRegression().setSolver(solver)
      // The result should be the same regardless of standardization without regularization
      val trainer2 = (new LinearRegression).setStandardization(false).setSolver(solver)
      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         Using the following R code to load the data and train the model using glmnet package.

         library("glmnet")
         data <- read.csv("path", header=FALSE, stringsAsFactors=FALSE)
         features <- as.matrix(data.frame(as.numeric(data$V2), as.numeric(data$V3)))
         label <- as.numeric(data$V1)
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0, lambda = 0))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)         6.298698
         as.numeric.data.V2. 4.700706
         as.numeric.data.V3. 7.199082
       */
      val interceptR = 6.298698
      val coefficientsR = Vectors.dense(4.700706, 7.199082)

      assert(model1.intercept ~== interceptR relTol 1E-3)
      assert(model1.coefficients ~= coefficientsR relTol 1E-3)
      assert(model2.intercept ~== interceptR relTol 1E-3)
      assert(model2.coefficients ~= coefficientsR relTol 1E-3)

      model1.transform(datasetWithDenseFeature).select("features", "prediction").collect().foreach {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression without intercept without regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setFitIntercept(false).setSolver(solver)
      // Without regularization the results should be the same
      val trainer2 = (new LinearRegression).setFitIntercept(false).setStandardization(false)
        .setSolver(solver)
      val model1 = trainer1.fit(datasetWithDenseFeature)
      val modelWithoutIntercept1 = trainer1.fit(datasetWithDenseFeatureWithoutIntercept)
      val model2 = trainer2.fit(datasetWithDenseFeature)
      val modelWithoutIntercept2 = trainer2.fit(datasetWithDenseFeatureWithoutIntercept)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0, lambda = 0,
           intercept = FALSE))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)         .
         as.numeric.data.V2. 6.973403
         as.numeric.data.V3. 5.284370
       */
      val coefficientsR = Vectors.dense(6.973403, 5.284370)

      assert(model1.intercept ~== 0 absTol 1E-2)
      assert(model1.coefficients ~= coefficientsR relTol 1E-2)
      assert(model2.intercept ~== 0 absTol 1E-2)
      assert(model2.coefficients ~= coefficientsR relTol 1E-2)

      /*
         Then again with the data with no intercept:
         > coefficientsWithoutIntercept
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)           .
         as.numeric.data3.V2. 4.70011
         as.numeric.data3.V3. 7.19943
       */
      val coefficientsWithoutInterceptR = Vectors.dense(4.70011, 7.19943)

      assert(modelWithoutIntercept1.intercept ~== 0 absTol 1E-3)
      assert(modelWithoutIntercept1.coefficients ~= coefficientsWithoutInterceptR relTol 1E-3)
      assert(modelWithoutIntercept2.intercept ~== 0 absTol 1E-3)
      assert(modelWithoutIntercept2.coefficients ~= coefficientsWithoutInterceptR relTol 1E-3)
    }
  }

  test("linear regression with intercept with L1 regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
        .setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
        .setSolver(solver).setStandardization(false)

      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian",
           alpha = 1.0, lambda = 0.57 ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                  s0
         (Intercept)       6.242284
         as.numeric.d1.V2. 4.019605
         as.numeric.d1.V3. 6.679538
       */
      val interceptR1 = 6.242284
      val coefficientsR1 = Vectors.dense(4.019605, 6.679538)
      assert(model1.intercept ~== interceptR1 relTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 1.0,
           lambda = 0.57, standardize=FALSE ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                  s0
         (Intercept)         6.416948
         as.numeric.data.V2. 3.893869
         as.numeric.data.V3. 6.724286
       */
      val interceptR2 = 6.416948
      val coefficientsR2 = Vectors.dense(3.893869, 6.724286)

      assert(model2.intercept ~== interceptR2 relTol 1E-3)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-3)

      model1.transform(datasetWithDenseFeature).select("features", "prediction")
        .collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val prediction2 =
              features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
                model1.intercept
            assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression without intercept with L1 regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
        .setFitIntercept(false).setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(1.0).setRegParam(0.57)
        .setFitIntercept(false).setStandardization(false).setSolver(solver)

      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 1.0,
           lambda = 0.57, intercept=FALSE ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)          .
         as.numeric.data.V2. 6.272927
         as.numeric.data.V3. 4.782604
       */
      val interceptR1 = 0.0
      val coefficientsR1 = Vectors.dense(6.272927, 4.782604)

      assert(model1.intercept ~== interceptR1 absTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 1.0,
           lambda = 0.57, intercept=FALSE, standardize=FALSE ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)         .
         as.numeric.data.V2. 6.207817
         as.numeric.data.V3. 4.775780
       */
      val interceptR2 = 0.0
      val coefficientsR2 = Vectors.dense(6.207817, 4.775780)

      assert(model2.intercept ~== interceptR2 absTol 1E-2)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-2)

      model1.transform(datasetWithDenseFeature).select("features", "prediction")
        .collect().foreach {
          case Row(features: DenseVector, prediction1: Double) =>
            val prediction2 =
              features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
                model1.intercept
            assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression with intercept with L2 regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
        .setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
        .setStandardization(false).setSolver(solver)
      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)       5.260103
         as.numeric.d1.V2. 3.725522
         as.numeric.d1.V3. 5.711203
       */
      val interceptR1 = 5.260103
      val coefficientsR1 = Vectors.dense(3.725522, 5.711203)

      assert(model1.intercept ~== interceptR1 relTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3,
           standardize=FALSE))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)       5.790885
         as.numeric.d1.V2. 3.432373
         as.numeric.d1.V3. 5.919196
       */
      val interceptR2 = 5.790885
      val coefficientsR2 = Vectors.dense(3.432373, 5.919196)

      assert(model2.intercept ~== interceptR2 relTol 1E-2)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-2)

      model1.transform(datasetWithDenseFeature).select("features", "prediction").collect().foreach {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression without intercept with L2 regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
        .setFitIntercept(false).setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(0.0).setRegParam(2.3)
        .setFitIntercept(false).setStandardization(false).setSolver(solver)
      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3,
           intercept = FALSE))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)       .
         as.numeric.d1.V2. 5.493430
         as.numeric.d1.V3. 4.223082
       */
      val interceptR1 = 0.0
      val coefficientsR1 = Vectors.dense(5.493430, 4.223082)

      assert(model1.intercept ~== interceptR1 absTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.0, lambda = 2.3,
           intercept = FALSE, standardize=FALSE))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)         .
         as.numeric.d1.V2. 5.244324
         as.numeric.d1.V3. 4.203106
       */
      val interceptR2 = 0.0
      val coefficientsR2 = Vectors.dense(5.244324, 4.203106)

      assert(model2.intercept ~== interceptR2 absTol 1E-2)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-2)

      model1.transform(datasetWithDenseFeature).select("features", "prediction").collect().foreach {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression with intercept with ElasticNet regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
        .setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
        .setStandardization(false).setSolver(solver)

      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.3,
           lambda = 1.6 ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)       5.689855
         as.numeric.d1.V2. 3.661181
         as.numeric.d1.V3. 6.000274
       */
      val interceptR1 = 5.689855
      val coefficientsR1 = Vectors.dense(3.661181, 6.000274)

      assert(model1.intercept ~== interceptR1 relTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.3, lambda = 1.6
           standardize=FALSE))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)       6.113890
         as.numeric.d1.V2. 3.407021
         as.numeric.d1.V3. 6.152512
       */
      val interceptR2 = 6.113890
      val coefficientsR2 = Vectors.dense(3.407021, 6.152512)

      assert(model2.intercept ~== interceptR2 relTol 1E-2)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-2)

      model1.transform(datasetWithDenseFeature).select("features", "prediction")
        .collect().foreach {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression without intercept with ElasticNet regularization") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
        .setFitIntercept(false).setSolver(solver)
      val trainer2 = (new LinearRegression).setElasticNetParam(0.3).setRegParam(1.6)
        .setFitIntercept(false).setStandardization(false).setSolver(solver)

      val model1 = trainer1.fit(datasetWithDenseFeature)
      val model2 = trainer2.fit(datasetWithDenseFeature)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.3,
           lambda = 1.6, intercept=FALSE ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                    s0
         (Intercept)       .
         as.numeric.d1.V2. 5.643748
         as.numeric.d1.V3. 4.331519
       */
      val interceptR1 = 0.0
      val coefficientsR1 = Vectors.dense(5.643748, 4.331519)

      assert(model1.intercept ~== interceptR1 absTol 1E-2)
      assert(model1.coefficients ~= coefficientsR1 relTol 1E-2)

      /*
         coefficients <- coef(glmnet(features, label, family="gaussian", alpha = 0.3,
           lambda = 1.6, intercept=FALSE, standardize=FALSE ))
         > coefficients
          3 x 1 sparse Matrix of class "dgCMatrix"
                                   s0
         (Intercept)         .
         as.numeric.d1.V2. 5.455902
         as.numeric.d1.V3. 4.312266

       */
      val interceptR2 = 0.0
      val coefficientsR2 = Vectors.dense(5.455902, 4.312266)

      assert(model2.intercept ~== interceptR2 absTol 1E-2)
      assert(model2.coefficients ~= coefficientsR2 relTol 1E-2)

      model1.transform(datasetWithDenseFeature).select("features", "prediction")
        .collect().foreach {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("linear regression model with constant label") {
    /*
       R code:
       for (formula in c(b.const ~ . -1, b.const ~ .)) {
         model <- lm(formula, data=df.const.label, weights=w)
         print(as.vector(coef(model)))
       }
      [1] -9.221298  3.394343
      [1] 17  0  0
    */
    val expected = Seq(
      Vectors.dense(0.0, -9.221298, 3.394343),
      Vectors.dense(17.0, 0.0, 0.0))

    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      var idx = 0
      for (fitIntercept <- Seq(false, true)) {
        val model1 = new LinearRegression()
          .setFitIntercept(fitIntercept)
          .setWeightCol("weight")
          .setPredictionCol("myPrediction")
          .setSolver(solver)
          .fit(datasetWithWeightConstantLabel)
        val actual1 = Vectors.dense(model1.intercept, model1.coefficients(0),
            model1.coefficients(1))
        assert(actual1 ~== expected(idx) absTol 1e-4)

        // Schema of summary.predictions should be a superset of the input dataset
        assert((datasetWithWeightConstantLabel.schema.fieldNames.toSet + model1.getPredictionCol)
          .subsetOf(model1.summary.predictions.schema.fieldNames.toSet))

        val model2 = new LinearRegression()
          .setFitIntercept(fitIntercept)
          .setWeightCol("weight")
          .setPredictionCol("myPrediction")
          .setSolver(solver)
          .fit(datasetWithWeightZeroLabel)
        val actual2 = Vectors.dense(model2.intercept, model2.coefficients(0),
            model2.coefficients(1))
        assert(actual2 ~==  Vectors.dense(0.0, 0.0, 0.0) absTol 1e-4)

        // Schema of summary.predictions should be a superset of the input dataset
        assert((datasetWithWeightZeroLabel.schema.fieldNames.toSet + model2.getPredictionCol)
          .subsetOf(model2.summary.predictions.schema.fieldNames.toSet))

        idx += 1
      }
    }
  }

  test("regularized linear regression through origin with constant label") {
    // The problem is ill-defined if fitIntercept=false, regParam is non-zero.
    // An exception is thrown in this case.
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      for (standardization <- Seq(false, true)) {
        val model = new LinearRegression().setFitIntercept(false)
          .setRegParam(0.1).setStandardization(standardization).setSolver(solver)
        intercept[IllegalArgumentException] {
          model.fit(datasetWithWeightConstantLabel)
        }
      }
    }
  }

  test("linear regression with l-bfgs when training is not needed") {
    // When label is constant, l-bfgs solver returns results without training.
    // There are two possibilities: If the label is non-zero but constant,
    // and fitIntercept is true, then the model return yMean as intercept without training.
    // If label is all zeros, then all coefficients are zero regardless of fitIntercept, so
    // no training is needed.
    for (fitIntercept <- Seq(false, true)) {
      for (standardization <- Seq(false, true)) {
        val model1 = new LinearRegression()
          .setFitIntercept(fitIntercept)
          .setStandardization(standardization)
          .setWeightCol("weight")
          .setSolver("l-bfgs")
          .fit(datasetWithWeightConstantLabel)
        if (fitIntercept) {
          assert(model1.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)
        }
        val model2 = new LinearRegression()
          .setFitIntercept(fitIntercept)
          .setWeightCol("weight")
          .setSolver("l-bfgs")
          .fit(datasetWithWeightZeroLabel)
        assert(model2.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)
      }
    }
  }

  test("linear regression model training summary") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer = new LinearRegression().setSolver(solver).setPredictionCol("myPrediction")
      val model = trainer.fit(datasetWithDenseFeature)
      val trainerNoPredictionCol = trainer.setPredictionCol("")
      val modelNoPredictionCol = trainerNoPredictionCol.fit(datasetWithDenseFeature)

      // Training results for the model should be available
      assert(model.hasSummary)
      assert(modelNoPredictionCol.hasSummary)

      // Schema should be a superset of the input dataset
      assert((datasetWithDenseFeature.schema.fieldNames.toSet + model.getPredictionCol).subsetOf(
        model.summary.predictions.schema.fieldNames.toSet))
      // Validate that we re-insert a prediction column for evaluation
      val modelNoPredictionColFieldNames
      = modelNoPredictionCol.summary.predictions.schema.fieldNames
      assert(datasetWithDenseFeature.schema.fieldNames.toSet.subsetOf(
        modelNoPredictionColFieldNames.toSet))
      assert(modelNoPredictionColFieldNames.exists(s => s.startsWith("prediction_")))

      // Residuals in [[LinearRegressionResults]] should equal those manually computed
      val expectedResiduals = datasetWithDenseFeature.select("features", "label")
        .rdd
        .map { case Row(features: DenseVector, label: Double) =>
          val prediction =
            features(0) * model.coefficients(0) + features(1) * model.coefficients(1) +
              model.intercept
          label - prediction
        }
        .zip(model.summary.residuals.rdd.map(_.getDouble(0)))
        .collect()
        .foreach { case (manualResidual: Double, resultResidual: Double) =>
          assert(manualResidual ~== resultResidual relTol 1E-5)
        }

      /*
         # Use the following R code to generate model training results.

         # path/part-00000 is the file generated by running LinearDataGenerator.generateLinearInput
         # as described before the beforeAll() method.
         d1 <- read.csv("path/part-00000", header=FALSE, stringsAsFactors=FALSE)
         fit <- glm(V1 ~ V2 + V3, data = d1, family = "gaussian")
         names(f1)[1] = c("V2")
         names(f1)[2] = c("V3")
         f1 <- data.frame(as.numeric(d1$V2), as.numeric(d1$V3))
         predictions <- predict(fit, newdata=f1)
         l1 <- as.numeric(d1$V1)

         residuals <- l1 - predictions
         > mean(residuals^2)           # MSE
         [1] 0.00985449
         > mean(abs(residuals))        # MAD
         [1] 0.07961668
         > cor(predictions, l1)^2   # r^2
         [1] 0.9998737

         > summary(fit)

          Call:
          glm(formula = V1 ~ V2 + V3, family = "gaussian", data = d1)

          Deviance Residuals:
               Min        1Q    Median        3Q       Max
          -0.47082  -0.06797   0.00002   0.06725   0.34635

          Coefficients:
                       Estimate Std. Error t value Pr(>|t|)
          (Intercept) 6.3022157  0.0018600    3388   <2e-16 ***
          V2          4.6982442  0.0011805    3980   <2e-16 ***
          V3          7.1994344  0.0009044    7961   <2e-16 ***
          ---

          ....
       */
      assert(model.summary.meanSquaredError ~== 0.00985449 relTol 1E-4)
      assert(model.summary.meanAbsoluteError ~== 0.07961668 relTol 1E-4)
      assert(model.summary.r2 ~== 0.9998737 relTol 1E-4)

      // Normal solver uses "WeightedLeastSquares". When no regularization is applied,
      // this algorithm uses a direct solver and does not generate an objective history because
      // it does not run through iterations.
      if (solver == "l-bfgs") {
        // Objective function should be monotonically decreasing for linear regression
        assert(
          model.summary
            .objectiveHistory
            .sliding(2)
            .forall(x => x(0) >= x(1)))
      } else {
        // To clarify that the normal solver is used here.
        assert(model.summary.objectiveHistory.length == 1)
        assert(model.summary.objectiveHistory(0) == 0.0)
        val devianceResidualsR = Array(-0.47082, 0.34635)
        val seCoefR = Array(0.0011805, 0.0009044, 0.0018600)
        val tValsR = Array(3980, 7961, 3388)
        val pValsR = Array(0, 0, 0)
        model.summary.devianceResiduals.zip(devianceResidualsR).foreach { x =>
          assert(x._1 ~== x._2 absTol 1E-4) }
        model.summary.coefficientStandardErrors.zip(seCoefR).foreach { x =>
          assert(x._1 ~== x._2 absTol 1E-4) }
        model.summary.tValues.map(_.round).zip(tValsR).foreach{ x => assert(x._1 === x._2) }
        model.summary.pValues.map(_.round).zip(pValsR).foreach{ x => assert(x._1 === x._2) }
      }
    }
  }

  test("linear regression model testset evaluation summary") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer = new LinearRegression().setSolver(solver)
      val model = trainer.fit(datasetWithDenseFeature)

      // Evaluating on training dataset should yield results summary equal to training summary
      val testSummary = model.evaluate(datasetWithDenseFeature)
      assert(model.summary.meanSquaredError ~== testSummary.meanSquaredError relTol 1E-5)
      assert(model.summary.r2 ~== testSummary.r2 relTol 1E-5)
      model.summary.residuals.select("residuals").collect()
        .zip(testSummary.residuals.select("residuals").collect())
        .forall { case (Row(r1: Double), Row(r2: Double)) => r1 ~== r2 relTol 1E-5 }
    }
  }

  test("linear regression with weighted samples") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val (data, weightedData) = {
        val activeData = LinearDataGenerator.generateLinearInput(
          6.3, Array(4.7, 7.2), Array(0.9, -1.3), Array(0.7, 1.2), 500, 1, 0.1).map(_.asML)

        val rnd = new Random(8392)
        val signedData = activeData.map { case p: LabeledPoint =>
          (rnd.nextGaussian() > 0.0, p)
        }

        val data1 = signedData.flatMap {
          case (true, p) => Iterator(p, p)
          case (false, p) => Iterator(p)
        }

        val weightedSignedData = signedData.flatMap {
          case (true, LabeledPoint(label, features)) =>
            Iterator(
              Instance(label, weight = 1.2, features),
              Instance(label, weight = 0.8, features)
            )
          case (false, LabeledPoint(label, features)) =>
            Iterator(
              Instance(label, weight = 0.3, features),
              Instance(label, weight = 0.1, features),
              Instance(label, weight = 0.6, features)
            )
        }

        val noiseData = LinearDataGenerator.generateLinearInput(
          2, Array(1, 3), Array(0.9, -1.3), Array(0.7, 1.2), 500, 1, 0.1).map(_.asML)
        val weightedNoiseData = noiseData.map {
          case LabeledPoint(label, features) => Instance(label, weight = 0, features)
        }
        val data2 = weightedSignedData ++ weightedNoiseData

        (sc.parallelize(data1, 4).toDF(), sc.parallelize(data2, 4).toDF())
      }

      val trainer1a = (new LinearRegression).setFitIntercept(true)
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(true).setSolver(solver)
      val trainer1b = (new LinearRegression).setFitIntercept(true).setWeightCol("weight")
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(true).setSolver(solver)

      // Normal optimizer is not supported with non-zero elasticnet parameter.
      val model1a0 = trainer1a.fit(data)
      val model1a1 = trainer1a.fit(weightedData)
      val model1b = trainer1b.fit(weightedData)

      assert(model1a0.coefficients !~= model1a1.coefficients absTol 1E-3)
      assert(model1a0.intercept !~= model1a1.intercept absTol 1E-3)
      assert(model1a0.coefficients ~== model1b.coefficients absTol 1E-3)
      assert(model1a0.intercept ~== model1b.intercept absTol 1E-3)

      val trainer2a = (new LinearRegression).setFitIntercept(true)
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(false).setSolver(solver)
      val trainer2b = (new LinearRegression).setFitIntercept(true).setWeightCol("weight")
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(false).setSolver(solver)
      val model2a0 = trainer2a.fit(data)
      val model2a1 = trainer2a.fit(weightedData)
      val model2b = trainer2b.fit(weightedData)
      assert(model2a0.coefficients !~= model2a1.coefficients absTol 1E-3)
      assert(model2a0.intercept !~= model2a1.intercept absTol 1E-3)
      assert(model2a0.coefficients ~== model2b.coefficients absTol 1E-3)
      assert(model2a0.intercept ~== model2b.intercept absTol 1E-3)

      val trainer3a = (new LinearRegression).setFitIntercept(false)
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(true).setSolver(solver)
      val trainer3b = (new LinearRegression).setFitIntercept(false).setWeightCol("weight")
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(true).setSolver(solver)
      val model3a0 = trainer3a.fit(data)
      val model3a1 = trainer3a.fit(weightedData)
      val model3b = trainer3b.fit(weightedData)
      assert(model3a0.coefficients !~= model3a1.coefficients absTol 1E-3)
      assert(model3a0.coefficients ~== model3b.coefficients absTol 1E-3)

      val trainer4a = (new LinearRegression).setFitIntercept(false)
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(false).setSolver(solver)
      val trainer4b = (new LinearRegression).setFitIntercept(false).setWeightCol("weight")
        .setElasticNetParam(0.0).setRegParam(0.21).setStandardization(false).setSolver(solver)
      val model4a0 = trainer4a.fit(data)
      val model4a1 = trainer4a.fit(weightedData)
      val model4b = trainer4b.fit(weightedData)
      assert(model4a0.coefficients !~= model4a1.coefficients absTol 1E-3)
      assert(model4a0.coefficients ~== model4b.coefficients absTol 1E-3)
    }
  }

  test("linear regression model with l-bfgs with big feature datasets") {
    val trainer = new LinearRegression().setSolver("auto")
    val model = trainer.fit(datasetWithSparseFeature)

    // Training results for the model should be available
    assert(model.hasSummary)
    // When LBFGS is used as optimizer, objective history can be restored.
    assert(
      model.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))
  }

  test("linear regression summary with weighted samples and intercept by normal solver") {
    /*
       R code:

       model <- glm(formula = "b ~ .", data = df, weights = w)
       summary(model)

       Call:
       glm(formula = "b ~ .", data = df, weights = w)

       Deviance Residuals:
            1       2       3       4
        1.920  -1.358  -1.109   0.960

       Coefficients:
                   Estimate Std. Error t value Pr(>|t|)
       (Intercept)   18.080      9.608   1.882    0.311
       V1             6.080      5.556   1.094    0.471
       V2            -0.600      1.960  -0.306    0.811

       (Dispersion parameter for gaussian family taken to be 7.68)

           Null deviance: 202.00  on 3  degrees of freedom
       Residual deviance:   7.68  on 1  degrees of freedom
       AIC: 18.783

       Number of Fisher Scoring iterations: 2
     */

    val model = new LinearRegression()
      .setWeightCol("weight")
      .setSolver("normal")
      .fit(datasetWithWeight)
    val coefficientsR = Vectors.dense(Array(6.080, -0.600))
    val interceptR = 18.080
    val devianceResidualsR = Array(-1.358, 1.920)
    val seCoefR = Array(5.556, 1.960, 9.608)
    val tValsR = Array(1.094, -0.306, 1.882)
    val pValsR = Array(0.471, 0.811, 0.311)

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept ~== interceptR absTol 1E-3)
    model.summary.devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }

    val modelWithL1 = new LinearRegression()
      .setWeightCol("weight")
      .setSolver("normal")
      .setRegParam(0.5)
      .setElasticNetParam(1.0)
      .fit(datasetWithWeight)

    assert(modelWithL1.summary.objectiveHistory !== Array(0.0))
    assert(
      modelWithL1.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))
  }

  test("linear regression summary with weighted samples and w/o intercept by normal solver") {
    /*
       R code:

       model <- glm(formula = "b ~ . -1", data = df, weights = w)
       summary(model)

       Call:
       glm(formula = "b ~ . -1", data = df, weights = w)

       Deviance Residuals:
            1       2       3       4
        1.950   2.344  -4.600   2.103

       Coefficients:
          Estimate Std. Error t value Pr(>|t|)
       V1  -3.7271     2.9032  -1.284   0.3279
       V2   3.0100     0.6022   4.998   0.0378 *
       ---
       Signif. codes:  0 '***' 0.001 '**' 0.01 '*' 0.05 '.' 0.1 ' ' 1

       (Dispersion parameter for gaussian family taken to be 17.4376)

           Null deviance: 5962.000  on 4  degrees of freedom
       Residual deviance:   34.875  on 2  degrees of freedom
       AIC: 22.835

       Number of Fisher Scoring iterations: 2
     */

    val model = new LinearRegression()
      .setWeightCol("weight")
      .setSolver("normal")
      .setFitIntercept(false)
      .fit(datasetWithWeight)
    val coefficientsR = Vectors.dense(Array(-3.7271, 3.0100))
    val interceptR = 0.0
    val devianceResidualsR = Array(-4.600, 2.344)
    val seCoefR = Array(2.9032, 0.6022)
    val tValsR = Array(-1.284, 4.998)
    val pValsR = Array(0.3279, 0.0378)

    assert(model.coefficients ~== coefficientsR absTol 1E-3)
    assert(model.intercept === interceptR)
    model.summary.devianceResiduals.zip(devianceResidualsR).foreach { x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.coefficientStandardErrors.zip(seCoefR).foreach{ x =>
      assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.tValues.zip(tValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
    model.summary.pValues.zip(pValsR).foreach{ x => assert(x._1 ~== x._2 absTol 1E-3) }
  }

  test("read/write") {
    def checkModelData(model: LinearRegressionModel, model2: LinearRegressionModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients === model2.coefficients)
    }
    val lr = new LinearRegression()
    testEstimatorAndModelReadWrite(lr, datasetWithWeight, LinearRegressionSuite.allParamSettings,
      checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    for (solver <- Seq("auto", "l-bfgs", "normal")) {
      val lr = new LinearRegression().setMaxIter(1).setSolver(solver)
      MLTestingUtils.checkNumericTypes[LinearRegressionModel, LinearRegression](
        lr, spark, isClassification = false) { (expected, actual) =>
        assert(expected.intercept === actual.intercept)
        assert(expected.coefficients === actual.coefficients)
      }
    }
  }
}

object LinearRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "predictionCol" -> "myPrediction",
    "regParam" -> 0.01,
    "elasticNetParam" -> 0.1,
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "solver" -> "l-bfgs"
  )
}
