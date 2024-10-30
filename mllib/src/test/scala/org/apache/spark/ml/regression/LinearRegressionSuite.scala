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

import scala.jdk.CollectionConverters._
import scala.util.Random

import org.dmg.pmml.{OpType, PMML}
import org.dmg.pmml.regression.{RegressionModel => PMMLRegressionModel}

import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.LinearDataGenerator
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.lit


class LinearRegressionSuite extends MLTest with DefaultReadWriteTest with PMMLReadWriteTest {

  import testImplicits._

  private val seed: Int = 42
  @transient var datasetWithDenseFeature: DataFrame = _
  @transient var datasetWithStrongNoise: DataFrame = _
  @transient var datasetWithDenseFeatureWithoutIntercept: DataFrame = _
  @transient var datasetWithSparseFeature: DataFrame = _
  @transient var datasetWithWeight: DataFrame = _
  @transient var datasetWithWeightConstantLabel: DataFrame = _
  @transient var datasetWithWeightZeroLabel: DataFrame = _
  @transient var datasetWithOutlier: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    datasetWithDenseFeature = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 10000, seed, eps = 0.1), 2).map(_.asML).toDF()

    datasetWithStrongNoise = sc.parallelize(LinearDataGenerator.generateLinearInput(
      intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
      xVariance = Array(0.7, 1.2), nPoints = 100, seed, eps = 5.0), 2).map(_.asML).toDF()

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

    datasetWithOutlier = {
      val inlierData = LinearDataGenerator.generateLinearInput(
        intercept = 6.3, weights = Array(4.7, 7.2), xMean = Array(0.9, -1.3),
        xVariance = Array(0.7, 1.2), nPoints = 900, seed, eps = 0.1)
      val outlierData = LinearDataGenerator.generateLinearInput(
        intercept = -2.1, weights = Array(0.6, -1.2), xMean = Array(0.9, -1.3),
        xVariance = Array(1.5, 0.8), nPoints = 100, seed, eps = 0.1)
      sc.parallelize(inlierData ++ outlierData, 2).map(_.asML).toDF()
    }
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glmnet package.
   */
  ignore("export test data into CSV format") {
    datasetWithDenseFeature.rdd.map { case Row(label: Double, features: Vector) =>
      s"$label,${features.toArray.mkString(",")}"
    }.repartition(1).saveAsTextFile("target/tmp/LinearRegressionSuite/datasetWithDenseFeature")

    datasetWithDenseFeatureWithoutIntercept.rdd.map {
      case Row(label: Double, features: Vector) =>
        s"$label,${features.toArray.mkString(",")}"
    }.repartition(1).saveAsTextFile(
      "target/tmp/LinearRegressionSuite/datasetWithDenseFeatureWithoutIntercept")

    datasetWithSparseFeature.rdd.map { case Row(label: Double, features: Vector) =>
      s"$label,${features.toArray.mkString(",")}"
    }.repartition(1).saveAsTextFile("target/tmp/LinearRegressionSuite/datasetWithSparseFeature")

    datasetWithOutlier.rdd.map { case Row(label: Double, features: Vector) =>
      s"$label,${features.toArray.mkString(",")}"
    }.repartition(1).saveAsTextFile("target/tmp/LinearRegressionSuite/datasetWithOutlier")
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
    assert(lir.getSolver === "auto")
    assert(lir.getLoss === "squaredError")
    assert(lir.getEpsilon === 1.35)
    val model = lir.fit(datasetWithDenseFeature)

    MLTestingUtils.checkCopyAndUids(lir, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
    model.setSummary(None)
    assert(!model.hasSummary)

    model.transform(datasetWithDenseFeature)
      .select("label", "prediction")
      .collect()
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.intercept !== 0.0)
    assert(model.scale === 1.0)
    assert(model.hasParent)
    val numFeatures = datasetWithDenseFeature.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)
  }

  test("LinearRegression validate input dataset") {
    testInvalidRegressionLabels(new LinearRegression().fit(_))
    testInvalidWeights(new LinearRegression().setWeightCol("weight").fit(_))
    testInvalidVectors(new LinearRegression().fit(_))
  }

  test("linear regression: can transform data with LinearRegressionModel") {
    withClue("training related params like loss are only validated during fitting phase") {
      val original = new LinearRegression().fit(datasetWithDenseFeature)

      val deserialized = new LinearRegressionModel(uid = original.uid,
        coefficients = original.coefficients,
        intercept = original.intercept)
      val output = deserialized.transform(datasetWithDenseFeature)
      assert(output.collect().length > 0) // simple assertion to ensure no exception thrown
    }
  }

  test("linear regression: illegal params") {
    withClue("LinearRegression with huber loss only supports L2 regularization") {
      intercept[IllegalArgumentException] {
        new LinearRegression().setLoss("huber").setElasticNetParam(0.5)
          .fit(datasetWithDenseFeature)
      }
    }

    withClue("LinearRegression with huber loss doesn't support normal solver") {
      intercept[IllegalArgumentException] {
        new LinearRegression().setLoss("huber").setSolver("normal").fit(datasetWithDenseFeature)
      }
    }
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
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

      testTransformer[(Double, Vector)](datasetWithDenseFeature, model1,
        "features", "prediction") {
        case Row(features: DenseVector, prediction1: Double) =>
          val prediction2 =
            features(0) * model1.coefficients(0) + features(1) * model1.coefficients(1) +
              model1.intercept
          assert(prediction1 ~== prediction2 relTol 1E-5)
      }
    }
  }

  test("prediction on single instance") {
    val trainer = new LinearRegression
    val model = trainer.fit(datasetWithDenseFeature)

    testPredictionModelSinglePrediction(model, datasetWithDenseFeature)
  }

  test("LinearRegression on blocks") {
    for (dataset <- Seq(datasetWithDenseFeature, datasetWithStrongNoise,
      datasetWithDenseFeatureWithoutIntercept, datasetWithSparseFeature, datasetWithWeight,
      datasetWithWeightConstantLabel, datasetWithWeightZeroLabel, datasetWithOutlier);
         fitIntercept <- Seq(true, false);
         loss <- Seq("squaredError", "huber")) {
      val lir = new LinearRegression()
        .setFitIntercept(fitIntercept)
        .setLoss(loss)
        .setMaxIter(3)
      val model = lir.fit(dataset)
      Seq(0, 0.01, 0.1, 1, 2, 4).foreach { s =>
        val model2 = lir.setMaxBlockSizeInMB(s).fit(dataset)
        assert(model.intercept ~== model2.intercept relTol 1e-9)
        assert(model.coefficients ~== model2.coefficients relTol 1e-9)
        assert(model.scale ~== model2.scale relTol 1e-9)
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
          assert(model1.summary.totalIterations === 0)
        }
        val model2 = new LinearRegression()
          .setFitIntercept(fitIntercept)
          .setWeightCol("weight")
          .setSolver("l-bfgs")
          .fit(datasetWithWeightZeroLabel)
        assert(model2.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)
        assert(model2.summary.totalIterations === 0)
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
      datasetWithDenseFeature.select("features", "label")
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

          # R code for r2adj
          lm_fit <- lm(V1 ~ V2 + V3, data = d1)
          summary(lm_fit)$adj.r.squared
          [1] 0.9998736
          ---

          ....
       */
      assert(model.summary.meanSquaredError ~== 0.00985449 relTol 1E-4)
      assert(model.summary.meanAbsoluteError ~== 0.07961668 relTol 1E-4)
      assert(model.summary.r2 ~== 0.9998737 relTol 1E-4)
      assert(model.summary.r2adj ~== 0.9998736  relTol 1E-4)

      // Normal solver uses "WeightedLeastSquares". If no regularization is applied or only L2
      // regularization is applied, this algorithm uses a direct solver and does not generate an
      // objective history because it does not run through iterations.
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

  test("linear regression model training summary with weighted samples") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = new LinearRegression().setSolver(solver)
      val trainer2 = new LinearRegression().setSolver(solver).setWeightCol("weight")

      Seq(0.25, 1.0, 10.0, 50.00).foreach { w =>
        val model1 = trainer1.fit(datasetWithDenseFeature)
        val model2 = trainer2.fit(datasetWithDenseFeature.withColumn("weight", lit(w)))
        assert(model1.summary.explainedVariance ~== model2.summary.explainedVariance relTol 1e-6)
        assert(model1.summary.meanAbsoluteError ~== model2.summary.meanAbsoluteError relTol 1e-6)
        assert(model1.summary.meanSquaredError ~== model2.summary.meanSquaredError relTol 1e-6)
        assert(model1.summary.rootMeanSquaredError ~==
          model2.summary.rootMeanSquaredError relTol 1e-6)
        assert(model1.summary.r2 ~== model2.summary.r2 relTol 1e-6)
        assert(model1.summary.r2adj ~== model2.summary.r2adj relTol 1e-6)
      }
    }
  }

  test("linear regression model testset evaluation summary with weighted samples") {
    Seq("auto", "l-bfgs", "normal").foreach { solver =>
      val trainer1 = new LinearRegression().setSolver(solver)
      val trainer2 = new LinearRegression().setSolver(solver).setWeightCol("weight")

      Seq(0.25, 1.0, 10.0, 50.00).foreach { w =>
        val model1 = trainer1.fit(datasetWithDenseFeature)
        val model2 = trainer2.fit(datasetWithDenseFeature.withColumn("weight", lit(w)))
        val testSummary1 = model1.evaluate(datasetWithDenseFeature)
        val testSummary2 = model2.evaluate(datasetWithDenseFeature.withColumn("weight", lit(w)))
        assert(testSummary1.explainedVariance ~== testSummary2.explainedVariance relTol 1e-6)
        assert(testSummary1.meanAbsoluteError ~== testSummary2.meanAbsoluteError relTol 1e-6)
        assert(testSummary1.meanSquaredError ~== testSummary2.meanSquaredError relTol 1e-6)
        assert(testSummary1.rootMeanSquaredError ~==
          testSummary2.rootMeanSquaredError relTol 1e-6)
        assert(testSummary1.r2 ~== testSummary2.r2 relTol 1e-6)
        assert(testSummary1.r2adj ~== testSummary2.r2adj relTol 1e-6)
      }
    }
  }

  test("linear regression training summary totalIterations") {
    Seq(1, 5, 10, 20).foreach { maxIter =>
      val trainer = new LinearRegression().setSolver("l-bfgs").setMaxIter(maxIter)
      val model = trainer.fit(datasetWithDenseFeature)
      assert(model.summary.totalIterations <= maxIter)
    }
    Seq("auto", "normal").foreach { solver =>
      val trainer = new LinearRegression().setSolver(solver)
      val model = trainer.fit(datasetWithDenseFeature)
      assert(model.summary.totalIterations === 0)
    }
  }

  test("linear regression with weighted samples") {
    val session = spark
    import session.implicits._
    val numClasses = 0
    def modelEquals(m1: LinearRegressionModel, m2: LinearRegressionModel): Unit = {
      assert(m1.coefficients ~== m2.coefficients relTol 0.01)
      assert(m1.intercept ~== m2.intercept relTol 0.01)
    }
    val testParams = Seq(
      // (elasticNetParam, regParam, fitIntercept, standardization)
      (0.0, 0.21, true, true),
      (0.0, 0.21, true, false),
      (0.0, 0.21, false, false),
      (1.0, 0.21, true, true)
    )

    // For squaredError loss
    for (solver <- Seq("auto", "l-bfgs", "normal");
         (elasticNetParam, regParam, fitIntercept, standardization) <- testParams) {
      val estimator = new LinearRegression()
        .setFitIntercept(fitIntercept)
        .setStandardization(standardization)
        .setRegParam(regParam)
        .setElasticNetParam(elasticNetParam)
        .setSolver(solver)
        .setMaxIter(1)
      MLTestingUtils.testArbitrarilyScaledWeights[LinearRegressionModel, LinearRegression](
        datasetWithStrongNoise.as[LabeledPoint], estimator, modelEquals)
      MLTestingUtils.testOutliersWithSmallWeights[LinearRegressionModel, LinearRegression](
        datasetWithStrongNoise.as[LabeledPoint], estimator, numClasses, modelEquals,
        outlierRatio = 3)
      MLTestingUtils.testOversamplingVsWeighting[LinearRegressionModel, LinearRegression](
        datasetWithStrongNoise.as[LabeledPoint], estimator, modelEquals, seed)
    }

    // For huber loss
    for ((_, regParam, fitIntercept, standardization) <- testParams) {
      val estimator = new LinearRegression()
        .setLoss("huber")
        .setFitIntercept(fitIntercept)
        .setStandardization(standardization)
        .setRegParam(regParam)
        .setMaxIter(1)
      MLTestingUtils.testArbitrarilyScaledWeights[LinearRegressionModel, LinearRegression](
        datasetWithOutlier.as[LabeledPoint], estimator, modelEquals)
      MLTestingUtils.testOutliersWithSmallWeights[LinearRegressionModel, LinearRegression](
        datasetWithOutlier.as[LabeledPoint], estimator, numClasses, modelEquals,
        outlierRatio = 3)
      MLTestingUtils.testOversamplingVsWeighting[LinearRegressionModel, LinearRegression](
        datasetWithOutlier.as[LabeledPoint], estimator, modelEquals, seed)
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
      LinearRegressionSuite.allParamSettings, checkModelData)
  }

  test("pmml export") {
    val lr = new LinearRegression()
    val model = lr.fit(datasetWithWeight)
    def checkModel(pmml: PMML): Unit = {
      val dd = pmml.getDataDictionary
      assert(dd.getNumberOfFields === 3)
      val fields = dd.getDataFields.asScala
      assert(fields(0).getName().toString === "field_0")
      assert(fields(0).getOpType() == OpType.CONTINUOUS)
      val pmmlRegressionModel = pmml.getModels().get(0).asInstanceOf[PMMLRegressionModel]
      val pmmlPredictors = pmmlRegressionModel.getRegressionTables.get(0).getNumericPredictors
      val pmmlWeights = pmmlPredictors.asScala.map(_.getCoefficient()).toList
      assert(pmmlWeights(0) ~== model.coefficients(0) relTol 1E-3)
      assert(pmmlWeights(1) ~== model.coefficients(1) relTol 1E-3)
    }
    testPMMLWrite(sc, model, checkModel)
  }

  test("should support all NumericType labels and weights, and not support other types") {
    for (solver <- Seq("auto", "l-bfgs", "normal")) {
      val lr = new LinearRegression().setMaxIter(1).setSolver(solver)
      MLTestingUtils.checkNumericTypes[LinearRegressionModel, LinearRegression](
        lr, spark, isClassification = false) { (expected, actual) =>
        assert(expected.intercept === actual.intercept)
        assert(expected.coefficients === actual.coefficients)
      }
    }
  }

  test("linear regression (huber loss) with intercept without regularization") {
    val trainer1 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(true).setStandardization(true)
    val trainer2 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(true).setStandardization(false)

    val model1 = trainer1.fit(datasetWithOutlier)
    val model2 = trainer2.fit(datasetWithOutlier)

    /*
      Using the following Python code to load the data and train the model using
      scikit-learn package.

      import pandas as pd
      import numpy as np
      from sklearn.linear_model import HuberRegressor
      df = pd.read_csv("path", header = None)
      X = df[df.columns[1:3]]
      y = np.array(df[df.columns[0]])
      huber = HuberRegressor(fit_intercept=True, alpha=0.0, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 4.68998007,  7.19429011])
      >>> huber.intercept_
      6.3002404351083037
      >>> huber.scale_
      0.077810159205220747
     */
    val coefficientsPy = Vectors.dense(4.68998007, 7.19429011)
    val interceptPy = 6.30024044
    val scalePy = 0.07781016

    assert(model1.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model1.intercept ~== interceptPy relTol 1E-3)
    assert(model1.scale ~== scalePy relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model2.intercept ~== interceptPy relTol 1E-3)
    assert(model2.scale ~== scalePy relTol 1E-3)
  }

  test("linear regression (huber loss) without intercept without regularization") {
    val trainer1 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(false).setStandardization(true)
    val trainer2 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(false).setStandardization(false)

    val model1 = trainer1.fit(datasetWithOutlier)
    val model2 = trainer2.fit(datasetWithOutlier)

    /*
      huber = HuberRegressor(fit_intercept=False, alpha=0.0, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 6.71756703,  5.08873222])
      >>> huber.intercept_
      0.0
      >>> huber.scale_
      2.5560209922722317
     */
    val coefficientsPy = Vectors.dense(6.71756703, 5.08873222)
    val interceptPy = 0.0
    val scalePy = 2.55602099

    assert(model1.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model1.intercept === interceptPy)
    assert(model1.scale ~== scalePy relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.coefficients ~= coefficientsPy relTol 1E-3)
    assert(model2.intercept === interceptPy)
    assert(model2.scale ~== scalePy relTol 1E-3)
  }

  test("linear regression (huber loss) with intercept with L2 regularization") {
    val trainer1 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(true).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(true).setRegParam(0.21).setStandardization(false)

    val model1 = trainer1.fit(datasetWithOutlier)
    val model2 = trainer2.fit(datasetWithOutlier)

    /*
      Since scikit-learn HuberRegressor does not support standardization,
      we do it manually out of the estimator.

      xStd = np.std(X, axis=0)
      scaledX = X / xStd
      huber = HuberRegressor(fit_intercept=True, alpha=210, max_iter=100, epsilon=1.35)
      huber.fit(scaledX, y)

      >>> np.array(huber.coef_ / xStd)
      array([ 1.97732633,  3.38816722])
      >>> huber.intercept_
      3.7527581430531227
      >>> huber.scale_
      3.787363673371801
     */
    val coefficientsPy1 = Vectors.dense(1.97732633, 3.38816722)
    val interceptPy1 = 3.75275814
    val scalePy1 = 3.78736367

    assert(model1.coefficients ~= coefficientsPy1 relTol 1E-2)
    assert(model1.intercept ~== interceptPy1 relTol 1E-2)
    assert(model1.scale ~== scalePy1 relTol 1E-2)

    /*
      huber = HuberRegressor(fit_intercept=True, alpha=210, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 1.73346444,  3.63746999])
      >>> huber.intercept_
      4.3017134790781739
      >>> huber.scale_
      3.6472742809286793
     */
    val coefficientsPy2 = Vectors.dense(1.73346444, 3.63746999)
    val interceptPy2 = 4.30171347
    val scalePy2 = 3.64727428

    assert(model2.coefficients ~= coefficientsPy2 relTol 1E-3)
    assert(model2.intercept ~== interceptPy2 relTol 1E-3)
    assert(model2.scale ~== scalePy2 relTol 1E-3)
  }

  test("linear regression (huber loss) without intercept with L2 regularization") {
    val trainer1 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(false).setRegParam(0.21).setStandardization(true)
    val trainer2 = (new LinearRegression).setLoss("huber")
      .setFitIntercept(false).setRegParam(0.21).setStandardization(false)

    val model1 = trainer1.fit(datasetWithOutlier)
    val model2 = trainer2.fit(datasetWithOutlier)

    /*
      Since scikit-learn HuberRegressor does not support standardization,
      we do it manually out of the estimator.

      xStd = np.std(X, axis=0)
      scaledX = X / xStd
      huber = HuberRegressor(fit_intercept=False, alpha=210, max_iter=100, epsilon=1.35)
      huber.fit(scaledX, y)

      >>> np.array(huber.coef_ / xStd)
      array([ 2.59679008,  2.26973102])
      >>> huber.intercept_
      0.0
      >>> huber.scale_
      4.5766311924091791
     */
    val coefficientsPy1 = Vectors.dense(2.59679008, 2.26973102)
    val interceptPy1 = 0.0
    val scalePy1 = 4.57663119

    assert(model1.coefficients ~= coefficientsPy1 relTol 1E-2)
    assert(model1.intercept === interceptPy1)
    assert(model1.scale ~== scalePy1 relTol 1E-2)

    /*
      huber = HuberRegressor(fit_intercept=False, alpha=210, max_iter=100, epsilon=1.35)
      huber.fit(X, y)

      >>> huber.coef_
      array([ 2.28423908,  2.25196887])
      >>> huber.intercept_
      0.0
      >>> huber.scale_
      4.5979643506051753
     */
    val coefficientsPy2 = Vectors.dense(2.28423908, 2.25196887)
    val interceptPy2 = 0.0
    val scalePy2 = 4.59796435

    assert(model2.coefficients ~= coefficientsPy2 relTol 1E-3)
    assert(model2.intercept === interceptPy2)
    assert(model2.scale ~== scalePy2 relTol 1E-3)
  }

  test("huber loss model match squared error for large epsilon") {
    val trainer1 = new LinearRegression().setLoss("huber").setEpsilon(1E5)
    val model1 = trainer1.fit(datasetWithOutlier)
    val trainer2 = new LinearRegression()
    val model2 = trainer2.fit(datasetWithOutlier)
    assert(model1.coefficients ~== model2.coefficients relTol 1E-3)
    assert(model1.intercept ~== model2.intercept relTol 1E-3)
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
