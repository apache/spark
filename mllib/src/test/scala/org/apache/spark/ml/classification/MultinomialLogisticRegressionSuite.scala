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

import scala.language.existentials

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class MultinomialLogisticRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  @transient var dataset: Dataset[_] = _
  @transient var multinomialDataset: DataFrame = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

    dataset = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.057)
      val xVariance = Array(0.6856, 0.1899)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, 42)

      val df = spark.createDataFrame(sc.parallelize(testData, 4))
      df.cache()
      df
    }

    multinomialDataset = {
      val nPoints = 10000
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
        -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, 42)

      val df = spark.createDataFrame(sc.parallelize(testData, 4))
      df.cache()
      df
    }
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glmnet package.
   */
  ignore("export test data into CSV format") {
    val rdd = multinomialDataset.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1)
    rdd.saveAsTextFile("target/tmp/MultinomialLogisticRegressionSuite/multinomialDataset")
  }

//  test("params") {
//    ParamsSuite.checkParams(new LogisticRegression)
//    val model = new LogisticRegressionModel("mLogReg",
//      Matrices.dense(2, 1, Array(0.0, 0.0)), Vectors.dense(0.0, 0.0), 2, true)
//    ParamsSuite.checkParams(model)
//  }
//
//  test("multinomial logistic regression: default params") {
//    val mlr = new LogisticRegression
//    assert(mlr.getLabelCol === "label")
//    assert(mlr.getFeaturesCol === "features")
//    assert(mlr.getPredictionCol === "prediction")
//    assert(mlr.getRawPredictionCol === "rawPrediction")
//    assert(mlr.getProbabilityCol === "probability")
//    assert(!mlr.isDefined(mlr.weightCol))
//    assert(!mlr.isDefined(mlr.thresholds))
//    assert(mlr.getFitIntercept)
//    assert(mlr.getStandardization)
//    val model = mlr.fit(dataset)
//    model.transform(dataset)
//      .select("label", "probability", "prediction", "rawPrediction")
//      .collect()
//    assert(model.getFeaturesCol === "features")
//    assert(model.getPredictionCol === "prediction")
//    assert(model.getRawPredictionCol === "rawPrediction")
//    assert(model.getProbabilityCol === "probability")
//    assert(model.interceptVector !== Vectors.dense(0.0, 0.0))
//    assert(model.hasParent)
//  }

  test("multinomial logistic regression with intercept without regularization") {

    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(true).setMaxIter(100)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(false)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.
       > library("glmnet")
       > data <- read.csv("path", header=FALSE)
       > label = as.factor(data$V1)
       > features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       > coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0, lambda = 0))
       > coefficients
        $`0`
        5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
           -2.24493379
        V2  0.25096771
        V3 -0.03915938
        V4  0.14766639
        V5  0.36810817
        $`1`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
            0.3778931
        V2 -0.3327489
        V3  0.8893666
        V4 -0.2306948
        V5 -0.4442330
        $`2`
        5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
            1.86704066
        V2  0.08178121
        V3 -0.85020722
        V4  0.08302840
        V5  0.07612480
     */

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.2509677, -0.0391594, 0.1476664, 0.3681082,
      -0.3327489, 0.8893666, -0.2306948, -0.4442330,
      0.0817812, -0.8502072, 0.0830284, 0.0761248), isTransposed = true)
    val interceptsR = Vectors.dense(-2.2449338, 0.3778931, 1.8670407)

    assert(model1.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model1.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model1.interceptVector ~== interceptsR relTol 0.05)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model2.interceptVector ~== interceptsR relTol 0.05)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept without regularization") {

    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(false)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
       Using the following R code to load the data and train the model using glmnet package.
       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = as.factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0, lambda = 0,
        intercept=F))
       > coefficients
        $`0`
        5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
            .
        V2  0.06992464
        V3 -0.36562784
        V4  0.12142680
        V5  0.32052211
        $`1`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
            .
        V2 -0.3036269
        V3  0.9449630
        V4 -0.2271038
        V5 -0.4364839
        $`2`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
            .
        V2  0.2337022
        V3 -0.5793351
        V4  0.1056770
        V5  0.1159618
     */

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0699246, -0.3656278, 0.1214268, 0.3205221,
      -0.3036269, 0.9449630, -0.2271038, -0.4364839,
      0.2337022, -0.5793351, 0.1056770, 0.1159618), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model1.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept with L1 regularization") {

    // use tighter constraints because OWL-QN solver takes longer to converge
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(true)
      .setMaxIter(300).setTol(1e-10)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false)
      .setMaxIter(300).setTol(1e-10)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
       Use the following R code to load the data and train the model using glmnet package.
       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = as.factor(data$V1)
       features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
       coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 1,
        lambda = 0.05, standardization=T))
       coefficients = coef(glmnet(features, label, family="multinomial", alpha = 1, lambda = 0.05,
        standardization=F))
       > coefficientsStd
        $`0`
        5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
           -0.68988825
        V2  .
        V3  .
        V4  .
        V5  0.09404023

        $`1`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
           -0.2303499
        V2 -0.1232443
        V3  0.3258380
        V4 -0.1564688
        V5 -0.2053965

        $`2`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
            0.9202381
        V2  .
        V3 -0.4803856
        V4  .
        V5  .

       > coefficients
        $`0`
        5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
           -0.44893320
        V2  .
        V3  .
        V4  0.01933812
        V5  0.03666044

        $`1`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
            0.7376760
        V2 -0.0577182
        V3  .
        V4 -0.2081718
        V5 -0.1304592

        $`2`
        5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
           -0.2887428
        V2  .
        V3  .
        V4  .
        V5  .
     */

    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.09404023,
      -0.1232443, 0.3258380, -0.1564688, -0.2053965,
      0.0, -0.4803856, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.68988825, -0.2303499, 0.9202381)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.01933812, 0.03666044,
      -0.0577182, 0.0, -0.2081718, -0.1304592,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.44893320, 0.7376760, -0.2887428)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.02)
    assert(model1.interceptVector ~== interceptsRStd relTol 0.1)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.02)
    assert(model2.interceptVector ~== interceptsR relTol 0.1)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 1,
      lambda = 0.05, intercept=F, standardization=T))
      coefficients = coef(glmnet(features, label, family="multinomial", alpha = 1, lambda = 0.05,
      intercept=F, standardization=F))
      > coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
         .
      V2 .
      V3 .
      V4 .
      V5 0.01525105

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          .
      V2 -0.1502410
      V3  0.5134658
      V4 -0.1601146
      V5 -0.2500232

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
         .
      V2 0.003301875
      V3 .
      V4 .
      V5 .

      > coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
         s0
          .
      V2  .
      V3  .
      V4  .
      V5  .

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          .
      V2  .
      V3  0.1943624
      V4 -0.1902577
      V5 -0.1028789

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
         s0
          .
      V2  .
      V3  .
      V4  .
      V5  .
     */

    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.01525105,
      -0.1502410, 0.5134658, -0.1601146, -0.2500232,
      0.003301875, 0.0, 0.0, 0.0), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.1943624, -0.1902577, -0.1028789,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(false)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=T, standardization=T))
      coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=T, standardization=F))
      > coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
         -1.70040424
      V2  0.17576070
      V3  0.01527894
      V4  0.10216108
      V5  0.26099531

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          0.2438590
      V2 -0.2238875
      V3  0.5967610
      V4 -0.1555496
      V5 -0.3010479

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          1.45654525
      V2  0.04812679
      V3 -0.61203992
      V4  0.05338850
      V5  0.04005258

      > coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
         -1.65488543
      V2  0.15715048
      V3  0.01992903
      V4  0.12428858
      V5  0.22130317

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          1.1297533
      V2 -0.1974768
      V3  0.2776373
      V4 -0.1869445
      V5 -0.2510320

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          0.52513212
      V2  0.04032627
      V3 -0.29756637
      V4  0.06265594
      V5  0.02972883
     */

    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.17576070, 0.01527894, 0.10216108, 0.26099531,
      -0.2238875, 0.5967610, -0.1555496, -0.3010479,
      0.04812679, -0.61203992, 0.05338850, 0.04005258), isTransposed = true)
    val interceptsRStd = Vectors.dense(-1.70040424, 0.2438590, 1.45654525)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.15715048, 0.01992903, 0.12428858, 0.22130317,
      -0.1974768, 0.2776373, -0.1869445, -0.2510320,
      0.04032627, -0.29756637, 0.06265594, 0.02972883), isTransposed = true)
    val interceptsR = Vectors.dense(-1.65488543, 1.1297533, 0.52513212)

    assert(model1.coefficientMatrix ~== coefficientsRStd relTol 0.05)
    assert(model1.interceptVector ~== interceptsRStd relTol 0.05)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.interceptVector ~== interceptsR relTol 0.05)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(false)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=F, standardization=T))
      coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=F, standardization=F))
      > coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          .
      V2  0.03904171
      V3 -0.23354322
      V4  0.08288096
      V5  0.22706393

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          .
      V2 -0.2061848
      V3  0.6341398
      V4 -0.1530059
      V5 -0.2958455

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          .
      V2  0.16714312
      V3 -0.40059658
      V4  0.07012496
      V5  0.06878158
      > coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                   s0
          .
      V2 -0.005704542
      V3 -0.144466409
      V4  0.092080736
      V5  0.182927657

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          .
      V2 -0.08469036
      V3  0.38996748
      V4 -0.16468436
      V5 -0.22522976

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          .
      V2  0.09039490
      V3 -0.24550107
      V4  0.07260362
      V5  0.04230210
     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.03904171, -0.23354322, 0.08288096, 0.2270639,
      -0.2061848, 0.6341398, -0.1530059, -0.2958455,
      0.16714312, -0.40059658, 0.07012496, 0.06878158), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      -0.005704542, -0.144466409, 0.092080736, 0.182927657,
      -0.08469036, 0.38996748, -0.16468436, -0.22522976,
      0.0903949, -0.24550107, 0.07260362, 0.0423021), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setMaxIter(300).setTol(1e-10)
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setMaxIter(300).setTol(1e-10)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=T, standardization=T))
      coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=T, standardization=F))
      > coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                    s0
         -0.5521819483
      V2  0.0003092611
      V3  .
      V4  .
      V5  0.0913818490

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
         -0.27531989
      V2 -0.09790029
      V3  0.28502034
      V4 -0.12416487
      V5 -0.16513373

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          0.8275018
      V2  .
      V3 -0.4044859
      V4  .
      V5  .

      > coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
         -0.39876213
      V2  .
      V3  .
      V4  0.02547520
      V5  0.03893991

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          0.61089869
      V2 -0.04224269
      V3  .
      V4 -0.18923970
      V5 -0.09104249

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
         -0.2121366
      V2  .
      V3  .
      V4  .
      V5  .
     */

    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0003092611, 0.0, 0.0, 0.091381849,
      -0.09790029, 0.28502034, -0.12416487, -0.16513373,
      0.0, -0.4044859, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.5521819483, -0.27531989, 0.8275018)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0254752, 0.03893991,
      -0.04224269, 0.0, -0.1892397, -0.09104249,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.39876213, 0.61089869, -0.2121366)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector ~== interceptsRStd absTol 0.01)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector ~== interceptsR absTol 0.01)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setMaxIter(300).setTol(1e-10)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setMaxIter(300).setTol(1e-10)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      features = as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      coefficientsStd = coef(glmnet(features, label, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=F, standardization=T))
      coefficients = coef(glmnet(features, label, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=F, standardization=F))
      > coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
         .
      V2 .
      V3 .
      V4 .
      V5 0.03543706

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
          .
      V2 -0.1187387
      V3  0.4025482
      V4 -0.1270969
      V5 -0.1918386

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                 s0
         .
      V2 0.00774365
      V3 .
      V4 .
      V5 .

      > coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
         s0
          .
      V2  .
      V3  .
      V4  .
      V5  .

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                  s0
          .
      V2  .
      V3  0.14666497
      V4 -0.16570638
      V5 -0.05982875

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
         s0
          .
      V2  .
      V3  .
      V4  .
      V5  .
     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.03543706,
      -0.1187387, 0.4025482, -0.1270969, -0.1918386,
      0.0, 0.0, 0.0, 0.00774365), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.14666497, -0.16570638, -0.05982875,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

//  test("read/write") {
//    def checkModelData(
//        model: LogisticRegressionModel,
//        model2: LogisticRegressionModel): Unit = {
//      assert(model.interceptVector === model2.interceptVector)
//      assert(model.coefficientMatrix.toArray === model2.coefficients.toArray)
//      assert(model.numClasses === model2.numClasses)
//      assert(model.numFeatures === model2.numFeatures)
//    }
//    val mlr = new LogisticRegression()
//    testEstimatorAndModelReadWrite(mlr, dataset,
//      MultinomialLogisticRegressionSuite.allParamSettings,
//      checkModelData)
//  }
}

object MultinomialLogisticRegressionSuite {

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = ProbabilisticClassifierSuite.allParamSettings ++ Map(
    "probabilityCol" -> "myProbability",
    "thresholds" -> Array(0.4, 0.6),
    "regParam" -> 0.01,
    "elasticNetParam" -> 0.1,
    "maxIter" -> 2, // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false
  )
}
