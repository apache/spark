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

import scala.util.Random

import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.ml.classification.LinearSVCSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.optim.aggregator.HingeAggregator
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.udf


class LinearSVCSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val nPoints = 50
  @transient var smallBinaryDataset: Dataset[_] = _
  @transient var smallValidationDataset: Dataset[_] = _
  @transient var binaryDataset: Dataset[_] = _

  @transient var smallSparseBinaryDataset: Dataset[_] = _
  @transient var smallSparseValidationDataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    // NOTE: Intercept should be small for generating equal 0s and 1s
    val A = 0.01
    val B = -1.5
    val C = 1.0
    smallBinaryDataset = generateSVMInput(A, Array[Double](B, C), nPoints, 42).toDF()
    smallValidationDataset = generateSVMInput(A, Array[Double](B, C), nPoints, 17).toDF()
    binaryDataset = generateSVMInput(1.0, Array[Double](1.0, 2.0, 3.0, 4.0), 10000, 42).toDF()

    // Dataset for testing SparseVector
    val toSparse: Vector => SparseVector = _.asInstanceOf[DenseVector].toSparse
    val sparse = udf(toSparse)
    smallSparseBinaryDataset = smallBinaryDataset.withColumn("features", sparse('features))
    smallSparseValidationDataset = smallValidationDataset.withColumn("features", sparse('features))

  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's e1071 package.
   */
  ignore("export test data into CSV format") {
    binaryDataset.rdd.map { case Row(label: Double, features: Vector) =>
      label + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LinearSVC/binaryDataset")
  }

  test("Linear SVC binary classification") {
    val svm = new LinearSVC()
    val model = svm.fit(smallBinaryDataset)
    assert(model.transform(smallValidationDataset)
      .where("prediction=label").count() > nPoints * 0.8)
    val sparseModel = svm.fit(smallSparseBinaryDataset)
    checkModels(model, sparseModel)
  }

  test("Linear SVC binary classification with regularization") {
    val svm = new LinearSVC()
    val model = svm.setRegParam(0.1).fit(smallBinaryDataset)
    assert(model.transform(smallValidationDataset)
      .where("prediction=label").count() > nPoints * 0.8)
    val sparseModel = svm.fit(smallSparseBinaryDataset)
    checkModels(model, sparseModel)
  }

  test("params") {
    ParamsSuite.checkParams(new LinearSVC)
    val model = new LinearSVCModel("linearSVC", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("linear svc: default params") {
    val lsvc = new LinearSVC()
    assert(lsvc.getRegParam === 0.0)
    assert(lsvc.getMaxIter === 100)
    assert(lsvc.getFitIntercept)
    assert(lsvc.getTol === 1E-6)
    assert(lsvc.getStandardization)
    assert(!lsvc.isDefined(lsvc.weightCol))
    assert(lsvc.getThreshold === 0.0)
    assert(lsvc.getAggregationDepth === 2)
    assert(lsvc.getLabelCol === "label")
    assert(lsvc.getFeaturesCol === "features")
    assert(lsvc.getPredictionCol === "prediction")
    assert(lsvc.getRawPredictionCol === "rawPrediction")
    val model = lsvc.setMaxIter(5).fit(smallBinaryDataset)
    model.transform(smallBinaryDataset)
      .select("label", "prediction", "rawPrediction")
      .collect()
    assert(model.getThreshold === 0.0)
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
    assert(model.numFeatures === 2)

    MLTestingUtils.checkCopyAndUids(lsvc, model)
  }

  test("LinearSVC threshold acts on rawPrediction") {
    val lsvc =
      new LinearSVCModel(uid = "myLSVCM", coefficients = Vectors.dense(1.0), intercept = 0.0)
    val df = spark.createDataFrame(Seq(
      (1, Vectors.dense(1e-7)),
      (0, Vectors.dense(0.0)),
      (-1, Vectors.dense(-1e-7)))).toDF("id", "features")

    def checkOneResult(
        model: LinearSVCModel,
        threshold: Double,
        expected: Set[(Int, Double)]): Unit = {
      model.setThreshold(threshold)
      testTransformerByGlobalCheckFunc[(Int, Vector)](df, model, "id", "prediction") {
        rows: Seq[Row] =>
          val results = rows.map(r => (r.getInt(0), r.getDouble(1))).toSet
          assert(results === expected, s"Failed for threshold = $threshold")
      }
    }

    def checkResults(threshold: Double, expected: Set[(Int, Double)]): Unit = {
      // Check via code path using Classifier.raw2prediction
      lsvc.setRawPredictionCol("rawPrediction")
      checkOneResult(lsvc, threshold, expected)
      // Check via code path using Classifier.predict
      lsvc.setRawPredictionCol("")
      checkOneResult(lsvc, threshold, expected)
    }

    checkResults(0.0, Set((1, 1.0), (0, 0.0), (-1, 0.0)))
    checkResults(Double.PositiveInfinity, Set((1, 0.0), (0, 0.0), (-1, 0.0)))
    checkResults(Double.NegativeInfinity, Set((1, 1.0), (0, 1.0), (-1, 1.0)))
  }

  test("linear svc doesn't fit intercept when fitIntercept is off") {
    val lsvc = new LinearSVC().setFitIntercept(false).setMaxIter(5)
    val model = lsvc.fit(smallBinaryDataset)
    assert(model.intercept === 0.0)

    val lsvc2 = new LinearSVC().setFitIntercept(true).setMaxIter(5)
    val model2 = lsvc2.fit(smallBinaryDataset)
    assert(model2.intercept !== 0.0)
  }

  test("sparse coefficients in HingeAggregator") {
    val bcCoefficients = spark.sparkContext.broadcast(Vectors.sparse(2, Array(0), Array(1.0)))
    val bcFeaturesStd = spark.sparkContext.broadcast(Array(1.0))
    val agg = new HingeAggregator(bcFeaturesStd, true)(bcCoefficients)
    val thrown = withClue("LinearSVCAggregator cannot handle sparse coefficients") {
      intercept[IllegalArgumentException] {
        agg.add(Instance(1.0, 1.0, Vectors.dense(1.0)))
      }
    }
    assert(thrown.getMessage.contains("coefficients only supports dense"))

    bcCoefficients.destroy()
    bcFeaturesStd.destroy()
  }

  test("linearSVC with sample weights") {
    def modelEquals(m1: LinearSVCModel, m2: LinearSVCModel): Unit = {
      assert(m1.coefficients ~== m2.coefficients absTol 0.05)
      assert(m1.intercept ~== m2.intercept absTol 0.05)
    }

    val estimator = new LinearSVC().setRegParam(0.01).setTol(0.01)
    val dataset = smallBinaryDataset
    MLTestingUtils.testArbitrarilyScaledWeights[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, modelEquals)
    MLTestingUtils.testOutliersWithSmallWeights[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, 2, modelEquals, outlierRatio = 3)
    MLTestingUtils.testOversamplingVsWeighting[LinearSVCModel, LinearSVC](
      dataset.as[LabeledPoint], estimator, modelEquals, 42L)
  }

  test("prediction on single instance") {
    val trainer = new LinearSVC()
    val model = trainer.fit(smallBinaryDataset)
    testPredictionModelSinglePrediction(model, smallBinaryDataset)
  }

  test("linearSVC comparison with R e1071 and scikit-learn") {
    val trainer1 = new LinearSVC()
      .setRegParam(0.00002) // set regParam = 2.0 / datasize / c
      .setMaxIter(200)
      .setTol(1e-4)
    val model1 = trainer1.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library(e1071)
      data <- read.csv("path/target/tmp/LinearSVC/binaryDataset/part-00000", header=FALSE)
      label <- factor(data$V1)
      features <- as.matrix(data.frame(data$V2, data$V3, data$V4, data$V5))
      svm_model <- svm(features, label, type='C', kernel='linear', cost=10, scale=F, tolerance=1e-4)
      w <- -t(svm_model$coefs) %*% svm_model$SV
      w
      svm_model$rho

      > w
            data.V2  data.V3  data.V4  data.V5
      [1,] 7.310338 14.89741 22.21005 29.83508
      > svm_model$rho
      [1] 7.440177

     */
    val coefficientsR = Vectors.dense(7.310338, 14.89741, 22.21005, 29.83508)
    val interceptR = 7.440177
    assert(model1.intercept ~== interceptR relTol 1E-2)
    assert(model1.coefficients ~== coefficientsR relTol 1E-2)

    /*
      Use the following python code to load the data and train the model using scikit-learn package.

      import numpy as np
      from sklearn import svm
      f = open("path/target/tmp/LinearSVC/binaryDataset/part-00000")
      data = np.loadtxt(f,  delimiter=",")
      X = data[:, 1:]  # select columns 1 through end
      y = data[:, 0]   # select column 0 as label
      clf = svm.LinearSVC(fit_intercept=True, C=10, loss='hinge', tol=1e-4, random_state=42)
      m = clf.fit(X, y)
      print m.coef_
      print m.intercept_

      [[  7.24690165  14.77029087  21.99924004  29.5575729 ]]
      [ 7.36947518]
     */

    val coefficientsSK = Vectors.dense(7.24690165, 14.77029087, 21.99924004, 29.5575729)
    val interceptSK = 7.36947518
    assert(model1.intercept ~== interceptSK relTol 1E-3)
    assert(model1.coefficients ~== coefficientsSK relTol 4E-3)
  }

  test("read/write: SVM") {
    def checkModelData(model: LinearSVCModel, model2: LinearSVCModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients === model2.coefficients)
      assert(model.numFeatures === model2.numFeatures)
    }
    val svm = new LinearSVC()
    testEstimatorAndModelReadWrite(svm, smallBinaryDataset, LinearSVCSuite.allParamSettings,
      LinearSVCSuite.allParamSettings, checkModelData)
  }
}

object LinearSVCSuite {

  val allParamSettings: Map[String, Any] = Map(
    "regParam" -> 0.01,
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "threshold" -> 0.6,
    "predictionCol" -> "myPredict",
    "rawPredictionCol" -> "myRawPredict",
    "aggregationDepth" -> 3
  )

  // Generate noisy input of the form Y = signum(x.dot(weights) + intercept + noise)
  def generateSVMInput(
      intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val weightsMat = new BDV(weights)
    val x = Array.fill[Array[Double]](nPoints)(
        Array.fill[Double](weights.length)(rnd.nextDouble() * 2.0 - 1.0))
    val y = x.map { xi =>
      val yD = new BDV(xi).dot(weightsMat) + intercept + 0.01 * rnd.nextGaussian()
      if (yD > 0) 1.0 else 0.0
    }
    y.zip(x).map(p => LabeledPoint(p._1, Vectors.dense(p._2)))
  }

  def checkModels(model1: LinearSVCModel, model2: LinearSVCModel): Unit = {
    assert(model1.intercept == model2.intercept)
    assert(model1.coefficients.equals(model2.coefficients))
  }

}

