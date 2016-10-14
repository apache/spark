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

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.util.Random
import scala.util.control.Breaks._

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, SparseMatrix, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamsSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.types.LongType

class LogisticRegressionSuite
  extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 42
  @transient var smallBinaryDataset: Dataset[_] = _
  @transient var smallMultinomialDataset: Dataset[_] = _
  @transient var binaryDataset: Dataset[_] = _
  @transient var multinomialDataset: Dataset[_] = _
  private val eps: Double = 1e-5

  override def beforeAll(): Unit = {
    super.beforeAll()

    smallBinaryDataset = generateLogisticInput(1.0, 1.0, nPoints = 100, seed = seed).toDF()

    smallMultinomialDataset = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.057)
      val xVariance = Array(0.6856, 0.1899)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF()
      df.cache()
      df
    }

    binaryDataset = {
      val nPoints = 10000
      val coefficients = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData =
        generateMultinomialLogisticInput(coefficients, xMean, xVariance,
          addIntercept = true, nPoints, seed)

      sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
    }

    multinomialDataset = {
      val nPoints = 10000
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
        -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

      val xMean = Array(5.843, 3.057, 3.758, 1.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.581)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glmnet package.
   */
  ignore("export test data into CSV format") {
    binaryDataset.rdd.map { case Row(label: Double, features: Vector, weight: Double) =>
      label + "," + weight + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LogisticRegressionSuite/binaryDataset")
    multinomialDataset.rdd.map { case Row(label: Double, features: Vector, weight: Double) =>
      label + "," + weight + "," + features.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LogisticRegressionSuite/multinomialDataset")
  }

  test("params") {
    ParamsSuite.checkParams(new LogisticRegression)
    val model = new LogisticRegressionModel("logReg", Vectors.dense(0.0), 0.0)
    ParamsSuite.checkParams(model)
  }

  test("logistic regression: default params") {
    val lr = new LogisticRegression
    assert(lr.getLabelCol === "label")
    assert(lr.getFeaturesCol === "features")
    assert(lr.getPredictionCol === "prediction")
    assert(lr.getRawPredictionCol === "rawPrediction")
    assert(lr.getProbabilityCol === "probability")
    assert(lr.getFamily === "auto")
    assert(!lr.isDefined(lr.weightCol))
    assert(lr.getFitIntercept)
    assert(lr.getStandardization)
    val model = lr.fit(smallBinaryDataset)
    model.transform(smallBinaryDataset)
      .select("label", "probability", "prediction", "rawPrediction")
      .collect()
    assert(model.getThreshold === 0.5)
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")
    assert(model.getProbabilityCol === "probability")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)
  }

  test("empty probabilityCol") {
    val lr = new LogisticRegression().setProbabilityCol("")
    val model = lr.fit(smallBinaryDataset)
    assert(model.hasSummary)
    // Validate that we re-insert a probability column for evaluation
    val fieldNames = model.summary.predictions.schema.fieldNames
    assert(smallBinaryDataset.schema.fieldNames.toSet.subsetOf(
      fieldNames.toSet))
    assert(fieldNames.exists(s => s.startsWith("probability_")))
  }

  test("setThreshold, getThreshold") {
    val lr = new LogisticRegression().setFamily("binomial")
    // default
    assert(lr.getThreshold === 0.5, "LogisticRegression.threshold should default to 0.5")
    withClue("LogisticRegression should not have thresholds set by default.") {
      intercept[java.util.NoSuchElementException] { // Note: The exception type may change in future
        lr.getThresholds
      }
    }
    // Set via threshold.
    // Intuition: Large threshold or large thresholds(1) makes class 0 more likely.
    lr.setThreshold(1.0)
    assert(lr.getThresholds === Array(0.0, 1.0))
    lr.setThreshold(0.0)
    assert(lr.getThresholds === Array(1.0, 0.0))
    lr.setThreshold(0.5)
    assert(lr.getThresholds === Array(0.5, 0.5))
    // Set via thresholds
    val lr2 = new LogisticRegression().setFamily("binomial")
    lr2.setThresholds(Array(0.3, 0.7))
    val expectedThreshold = 1.0 / (1.0 + 0.3 / 0.7)
    assert(lr2.getThreshold ~== expectedThreshold relTol 1E-7)
    // thresholds and threshold must be consistent
    lr2.setThresholds(Array(0.1, 0.2, 0.3))
    withClue("getThreshold should throw error if thresholds has length != 2.") {
      intercept[IllegalArgumentException] {
        lr2.getThreshold
      }
    }
    // thresholds and threshold must be consistent: values
    withClue("fit with ParamMap should throw error if threshold, thresholds do not match.") {
      intercept[IllegalArgumentException] {
        val lr2model = lr2.fit(smallBinaryDataset,
          lr2.thresholds -> Array(0.3, 0.7), lr2.threshold -> (expectedThreshold / 2.0))
        lr2model.getThreshold
      }
    }
  }

  test("thresholds prediction") {
    val blr = new LogisticRegression().setFamily("binomial")
    val binaryModel = blr.fit(smallBinaryDataset)

    binaryModel.setThreshold(1.0)
    val binaryZeroPredictions =
      binaryModel.transform(smallBinaryDataset).select("prediction").collect()
    assert(binaryZeroPredictions.forall(_.getDouble(0) === 0.0))

    binaryModel.setThreshold(0.0)
    val binaryOnePredictions =
      binaryModel.transform(smallBinaryDataset).select("prediction").collect()
    assert(binaryOnePredictions.forall(_.getDouble(0) === 1.0))


    val mlr = new LogisticRegression().setFamily("multinomial")
    val model = mlr.fit(smallMultinomialDataset)
    val basePredictions = model.transform(smallMultinomialDataset).select("prediction").collect()

    // should predict all zeros
    model.setThresholds(Array(1, 1000, 1000))
    val zeroPredictions = model.transform(smallMultinomialDataset).select("prediction").collect()
    assert(zeroPredictions.forall(_.getDouble(0) === 0.0))

    // should predict all ones
    model.setThresholds(Array(1000, 1, 1000))
    val onePredictions = model.transform(smallMultinomialDataset).select("prediction").collect()
    assert(onePredictions.forall(_.getDouble(0) === 1.0))

    // should predict all twos
    model.setThresholds(Array(1000, 1000, 1))
    val twoPredictions = model.transform(smallMultinomialDataset).select("prediction").collect()
    assert(twoPredictions.forall(_.getDouble(0) === 2.0))

    // constant threshold scaling is the same as no thresholds
    model.setThresholds(Array(1000, 1000, 1000))
    val scaledPredictions = model.transform(smallMultinomialDataset).select("prediction").collect()
    assert(scaledPredictions.zip(basePredictions).forall { case (scaled, base) =>
      scaled.getDouble(0) === base.getDouble(0)
    })

    // force it to use the predict method
    model.setRawPredictionCol("").setProbabilityCol("").setThresholds(Array(0, 1, 1))
    val predictionsWithPredict =
      model.transform(smallMultinomialDataset).select("prediction").collect()
    assert(predictionsWithPredict.forall(_.getDouble(0) === 0.0))
  }

  test("logistic regression doesn't fit intercept when fitIntercept is off") {
    val lr = new LogisticRegression().setFamily("binomial")
    lr.setFitIntercept(false)
    val model = lr.fit(smallBinaryDataset)
    assert(model.intercept === 0.0)

    val mlr = new LogisticRegression().setFamily("multinomial")
    mlr.setFitIntercept(false)
    val mlrModel = mlr.fit(smallMultinomialDataset)
    assert(mlrModel.interceptVector === Vectors.sparse(3, Seq()))

    // copied model must have the same parent.
    MLTestingUtils.checkCopy(model)
  }

  test("logistic regression with setters") {
    // Set params, train, and check as many params as we can.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setProbabilityCol("myProbability")
    val model = lr.fit(smallBinaryDataset)
    val parent = model.parent.asInstanceOf[LogisticRegression]
    assert(parent.getMaxIter === 10)
    assert(parent.getRegParam === 1.0)
    assert(parent.getThreshold === 0.6)
    assert(model.getThreshold === 0.6)

    // Modify model params, and check that the params worked.
    model.setThreshold(1.0)
    val predAllZero = model.transform(smallBinaryDataset)
      .select("prediction", "myProbability")
      .collect()
      .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predAllZero.forall(_ === 0),
      s"With threshold=1.0, expected predictions to be all 0, but only" +
      s" ${predAllZero.count(_ === 0)} of ${smallBinaryDataset.count()} were 0.")
    // Call transform with params, and check that the params worked.
    val predNotAllZero =
      model.transform(smallBinaryDataset, model.threshold -> 0.0,
        model.probabilityCol -> "myProb")
        .select("prediction", "myProb")
        .collect()
        .map { case Row(pred: Double, prob: Vector) => pred }
    assert(predNotAllZero.exists(_ !== 0.0))

    // Call fit() with new params, and check as many params as we can.
    lr.setThresholds(Array(0.6, 0.4))
    val model2 = lr.fit(smallBinaryDataset, lr.maxIter -> 5, lr.regParam -> 0.1,
      lr.probabilityCol -> "theProb")
    val parent2 = model2.parent.asInstanceOf[LogisticRegression]
    assert(parent2.getMaxIter === 5)
    assert(parent2.getRegParam === 0.1)
    assert(parent2.getThreshold === 0.4)
    assert(model2.getThreshold === 0.4)
    assert(model2.getProbabilityCol === "theProb")
  }

  test("multinomial logistic regression: Predictor, Classifier methods") {
    val sqlContext = smallMultinomialDataset.sqlContext
    import sqlContext.implicits._
    val mlr = new LogisticRegression().setFamily("multinomial")

    val model = mlr.fit(smallMultinomialDataset)
    assert(model.numClasses === 3)
    val numFeatures = smallMultinomialDataset.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)

    val results = model.transform(smallMultinomialDataset)
    // check that raw prediction is coefficients dot features + intercept
    results.select("rawPrediction", "features").collect().foreach {
      case Row(raw: Vector, features: Vector) =>
        assert(raw.size === 3)
        val margins = Array.tabulate(3) { k =>
          var margin = 0.0
          features.foreachActive { (index, value) =>
            margin += value * model.coefficientMatrix(k, index)
          }
          margin += model.interceptVector(k)
          margin
        }
        assert(raw ~== Vectors.dense(margins) relTol eps)
    }

    // Compare rawPrediction with probability
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 3)
        assert(prob.size === 3)
        val max = raw.toArray.max
        val subtract = if (max > 0) max else 0.0
        val sum = raw.toArray.map(x => math.exp(x - subtract)).sum
        val probFromRaw0 = math.exp(raw(0) - subtract) / sum
        val probFromRaw1 = math.exp(raw(1) - subtract) / sum
        assert(prob(0) ~== probFromRaw0 relTol eps)
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(2) ~== 1.0 - probFromRaw1 - probFromRaw0 relTol eps)
    }

    // Compare prediction with probability
    results.select("prediction", "probability").collect().foreach {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }

    // force it to use probability2prediction
    model.setProbabilityCol("")
    val resultsUsingProb2Predict =
      model.transform(smallMultinomialDataset).select("prediction").as[Double].collect()
    resultsUsingProb2Predict.zip(results.select("prediction").as[Double].collect()).foreach {
      case (pred1, pred2) => assert(pred1 === pred2)
    }

    // force it to use predict
    model.setRawPredictionCol("").setProbabilityCol("")
    val resultsUsingPredict =
      model.transform(smallMultinomialDataset).select("prediction").as[Double].collect()
    resultsUsingPredict.zip(results.select("prediction").as[Double].collect()).foreach {
      case (pred1, pred2) => assert(pred1 === pred2)
    }
  }

  test("binary logistic regression: Predictor, Classifier methods") {
    val sqlContext = smallBinaryDataset.sqlContext
    import sqlContext.implicits._
    val lr = new LogisticRegression().setFamily("binomial")

    val model = lr.fit(smallBinaryDataset)
    assert(model.numClasses === 2)
    val numFeatures = smallBinaryDataset.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)

    val results = model.transform(smallBinaryDataset)

    // Compare rawPrediction with probability
    results.select("rawPrediction", "probability").collect().foreach {
      case Row(raw: Vector, prob: Vector) =>
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
    }

    // Compare prediction with probability
    results.select("prediction", "probability").collect().foreach {
      case Row(pred: Double, prob: Vector) =>
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }

    // force it to use probability2prediction
    model.setProbabilityCol("")
    val resultsUsingProb2Predict =
      model.transform(smallBinaryDataset).select("prediction").as[Double].collect()
    resultsUsingProb2Predict.zip(results.select("prediction").as[Double].collect()).foreach {
      case (pred1, pred2) => assert(pred1 === pred2)
    }

    // force it to use predict
    model.setRawPredictionCol("").setProbabilityCol("")
    val resultsUsingPredict =
      model.transform(smallBinaryDataset).select("prediction").as[Double].collect()
    resultsUsingPredict.zip(results.select("prediction").as[Double].collect()).foreach {
      case (pred1, pred2) => assert(pred1 === pred2)
    }
  }

  test("coefficients and intercept methods") {
    val mlr = new LogisticRegression().setMaxIter(1).setFamily("multinomial")
    val mlrModel = mlr.fit(smallMultinomialDataset)
    val thrownCoef = intercept[SparkException] {
      mlrModel.coefficients
    }
    val thrownIntercept = intercept[SparkException] {
      mlrModel.intercept
    }
    assert(thrownCoef.getMessage().contains("use coefficientMatrix instead"))
    assert(thrownIntercept.getMessage().contains("use interceptVector instead"))

    val blr = new LogisticRegression().setMaxIter(1).setFamily("binomial")
    val blrModel = blr.fit(smallBinaryDataset)
    assert(blrModel.coefficients.size === 1)
    assert(blrModel.intercept !== 0.0)
  }

  test("overflow prediction for multiclass") {
    val model = new LogisticRegressionModel("mLogReg",
      Matrices.dense(3, 2, Array(0.0, 0.0, 0.0, 1.0, 2.0, 3.0)),
      Vectors.dense(0.0, 0.0, 0.0), 3, true)
    val overFlowData = Seq(
      LabeledPoint(1.0, Vectors.dense(0.0, 1000.0)),
      LabeledPoint(1.0, Vectors.dense(0.0, -1.0))
    ).toDF()
    val results = model.transform(overFlowData).select("rawPrediction", "probability").collect()

    // probabilities are correct when margins have to be adjusted
    val raw1 = results(0).getAs[Vector](0)
    val prob1 = results(0).getAs[Vector](1)
    assert(raw1 === Vectors.dense(1000.0, 2000.0, 3000.0))
    assert(prob1 ~== Vectors.dense(0.0, 0.0, 1.0) absTol eps)

    // probabilities are correct when margins don't have to be adjusted
    val raw2 = results(1).getAs[Vector](0)
    val prob2 = results(1).getAs[Vector](1)
    assert(raw2 === Vectors.dense(-1.0, -2.0, -3.0))
    assert(prob2 ~== Vectors.dense(0.66524096, 0.24472847, 0.09003057) relTol eps)
  }

  test("MultiClassSummarizer") {
    val summarizer1 = (new MultiClassSummarizer)
      .add(0.0).add(3.0).add(4.0).add(3.0).add(6.0)
    assert(summarizer1.histogram === Array[Double](1, 0, 0, 2, 1, 0, 1))
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0).add(5.0).add(3.0).add(0.0).add(4.0).add(1.0)
    assert(summarizer2.histogram === Array[Double](1, 2, 0, 1, 1, 1))
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer3 = (new MultiClassSummarizer)
      .add(0.0).add(1.3).add(5.2).add(2.5).add(2.0).add(4.0).add(4.0).add(4.0).add(1.0)
    assert(summarizer3.histogram === Array[Double](1, 1, 1, 0, 3))
    assert(summarizer3.countInvalid === 3)
    assert(summarizer3.numClasses === 5)

    val summarizer4 = (new MultiClassSummarizer)
      .add(3.1).add(4.3).add(2.0).add(1.0).add(3.0)
    assert(summarizer4.histogram === Array[Double](0, 1, 1, 1))
    assert(summarizer4.countInvalid === 2)
    assert(summarizer4.numClasses === 4)

    val summarizer5 = new MultiClassSummarizer
    assert(summarizer5.histogram.isEmpty)
    assert(summarizer5.numClasses === 0)

    // small map merges large one
    val summarizerA = summarizer1.merge(summarizer2)
    assert(summarizerA.hashCode() === summarizer2.hashCode())
    assert(summarizerA.histogram === Array[Double](2, 2, 0, 3, 2, 1, 1))
    assert(summarizerA.countInvalid === 0)
    assert(summarizerA.numClasses === 7)

    // large map merges small one
    val summarizerB = summarizer3.merge(summarizer4)
    assert(summarizerB.hashCode() === summarizer3.hashCode())
    assert(summarizerB.histogram === Array[Double](1, 2, 2, 1, 3))
    assert(summarizerB.countInvalid === 5)
    assert(summarizerB.numClasses === 5)
  }

  test("MultiClassSummarizer with weighted samples") {
    val summarizer1 = (new MultiClassSummarizer)
      .add(label = 0.0, weight = 0.2).add(3.0, 0.8).add(4.0, 3.2).add(3.0, 1.3).add(6.0, 3.1)
    assert(Vectors.dense(summarizer1.histogram) ~==
      Vectors.dense(Array(0.2, 0, 0, 2.1, 3.2, 0, 3.1)) absTol 1E-10)
    assert(summarizer1.countInvalid === 0)
    assert(summarizer1.numClasses === 7)

    val summarizer2 = (new MultiClassSummarizer)
      .add(1.0, 1.1).add(5.0, 2.3).add(3.0).add(0.0).add(4.0).add(1.0).add(2, 0.0)
    assert(Vectors.dense(summarizer2.histogram) ~==
      Vectors.dense(Array[Double](1.0, 2.1, 0.0, 1, 1, 2.3)) absTol 1E-10)
    assert(summarizer2.countInvalid === 0)
    assert(summarizer2.numClasses === 6)

    val summarizer = summarizer1.merge(summarizer2)
    assert(Vectors.dense(summarizer.histogram) ~==
      Vectors.dense(Array(1.2, 2.1, 0.0, 3.1, 4.2, 2.3, 3.1)) absTol 1E-10)
    assert(summarizer.countInvalid === 0)
    assert(summarizer.numClasses === 7)
  }

  test("binary logistic regression with intercept without regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0, lambda = 0))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  2.7355261
      data.V3     -0.5734389
      data.V4      0.8911736
      data.V5     -0.3878645
      data.V6     -0.8060570

    */
    val coefficientsR = Vectors.dense(-0.5734389, 0.8911736, -0.3878645, -0.8060570)
    val interceptR = 2.7355261

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.coefficients ~= coefficientsR relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-3)
  }

  test("binary logistic regression without intercept without regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false).setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false).setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 0, intercept=FALSE))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  .
      data.V3     -0.3448461
      data.V4      1.2776453
      data.V5     -0.3539178
      data.V6     -0.7469384

    */
    val coefficientsR = Vectors.dense(-0.3448461, 1.2776453, -0.3539178, -0.7469384)

    assert(model1.intercept ~== 0.0 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsR relTol 1E-2)

    // Without regularization, with or without standardization should converge to the same solution.
    assert(model2.intercept ~== 0.0 relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-2)
  }

  test("binary logistic regression with intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 1,
      lambda = 0.12, standardize=T))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept) -0.06775980
      data.V3      .
      data.V4      .
      data.V5     -0.03933146
      data.V6     -0.03047580

    */
    val coefficientsRStd = Vectors.dense(0.0, 0.0, -0.03933146, -0.03047580)
    val interceptRStd = -0.06775980

    assert(model1.intercept ~== interceptRStd relTol 1E-2)
    assert(model1.coefficients ~= coefficientsRStd absTol 2E-2)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 1,
      lambda = 0.12, standardize=F))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  0.3544768
      data.V3      .
      data.V4      .
      data.V5     -0.1626191
      data.V6      .

    */
    val coefficientsR = Vectors.dense(0.0, 0.0, -0.1626191, 0.0)
    val interceptR = 0.3544768

    assert(model2.intercept ~== interceptR relTol 1E-2)
    assert(model2.coefficients ~== coefficientsR absTol 1E-3)
    // TODO: move this to a standalone test of compression after SPARK-17471
    assert(model2.coefficients.isInstanceOf[SparseVector])
  }

  test("binary logistic regression without intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.12).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 1,
      lambda = 0.12, intercept=F, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 1,
      lambda = 0.12, intercept=F, standardize=F))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3      .
      data.V4      .
      data.V5     -0.04967635
      data.V6     -0.04757757

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3      .
      data.V4      .
      data.V5     -0.08433195
      data.V6      .

    */
    val coefficientsRStd = Vectors.dense(0.0, 0.0, -0.04967635, -0.04757757)

    val coefficientsR = Vectors.dense(0.0, 0.0, -0.08433195, 0.0)

    assert(model1.intercept ~== 0.0 absTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd absTol 1E-3)
    assert(model2.intercept ~== 0.0 absTol 1E-3)
    assert(model2.coefficients ~= coefficientsR absTol 1E-3)
  }

  test("binary logistic regression with intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 1.37, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 1.37, standardize=F))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  0.12707703
      data.V3     -0.06980967
      data.V4      0.10803933
      data.V5     -0.04800404
      data.V6     -0.10165096

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  0.46613016
      data.V3     -0.04944529
      data.V4      0.02326772
      data.V5     -0.11362772
      data.V6     -0.06312848

    */
    val coefficientsRStd = Vectors.dense(-0.06980967, 0.10803933, -0.04800404, -0.10165096)
    val interceptRStd = 0.12707703
    val coefficientsR = Vectors.dense(-0.04944529, 0.02326772, -0.11362772, -0.06312848)
    val interceptR = 0.46613016

    assert(model1.intercept ~== interceptRStd relTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd relTol 1E-3)
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-3)
  }

  test("binary logistic regression without intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(1.37).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 1.37, intercept=F, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 1.37, intercept=F, standardize=F))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3     -0.06000152
      data.V4      0.12598737
      data.V5     -0.04669009
      data.V6     -0.09941025

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
      (Intercept)  .
      data.V3     -0.005482255
      data.V4      0.048106338
      data.V5     -0.093411640
      data.V6     -0.054149798

    */
    val coefficientsRStd = Vectors.dense(-0.06000152, 0.12598737, -0.04669009, -0.09941025)
    val coefficientsR = Vectors.dense(-0.005482255, 0.048106338, -0.093411640, -0.054149798)

    assert(model1.intercept ~== 0.0 absTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd relTol 1E-2)
    assert(model2.intercept ~== 0.0 absTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-2)
  }

  test("binary logistic regression with intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setMaxIter(200)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0.38,
      lambda = 0.21, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0.38,
      lambda = 0.21, standardize=F))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  0.49991996
      data.V3     -0.04131110
      data.V4      .
      data.V5     -0.08585233
      data.V6     -0.15875400

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  0.5024256
      data.V3      .
      data.V4      .
      data.V5     -0.1846038
      data.V6     -0.0559614

    */
    val coefficientsRStd = Vectors.dense(-0.04131110, 0.0, -0.08585233, -0.15875400)
    val interceptRStd = 0.49991996
    val coefficientsR = Vectors.dense(0.0, 0.0, -0.1846038, -0.0559614)
    val interceptR = 0.5024256

    assert(model1.intercept ~== interceptRStd relTol 6E-3)
    assert(model1.coefficients ~== coefficientsRStd absTol 5E-3)
    assert(model2.intercept ~== interceptR relTol 6E-3)
    assert(model2.coefficients ~= coefficientsR absTol 1E-3)
  }

  test("binary logistic regression without intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0.38,
      lambda = 0.21, intercept=FALSE, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0.38,
      lambda = 0.21, intercept=FALSE, standardize=F))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3      .
      data.V4      0.06859390
      data.V5     -0.07900058
      data.V6     -0.14684320

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3      .
      data.V4      0.03060637
      data.V5     -0.11126742
      data.V6      .

    */
    val coefficientsRStd = Vectors.dense(0.0, 0.06859390, -0.07900058, -0.14684320)
    val coefficientsR = Vectors.dense(0.0, 0.03060637, -0.11126742, 0.0)

    assert(model1.intercept ~== 0.0 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd absTol 1E-2)
    assert(model2.intercept ~== 0.0 absTol 1E-3)
    assert(model2.coefficients ~= coefficientsR absTol 1E-3)
  }

  test("binary logistic regression with intercept with strong L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(false)

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    val histogram = binaryDataset.as[Instance].rdd.map { i => (i.label, i.weight)}
      .treeAggregate(new MultiClassSummarizer)(
        seqOp = (c, v) => (c, v) match {
          case (classSummarizer: MultiClassSummarizer, (label: Double, weight: Double)) =>
            classSummarizer.add(label, weight)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (classSummarizer1: MultiClassSummarizer, classSummarizer2: MultiClassSummarizer) =>
            classSummarizer1.merge(classSummarizer2)
        }).histogram

    /*
       For binary logistic regression with strong L1 regularization, all the coefficients
       will be zeros. As a result,
       {{{
       P(0) = 1 / (1 + \exp(b)), and
       P(1) = \exp(b) / (1 + \exp(b))
       }}}, hence
       {{{
       b = \log{P(1) / P(0)} = \log{count_1 / count_0}
       }}}
     */
    val interceptTheory = math.log(histogram(1) / histogram(0))
    val coefficientsTheory = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptTheory relTol 1E-5)
    assert(model1.coefficients ~= coefficientsTheory absTol 1E-6)

    assert(model2.intercept ~== interceptTheory relTol 1E-5)
    assert(model2.coefficients ~= coefficientsTheory absTol 1E-6)

    /*
       Using the following R code to load the data and train the model using glmnet package.

       library("glmnet")
       data <- read.csv("path", header=FALSE)
       label = factor(data$V1)
       w = data$V2
       features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
       coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 1.0,
       lambda = 6.0))
       coefficients

       5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
       (Intercept) -0.2516986
       data.V3      0.0000000
       data.V4      .
       data.V5      .
       data.V6      .
     */
    val interceptR = -0.2516986
    val coefficientsR = Vectors.dense(0.0, 0.0, 0.0, 0.0)

    assert(model1.intercept ~== interceptR relTol 1E-5)
    assert(model1.coefficients ~== coefficientsR absTol 1E-6)
  }

  test("multinomial logistic regression with intercept with strong L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(true)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(1.0).setRegParam(6.0).setStandardization(false)

    val sqlContext = multinomialDataset.sqlContext
    import sqlContext.implicits._
    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    val histogram = multinomialDataset.as[Instance].rdd.map(i => (i.label, i.weight))
      .treeAggregate(new MultiClassSummarizer)(
        seqOp = (c, v) => (c, v) match {
          case (classSummarizer: MultiClassSummarizer, (label: Double, weight: Double)) =>
            classSummarizer.add(label, weight)
        },
        combOp = (c1, c2) => (c1, c2) match {
          case (classSummarizer1: MultiClassSummarizer, classSummarizer2: MultiClassSummarizer) =>
            classSummarizer1.merge(classSummarizer2)
        }).histogram
    val numFeatures = multinomialDataset.as[Instance].first().features.size
    val numClasses = histogram.length

    /*
       For multinomial logistic regression with strong L1 regularization, all the coefficients
       will be zeros. As a result, the intercepts will be proportional to the log counts in the
       histogram.
       {{{
         \exp(b_k) = count_k * \exp(\lambda)
         b_k = \log(count_k) * \lambda
       }}}
       \lambda is a free parameter, so choose the phase \lambda such that the
       mean is centered. This yields
       {{{
         b_k = \log(count_k)
         b_k' = b_k - \mean(b_k)
       }}}
     */
    val rawInterceptsTheory = histogram.map(c => math.log(c + 1)) // add 1 for smoothing
    val rawMean = rawInterceptsTheory.sum / rawInterceptsTheory.length
    val interceptsTheory = Vectors.dense(rawInterceptsTheory.map(_ - rawMean))
    val coefficientsTheory = new DenseMatrix(numClasses, numFeatures,
      Array.fill[Double](numClasses * numFeatures)(0.0), isTransposed = true)

    assert(model1.interceptVector ~== interceptsTheory relTol 1E-3)
    assert(model1.coefficientMatrix ~= coefficientsTheory absTol 1E-6)

    assert(model2.interceptVector ~== interceptsTheory relTol 1E-3)
    assert(model2.coefficientMatrix ~= coefficientsTheory absTol 1E-6)
  }

  test("multinomial logistic regression with intercept without regularization") {

    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial",
      alpha = 0, lambda = 0))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -2.10320093
      data.V3  0.24337896
      data.V4 -0.05916156
      data.V5  0.14446790
      data.V6  0.35976165

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.3394473
      data.V3 -0.3443375
      data.V4  0.9181331
      data.V5 -0.2283959
      data.V6 -0.4388066

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               1.76375361
      data.V3  0.10095851
      data.V4 -0.85897154
      data.V5  0.08392798
      data.V6  0.07904499


    */
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.24337896, -0.05916156, 0.14446790, 0.35976165,
      -0.3443375, 0.9181331, -0.2283959, -0.4388066,
      0.10095851, -0.85897154, 0.08392798, 0.07904499), isTransposed = true)
    val interceptsR = Vectors.dense(-2.10320093, 0.3394473, 1.76375361)

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
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0,
      lambda = 0, intercept=F))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.07276291
      data.V4 -0.36325496
      data.V5  0.12015088
      data.V6  0.31397340

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.3180040
      data.V4  0.9679074
      data.V5 -0.2252219
      data.V6 -0.4319914

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3  0.2452411
      data.V4 -0.6046524
      data.V5  0.1050710
      data.V6  0.1180180


    */
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.07276291, -0.36325496, 0.12015088, 0.31397340,
      -0.3180040, 0.9679074, -0.2252219, -0.4319914,
      0.2452411, -0.6046524, 0.1050710, 0.1180180), isTransposed = true)

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
      .setMaxIter(300).setTol(1e-10).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false)
      .setMaxIter(300).setTol(1e-10).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial",
      alpha = 1, lambda = 0.05, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 1,
      lambda = 0.05, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.62244703
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  0.08419825

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.2804845
      data.V3 -0.1336960
      data.V4  0.3717091
      data.V5 -0.1530363
      data.V6 -0.2035286

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.9029315
      data.V3  .
      data.V4 -0.4629737
      data.V5  .
      data.V6  .


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.44215290
      data.V3  .
      data.V4  .
      data.V5  0.01767089
      data.V6  0.02542866

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.76308326
      data.V3 -0.06818576
      data.V4  .
      data.V5 -0.20446351
      data.V6 -0.13017924

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.3209304
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.08419825,
      -0.1336960, 0.3717091, -0.1530363, -0.2035286,
      0.0, -0.4629737, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.62244703, -0.2804845, 0.9029315)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.01767089, 0.02542866,
      -0.06818576, 0.0, -0.20446351, -0.13017924,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.44215290, 0.76308326, -0.3209304)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.02)
    assert(model1.interceptVector ~== interceptsRStd relTol 0.1)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.02)
    assert(model2.interceptVector ~== interceptsR relTol 0.1)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 1,
      lambda = 0.05, intercept=F, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 1,
      lambda = 0.05, intercept=F, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              .
      data.V3 .
      data.V4 .
      data.V5 .
      data.V6 0.01144225

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.1678787
      data.V4  0.5385351
      data.V5 -0.1573039
      data.V6 -0.2471624

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
              s0
               .
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
              s0
               .
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3  .
      data.V4  0.1929409
      data.V5 -0.1889121
      data.V6 -0.1010413

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
              s0
               .
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.01144225,
      -0.1678787, 0.5385351, -0.1573039, -0.2471624,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.1929409, -0.1889121, -0.1010413,
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
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame( data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial",
      alpha = 0, lambda = 0.1, intercept=T, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=T, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                         s0
              -1.5898288335
      data.V3  0.1691226336
      data.V4  0.0002983651
      data.V5  0.1001732896
      data.V6  0.2554575585

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.2125746
      data.V3 -0.2304586
      data.V4  0.6153492
      data.V5 -0.1537017
      data.V6 -0.2975443

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               1.37725427
      data.V3  0.06133600
      data.V4 -0.61564761
      data.V5  0.05352840
      data.V6  0.04208671


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -1.5681088
      data.V3  0.1508182
      data.V4  0.0121955
      data.V5  0.1217930
      data.V6  0.2162850

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               1.1217130
      data.V3 -0.2028984
      data.V4  0.2862431
      data.V5 -0.1843559
      data.V6 -0.2481218

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.44639579
      data.V3  0.05208012
      data.V4 -0.29843864
      data.V5  0.06256289
      data.V6  0.03183676


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.1691226336, 0.0002983651, 0.1001732896, 0.2554575585,
      -0.2304586, 0.6153492, -0.1537017, -0.2975443,
      0.06133600, -0.61564761, 0.05352840, 0.04208671), isTransposed = true)
    val interceptsRStd = Vectors.dense(-1.5898288335, 0.2125746, 1.37725427)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.1508182, 0.0121955, 0.1217930, 0.2162850,
      -0.2028984, 0.2862431, -0.1843559, -0.2481218,
      0.05208012, -0.29843864, 0.06256289, 0.03183676), isTransposed = true)
    val interceptsR = Vectors.dense(-1.5681088, 1.1217130, 0.44639579)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.001)
    assert(model1.interceptVector ~== interceptsRStd relTol 0.05)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.interceptVector ~== interceptsR relTol 0.05)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with L2 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(true).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(0.0).setRegParam(0.1).setStandardization(false).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=F, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0,
      lambda = 0.1, intercept=F, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.04048126
      data.V4 -0.23075758
      data.V5  0.08228864
      data.V6  0.22277648

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.2149745
      data.V4  0.6478666
      data.V5 -0.1515158
      data.V6 -0.2930498

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.17449321
      data.V4 -0.41710901
      data.V5  0.06922716
      data.V6  0.07027332


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                        s0
               .
      data.V3 -0.003949652
      data.V4 -0.142982415
      data.V5  0.091439598
      data.V6  0.179286241

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3 -0.09071124
      data.V4  0.39752531
      data.V5 -0.16233832
      data.V6 -0.22206059

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.09466090
      data.V4 -0.25454290
      data.V5  0.07089872
      data.V6  0.04277435


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.04048126, -0.23075758, 0.08228864, 0.22277648,
      -0.2149745, 0.6478666, -0.1515158, -0.2930498,
      0.17449321, -0.41710901, 0.06922716, 0.07027332), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      -0.003949652, -0.142982415, 0.091439598, 0.179286241,
      -0.09071124, 0.39752531, -0.16233832, -0.22206059,
      0.09466090, -0.25454290, 0.07089872, 0.04277435), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setMaxIter(300).setTol(1e-10)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setMaxIter(300).setTol(1e-10)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=T, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=T, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.50133383
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  0.08351653

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.3151913
      data.V3 -0.1058702
      data.V4  0.3183251
      data.V5 -0.1212969
      data.V6 -0.1629778

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.8165252
      data.V3  .
      data.V4 -0.3943069
      data.V5  .
      data.V6  .


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.38857157
      data.V3  .
      data.V4  .
      data.V5  0.02384198
      data.V6  0.03127749

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.62492165
      data.V3 -0.04949061
      data.V4  .
      data.V5 -0.18584462
      data.V6 -0.08952455

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.2363501
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.08351653,
      -0.1058702, 0.3183251, -0.1212969, -0.1629778,
      0.0, -0.3943069, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.50133383, -0.3151913, 0.8165252)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.02384198, 0.03127749,
      -0.04949061, 0.0, -0.18584462, -0.08952455,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.38857157, 0.62492165, -0.2363501)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector ~== interceptsRStd absTol 0.01)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector ~== interceptsR absTol 0.01)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setMaxIter(300).setTol(1e-10)
    val trainer2 = (new LogisticRegression).setFitIntercept(false).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setMaxIter(300).setTol(1e-10)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)
    /*
      Use the following R code to load the data and train the model using glmnet package.

      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = as.factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=F, standardize=T))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0.5,
      lambda = 0.1, intercept=F, standardize=F))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              .
      data.V3 .
      data.V4 .
      data.V5 .
      data.V6 0.03238285

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.1328284
      data.V4  0.4219321
      data.V5 -0.1247544
      data.V6 -0.1893318

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              .
      data.V3 0.004572312
      data.V4 .
      data.V5 .
      data.V6 .


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
              s0
               .
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  .
      data.V4  0.14571623
      data.V5 -0.16456351
      data.V6 -0.05866264

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
              s0
               .
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


    */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.03238285,
      -0.1328284, 0.4219321, -0.1247544, -0.1893318,
      0.004572312, 0.0, 0.0, 0.0), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.14571623, -0.16456351, -0.05866264,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("evaluate on test set") {
    // TODO: add for multiclass when model summary becomes available
    // Evaluate on test set should be same as that of the transformed training data.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
    val model = lr.fit(smallBinaryDataset)
    val summary = model.summary.asInstanceOf[BinaryLogisticRegressionSummary]

    val sameSummary =
      model.evaluate(smallBinaryDataset).asInstanceOf[BinaryLogisticRegressionSummary]
    assert(summary.areaUnderROC === sameSummary.areaUnderROC)
    assert(summary.roc.collect() === sameSummary.roc.collect())
    assert(summary.pr.collect === sameSummary.pr.collect())
    assert(
      summary.fMeasureByThreshold.collect() === sameSummary.fMeasureByThreshold.collect())
    assert(summary.recallByThreshold.collect() === sameSummary.recallByThreshold.collect())
    assert(
      summary.precisionByThreshold.collect() === sameSummary.precisionByThreshold.collect())
  }

  test("evaluate with labels that are not doubles") {
    // Evaluate a test set with Label that is a numeric type other than Double
    val lr = new LogisticRegression()
      .setMaxIter(1)
      .setRegParam(1.0)
    val model = lr.fit(smallBinaryDataset)
    val summary = model.evaluate(smallBinaryDataset).asInstanceOf[BinaryLogisticRegressionSummary]

    val longLabelData = smallBinaryDataset.select(col(model.getLabelCol).cast(LongType),
      col(model.getFeaturesCol))
    val longSummary = model.evaluate(longLabelData).asInstanceOf[BinaryLogisticRegressionSummary]

    assert(summary.areaUnderROC === longSummary.areaUnderROC)
  }

  test("statistics on training data") {
    // Test that loss is monotonically decreasing.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
    val model = lr.fit(smallBinaryDataset)
    assert(
      model.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))

  }

  test("binary logistic regression with weighted data") {
    val numClasses = 2
    val numPoints = 40
    val outlierData = MLTestingUtils.genClassificationInstancesWithWeightedOutliers(spark,
      numClasses, numPoints)
    val testData = Array.tabulate[LabeledPoint](numClasses) { i =>
      LabeledPoint(i.toDouble, Vectors.dense(i.toDouble))
    }.toSeq.toDF()
    val lr = new LogisticRegression().setFamily("binomial").setWeightCol("weight")
    val model = lr.fit(outlierData)
    val results = model.transform(testData).select("label", "prediction").collect()

    // check that the predictions are the one to one mapping
    results.foreach { case Row(label: Double, pred: Double) =>
      assert(label === pred)
    }
    val (overSampledData, weightedData) =
      MLTestingUtils.genEquivalentOversampledAndWeightedInstances(outlierData, "label", "features",
        42L)
    val weightedModel = lr.fit(weightedData)
    val overSampledModel = lr.setWeightCol("").fit(overSampledData)
    assert(weightedModel.coefficientMatrix ~== overSampledModel.coefficientMatrix relTol 0.01)
  }

  test("multinomial logistic regression with weighted data") {
    val numClasses = 5
    val numPoints = 40
    val outlierData = MLTestingUtils.genClassificationInstancesWithWeightedOutliers(spark,
      numClasses, numPoints)
    val testData = Array.tabulate[LabeledPoint](numClasses) { i =>
      LabeledPoint(i.toDouble, Vectors.dense(i.toDouble))
    }.toSeq.toDF()
    val mlr = new LogisticRegression().setFamily("multinomial").setWeightCol("weight")
    val model = mlr.fit(outlierData)
    val results = model.transform(testData).select("label", "prediction").collect()

    // check that the predictions are the one to one mapping
    results.foreach { case Row(label: Double, pred: Double) =>
      assert(label === pred)
    }
    val (overSampledData, weightedData) =
      MLTestingUtils.genEquivalentOversampledAndWeightedInstances(outlierData, "label", "features",
        42L)
    val weightedModel = mlr.fit(weightedData)
    val overSampledModel = mlr.setWeightCol("").fit(overSampledData)
    assert(weightedModel.coefficientMatrix ~== overSampledModel.coefficientMatrix relTol 0.01)
  }

  test("set family") {
    val lr = new LogisticRegression().setMaxIter(1)
    // don't set anything for binary classification
    val model1 = lr.fit(binaryDataset)
    assert(model1.coefficientMatrix.numRows === 1 && model1.coefficientMatrix.numCols === 4)
    assert(model1.interceptVector.size === 1)

    // set to multinomial for binary classification
    val model2 = lr.setFamily("multinomial").fit(binaryDataset)
    assert(model2.coefficientMatrix.numRows === 2 && model2.coefficientMatrix.numCols === 4)
    assert(model2.interceptVector.size === 2)

    // set to binary for binary classification
    val model3 = lr.setFamily("binomial").fit(binaryDataset)
    assert(model3.coefficientMatrix.numRows === 1 && model3.coefficientMatrix.numCols === 4)
    assert(model3.interceptVector.size === 1)

    // don't set anything for multiclass classification
    val mlr = new LogisticRegression().setMaxIter(1)
    val model4 = mlr.fit(multinomialDataset)
    assert(model4.coefficientMatrix.numRows === 3 && model4.coefficientMatrix.numCols === 4)
    assert(model4.interceptVector.size === 3)

    // set to binary for multiclass classification
    mlr.setFamily("binomial")
    val thrown = intercept[IllegalArgumentException] {
      mlr.fit(multinomialDataset)
    }
    assert(thrown.getMessage.contains("Binomial family only supports 1 or 2 outcome classes"))

    // set to multinomial for multiclass
    mlr.setFamily("multinomial")
    val model5 = mlr.fit(multinomialDataset)
    assert(model5.coefficientMatrix.numRows === 3 && model5.coefficientMatrix.numCols === 4)
    assert(model5.interceptVector.size === 3)
  }

  test("set initial model") {
    val lr = new LogisticRegression().setFamily("binomial")
    val model1 = lr.fit(smallBinaryDataset)
    val lr2 = new LogisticRegression().setInitialModel(model1).setMaxIter(5).setFamily("binomial")
    val model2 = lr2.fit(smallBinaryDataset)
    val predictions1 = model1.transform(smallBinaryDataset).select("prediction").collect()
    val predictions2 = model2.transform(smallBinaryDataset).select("prediction").collect()
    predictions1.zip(predictions2).foreach { case (Row(p1: Double), Row(p2: Double)) =>
      assert(p1 === p2)
    }
    assert(model2.summary.totalIterations === 1)

    val lr3 = new LogisticRegression().setFamily("multinomial")
    val model3 = lr3.fit(smallMultinomialDataset)
    val lr4 = new LogisticRegression()
      .setInitialModel(model3).setMaxIter(5).setFamily("multinomial")
    val model4 = lr4.fit(smallMultinomialDataset)
    val predictions3 = model3.transform(smallMultinomialDataset).select("prediction").collect()
    val predictions4 = model4.transform(smallMultinomialDataset).select("prediction").collect()
    predictions3.zip(predictions4).foreach { case (Row(p1: Double), Row(p2: Double)) =>
      assert(p1 === p2)
    }
    // TODO: check that it converges in a single iteration when model summary is available
  }

  test("binary logistic regression with all labels the same") {
    val sameLabels = smallBinaryDataset
      .withColumn("zeroLabel", lit(0.0))
      .withColumn("oneLabel", lit(1.0))

    // fitIntercept=true
    val lrIntercept = new LogisticRegression()
      .setFitIntercept(true)
      .setMaxIter(3)
      .setFamily("binomial")

    val allZeroInterceptModel = lrIntercept
      .setLabelCol("zeroLabel")
      .fit(sameLabels)
    assert(allZeroInterceptModel.coefficients ~== Vectors.dense(0.0) absTol 1E-3)
    assert(allZeroInterceptModel.intercept === Double.NegativeInfinity)
    assert(allZeroInterceptModel.summary.totalIterations === 0)

    val allOneInterceptModel = lrIntercept
      .setLabelCol("oneLabel")
      .fit(sameLabels)
    assert(allOneInterceptModel.coefficients ~== Vectors.dense(0.0) absTol 1E-3)
    assert(allOneInterceptModel.intercept === Double.PositiveInfinity)
    assert(allOneInterceptModel.summary.totalIterations === 0)

    // fitIntercept=false
    val lrNoIntercept = new LogisticRegression()
      .setFitIntercept(false)
      .setMaxIter(3)
      .setFamily("binomial")

    val allZeroNoInterceptModel = lrNoIntercept
      .setLabelCol("zeroLabel")
      .fit(sameLabels)
    assert(allZeroNoInterceptModel.intercept === 0.0)
    assert(allZeroNoInterceptModel.summary.totalIterations > 0)

    val allOneNoInterceptModel = lrNoIntercept
      .setLabelCol("oneLabel")
      .fit(sameLabels)
    assert(allOneNoInterceptModel.intercept === 0.0)
    assert(allOneNoInterceptModel.summary.totalIterations > 0)
  }

  test("multiclass logistic regression with all labels the same") {
    val constantData = Seq(
      LabeledPoint(4.0, Vectors.dense(0.0)),
      LabeledPoint(4.0, Vectors.dense(1.0)),
      LabeledPoint(4.0, Vectors.dense(2.0))).toDF()
    val mlr = new LogisticRegression().setFamily("multinomial")
    val model = mlr.fit(constantData)
    val results = model.transform(constantData)
    results.select("rawPrediction", "probability", "prediction").collect().foreach {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(raw === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, Double.PositiveInfinity)))
        assert(prob === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, 1.0)))
        assert(pred === 4.0)
    }

    // force the model to be trained with only one class
    val constantZeroData = Seq(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0))).toDF()
    val modelZeroLabel = mlr.setFitIntercept(false).fit(constantZeroData)
    val resultsZero = modelZeroLabel.transform(constantZeroData)
    resultsZero.select("rawPrediction", "probability", "prediction").collect().foreach {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(prob === Vectors.dense(Array(1.0)))
        assert(pred === 0.0)
    }

    // ensure that the correct value is predicted when numClasses passed through metadata
    val labelMeta = NominalAttribute.defaultAttr.withName("label").withNumValues(6).toMetadata()
    val constantDataWithMetadata = constantData
      .select(constantData("label").as("label", labelMeta), constantData("features"))
    val modelWithMetadata = mlr.setFitIntercept(true).fit(constantDataWithMetadata)
    val resultsWithMetadata = modelWithMetadata.transform(constantDataWithMetadata)
    resultsWithMetadata.select("rawPrediction", "probability", "prediction").collect().foreach {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(raw === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, Double.PositiveInfinity, 0.0)))
        assert(prob === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, 1.0, 0.0)))
        assert(pred === 4.0)
    }
    // TODO: check num iters is zero when it become available in the model
  }

  test("compressed storage") {
    val moreClassesThanFeatures = Seq(
      LabeledPoint(4.0, Vectors.dense(0.0, 0.0, 0.0)),
      LabeledPoint(4.0, Vectors.dense(1.0, 1.0, 1.0)),
      LabeledPoint(4.0, Vectors.dense(2.0, 2.0, 2.0))).toDF()
    val mlr = new LogisticRegression().setFamily("multinomial")
    val model = mlr.fit(moreClassesThanFeatures)
    assert(model.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model.coefficientMatrix.asInstanceOf[SparseMatrix].colPtrs.length === 4)
    val moreFeaturesThanClasses = Seq(
      LabeledPoint(1.0, Vectors.dense(0.0, 0.0, 0.0)),
      LabeledPoint(1.0, Vectors.dense(1.0, 1.0, 1.0)),
      LabeledPoint(1.0, Vectors.dense(2.0, 2.0, 2.0))).toDF()
    val model2 = mlr.fit(moreFeaturesThanClasses)
    assert(model2.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model2.coefficientMatrix.asInstanceOf[SparseMatrix].colPtrs.length === 3)

    val blr = new LogisticRegression().setFamily("binomial")
    val blrModel = blr.fit(moreFeaturesThanClasses)
    assert(blrModel.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(blrModel.coefficientMatrix.asInstanceOf[SparseMatrix].colPtrs.length === 2)
  }

  test("numClasses specified in metadata/inferred") {
    val lr = new LogisticRegression().setMaxIter(1).setFamily("multinomial")

    // specify more classes than unique label values
    val labelMeta = NominalAttribute.defaultAttr.withName("label").withNumValues(4).toMetadata()
    val df = smallMultinomialDataset.select(smallMultinomialDataset("label").as("label", labelMeta),
      smallMultinomialDataset("features"))
    val model1 = lr.fit(df)
    assert(model1.numClasses === 4)
    assert(model1.interceptVector.size === 4)

    // specify two classes when there are really three
    val labelMeta1 = NominalAttribute.defaultAttr.withName("label").withNumValues(2).toMetadata()
    val df1 = smallMultinomialDataset
      .select(smallMultinomialDataset("label").as("label", labelMeta1),
        smallMultinomialDataset("features"))
    val thrown = intercept[IllegalArgumentException] {
      lr.fit(df1)
    }
    assert(thrown.getMessage.contains("less than the number of unique labels"))

    // lr should infer the number of classes if not specified
    val model3 = lr.fit(smallMultinomialDataset)
    assert(model3.numClasses === 3)
  }

  test("read/write") {
    def checkModelData(model: LogisticRegressionModel, model2: LogisticRegressionModel): Unit = {
      assert(model.intercept === model2.intercept)
      assert(model.coefficients.toArray === model2.coefficients.toArray)
      assert(model.numClasses === model2.numClasses)
      assert(model.numFeatures === model2.numFeatures)
    }
    val lr = new LogisticRegression()
    testEstimatorAndModelReadWrite(lr, smallBinaryDataset, LogisticRegressionSuite.allParamSettings,
      checkModelData)
  }

  test("should support all NumericType labels and not support other types") {
    val lr = new LogisticRegression().setMaxIter(1)
    MLTestingUtils.checkNumericTypes[LogisticRegressionModel, LogisticRegression](
      lr, spark) { (expected, actual) =>
        assert(expected.intercept === actual.intercept)
        assert(expected.coefficients.toArray === actual.coefficients.toArray)
      }
  }
}

object LogisticRegressionSuite {

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
    "maxIter" -> 2,  // intentionally small
    "fitIntercept" -> true,
    "tol" -> 0.8,
    "standardization" -> false,
    "threshold" -> 0.6
  )

  def generateLogisticInputAsList(
    offset: Double,
    scale: Double,
    nPoints: Int,
    seed: Int): java.util.List[LabeledPoint] = {
    generateLogisticInput(offset, scale, nPoints, seed).asJava
  }

  // Generate input of the form Y = logistic(offset + scale*X)
  def generateLogisticInput(
      offset: Double,
      scale: Double,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)
    val x1 = Array.fill[Double](nPoints)(rnd.nextGaussian())

    val y = (0 until nPoints).map { i =>
      val p = 1.0 / (1.0 + math.exp(-(offset + scale * x1(i))))
      if (rnd.nextDouble() < p) 1.0 else 0.0
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), Vectors.dense(Array(x1(i)))))
    testData
  }

  /**
   * Generates `k` classes multinomial synthetic logistic input in `n` dimensional space given the
   * model weights and mean/variance of the features. The synthetic data will be drawn from
   * the probability distribution constructed by weights using the following formula.
   *
   * P(y = 0 | x) = 1 / norm
   * P(y = 1 | x) = exp(x * w_1) / norm
   * P(y = 2 | x) = exp(x * w_2) / norm
   * ...
   * P(y = k-1 | x) = exp(x * w_{k-1}) / norm
   * where norm = 1 + exp(x * w_1) + exp(x * w_2) + ... + exp(x * w_{k-1})
   *
   * @param weights matrix is flatten into a vector; as a result, the dimension of weights vector
   *                will be (k - 1) * (n + 1) if `addIntercept == true`, and
   *                if `addIntercept != true`, the dimension will be (k - 1) * n.
   * @param xMean the mean of the generated features. Lots of time, if the features are not properly
   *              standardized, the algorithm with poor implementation will have difficulty
   *              to converge.
   * @param xVariance the variance of the generated features.
   * @param addIntercept whether to add intercept.
   * @param nPoints the number of instance of generated data.
   * @param seed the seed for random generator. For consistent testing result, it will be fixed.
   */
  def generateMultinomialLogisticInput(
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      addIntercept: Boolean,
      nPoints: Int,
      seed: Int): Seq[LabeledPoint] = {
    val rnd = new Random(seed)

    val xDim = xMean.length
    val xWithInterceptsDim = if (addIntercept) xDim + 1 else xDim
    val nClasses = weights.length / xWithInterceptsDim + 1

    val x = Array.fill[Vector](nPoints)(Vectors.dense(Array.fill[Double](xDim)(rnd.nextGaussian())))

    x.foreach { vector =>
      // This doesn't work if `vector` is a sparse vector.
      val vectorArray = vector.toArray
      var i = 0
      val len = vectorArray.length
      while (i < len) {
        vectorArray(i) = vectorArray(i) * math.sqrt(xVariance(i)) + xMean(i)
        i += 1
      }
    }

    val y = (0 until nPoints).map { idx =>
      val xArray = x(idx).toArray
      val margins = Array.ofDim[Double](nClasses)
      val probs = Array.ofDim[Double](nClasses)

      for (i <- 0 until nClasses - 1) {
        for (j <- 0 until xDim) margins(i + 1) += weights(i * xWithInterceptsDim + j) * xArray(j)
        if (addIntercept) margins(i + 1) += weights((i + 1) * xWithInterceptsDim - 1)
      }
      // Preventing the overflow when we compute the probability
      val maxMargin = margins.max
      if (maxMargin > 0) for (i <- 0 until nClasses) margins(i) -= maxMargin

      // Computing the probabilities for each class from the margins.
      val norm = {
        var temp = 0.0
        for (i <- 0 until nClasses) {
          probs(i) = math.exp(margins(i))
          temp += probs(i)
        }
        temp
      }
      for (i <- 0 until nClasses) probs(i) /= norm

      // Compute the cumulative probability so we can generate a random number and assign a label.
      for (i <- 1 until nClasses) probs(i) += probs(i - 1)
      val p = rnd.nextDouble()
      var y = 0
      breakable {
        for (i <- 0 until nClasses) {
          if (p < probs(i)) {
            y = i
            break
          }
        }
      }
      y
    }

    val testData = (0 until nPoints).map(i => LabeledPoint(y(i), x(i)))
    testData
  }
}
