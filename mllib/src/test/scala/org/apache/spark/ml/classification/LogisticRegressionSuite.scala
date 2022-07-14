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
import scala.util.Random
import scala.util.control.Breaks._

import org.scalatest.Assertions._

import org.apache.spark.SparkException
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.classification.LogisticRegressionSuite._
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{DenseMatrix, Matrices, Matrix, SparseMatrix, Vector, Vectors}
import org.apache.spark.ml.param.{ParamMap, ParamsSuite}
import org.apache.spark.ml.stat.MultiClassSummarizer
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, rand}
import org.apache.spark.sql.types.LongType

class LogisticRegressionSuite extends MLTest with DefaultReadWriteTest {

  import testImplicits._

  private val seed = 42
  @transient var smallBinaryDataset: DataFrame = _
  @transient var smallMultinomialDataset: DataFrame = _
  @transient var binaryDataset: DataFrame = _
  @transient var binaryDatasetWithSmallVar: DataFrame = _
  @transient var multinomialDataset: DataFrame = _
  @transient var multinomialDatasetWithSmallVar: DataFrame = _
  @transient var multinomialDatasetWithZeroVar: DataFrame = _
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

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }

    binaryDatasetWithSmallVar = {
      val nPoints = 10000
      val coefficients = Array(-0.57997, 0.912083, -0.371077, -0.819866, 2.688191)
      val xMean = Array(5.843, 3.057, 3.758, 10.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.0001)

      val testData =
        generateMultinomialLogisticInput(coefficients, xMean, xVariance,
          addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
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
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }

    multinomialDatasetWithSmallVar = {
      val nPoints = 50000
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077, -0.819866, 2.688191,
        -0.16624, -0.84355, -0.048509, -0.301789, 4.170682)

      val xMean = Array(5.843, 3.057, 3.758, 10.199)
      val xVariance = Array(0.6856, 0.1899, 3.116, 0.001)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", rand(seed))
      df.cache()
      df
    }

    multinomialDatasetWithZeroVar = {
      val nPoints = 100
      val coefficients = Array(
        -0.57997, 0.912083, -0.371077,
        -0.16624, -0.84355, -0.048509)

      val xMean = Array(5.843, 3.0)
      val xVariance = Array(0.6856, 0.0)

      val testData = generateMultinomialLogisticInput(
        coefficients, xMean, xVariance, addIntercept = true, nPoints, seed)

      val df = sc.parallelize(testData, 4).toDF().withColumn("weight", lit(1.0))
      df.cache()
      df
    }
  }

  /**
   * Enable the ignored test to export the dataset into CSV format,
   * so we can validate the training accuracy compared with R's glmnet package.
   */
  ignore("export test data into CSV format") {
    binaryDataset.rdd.map { case Row(l: Double, f: Vector, w: Double) =>
      l + "," + w + "," + f.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LogisticRegressionSuite/binaryDataset")
    binaryDatasetWithSmallVar.rdd.map { case Row(l: Double, f: Vector, w: Double) =>
      l + "," + w + "," + f.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LogisticRegressionSuite/binaryDatasetWithSmallVar")
    multinomialDataset.rdd.map { case Row(l: Double, f: Vector, w: Double) =>
      l + "," + w + "," + f.toArray.mkString(",")
    }.repartition(1).saveAsTextFile("target/tmp/LogisticRegressionSuite/multinomialDataset")
    multinomialDatasetWithSmallVar.rdd.map { case Row(l: Double, f: Vector, w: Double) =>
      l + "," + w + "," + f.toArray.mkString(",")
    }.repartition(1)
     .saveAsTextFile("target/tmp/LogisticRegressionSuite/multinomialDatasetWithSmallVar")
    multinomialDatasetWithZeroVar.rdd.map { case Row(l: Double, f: Vector, w: Double) =>
        l + "," + w + "," + f.toArray.mkString(",")
    }.repartition(1)
     .saveAsTextFile("target/tmp/LogisticRegressionSuite/multinomialDatasetWithZeroVar")
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
    val transformed = model.transform(smallBinaryDataset)
    checkNominalOnDF(transformed, "prediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "rawPrediction", model.numClasses)
    checkVectorSizeOnDF(transformed, "probability", model.numClasses)

    transformed
      .select("label", "probability", "prediction", "rawPrediction")
      .collect()
    assert(model.getThreshold === 0.5)
    assert(model.getFeaturesCol === "features")
    assert(model.getPredictionCol === "prediction")
    assert(model.getRawPredictionCol === "rawPrediction")
    assert(model.getProbabilityCol === "probability")
    assert(model.intercept !== 0.0)
    assert(model.hasParent)

    MLTestingUtils.checkCopyAndUids(lr, model)
    assert(model.hasSummary)
    val copiedModel = model.copy(ParamMap.empty)
    assert(copiedModel.hasSummary)
    model.setSummary(None)
    assert(!model.hasSummary)
  }

  test("LogisticRegression validate input dataset") {
    testInvalidClassificationLabels(new LogisticRegression().fit(_), None)
    testInvalidWeights(new LogisticRegression().setWeightCol("weight").fit(_))
    testInvalidVectors(new LogisticRegression().fit(_))
  }

  test("logistic regression: illegal params") {
    val lowerBoundsOnCoefficients = Matrices.dense(1, 4, Array(1.0, 0.0, 1.0, 0.0))
    val upperBoundsOnCoefficients1 = Matrices.dense(1, 4, Array(0.0, 1.0, 1.0, 0.0))
    val upperBoundsOnCoefficients2 = Matrices.dense(1, 3, Array(1.0, 0.0, 1.0))
    val lowerBoundsOnIntercepts = Vectors.dense(1.0)

    // Work well when only set bound in one side.
    new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .fit(binaryDataset)

    withClue("bound constrained optimization only supports L2 regularization") {
      intercept[IllegalArgumentException] {
        new LogisticRegression()
          .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
          .setElasticNetParam(1.0)
          .fit(binaryDataset)
      }
    }

    withClue("lowerBoundsOnCoefficients should less than or equal to upperBoundsOnCoefficients") {
      intercept[IllegalArgumentException] {
        new LogisticRegression()
          .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
          .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients1)
          .fit(binaryDataset)
      }
    }

    withClue("the coefficients bound matrix mismatched with shape (1, number of features)") {
      intercept[IllegalArgumentException] {
        new LogisticRegression()
          .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
          .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients2)
          .fit(binaryDataset)
      }
    }

    withClue("bounds on intercepts should not be set if fitting without intercept") {
      intercept[IllegalArgumentException] {
        new LogisticRegression()
          .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
          .setFitIntercept(false)
          .fit(binaryDataset)
      }
    }
  }

  test("empty probabilityCol or predictionCol") {
    val lr = new LogisticRegression().setMaxIter(1)
    val datasetFieldNames = smallBinaryDataset.schema.fieldNames.toSet
    def checkSummarySchema(model: LogisticRegressionModel, columns: Seq[String]): Unit = {
      val fieldNames = model.summary.predictions.schema.fieldNames
      assert(model.hasSummary)
      assert(datasetFieldNames.subsetOf(fieldNames.toSet))
      columns.foreach { c => assert(fieldNames.exists(_.startsWith(c))) }
    }
    // check that the summary model adds the appropriate columns
    Seq(("binomial", smallBinaryDataset), ("multinomial", smallMultinomialDataset)).foreach {
      case (family, dataset) =>
        lr.setFamily(family)
        lr.setProbabilityCol("").setPredictionCol("prediction")
        val modelNoProb = lr.fit(dataset)
        checkSummarySchema(modelNoProb, Seq("probability_"))

        lr.setProbabilityCol("probability").setPredictionCol("")
        val modelNoPred = lr.fit(dataset)
        checkSummarySchema(modelNoPred, Seq("prediction_"))

        lr.setProbabilityCol("").setPredictionCol("")
        val modelNoPredNoProb = lr.fit(dataset)
        checkSummarySchema(modelNoPredNoProb, Seq("prediction_", "probability_"))
    }
  }

  test("check summary types for binary and multiclass") {
    val lr = new LogisticRegression()
      .setFamily("binomial")
      .setMaxIter(1)

    val blorModel = lr.fit(smallBinaryDataset)
    assert(blorModel.summary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])
    assert(blorModel.summary.asBinary.isInstanceOf[BinaryLogisticRegressionSummary])
    assert(blorModel.binarySummary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])
    assert(blorModel.summary.totalIterations == 1)
    assert(blorModel.binarySummary.totalIterations == 1)

    val mlorModel = lr.setFamily("multinomial").fit(smallMultinomialDataset)
    assert(mlorModel.summary.isInstanceOf[LogisticRegressionTrainingSummary])
    withClue("cannot get binary summary for multiclass model") {
      intercept[RuntimeException] {
        mlorModel.binarySummary
      }
    }
    withClue("cannot cast summary to binary summary multiclass model") {
      intercept[RuntimeException] {
        mlorModel.summary.asBinary
      }
    }
    assert(mlorModel.summary.totalIterations == 1)

    val mlorBinaryModel = lr.setFamily("multinomial").fit(smallBinaryDataset)
    assert(mlorBinaryModel.summary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])
    assert(mlorBinaryModel.binarySummary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])

    val blorSummary = blorModel.evaluate(smallBinaryDataset)
    val mlorSummary = mlorModel.evaluate(smallMultinomialDataset)
    assert(blorSummary.isInstanceOf[BinaryLogisticRegressionSummary])
    assert(mlorSummary.isInstanceOf[LogisticRegressionSummary])

    // verify instance weight works
    val lr2 = new LogisticRegression()
      .setFamily("binomial")
      .setMaxIter(1)
      .setWeightCol("weight")

    val smallBinaryDatasetWithWeight =
      smallBinaryDataset.select(col("label"), col("features"), lit(2.5).as("weight"))

    val smallMultinomialDatasetWithWeight =
      smallMultinomialDataset.select(col("label"), col("features"), lit(10.0).as("weight"))

    val blorModel2 = lr2.fit(smallBinaryDatasetWithWeight)
    assert(blorModel2.summary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])
    assert(blorModel2.summary.asBinary.isInstanceOf[BinaryLogisticRegressionSummary])
    assert(blorModel2.binarySummary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])

    val mlorModel2 = lr2.setFamily("multinomial").fit(smallMultinomialDatasetWithWeight)
    assert(mlorModel2.summary.isInstanceOf[LogisticRegressionTrainingSummary])
    withClue("cannot get binary summary for multiclass model") {
      intercept[RuntimeException] {
        mlorModel2.binarySummary
      }
    }
    withClue("cannot cast summary to binary summary multiclass model") {
      intercept[RuntimeException] {
        mlorModel2.summary.asBinary
      }
    }

    val mlorBinaryModel2 = lr2.setFamily("multinomial").fit(smallBinaryDatasetWithWeight)
    assert(mlorBinaryModel2.summary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])
    assert(mlorBinaryModel2.binarySummary.isInstanceOf[BinaryLogisticRegressionTrainingSummary])

    val blorSummary2 = blorModel2.evaluate(smallBinaryDatasetWithWeight)
    val mlorSummary2 = mlorModel2.evaluate(smallMultinomialDatasetWithWeight)
    assert(blorSummary2.isInstanceOf[BinaryLogisticRegressionSummary])
    assert(mlorSummary2.isInstanceOf[LogisticRegressionSummary])

    assert(blorSummary.accuracy ~== blorSummary2.accuracy relTol 1e-6)
    assert(blorSummary.weightedPrecision ~== blorSummary2.weightedPrecision relTol 1e-6)
    assert(blorSummary.weightedRecall ~== blorSummary2.weightedRecall relTol 1e-6)
    assert(blorSummary.asBinary.areaUnderROC ~== blorSummary2.asBinary.areaUnderROC relTol 1e-6)

    assert(blorModel.summary.asBinary.accuracy ~==
      blorModel2.summary.asBinary.accuracy relTol 1e-6)
    assert(blorModel.summary.asBinary.weightedPrecision ~==
      blorModel2.summary.asBinary.weightedPrecision relTol 1e-6)
    assert(blorModel.summary.asBinary.weightedRecall ~==
      blorModel2.summary.asBinary.weightedRecall relTol 1e-6)
    assert(blorModel.summary.asBinary.areaUnderROC ~==
      blorModel2.summary.asBinary.areaUnderROC relTol 1e-6)

    assert(mlorSummary.accuracy ~== mlorSummary2.accuracy relTol 1e-6)
    assert(mlorSummary.weightedPrecision ~== mlorSummary2.weightedPrecision relTol 1e-6)
    assert(mlorSummary.weightedRecall ~== mlorSummary2.weightedRecall relTol 1e-6)

    assert(mlorModel.summary.accuracy ~== mlorModel2.summary.accuracy relTol 1e-6)
    assert(mlorModel.summary.weightedPrecision ~== mlorModel2.summary.weightedPrecision relTol 1e-6)
    assert(mlorModel.summary.weightedRecall ~==mlorModel2.summary.weightedRecall relTol 1e-6)
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
        lr2.fit(smallBinaryDataset,
          lr2.thresholds -> Array(0.3, 0.7), lr2.threshold -> (expectedThreshold / 2.0))
      }
    }
    withClue("fit with ParamMap should throw error if threshold, thresholds do not match.") {
      intercept[IllegalArgumentException] {
        val lr2model = lr2.fit(smallBinaryDataset,
          lr2.thresholds -> Array(0.3, 0.7), lr2.threshold -> (expectedThreshold / 2.0))
        lr2model.getThreshold
      }
    }
  }

  test("thresholds prediction") {
    val blr = new LogisticRegression().setFamily("binomial").setThreshold(1.0)
    val binaryModel = blr.fit(smallBinaryDataset)

    testTransformer[(Double, Vector)](smallBinaryDataset.toDF(), binaryModel, "prediction") {
      row => assert(row.getDouble(0) === 0.0)
    }

    binaryModel.setThreshold(0.0)
    testTransformer[(Double, Vector)](smallBinaryDataset.toDF(), binaryModel, "prediction") {
      row => assert(row.getDouble(0) === 1.0)
    }

    val mlr = new LogisticRegression().setFamily("multinomial")
    val model = mlr.fit(smallMultinomialDataset)
    val basePredictions = model.transform(smallMultinomialDataset).select("prediction").collect()

    // should predict all zeros
    model.setThresholds(Array(1, 1000, 1000))
    testTransformer[(Double, Vector)](smallMultinomialDataset.toDF(), model, "prediction") {
      row => assert(row.getDouble(0) === 0.0)
    }

    // should predict all ones
    model.setThresholds(Array(1000, 1, 1000))
    testTransformer[(Double, Vector)](smallMultinomialDataset.toDF(), model, "prediction") {
      row => assert(row.getDouble(0) === 1.0)
    }

    // should predict all twos
    model.setThresholds(Array(1000, 1000, 1))
    testTransformer[(Double, Vector)](smallMultinomialDataset.toDF(), model, "prediction") {
      row => assert(row.getDouble(0) === 2.0)
    }

    // constant threshold scaling is the same as no thresholds
    model.setThresholds(Array(1000, 1000, 1000))
    testTransformerByGlobalCheckFunc[(Double, Vector)](smallMultinomialDataset.toDF(), model,
      "prediction") { scaledPredictions: Seq[Row] =>
      assert(scaledPredictions.zip(basePredictions).forall { case (scaled, base) =>
        scaled.getDouble(0) === base.getDouble(0)
      })
    }

    // force it to use the predict method
    model.setRawPredictionCol("").setProbabilityCol("").setThresholds(Array(0, 1, 1))
    testTransformer[(Double, Vector)](smallMultinomialDataset.toDF(), model, "prediction") {
      row => assert(row.getDouble(0) === 0.0)
    }
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
    testTransformerByGlobalCheckFunc[(Double, Vector)](smallBinaryDataset.toDF(),
      model, "prediction", "myProbability") { rows =>
      val predAllZero = rows.map(_.getDouble(0))
      assert(predAllZero.forall(_ === 0),
        s"With threshold=1.0, expected predictions to be all 0, but only" +
        s" ${predAllZero.count(_ === 0)} of ${smallBinaryDataset.count()} were 0.")
    }
    // Call transform with params, and check that the params worked.
    testTransformerByGlobalCheckFunc[(Double, Vector)](smallBinaryDataset.toDF(),
      model.copy(ParamMap(model.threshold -> 0.0,
        model.probabilityCol -> "myProb")), "prediction", "myProb") {
      rows => assert(rows.map(_.getDouble(0)).exists(_ !== 0.0))
    }

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

    testTransformer[(Double, Vector)](smallMultinomialDataset.toDF(),
      model, "rawPrediction", "features", "probability") {
      case Row(raw: Vector, features: Vector, prob: Vector) =>
        // check that raw prediction is coefficients dot features + intercept
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
        // Compare rawPrediction with probability
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

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, LogisticRegressionModel](this, model, smallMultinomialDataset)
  }

  test("binary logistic regression: Predictor, Classifier methods") {
    val sqlContext = smallBinaryDataset.sqlContext
    import sqlContext.implicits._
    val lr = new LogisticRegression().setFamily("binomial")

    val model = lr.fit(smallBinaryDataset)
    assert(model.numClasses === 2)
    val numFeatures = smallBinaryDataset.select("features").first().getAs[Vector](0).size
    assert(model.numFeatures === numFeatures)

    testTransformer[(Double, Vector)](smallBinaryDataset.toDF(),
      model, "rawPrediction", "probability", "prediction") {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        // Compare rawPrediction with probability
        assert(raw.size === 2)
        assert(prob.size === 2)
        val probFromRaw1 = 1.0 / (1.0 + math.exp(-raw(1)))
        assert(prob(1) ~== probFromRaw1 relTol eps)
        assert(prob(0) ~== 1.0 - probFromRaw1 relTol eps)
        // Compare prediction with probability
        val predFromProb = prob.toArray.zipWithIndex.maxBy(_._1)._2
        assert(pred == predFromProb)
    }

    ProbabilisticClassifierSuite.testPredictMethods[
      Vector, LogisticRegressionModel](this, model, smallBinaryDataset)
  }

  test("prediction on single instance") {
    val blor = new LogisticRegression().setFamily("binomial")
    val blorModel = blor.fit(smallBinaryDataset)
    testPredictionModelSinglePrediction(blorModel, smallBinaryDataset)
    testClassificationModelSingleRawPrediction(blorModel, smallBinaryDataset)
    testProbClassificationModelSingleProbPrediction(blorModel, smallBinaryDataset)
    val mlor = new LogisticRegression().setFamily("multinomial")
    val mlorModel = mlor.fit(smallMultinomialDataset)
    testPredictionModelSinglePrediction(mlorModel, smallMultinomialDataset)
    testClassificationModelSingleRawPrediction(mlorModel, smallMultinomialDataset)
    testProbClassificationModelSingleProbPrediction(mlorModel, smallMultinomialDataset)
  }

  test("LogisticRegression on blocks") {
    for (dataset <- Seq(smallBinaryDataset, smallMultinomialDataset, binaryDataset,
      multinomialDataset, multinomialDatasetWithZeroVar); fitIntercept <- Seq(true, false)) {
      val mlor = new LogisticRegression()
        .setFitIntercept(fitIntercept)
        .setMaxIter(5)
        .setFamily("multinomial")
      val model = mlor.fit(dataset)
      Seq(0, 0.01, 0.1, 1, 2, 4).foreach { s =>
        val model2 = mlor.setMaxBlockSizeInMB(s).fit(dataset)
        assert(model.interceptVector ~== model2.interceptVector relTol 1e-6)
        assert(model.coefficientMatrix ~== model2.coefficientMatrix relTol 1e-6)
      }
    }

    for (dataset <- Seq(smallBinaryDataset, binaryDataset); fitIntercept <- Seq(true, false)) {
      val blor = new LogisticRegression()
        .setFitIntercept(fitIntercept)
        .setMaxIter(5)
        .setFamily("binomial")
      val model = blor.fit(dataset)
      Seq(0, 0.01, 0.1, 1, 2, 4).foreach { s =>
        val model2 = blor.setMaxBlockSizeInMB(s).fit(dataset)
        assert(model.intercept ~== model2.intercept relTol 1e-6)
        assert(model.coefficients ~== model2.coefficients relTol 1e-6)
      }
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

    testTransformerByGlobalCheckFunc[(Double, Vector)](overFlowData.toDF(),
      model, "rawPrediction", "probability") { results: Seq[Row] =>
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
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 0))
      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  2.7114519
      data.V3     -0.5667801
      data.V4      0.8818754
      data.V5     -0.3882505
      data.V6     -0.7891183
     */
    val coefficientsR = Vectors.dense(-0.5667801, 0.8818754, -0.3882505, -0.7891183)
    val interceptR = 2.7114519

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.coefficients ~= coefficientsR relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-3)
  }

  test("binary logistic regression with intercept without regularization with bound") {
    // Bound constrained optimization with bound on one side.
    val upperBoundsOnCoefficients = Matrices.dense(1, 4, Array(1.0, 0.0, 1.0, 0.0))
    val upperBoundsOnIntercepts = Vectors.dense(1.0)

    val trainer1 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected1 = Vectors.dense(
      0.05997387390575594, 0.0, -0.26536616889454984, -0.5793842425088045)
    val interceptExpected1 = 1.0

    assert(model1.intercept ~== interceptExpected1 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsExpected1 relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptExpected1 relTol 1E-3)
    assert(model2.coefficients ~= coefficientsExpected1 relTol 1E-3)

    // Bound constrained optimization with bound on both side.
    val lowerBoundsOnCoefficients = Matrices.dense(1, 4, Array(0.0, -1.0, 0.0, -1.0))
    val lowerBoundsOnIntercepts = Vectors.dense(0.0)

    val trainer3 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer4 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model3 = trainer3.fit(binaryDataset)
    val model4 = trainer4.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected3 = Vectors.dense(0.0, 0.0, 0.0, -0.7003382019888361)
    val interceptExpected3 = 0.5673234605102715

    assert(model3.intercept ~== interceptExpected3 relTol 1E-3)
    assert(model3.coefficients ~= coefficientsExpected3 relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model4.intercept ~== interceptExpected3 relTol 1E-3)
    assert(model4.coefficients ~= coefficientsExpected3 relTol 1E-3)

    // Bound constrained optimization with infinite bound on both side.
    val trainer5 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(Matrices.dense(1, 4, Array.fill(4)(Double.PositiveInfinity)))
      .setUpperBoundsOnIntercepts(Vectors.dense(Double.PositiveInfinity))
      .setLowerBoundsOnCoefficients(Matrices.dense(1, 4, Array.fill(4)(Double.NegativeInfinity)))
      .setLowerBoundsOnIntercepts(Vectors.dense(Double.NegativeInfinity))
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer6 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(Matrices.dense(1, 4, Array.fill(4)(Double.PositiveInfinity)))
      .setUpperBoundsOnIntercepts(Vectors.dense(Double.PositiveInfinity))
      .setLowerBoundsOnCoefficients(Matrices.dense(1, 4, Array.fill(4)(Double.NegativeInfinity)))
      .setLowerBoundsOnIntercepts(Vectors.dense(Double.NegativeInfinity))
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model5 = trainer5.fit(binaryDataset)
    val model6 = trainer6.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    // It should be same as unbound constrained optimization with LBFGS.
    val coefficientsExpected5 = Vectors.dense(
      -0.5667990118366208, 0.8819300812352234, -0.38825593561750166, -0.7891233856979563)
    val interceptExpected5 = 2.711413425425

    assert(model5.intercept ~== interceptExpected5 relTol 1E-3)
    assert(model5.coefficients ~= coefficientsExpected5 relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model6.intercept ~== interceptExpected5 relTol 1E-3)
    assert(model6.coefficients ~= coefficientsExpected5 relTol 1E-3)
  }

  test("SPARK-34448: binary logistic regression with intercept, with features with small var") {
    val trainer1 = new LogisticRegression().setFitIntercept(true).setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression().setFitIntercept(true).setStandardization(false)
      .setWeightCol("weight")
    val trainer3 = new LogisticRegression().setFitIntercept(true).setStandardization(true)
      .setElasticNetParam(0.0001).setRegParam(0.5).setWeightCol("weight")

    val model1 = trainer1.fit(binaryDatasetWithSmallVar)
    val model2 = trainer2.fit(binaryDatasetWithSmallVar)
    val model3 = trainer3.fit(binaryDatasetWithSmallVar)

    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0,
      lambda = 0))
      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept) -348.2955812
      data.V3       -0.8145023
      data.V4        0.8979252
      data.V5       -0.6082397
      data.V6       33.8070109

      coefficientsStd = coef(glmnet(features, label, weights=w, family="binomial", alpha = 0.0001,
      lambda = 0.5, standardize=T))
      coefficientsStd
      5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
      (Intercept) -7.403746510
      data.V3     -0.001443382
      data.V4      0.001454470
      data.V5     -0.001097110
      data.V6      0.048747722
     */
    val coefficientsR = Vectors.dense(-0.8145023, 0.8979252, -0.6082397, 33.8070109)
    val interceptR = -348.2955812

    assert(model1.intercept ~== interceptR relTol 1E-3)
    assert(model1.coefficients ~= coefficientsR relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-3)

    val coefficientsR2 = Vectors.dense(-0.001443382, 0.001454470, -0.001097110, 0.048747722)
    val interceptR2 = -7.403746510

    assert(model3.intercept ~== interceptR2 relTol 1E-3)
    assert(model3.coefficients ~= coefficientsR2 relTol 1E-3)
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
      data.V3     -0.3451301
      data.V4      1.2721785
      data.V5     -0.3537743
      data.V6     -0.7315618

     */
    val coefficientsR = Vectors.dense(-0.3451301, 1.2721785, -0.3537743, -0.7315618)

    assert(model1.intercept ~== 0.0 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsR relTol 1E-2)

    // Without regularization, with or without standardization should converge to the same solution.
    assert(model2.intercept ~== 0.0 relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-2)
  }

  test("binary logistic regression without intercept without regularization with bound") {
    val upperBoundsOnCoefficients = Matrices.dense(1, 4, Array(1.0, 0.0, 1.0, 0.0)).toSparse

    val trainer1 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setFitIntercept(false)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setFitIntercept(false)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected = Vectors.dense(
      0.20721074484293306, 0.0, -0.24389739190279183, -0.5446655961212726)

    assert(model1.intercept ~== 0.0 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsExpected relTol 1E-3)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.intercept ~== 0.0 relTol 1E-3)
    assert(model2.coefficients ~= coefficientsExpected relTol 1E-3)
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
      (Intercept) -0.07157076
      data.V3      .
      data.V4      .
      data.V5     -0.04058143
      data.V6     -0.02322760

     */
    val coefficientsRStd = Vectors.dense(0.0, 0.0, -0.04058143, -0.02322760)
    val interceptRStd = -0.07157076

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
      (Intercept)  0.3602029
      data.V3      .
      data.V4      .
      data.V5     -0.1635707
      data.V6      .

     */
    val coefficientsR = Vectors.dense(0.0, 0.0, -0.1635707, 0.0)
    val interceptR = 0.3602029

    assert(model2.intercept ~== interceptR relTol 1E-2)
    assert(model2.coefficients ~== coefficientsR absTol 1E-3)
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
      data.V5     -0.05164150
      data.V6     -0.04079129

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  .
      data.V3      .
      data.V4      .
      data.V5     -0.08408014
      data.V6      .

     */
    val coefficientsRStd = Vectors.dense(0.0, 0.0, -0.05164150, -0.04079129)

    val coefficientsR = Vectors.dense(0.0, 0.0, -0.08408014, 0.0)

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
      (Intercept)  0.12943705
      data.V3     -0.06979418
      data.V4      0.10691465
      data.V5     -0.04835674
      data.V6     -0.09939108

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                           s0
      (Intercept)  0.47553535
      data.V3     -0.05058465
      data.V4      0.02296823
      data.V5     -0.11368284
      data.V6     -0.06309008

     */
    val coefficientsRStd = Vectors.dense(-0.06979418, 0.10691465, -0.04835674, -0.09939108)
    val interceptRStd = 0.12943705
    val coefficientsR = Vectors.dense(-0.05058465, 0.02296823, -0.11368284, -0.06309008)
    val interceptR = 0.47553535

    assert(model1.intercept ~== interceptRStd relTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd relTol 1E-3)
    assert(model2.intercept ~== interceptR relTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-3)
  }

  test("binary logistic regression with intercept with L2 regularization with bound") {
    val upperBoundsOnCoefficients = Matrices.dense(1, 4, Array(1.0, 0.0, 1.0, 0.0))
    val upperBoundsOnIntercepts = Vectors.dense(1.0)

    val trainer1 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setRegParam(1.37)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setRegParam(1.37)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpectedWithStd = Vectors.dense(
      -0.06974410278847253, 0.0, -0.04833486093952599, -0.09941770618793982)
    val interceptExpectedWithStd = 0.4564981350661977
    val coefficientsExpected = Vectors.dense(
      -0.050579069523730306, 0.0, -0.11367447252893222, -0.06309435539607525)
    val interceptExpected = 0.5457873335999178

    assert(model1.intercept ~== interceptExpectedWithStd relTol 1E-3)
    assert(model1.coefficients ~= coefficientsExpectedWithStd relTol 1E-3)
    assert(model2.intercept ~== interceptExpected relTol 1E-3)
    assert(model2.coefficients ~= coefficientsExpected relTol 1E-3)
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
      data.V3     -0.05998915
      data.V4      0.12541885
      data.V5     -0.04697872
      data.V6     -0.09713973

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                            s0
      (Intercept)  .
      data.V3     -0.005927466
      data.V4      0.048313659
      data.V5     -0.092956052
      data.V6     -0.053974895

     */
    val coefficientsRStd = Vectors.dense(-0.05998915, 0.12541885, -0.04697872, -0.09713973)
    val coefficientsR = Vectors.dense(
      -0.0059320221190687205, 0.04834399477383437, -0.09296353778288495, -0.05398080548228108)

    assert(model1.intercept ~== 0.0 absTol 1E-3)
    assert(model1.coefficients ~= coefficientsRStd relTol 1E-2)
    assert(model2.intercept ~== 0.0 absTol 1E-3)
    assert(model2.coefficients ~= coefficientsR relTol 1E-2)
  }

  test("binary logistic regression without intercept with L2 regularization with bound") {
    val upperBoundsOnCoefficients = Matrices.dense(1, 4, Array(1.0, 0.0, 1.0, 0.0))

    val trainer1 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setRegParam(1.37)
      .setFitIntercept(false)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setRegParam(1.37)
      .setFitIntercept(false)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(binaryDataset)
    val model2 = trainer2.fit(binaryDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpectedWithStd = Vectors.dense(
      -0.00845365508769699, 0.0, -0.03954848648474558, -0.0851639471468608)
    val coefficientsExpected = Vectors.dense(
      0.010675769768102661, 0.0, -0.0852582080623827, -0.050615535080106376)

    assert(model1.intercept ~== 0.0 relTol 1E-3)
    assert(model1.coefficients ~= coefficientsExpectedWithStd relTol 1E-3)
    assert(model2.intercept ~== 0.0 relTol 1E-3)
    assert(model2.coefficients ~= coefficientsExpected relTol 1E-3)
  }

  test("binary logistic regression with intercept with ElasticNet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setMaxIter(120)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(true).setWeightCol("weight")
      .setTol(1e-5)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setMaxIter(60)
      .setElasticNetParam(0.38).setRegParam(0.21).setStandardization(false).setWeightCol("weight")
      .setTol(1e-5)

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
      (Intercept)  0.51344133
      data.V3     -0.04395595
      data.V4      .
      data.V5     -0.08699271
      data.V6     -0.15249200

      coefficients
      5 x 1 sparse Matrix of class "dgCMatrix"
                          s0
      (Intercept)  0.50936159
      data.V3      .
      data.V4      .
      data.V5     -0.18569346
      data.V6     -0.05625862

     */
    val coefficientsRStd = Vectors.dense(-0.04395595, 0.0, -0.08699271, -0.15249200)
    val interceptRStd = 0.51344133
    val coefficientsR = Vectors.dense(0.0, 0.0, -0.18569346, -0.05625862)
    val interceptR = 0.50936159

    assert(model1.intercept ~== interceptRStd relTol 6E-2)
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
       (Intercept) -0.2521953
       data.V3      0.0000000
       data.V4      .
       data.V5      .
       data.V6      .
     */
    val interceptR = -0.2521953
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
    val rawInterceptsTheory = histogram.map(math.log1p) // add 1 for smoothing
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
              -2.22347257
      data.V3  0.24574397
      data.V4 -0.04054235
      data.V5  0.14963756
      data.V6  0.37504027

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.3674309
      data.V3 -0.3266910
      data.V4  0.8939282
      data.V5 -0.2363519
      data.V6 -0.4631336

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               1.85604170
      data.V3  0.08094703
      data.V4 -0.85338588
      data.V5  0.08671439
      data.V6  0.08809332

     */
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.24574397, -0.04054235, 0.14963756, 0.37504027,
      -0.3266910, 0.8939282, -0.2363519, -0.4631336,
      0.08094703, -0.85338588, 0.08671439, 0.08809332), isTransposed = true)
    val interceptsR = Vectors.dense(-2.22347257, 0.3674309, 1.85604170)

    model1.coefficientMatrix.colIter.foreach(v => assert(v.toArray.sum ~== 0.0 absTol eps))
    model2.coefficientMatrix.colIter.foreach(v => assert(v.toArray.sum ~== 0.0 absTol eps))

    assert(model1.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model1.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model1.interceptVector ~== interceptsR relTol 0.05)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model2.interceptVector ~== interceptsR relTol 0.05)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with zero variance (SPARK-21681)") {
    val mlr = new LogisticRegression().setFamily("multinomial").setFitIntercept(true)
      .setElasticNetParam(0.0).setRegParam(0.0).setStandardization(true).setWeightCol("weight")

    val model = mlr.fit(multinomialDatasetWithZeroVar)

    /*
     Use the following R code to load the data and train the model using glmnet package.

     library("glmnet")
     data <- read.csv("path", header=FALSE)
     label = as.factor(data$V1)
     w = data$V2
     features = as.matrix(data.frame(data$V3, data$V4))
     coefficients = coef(glmnet(features, label, weights=w, family="multinomial",
     alpha = 0, lambda = 0))
     coefficients
     $`0`
     3 x 1 sparse Matrix of class "dgCMatrix"
                    s0
             0.2658824
     data.V3 0.1881871
     data.V4 .

     $`1`
     3 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              0.53604701
     data.V3 -0.02412645
     data.V4  .

     $`2`
     3 x 1 sparse Matrix of class "dgCMatrix"
                     s0
             -0.8019294
     data.V3 -0.1640607
     data.V4  .
    */

    val coefficientsR = new DenseMatrix(3, 2, Array(
      0.1881871, 0.0,
      -0.02412645, 0.0,
      -0.1640607, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(0.2658824, 0.53604701, -0.8019294)

    model.coefficientMatrix.colIter.foreach(v => assert(v.toArray.sum ~== 0.0 absTol eps))

    assert(model.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model.interceptVector ~== interceptsR relTol 0.05)
    assert(model.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept without regularization with bound") {
    // Bound constrained optimization with bound on one side.
    val lowerBoundsOnCoefficients = Matrices.dense(3, 4, Array.fill(12)(1.0))
    val lowerBoundsOnIntercepts = Vectors.dense(Array.fill(3)(1.0))

    val trainer1 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
      .setTol(1e-5)
    val trainer2 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")
      .setTol(1e-5)

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected1 = new DenseMatrix(3, 4, Array(
      2.1156620676212325, 2.7146375863138825, 1.8108730417428125, 2.711975470258063,
      1.54314110882009, 3.648963914233324, 1.4248901324480239, 1.8737908246138315,
      1.950852726788052, 1.9017484391817425, 1.7479497661988832, 2.425055298693075),
      isTransposed = true)
    val interceptsExpected1 = Vectors.dense(
      1.0000152482448372, 3.591773288423673, 5.079685953744937)

    checkBoundedMLORCoefficientsEquivalent(model1.coefficientMatrix, coefficientsExpected1)
    assert(model1.interceptVector ~== interceptsExpected1 relTol 0.01)
    checkBoundedMLORCoefficientsEquivalent(model2.coefficientMatrix, coefficientsExpected1)
    assert(model2.interceptVector ~== interceptsExpected1 relTol 0.01)

    // Bound constrained optimization with bound on both side.
    val upperBoundsOnCoefficients = Matrices.dense(3, 4, Array.fill(12)(2.0))
    val upperBoundsOnIntercepts = Vectors.dense(Array.fill(3)(2.0))

    val trainer3 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer4 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setUpperBoundsOnCoefficients(upperBoundsOnCoefficients)
      .setUpperBoundsOnIntercepts(upperBoundsOnIntercepts)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model3 = trainer3.fit(multinomialDataset)
    val model4 = trainer4.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected3 = new DenseMatrix(3, 4, Array(
      1.641980508924569, 1.1579023489264648, 1.434651352010351, 1.9541352988127463,
      1.3416273422126057, 2.0, 1.1014102844446283, 1.2076556940852765,
      1.6371808928302913, 1.0, 1.3936094723717016, 1.71022540576362),
      isTransposed = true)
    val interceptsExpected3 = Vectors.dense(1.0, 2.0, 2.0)

    checkBoundedMLORCoefficientsEquivalent(model3.coefficientMatrix, coefficientsExpected3)
    assert(model3.interceptVector ~== interceptsExpected3 relTol 0.01)
    checkBoundedMLORCoefficientsEquivalent(model4.coefficientMatrix, coefficientsExpected3)
    assert(model4.interceptVector ~== interceptsExpected3 relTol 0.01)

    // Bound constrained optimization with infinite bound on both side.
    val trainer5 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(Matrices.dense(3, 4, Array.fill(12)(Double.NegativeInfinity)))
      .setLowerBoundsOnIntercepts(Vectors.dense(Array.fill(3)(Double.NegativeInfinity)))
      .setUpperBoundsOnCoefficients(Matrices.dense(3, 4, Array.fill(12)(Double.PositiveInfinity)))
      .setUpperBoundsOnIntercepts(Vectors.dense(Array.fill(3)(Double.PositiveInfinity)))
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer6 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(Matrices.dense(3, 4, Array.fill(12)(Double.NegativeInfinity)))
      .setLowerBoundsOnIntercepts(Vectors.dense(Array.fill(3)(Double.NegativeInfinity)))
      .setUpperBoundsOnCoefficients(Matrices.dense(3, 4, Array.fill(12)(Double.PositiveInfinity)))
      .setUpperBoundsOnIntercepts(Vectors.dense(Array.fill(3)(Double.PositiveInfinity)))
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model5 = trainer5.fit(multinomialDataset)
    val model6 = trainer6.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    // It should be same as unbound constrained optimization with LBFGS.
    val coefficientsExpected5 = new DenseMatrix(3, 4, Array(
      0.24573204902629314, -0.040610820463585905, 0.14962716893619094, 0.37502549108817784,
      -0.3266914048842952, 0.8940567211111817, -0.23633898260880218, -0.4631024664883818,
      0.08095935585808962, -0.8534459006476851, 0.0867118136726069, 0.0880769754002182),
      isTransposed = true)
    val interceptsExpected5 = Vectors.dense(
      -2.2231282183460723, 0.3669496747012527, 1.856178543644802)

    checkBoundedMLORCoefficientsEquivalent(model5.coefficientMatrix, coefficientsExpected5)
    assert(model5.interceptVector ~== interceptsExpected5 relTol 0.01)
    checkBoundedMLORCoefficientsEquivalent(model6.coefficientMatrix, coefficientsExpected5)
    assert(model6.interceptVector ~== interceptsExpected5 relTol 0.01)
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
      data.V3  0.06892068
      data.V4 -0.36546704
      data.V5  0.12274583
      data.V6  0.32616580

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.2987384
      data.V4  0.9483147
      data.V5 -0.2328113
      data.V6 -0.4555157

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3  0.2298177
      data.V4 -0.5828477
      data.V5  0.1100655
      data.V6  0.1293499


     */
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.06892068, -0.36546704, 0.12274583, 0.32616580,
      -0.2987384, 0.9483147, -0.2328113, -0.4555157,
      0.2298177, -0.5828477, 0.1100655, 0.1293499), isTransposed = true)

    model1.coefficientMatrix.colIter.foreach(v => assert(v.toArray.sum ~== 0.0 absTol eps))
    model2.coefficientMatrix.colIter.foreach(v => assert(v.toArray.sum ~== 0.0 absTol eps))

    assert(model1.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model1.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.coefficientMatrix.toArray.sum ~== 0.0 absTol eps)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept without regularization with bound") {
    val lowerBoundsOnCoefficients = Matrices.dense(3, 4, Array.fill(12)(1.0))

    val trainer1 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setFitIntercept(false)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setFitIntercept(false)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpected = new DenseMatrix(3, 4, Array(
      1.5933935326002155, 1.4427758360562475, 1.356079506266844, 1.7818682794856215,
      1.2224266732592248, 2.762691362720858, 1.0005885171478472, 1.0000022613855966,
      1.7524631428961193, 1.2292565990448736, 1.3433784431904323, 1.5846063017678864),
      isTransposed = true)

    checkBoundedMLORCoefficientsEquivalent(model1.coefficientMatrix, coefficientsExpected)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    checkBoundedMLORCoefficientsEquivalent(model2.coefficientMatrix, coefficientsExpected)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
  }

  test("multinomial logistic regression with intercept with L1 regularization") {

    // use tighter constraints because OWL-QN solver takes longer to converge
    val trainer1 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(true)
      .setMaxIter(160).setTol(1e-5).setWeightCol("weight")
    val trainer2 = (new LogisticRegression).setFitIntercept(true)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false)
      .setMaxIter(110).setTol(1e-5).setWeightCol("weight")

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
              -0.69265374
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  0.09064661

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.2260274
      data.V3 -0.1144333
      data.V4  0.3204703
      data.V5 -0.1621061
      data.V6 -0.2308192

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.9186811
      data.V3  .
      data.V4 -0.4832131
      data.V5  .
      data.V6  .


      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.44707756
      data.V3  .
      data.V4  .
      data.V5  0.01641412
      data.V6  0.03570376

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.75180900
      data.V3 -0.05110822
      data.V4  .
      data.V5 -0.21595670
      data.V6 -0.16162836

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.3047314
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.09064661,
      -0.1144333, 0.3204703, -0.1621061, -0.2308192,
      0.0, -0.4832131, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.69265374, -0.2260274, 0.9186811)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.01641412, 0.03570376,
      -0.05110822, 0.0, -0.21595670, -0.16162836,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.44707756, 0.75180900, -0.3047314)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 1e-3)
    assert(model1.interceptVector ~== interceptsRStd relTol 1e-3)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 1e-3)
    assert(model2.interceptVector ~== interceptsR relTol 1e-3)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("SPARK-34860: multinomial logistic regression with intercept, with small var") {
    val trainer1 = new LogisticRegression().setFitIntercept(true).setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression().setFitIntercept(true).setStandardization(false)
      .setWeightCol("weight")
    val trainer3 = new LogisticRegression().setFitIntercept(true).setStandardization(true)
      .setElasticNetParam(0.0001).setRegParam(0.5).setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDatasetWithSmallVar)
    val model2 = trainer2.fit(multinomialDatasetWithSmallVar)
    val model3 = trainer3.fit(multinomialDatasetWithSmallVar)

    /*
      Use the following R code to load the data and train the model using glmnet package.
      library("glmnet")
      data <- read.csv("path", header=FALSE)
      label = factor(data$V1)
      w = data$V2
      features = as.matrix(data.frame(data$V3, data$V4, data$V5, data$V6))
      coefficients = coef(glmnet(features, label, weights=w, family="multinomial", alpha = 0,
      lambda = 0))
      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               2.91748298
      data.V3  0.21755977
      data.V4  0.01647541
      data.V5  0.16507778
      data.V6 -0.14016680

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -17.5107460
      data.V3  -0.2443600
      data.V4   0.7564655
      data.V5  -0.2955698
      data.V6   1.3262009

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              14.59326301
      data.V3  0.02680026
      data.V4 -0.77294095
      data.V5  0.13049206
      data.V6 -1.18603411



      coefficientsStd = coef(glmnet(features, label, weights=w, family="multinomial",
      alpha = 0.0001, lambda = 0.5, standardize=T))
      coefficientsStd
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              1.751626027
      data.V3 0.019970169
      data.V4 0.079611293
      data.V5 0.003959452
      data.V6 0.110024399

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                         s0
              -3.9297124987
      data.V3 -0.0004788494
      data.V4  0.0010097453
      data.V5 -0.0005832701
      data.V6  .

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                        s0
               2.178086472
      data.V3 -0.019369990
      data.V4 -0.080851149
      data.V5 -0.003319687
      data.V6 -0.112435972
     */
    val interceptsR = Vectors.dense(2.91748298, -17.5107460, 14.59326301)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.21755977, 0.01647541, 0.16507778, -0.14016680,
      -0.2443600, 0.7564655, -0.2955698, 1.3262009,
      0.02680026, -0.77294095, 0.13049206, -1.18603411), isTransposed = true)

    assert(model1.interceptVector ~== interceptsR relTol 1e-2)
    assert(model1.coefficientMatrix ~= coefficientsR relTol 1e-1)

    // Without regularization, with or without standardization will converge to the same solution.
    assert(model2.interceptVector ~== interceptsR relTol 1e-2)
    assert(model2.coefficientMatrix ~= coefficientsR relTol 1e-1)

    val interceptsR2 = Vectors.dense(1.751626027, -3.9297124987, 2.178086472)
    val coefficientsR2 = new DenseMatrix(3, 4, Array(
      0.019970169, 0.079611293, 0.003959452, 0.110024399,
      -0.0004788494, 0.0010097453, -0.0005832701, 0.0,
      -0.019369990, -0.080851149, -0.003319687, -0.112435972), isTransposed = true)

    assert(model3.interceptVector ~== interceptsR2 relTol 1e-3)
    assert(model3.coefficientMatrix ~= coefficientsR2 relTol 1e-2)
  }

  test("multinomial logistic regression without intercept with L1 regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(true).setWeightCol("weight")
      .setTol(1e-5)
    val trainer2 = (new LogisticRegression).setFitIntercept(false)
      .setElasticNetParam(1.0).setRegParam(0.05).setStandardization(false).setWeightCol("weight")
      .setTol(1e-5)

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
      data.V6 0.01167

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.1413518
      data.V4  0.5100469
      data.V5 -0.1658025
      data.V6 -0.2755998

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              .
      data.V3 0.001536337
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
      data.V4  0.2094410
      data.V5 -0.1944582
      data.V6 -0.1307681

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
      0.0, 0.0, 0.0, 0.01167,
      -0.1413518, 0.5100469, -0.1658025, -0.2755998,
      0.001536337, 0.0, 0.0, 0.0), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.2094410, -0.1944582, -0.1307681,
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
              -1.68571384
      data.V3  0.17156077
      data.V4  0.01658014
      data.V5  0.10303296
      data.V6  0.26459585

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.2364585
      data.V3 -0.2182805
      data.V4  0.5960025
      data.V5 -0.1587441
      data.V6 -0.3121284

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               1.44925536
      data.V3  0.04671972
      data.V4 -0.61258267
      data.V5  0.05571116
      data.V6  0.04753251

      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -1.65140201
      data.V3  0.15446206
      data.V4  0.02134769
      data.V5  0.12524946
      data.V6  0.22607972

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               1.1367722
      data.V3 -0.1931713
      data.V4  0.2766548
      data.V5 -0.1910455
      data.V6 -0.2629336

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.51462979
      data.V3  0.03870921
      data.V4 -0.29800245
      data.V5  0.06579606
      data.V6  0.03685390


     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.17156077, 0.01658014, 0.10303296, 0.26459585,
      -0.2182805, 0.5960025, -0.1587441, -0.3121284,
      0.04671972, -0.61258267, 0.05571116, 0.04753251), isTransposed = true)
    val interceptsRStd = Vectors.dense(-1.68571384, 0.2364585, 1.44925536)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.15446206, 0.02134769, 0.12524946, 0.22607972,
      -0.1931713, 0.2766548, -0.1910455, -0.2629336,
      0.03870921, -0.29800245, 0.06579606, 0.03685390), isTransposed = true)
    val interceptsR = Vectors.dense(-1.65140201, 1.1367722, 0.51462979)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.001)
    assert(model1.interceptVector ~== interceptsRStd relTol 0.05)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR relTol 0.05)
    assert(model2.interceptVector ~== interceptsR relTol 0.05)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression with intercept with L2 regularization with bound") {
    val lowerBoundsOnCoefficients = Matrices.dense(3, 4, Array.fill(12)(1.0))
    val lowerBoundsOnIntercepts = Vectors.dense(Array.fill(3)(1.0))

    val trainer1 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setRegParam(0.1)
      .setFitIntercept(true)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setLowerBoundsOnIntercepts(lowerBoundsOnIntercepts)
      .setRegParam(0.1)
      .setFitIntercept(true)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpectedWithStd = new DenseMatrix(3, 4, Array(
      1.0, 1.0, 1.0, 1.025970328910313,
      1.0, 1.4150672323873024, 1.0, 1.0,
      1.0, 1.0, 1.0, 1.0), isTransposed = true)
    val interceptsExpectedWithStd = Vectors.dense(
      2.4259954221861473, 1.0000087410832004, 2.490461716522559)
    val coefficientsExpected = new DenseMatrix(3, 4, Array(
      1.0, 1.0, 1.0336746541813002, 1.0,
      1.0, 1.0, 1.0, 1.0,
      1.0, 1.0, 1.0, 1.0), isTransposed = true)
    val interceptsExpected = Vectors.dense(1.0521598454128, 1.0, 1.213158241431565)

    assert(model1.coefficientMatrix ~== coefficientsExpectedWithStd relTol 0.01)
    assert(model1.interceptVector ~== interceptsExpectedWithStd relTol 0.01)
    assert(model2.coefficientMatrix ~== coefficientsExpected relTol 0.01)
    assert(model2.interceptVector ~== interceptsExpected relTol 0.01)
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
      data.V3  0.03804571
      data.V4 -0.23204409
      data.V5  0.08337512
      data.V6  0.23029089

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.2015495
      data.V4  0.6328705
      data.V5 -0.1562475
      data.V6 -0.3071447

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.16350376
      data.V4 -0.40082637
      data.V5  0.07287239
      data.V6  0.07685379

      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                        s0
               .
      data.V3 -0.006493452
      data.V4 -0.143831823
      data.V5  0.092538445
      data.V6  0.187244839

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3 -0.08068443
      data.V4  0.39038929
      data.V5 -0.16822390
      data.V6 -0.23667470

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               .
      data.V3  0.08717788
      data.V4 -0.24655746
      data.V5  0.07568546
      data.V6  0.04942986


     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.03804571, -0.23204409, 0.08337512, 0.23029089,
      -0.2015495, 0.6328705, -0.1562475, -0.3071447,
      0.16350376, -0.40082637, 0.07287239, 0.07685379), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      -0.006493452, -0.143831823, 0.092538445, 0.187244839,
      -0.08068443, 0.39038929, -0.16822390, -0.23667470,
      0.08717788, -0.24655746, 0.07568546, 0.04942986), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with L2 regularization with bound") {
    val lowerBoundsOnCoefficients = Matrices.dense(3, 4, Array.fill(12)(1.0))

    val trainer1 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setRegParam(0.1)
      .setFitIntercept(false)
      .setStandardization(true)
      .setWeightCol("weight")
    val trainer2 = new LogisticRegression()
      .setLowerBoundsOnCoefficients(lowerBoundsOnCoefficients)
      .setRegParam(0.1)
      .setFitIntercept(false)
      .setStandardization(false)
      .setWeightCol("weight")

    val model1 = trainer1.fit(multinomialDataset)
    val model2 = trainer2.fit(multinomialDataset)

    // The solution is generated by https://github.com/yanboliang/bound-optimization.
    val coefficientsExpectedWithStd = new DenseMatrix(3, 4, Array(
      1.01324653, 1.0, 1.0, 1.0415767,
      1.0, 1.0, 1.0, 1.0,
      1.02244888, 1.0, 1.0, 1.0), isTransposed = true)
    val coefficientsExpected = new DenseMatrix(3, 4, Array(
      1.0, 1.0, 1.03932259, 1.0,
      1.0, 1.0, 1.0, 1.0,
      1.0, 1.0, 1.03274649, 1.0), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsExpectedWithStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.coefficientMatrix ~== coefficientsExpected absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
  }

  test("multinomial logistic regression with intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setMaxIter(180).setTol(1e-5)
    val trainer2 = (new LogisticRegression).setFitIntercept(true).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setMaxIter(150).setTol(1e-5)

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
              -0.55325803
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  0.09074857

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.27291366
      data.V3 -0.09093399
      data.V4  0.28078251
      data.V5 -0.12854559
      data.V6 -0.18382494

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               0.8261717
      data.V3  .
      data.V4 -0.4064444
      data.V5  .
      data.V6  .

      coefficients
      $`0`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              -0.40016908
      data.V3  .
      data.V4  .
      data.V5  0.02312769
      data.V6  0.04159224

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
               0.62474768
      data.V3 -0.03776471
      data.V4  .
      data.V5 -0.19588206
      data.V6 -0.11187712

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
              -0.2245786
      data.V3  .
      data.V4  .
      data.V5  .
      data.V6  .


     */
    val coefficientsRStd = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.09074857,
      -0.09093399, 0.28078251, -0.12854559, -0.18382494,
      0.0, -0.4064444, 0.0, 0.0), isTransposed = true)
    val interceptsRStd = Vectors.dense(-0.55325803, -0.27291366, 0.8261717)
    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.02312769, 0.04159224,
      -0.03776471, 0.0, -0.19588206, -0.11187712,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)
    val interceptsR = Vectors.dense(-0.40016908, 0.62474768, -0.2245786)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.05)
    assert(model1.interceptVector ~== interceptsRStd absTol 0.1)
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector ~== interceptsR absTol 0.01)
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("multinomial logistic regression without intercept with elasticnet regularization") {
    val trainer1 = (new LogisticRegression).setFitIntercept(false).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(true)
      .setTol(1e-5)
    val trainer2 = (new LogisticRegression).setFitIntercept(false).setWeightCol("weight")
      .setElasticNetParam(0.5).setRegParam(0.1).setStandardization(false)
      .setTol(1e-5)

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
      data.V6 0.03418889

      $`1`
      5 x 1 sparse Matrix of class "dgCMatrix"
                      s0
               .
      data.V3 -0.1114779
      data.V4  0.3992145
      data.V5 -0.1315371
      data.V6 -0.2107956

      $`2`
      5 x 1 sparse Matrix of class "dgCMatrix"
                       s0
              .
      data.V3 0.006442826
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
      data.V4  0.15710979
      data.V5 -0.16871602
      data.V6 -0.07928527

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
      0.0, 0.0, 0.0, 0.03418889,
      -0.1114779, 0.3992145, -0.1315371, -0.2107956,
      0.006442826, 0.0, 0.0, 0.0), isTransposed = true)

    val coefficientsR = new DenseMatrix(3, 4, Array(
      0.0, 0.0, 0.0, 0.0,
      0.0, 0.15710979, -0.16871602, -0.07928527,
      0.0, 0.0, 0.0, 0.0), isTransposed = true)

    assert(model1.coefficientMatrix ~== coefficientsRStd absTol 0.01)
    assert(model1.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model1.interceptVector.toArray.sum ~== 0.0 absTol eps)
    assert(model2.coefficientMatrix ~== coefficientsR absTol 0.01)
    assert(model2.interceptVector.toArray === Array.fill(3)(0.0))
    assert(model2.interceptVector.toArray.sum ~== 0.0 absTol eps)
  }

  test("evaluate on test set") {
    // Evaluate on test set should be same as that of the transformed training data.
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setThreshold(0.6)
      .setFamily("binomial")
    val blorModel = lr.fit(smallBinaryDataset)
    val blorSummary = blorModel.binarySummary

    val sameBlorSummary =
      blorModel.evaluate(smallBinaryDataset).asInstanceOf[BinaryLogisticRegressionSummary]
    assert(blorSummary.areaUnderROC === sameBlorSummary.areaUnderROC)
    assert(blorSummary.roc.collect() === sameBlorSummary.roc.collect())
    assert(blorSummary.pr.collect === sameBlorSummary.pr.collect())
    assert(
      blorSummary.fMeasureByThreshold.collect() === sameBlorSummary.fMeasureByThreshold.collect())
    assert(
      blorSummary.recallByThreshold.collect() === sameBlorSummary.recallByThreshold.collect())
    assert(
      blorSummary.precisionByThreshold.collect() === sameBlorSummary.precisionByThreshold.collect())
    assert(blorSummary.labels === sameBlorSummary.labels)
    assert(blorSummary.truePositiveRateByLabel === sameBlorSummary.truePositiveRateByLabel)
    assert(blorSummary.falsePositiveRateByLabel === sameBlorSummary.falsePositiveRateByLabel)
    assert(blorSummary.precisionByLabel === sameBlorSummary.precisionByLabel)
    assert(blorSummary.recallByLabel === sameBlorSummary.recallByLabel)
    assert(blorSummary.fMeasureByLabel === sameBlorSummary.fMeasureByLabel)
    assert(blorSummary.accuracy === sameBlorSummary.accuracy)
    assert(blorSummary.weightedTruePositiveRate === sameBlorSummary.weightedTruePositiveRate)
    assert(blorSummary.weightedFalsePositiveRate === sameBlorSummary.weightedFalsePositiveRate)
    assert(blorSummary.weightedRecall === sameBlorSummary.weightedRecall)
    assert(blorSummary.weightedPrecision === sameBlorSummary.weightedPrecision)
    assert(blorSummary.weightedFMeasure === sameBlorSummary.weightedFMeasure)

    lr.setFamily("multinomial")
    val mlorModel = lr.fit(smallMultinomialDataset)
    val mlorSummary = mlorModel.summary

    val mlorSameSummary = mlorModel.evaluate(smallMultinomialDataset)

    assert(mlorSummary.truePositiveRateByLabel === mlorSameSummary.truePositiveRateByLabel)
    assert(mlorSummary.falsePositiveRateByLabel === mlorSameSummary.falsePositiveRateByLabel)
    assert(mlorSummary.precisionByLabel === mlorSameSummary.precisionByLabel)
    assert(mlorSummary.recallByLabel === mlorSameSummary.recallByLabel)
    assert(mlorSummary.fMeasureByLabel === mlorSameSummary.fMeasureByLabel)
    assert(mlorSummary.accuracy === mlorSameSummary.accuracy)
    assert(mlorSummary.weightedTruePositiveRate === mlorSameSummary.weightedTruePositiveRate)
    assert(mlorSummary.weightedFalsePositiveRate === mlorSameSummary.weightedFalsePositiveRate)
    assert(mlorSummary.weightedPrecision === mlorSameSummary.weightedPrecision)
    assert(mlorSummary.weightedRecall === mlorSameSummary.weightedRecall)
    assert(mlorSummary.weightedFMeasure === mlorSameSummary.weightedFMeasure)
  }

  test("evaluate with labels that are not doubles") {
    // Evaluate a test set with Label that is a numeric type other than Double
    val blor = new LogisticRegression()
      .setMaxIter(1)
      .setRegParam(1.0)
      .setFamily("binomial")
    val blorModel = blor.fit(smallBinaryDataset)
    val blorSummary = blorModel.evaluate(smallBinaryDataset)
      .asInstanceOf[BinaryLogisticRegressionSummary]

    val blorLongLabelData = smallBinaryDataset.select(col(blorModel.getLabelCol).cast(LongType),
      col(blorModel.getFeaturesCol))
    val blorLongSummary = blorModel.evaluate(blorLongLabelData)
      .asInstanceOf[BinaryLogisticRegressionSummary]

    assert(blorSummary.areaUnderROC === blorLongSummary.areaUnderROC)

    val mlor = new LogisticRegression()
      .setMaxIter(1)
      .setRegParam(1.0)
      .setFamily("multinomial")
    val mlorModel = mlor.fit(smallMultinomialDataset)
    val mlorSummary = mlorModel.evaluate(smallMultinomialDataset)

    val mlorLongLabelData = smallMultinomialDataset.select(
      col(mlorModel.getLabelCol).cast(LongType),
      col(mlorModel.getFeaturesCol))
    val mlorLongSummary = mlorModel.evaluate(mlorLongLabelData)

    assert(mlorSummary.accuracy === mlorLongSummary.accuracy)
  }

  test("statistics on training data") {
    // Test that loss is monotonically decreasing.
    val blor = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setFamily("binomial")
    val blorModel = blor.fit(smallBinaryDataset)
    assert(
      blorModel.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))

    val mlor = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(1.0)
      .setFamily("multinomial")
    val mlorModel = mlor.fit(smallMultinomialDataset)
    assert(
      mlorModel.summary
        .objectiveHistory
        .sliding(2)
        .forall(x => x(0) >= x(1)))
  }

  test("logistic regression with sample weights") {
    def modelEquals(m1: LogisticRegressionModel, m2: LogisticRegressionModel): Unit = {
      assert(m1.coefficientMatrix ~== m2.coefficientMatrix absTol 0.05)
      assert(m1.interceptVector ~== m2.interceptVector absTol 0.05)
    }
    val testParams = Seq(
      ("binomial", smallBinaryDataset, 2),
      ("multinomial", smallMultinomialDataset, 3)
    )
    testParams.foreach { case (family, dataset, numClasses) =>
      val estimator = new LogisticRegression().setFamily(family)
      MLTestingUtils.testArbitrarilyScaledWeights[LogisticRegressionModel, LogisticRegression](
        dataset.as[LabeledPoint], estimator, modelEquals)
      MLTestingUtils.testOutliersWithSmallWeights[LogisticRegressionModel, LogisticRegression](
        dataset.as[LabeledPoint], estimator, numClasses, modelEquals, outlierRatio = 3)
      MLTestingUtils.testOversamplingVsWeighting[LogisticRegressionModel, LogisticRegression](
        dataset.as[LabeledPoint], estimator, modelEquals, seed)
    }
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
    val binaryExpected = model1.transform(smallBinaryDataset).select("prediction").collect()
      .map(_.getDouble(0))
    for (model <- Seq(model1, model2)) {
      testTransformerByGlobalCheckFunc[(Double, Vector)](smallBinaryDataset.toDF(), model,
        "prediction") { rows: Seq[Row] =>
        rows.map(_.getDouble(0)).toArray === binaryExpected
      }
    }
    assert(model2.summary.totalIterations === 0)

    val lr3 = new LogisticRegression().setFamily("multinomial")
    val model3 = lr3.fit(smallMultinomialDataset)
    val lr4 = new LogisticRegression()
      .setInitialModel(model3).setMaxIter(5).setFamily("multinomial")
    val model4 = lr4.fit(smallMultinomialDataset)
    val multinomialExpected = model3.transform(smallMultinomialDataset).select("prediction")
      .collect().map(_.getDouble(0))
    for (model <- Seq(model3, model4)) {
      testTransformerByGlobalCheckFunc[(Double, Vector)](smallMultinomialDataset.toDF(), model,
        "prediction") { rows: Seq[Row] =>
        rows.map(_.getDouble(0)).toArray === multinomialExpected
      }
    }
    assert(model4.summary.totalIterations === 0)
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
    assert(allZeroInterceptModel.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)

    val allOneInterceptModel = lrIntercept
      .setLabelCol("oneLabel")
      .fit(sameLabels)
    assert(allOneInterceptModel.coefficients ~== Vectors.dense(0.0) absTol 1E-3)
    assert(allOneInterceptModel.intercept === Double.PositiveInfinity)
    assert(allOneInterceptModel.summary.totalIterations === 0)
    assert(allOneInterceptModel.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)

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
    testTransformer[(Double, Vector)](constantData, model,
      "rawPrediction", "probability", "prediction") {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(raw === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, Double.PositiveInfinity)))
        assert(prob === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, 1.0)))
        assert(pred === 4.0)
    }
    assert(model.summary.totalIterations === 0)
    assert(model.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)

    // force the model to be trained with only one class
    val constantZeroData = Seq(
      LabeledPoint(0.0, Vectors.dense(0.0)),
      LabeledPoint(0.0, Vectors.dense(1.0)),
      LabeledPoint(0.0, Vectors.dense(2.0))).toDF()
    val modelZeroLabel = mlr.setFitIntercept(false).fit(constantZeroData)
    testTransformer[(Double, Vector)](constantZeroData, modelZeroLabel,
      "rawPrediction", "probability", "prediction") {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(prob === Vectors.dense(Array(1.0)))
        assert(pred === 0.0)
    }
    assert(modelZeroLabel.summary.totalIterations === 0)

    // ensure that the correct value is predicted when numClasses passed through metadata
    val labelMeta = NominalAttribute.defaultAttr.withName("label").withNumValues(6).toMetadata()
    val constantDataWithMetadata = constantData
      .select(constantData("label").as("label", labelMeta), constantData("features"))
    val modelWithMetadata = mlr.setFitIntercept(true).fit(constantDataWithMetadata)
    testTransformer[(Double, Vector)](constantDataWithMetadata, modelWithMetadata,
      "rawPrediction", "probability", "prediction") {
      case Row(raw: Vector, prob: Vector, pred: Double) =>
        assert(raw === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, Double.PositiveInfinity, 0.0)))
        assert(prob === Vectors.dense(Array(0.0, 0.0, 0.0, 0.0, 1.0, 0.0)))
        assert(pred === 4.0)
    }
    require(modelWithMetadata.summary.totalIterations === 0)
    assert(model.summary.objectiveHistory(0) ~== 0.0 absTol 1e-4)
  }

  test("compressed storage for constant label") {
    /*
      When the label is constant and fit intercept is true, all the coefficients will be
      zeros, and so the model coefficients should be stored as sparse data structures, except
      when the matrix dimensions are very small.
     */
    val moreClassesThanFeatures = Seq(
      LabeledPoint(4.0, Vectors.dense(Array.fill(5)(0.0))),
      LabeledPoint(4.0, Vectors.dense(Array.fill(5)(1.0))),
      LabeledPoint(4.0, Vectors.dense(Array.fill(5)(2.0)))).toDF()
    val mlr = new LogisticRegression().setFamily("multinomial").setFitIntercept(true)
    val model = mlr.fit(moreClassesThanFeatures)
    assert(model.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model.coefficientMatrix.isColMajor)

    // in this case, it should be stored as row major
    val moreFeaturesThanClasses = Seq(
      LabeledPoint(1.0, Vectors.dense(Array.fill(5)(0.0))),
      LabeledPoint(1.0, Vectors.dense(Array.fill(5)(1.0))),
      LabeledPoint(1.0, Vectors.dense(Array.fill(5)(2.0)))).toDF()
    val model2 = mlr.fit(moreFeaturesThanClasses)
    assert(model2.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model2.coefficientMatrix.isRowMajor)

    val blr = new LogisticRegression().setFamily("binomial").setFitIntercept(true)
    val blrModel = blr.fit(moreFeaturesThanClasses)
    assert(blrModel.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(blrModel.coefficientMatrix.asInstanceOf[SparseMatrix].colPtrs.length === 2)
  }

  test("compressed coefficients") {

    val trainer1 = new LogisticRegression()
      .setRegParam(0.1)
      .setElasticNetParam(1.0)
      .setMaxIter(20)

    // compressed row major is optimal
    val model1 = trainer1.fit(multinomialDataset.limit(100))
    assert(model1.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model1.coefficientMatrix.isRowMajor)

    // compressed column major is optimal since there are more classes than features
    val labelMeta = NominalAttribute.defaultAttr.withName("label").withNumValues(6).toMetadata()
    val model2 = trainer1.fit(multinomialDataset
      .withColumn("label", col("label").as("label", labelMeta)).limit(100))
    assert(model2.coefficientMatrix.isInstanceOf[SparseMatrix])
    assert(model2.coefficientMatrix.isColMajor)

    // coefficients are dense without L1 regularization
    val trainer2 = new LogisticRegression()
      .setElasticNetParam(0.0).setMaxIter(1)
    val model3 = trainer2.fit(multinomialDataset.limit(100))
    assert(model3.coefficientMatrix.isInstanceOf[DenseMatrix])
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
      LogisticRegressionSuite.allParamSettings, checkModelData)

    // test lr with bounds on coefficients, need to set elasticNetParam to 0.
    val numFeatures = smallBinaryDataset.select("features").head().getAs[Vector](0).size
    val lowerBounds = new DenseMatrix(1, numFeatures, (1 to numFeatures).map(_ / 1000.0).toArray)
    val upperBounds = new DenseMatrix(1, numFeatures, (1 to numFeatures).map(_ * 1000.0).toArray)
    val paramSettings = Map("lowerBoundsOnCoefficients" -> lowerBounds,
      "upperBoundsOnCoefficients" -> upperBounds,
      "elasticNetParam" -> 0.0
    )
    testEstimatorAndModelReadWrite(lr, smallBinaryDataset, paramSettings,
      paramSettings, checkModelData)
  }

  test("should support all NumericType labels and weights, and not support other types") {
    val lr = new LogisticRegression().setMaxIter(1)
    MLTestingUtils.checkNumericTypes[LogisticRegressionModel, LogisticRegression](
      lr, spark) { (expected, actual) =>
        assert(expected.intercept === actual.intercept)
        assert(expected.coefficients.toArray === actual.coefficients.toArray)
      }
  }

  test("string params should be case-insensitive") {
    val lr = new LogisticRegression()
    Seq(("AuTo", smallBinaryDataset), ("biNoMial", smallBinaryDataset),
      ("mulTinomIAl", smallMultinomialDataset)).foreach { case (family, data) =>
      lr.setFamily(family)
      assert(lr.getFamily === family)
      val model = lr.fit(data)
      assert(model.getFamily === family)
    }
  }

  test("toString") {
    val model = new LogisticRegressionModel("logReg", Vectors.dense(0.1, 0.2, 0.3), 0.0)
    val expected = "LogisticRegressionModel: uid=logReg, numClasses=2, numFeatures=3"
    assert(model.toString === expected)
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

  /**
   * Note: This method is only used in Bounded MLOR (without regularization) test
   * When no regularization is applied, the multinomial coefficients lack identifiability
   * because we do not use a pivot class. We can add any constant value to the coefficients
   * and get the same likelihood. If fitting under bound constrained optimization, we don't
   * choose the mean centered coefficients like what we do for unbound problems, since they
   * may out of the bounds. We use this function to check whether two coefficients are equivalent.
   */
  def checkBoundedMLORCoefficientsEquivalent(coefficients1: Matrix, coefficients2: Matrix): Unit = {
    coefficients1.colIter.zip(coefficients2.colIter).foreach { case (col1: Vector, col2: Vector) =>
      (col1.asBreeze - col2.asBreeze).toArray.toSeq.sliding(2).foreach {
        case Seq(v1, v2) => assert(v1 ~= v2 absTol 1E-2)
      }
    }
  }
}
