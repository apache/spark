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

package org.apache.spark.ml.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml._
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.feature.{Instance, LabeledPoint}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.HasWeightCol
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MLTestingUtils extends SparkFunSuite {

  def checkCopyAndUids[T <: Estimator[_]](estimator: T, model: Model[_]): Unit = {
    assert(estimator.uid === model.uid, "Model uid does not match parent estimator")

    // copied model must have the same parent
    val copied = model.copy(ParamMap.empty)
      .asInstanceOf[Model[_]]
    assert(copied.parent == model.parent)
    assert(copied.parent.uid == model.parent.uid)
  }

  def checkNumericTypes[M <: Model[M], T <: Estimator[M]](
      estimator: T,
      spark: SparkSession,
      isClassification: Boolean = true)(check: (M, M) => Unit): Unit = {
    val dfs = if (isClassification) {
      genClassifDFWithNumericLabelCol(spark)
    } else {
      genRegressionDFWithNumericLabelCol(spark)
    }

    val finalEstimator = estimator match {
      case weighted: Estimator[M] with HasWeightCol =>
        weighted.set(weighted.weightCol, "weight")
        weighted
      case _ => estimator
    }

    val expected = finalEstimator.fit(dfs(DoubleType))

    val actuals = dfs.keys.filter(_ != DoubleType).map { t =>
      finalEstimator.fit(dfs(t))
    }

    actuals.foreach(actual => check(expected, actual))

    val dfWithStringLabels = spark.createDataFrame(Seq(
      ("0", 1, Vectors.dense(0, 2, 3), 0.0)
    )).toDF("label", "weight", "features", "censor")
    val thrown = intercept[IllegalArgumentException] {
      estimator.fit(dfWithStringLabels)
    }
    assert(thrown.getMessage.contains(
      "Column label must be of type numeric but was actually of type string"))

    estimator match {
      case weighted: Estimator[M] with HasWeightCol =>
        val dfWithStringWeights = spark.createDataFrame(Seq(
          (0, "1", Vectors.dense(0, 2, 3), 0.0)
        )).toDF("label", "weight", "features", "censor")
        weighted.set(weighted.weightCol, "weight")
        val thrown = intercept[IllegalArgumentException] {
          weighted.fit(dfWithStringWeights)
        }
        assert(thrown.getMessage.contains(
          "Column weight must be of type numeric but was actually of type string"))
      case _ =>
    }
  }

  def checkNumericTypes[T <: Evaluator](evaluator: T, spark: SparkSession): Unit = {
    val dfs = genEvaluatorDFWithNumericLabelCol(spark, "label", "prediction")
    val expected = evaluator.evaluate(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => evaluator.evaluate(dfs(t)))
    actuals.foreach(actual => assert(expected === actual))

    val dfWithStringLabels = spark.createDataFrame(Seq(
      ("0", 0d)
    )).toDF("label", "prediction")
    val thrown = intercept[IllegalArgumentException] {
      evaluator.evaluate(dfWithStringLabels)
    }
    assert(thrown.getMessage.contains(
      "Column label must be of type numeric but was actually of type string"))
  }

  def genClassifDFWithNumericLabelCol(
      spark: SparkSession,
      labelColName: String = "label",
      featuresColName: String = "features",
      weightColName: String = "weight"): Map[NumericType, DataFrame] = {
    val df = spark.createDataFrame(Seq(
      (0, Vectors.dense(0, 2, 3)),
      (1, Vectors.dense(0, 3, 1)),
      (0, Vectors.dense(0, 2, 2)),
      (1, Vectors.dense(0, 3, 9)),
      (0, Vectors.dense(0, 2, 6))
    )).toDF(labelColName, featuresColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map { t =>
        val castDF = df.select(col(labelColName).cast(t), col(featuresColName))
        t -> TreeTests.setMetadata(castDF, 2, labelColName, featuresColName)
          .withColumn(weightColName, round(rand(seed = 42)).cast(t))
      }.toMap
  }

  def genRegressionDFWithNumericLabelCol(
      spark: SparkSession,
      labelColName: String = "label",
      weightColName: String = "weight",
      featuresColName: String = "features",
      censorColName: String = "censor"): Map[NumericType, DataFrame] = {
    val df = spark.createDataFrame(Seq(
      (1, Vectors.dense(1)),
      (2, Vectors.dense(2)),
      (3, Vectors.dense(3)),
      (4, Vectors.dense(4))
    )).toDF(labelColName, featuresColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map { t =>
      val castDF = df.select(col(labelColName).cast(t), col(featuresColName))
      t -> TreeTests.setMetadata(castDF, 0, labelColName, featuresColName)
        .withColumn(censorColName, lit(0.0))
        .withColumn(weightColName, round(rand(seed = 42)).cast(t))
    }.toMap
  }

  def genEvaluatorDFWithNumericLabelCol(
      spark: SparkSession,
      labelColName: String = "label",
      predictionColName: String = "prediction"): Map[NumericType, DataFrame] = {
    val df = spark.createDataFrame(Seq(
      (0, 0d),
      (1, 1d),
      (2, 2d),
      (3, 3d),
      (4, 4d)
    )).toDF(labelColName, predictionColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types
      .map(t => t -> df.select(col(labelColName).cast(t), col(predictionColName)))
      .toMap
  }

  /**
   * Given a DataFrame, generate two output DataFrames: one having the original rows oversampled
   * an integer number of times, and one having the original rows but with a column of weights
   * proportional to the number of oversampled instances in the oversampled DataFrames.
   */
  def genEquivalentOversampledAndWeightedInstances(
      data: Dataset[LabeledPoint],
      seed: Long): (Dataset[Instance], Dataset[Instance]) = {
    import data.sparkSession.implicits._
    val rng = new scala.util.Random(seed)
    val sample: () => Int = () => rng.nextInt(10) + 1
    val sampleUDF = udf(sample)
    val rawData = data.select("label", "features").withColumn("samples", sampleUDF())
    val overSampledData = rawData.rdd.flatMap { case Row(label: Double, features: Vector, n: Int) =>
      Iterator.fill(n)(Instance(label, 1.0, features))
    }.toDS()
    rng.setSeed(seed)
    val weightedData = rawData.rdd.map { case Row(label: Double, features: Vector, n: Int) =>
      Instance(label, n.toDouble, features)
    }.toDS()
    (overSampledData, weightedData)
  }

  /**
   * Helper function for testing sample weights. Tests that oversampling each point is equivalent
   * to assigning a sample weight proportional to the number of samples for each point.
   */
  def testOversamplingVsWeighting[M <: Model[M], E <: Estimator[M]](
      data: Dataset[LabeledPoint],
      estimator: E with HasWeightCol,
      modelEquals: (M, M) => Unit,
      seed: Long): Unit = {
    val (overSampledData, weightedData) = genEquivalentOversampledAndWeightedInstances(
      data, seed)
    val overSampledModel = estimator.set(estimator.weightCol, "").fit(overSampledData)
    val weightedModel = estimator.set(estimator.weightCol, "weight").fit(weightedData)
    modelEquals(weightedModel, overSampledModel)
  }

  /**
   * Helper function for testing sample weights. Tests that injecting a large number of outliers
   * with very small sample weights does not affect fitting. The predictor should learn the true
   * model despite the outliers.
   */
  def testOutliersWithSmallWeights[M <: Model[M], E <: Estimator[M]](
      data: Dataset[LabeledPoint],
      estimator: E with HasWeightCol,
      numClasses: Int,
      modelEquals: (M, M) => Unit,
      outlierRatio: Int): Unit = {
    import data.sqlContext.implicits._
    val outlierDS = data.withColumn("weight", lit(1.0)).as[Instance].flatMap {
      case Instance(l, w, f) =>
        val outlierLabel = if (numClasses == 0) -l else numClasses - l - 1
        List.fill(outlierRatio)(Instance(outlierLabel, 0.0001, f)) ++ List(Instance(l, w, f))
    }
    val trueModel = estimator.set(estimator.weightCol, "").fit(data)
    val outlierModel = estimator.set(estimator.weightCol, "weight")
      .fit(outlierDS)
    modelEquals(trueModel, outlierModel)
  }

  /**
   * Helper function for testing sample weights. Tests that giving constant weights to each data
   * point yields the same model, regardless of the magnitude of the weight.
   */
  def testArbitrarilyScaledWeights[M <: Model[M], E <: Estimator[M]](
      data: Dataset[LabeledPoint],
      estimator: E with HasWeightCol,
      modelEquals: (M, M) => Unit): Unit = {
    estimator.set(estimator.weightCol, "weight")
    val models = Seq(0.01, 1.0, 1000.0).map { w =>
      val df = data.withColumn("weight", lit(w))
      estimator.fit(df)
    }
    models.sliding(2).foreach { case Seq(m1, m2) => modelEquals(m1, m2)}
  }

  /**
   * Helper function for testing different input types for "features" column. Given a DataFrame,
   * generate three output DataFrames: one having vector "features" column with float precision,
   * one having double array "features" column with float precision, and one having float array
   * "features" column.
   */
  def generateArrayFeatureDataset(dataset: DataFrame,
    featuresColName: String = "features"): (DataFrame, DataFrame, DataFrame) = {
    val toFloatVectorUDF = udf { (features: Vector) =>
      Vectors.dense(features.toArray.map(_.toFloat.toDouble))}
    val toDoubleArrayUDF = udf { (features: Vector) => features.toArray}
    val toFloatArrayUDF = udf { (features: Vector) => features.toArray.map(_.toFloat)}
    val newDataset = dataset.withColumn(featuresColName, toFloatVectorUDF(col(featuresColName)))
    val newDatasetD = newDataset.withColumn(featuresColName, toDoubleArrayUDF(col(featuresColName)))
    val newDatasetF = newDataset.withColumn(featuresColName, toFloatArrayUDF(col(featuresColName)))
    assert(newDataset.schema(featuresColName).dataType.equals(new VectorUDT))
    assert(newDatasetD.schema(featuresColName).dataType.equals(new ArrayType(DoubleType, false)))
    assert(newDatasetF.schema(featuresColName).dataType.equals(new ArrayType(FloatType, false)))
    (newDataset, newDatasetD, newDatasetF)
  }

  def modelPredictionEquals[M <: PredictionModel[_, M]](
      data: DataFrame,
      compareFunc: (Double, Double) => Boolean,
      fractionInTol: Double)(
      model1: M,
      model2: M): Unit = {
    val pred1 = model1.transform(data).select(model1.getPredictionCol).collect()
    val pred2 = model2.transform(data).select(model2.getPredictionCol).collect()
    val inTol = pred1.zip(pred2).count { case (p1, p2) =>
      val x = p1.getDouble(0)
      val y = p2.getDouble(0)
      compareFunc(x, y)
    }
    assert(inTol / pred1.length.toDouble >= fractionInTol)
  }
}
