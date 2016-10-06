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
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.attribute.NominalAttribute
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.feature.Instance
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MLTestingUtils extends SparkFunSuite {
  def checkCopy(model: Model[_]): Unit = {
    val copied = model.copy(ParamMap.empty)
      .asInstanceOf[Model[_]]
    assert(copied.parent.uid == model.parent.uid)
    assert(copied.parent == model.parent)
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
    val expected = estimator.fit(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => estimator.fit(dfs(t)))
    actuals.foreach(actual => check(expected, actual))

    val dfWithStringLabels = spark.createDataFrame(Seq(
      ("0", Vectors.dense(0, 2, 3), 0.0)
    )).toDF("label", "features", "censor")
    val thrown = intercept[IllegalArgumentException] {
      estimator.fit(dfWithStringLabels)
    }
    assert(thrown.getMessage.contains(
      "Column label must be of type NumericType but was actually of type StringType"))
  }

  def checkNumericTypesALS(
      estimator: ALS,
      spark: SparkSession,
      column: String,
      baseType: NumericType)
      (check: (ALSModel, ALSModel) => Unit)
      (check2: (ALSModel, ALSModel, DataFrame) => Unit): Unit = {
    val dfs = genRatingsDFWithNumericCols(spark, column)
    val expected = estimator.fit(dfs(baseType))
    val actuals = dfs.keys.filter(_ != baseType).map(t => (t, estimator.fit(dfs(t))))
    actuals.foreach { case (_, actual) => check(expected, actual) }
    actuals.foreach { case (t, actual) => check2(expected, actual, dfs(t)) }

    val baseDF = dfs(baseType)
    val others = baseDF.columns.toSeq.diff(Seq(column)).map(col(_))
    val cols = Seq(col(column).cast(StringType)) ++ others
    val strDF = baseDF.select(cols: _*)
    val thrown = intercept[IllegalArgumentException] {
      estimator.fit(strDF)
    }
    assert(thrown.getMessage.contains(
      s"$column must be of type NumericType but was actually of type StringType"))
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
      "Column label must be of type NumericType but was actually of type StringType"))
  }

  def genClassifDFWithNumericLabelCol(
      spark: SparkSession,
      labelColName: String = "label",
      featuresColName: String = "features"): Map[NumericType, DataFrame] = {
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
        t -> TreeTests.setMetadata(castDF, 0, labelColName, featuresColName)
      }.toMap
  }

  def genRegressionDFWithNumericLabelCol(
      spark: SparkSession,
      labelColName: String = "label",
      featuresColName: String = "features",
      censorColName: String = "censor"): Map[NumericType, DataFrame] = {
    val df = spark.createDataFrame(Seq(
      (0, Vectors.dense(0)),
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
      }.toMap
  }

  def genRatingsDFWithNumericCols(
      spark: SparkSession,
      column: String): Map[NumericType, DataFrame] = {
    val df = spark.createDataFrame(Seq(
      (0, 10, 1.0),
      (1, 20, 2.0),
      (2, 30, 3.0),
      (3, 40, 4.0),
      (4, 50, 5.0)
    )).toDF("user", "item", "rating")

    val others = df.columns.toSeq.diff(Seq(column)).map(col(_))
    val types: Seq[NumericType] =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map { t =>
      val cols = Seq(col(column).cast(t)) ++ others
      t -> df.select(cols: _*)
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

  def genClassificationInstancesWithWeightedOutliers(
      spark: SparkSession,
      numClasses: Int,
      numInstances: Int): DataFrame = {
    val data = Array.tabulate[Instance](numInstances) { i =>
      val feature = i % numClasses
      if (i < numInstances / 3) {
        // give large weights to minority of data with 1 to 1 mapping feature to label
        Instance(feature, 1.0, Vectors.dense(feature))
      } else {
        // give small weights to majority of data points with reverse mapping
        Instance(numClasses - feature - 1, 0.01, Vectors.dense(feature))
      }
    }
    val labelMeta =
      NominalAttribute.defaultAttr.withName("label").withNumValues(numClasses).toMetadata()
    spark.createDataFrame(data).select(col("label").as("label", labelMeta), col("weight"),
      col("features"))
  }

  def genEquivalentOversampledAndWeightedInstances(
      data: DataFrame,
      labelCol: String,
      featuresCol: String,
      seed: Long): (DataFrame, DataFrame) = {
    import data.sparkSession.implicits._
    val rng = scala.util.Random
    rng.setSeed(seed)
    val sample: () => Int = () => rng.nextInt(10) + 1
    val sampleUDF = udf(sample)
    val rawData = data.select(labelCol, featuresCol).withColumn("samples", sampleUDF())
    val overSampledData = rawData.rdd.flatMap {
      case Row(label: Double, features: Vector, n: Int) =>
        Iterator.fill(n)(Instance(label, 1.0, features))
    }.toDF()
    rng.setSeed(seed)
    val weightedData = rawData.rdd.map {
      case Row(label: Double, features: Vector, n: Int) =>
        Instance(label, n.toDouble, features)
    }.toDF()
    (overSampledData, weightedData)
  }
}
