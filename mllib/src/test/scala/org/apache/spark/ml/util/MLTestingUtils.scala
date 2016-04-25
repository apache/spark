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
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tree.impl.TreeTests
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}
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
      isClassification: Boolean,
      sqlContext: SQLContext)(check: (M, M) => Unit): Unit = {
    val dfs = if (isClassification) {
      genClassifDFWithNumericLabelCol(sqlContext)
    } else {
      genRegressionDFWithNumericLabelCol(sqlContext)
    }
    val expected = estimator.fit(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => estimator.fit(dfs(t)))
    actuals.foreach(actual => check(expected, actual))

    val dfWithStringLabels = sqlContext.createDataFrame(Seq(
      ("0", Vectors.dense(0, 2, 3), 0.0)
    )).toDF("label", "features", "censor")
    val thrown = intercept[IllegalArgumentException] {
      estimator.fit(dfWithStringLabels)
    }
    assert(thrown.getMessage.contains(
      "Column label must be of type NumericType but was actually of type StringType"))
  }

  def checkNumericTypes[T <: Evaluator](evaluator: T, sqlContext: SQLContext): Unit = {
    val dfs = genEvaluatorDFWithNumericLabelCol(sqlContext, "label", "prediction")
    val expected = evaluator.evaluate(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => evaluator.evaluate(dfs(t)))
    actuals.foreach(actual => assert(expected === actual))

    val dfWithStringLabels = sqlContext.createDataFrame(Seq(
      ("0", 0d)
    )).toDF("label", "prediction")
    val thrown = intercept[IllegalArgumentException] {
      evaluator.evaluate(dfWithStringLabels)
    }
    assert(thrown.getMessage.contains(
      "Column label must be of type NumericType but was actually of type StringType"))
  }

  def genClassifDFWithNumericLabelCol(
      sqlContext: SQLContext,
      labelColName: String = "label",
      featuresColName: String = "features"): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
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
      }.toMap
  }

  def genRegressionDFWithNumericLabelCol(
      sqlContext: SQLContext,
      labelColName: String = "label",
      featuresColName: String = "features",
      censorColName: String = "censor"): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
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

  def genEvaluatorDFWithNumericLabelCol(
      sqlContext: SQLContext,
      labelColName: String = "label",
      predictionColName: String = "prediction"): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
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
}
