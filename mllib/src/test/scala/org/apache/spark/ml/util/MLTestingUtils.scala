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
import org.apache.spark.ml.{Model, Predictor}
import org.apache.spark.ml.classification.{ClassificationModel, Classifier}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.{RegressionModel, Regressor}
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

  def checkAcceptAllNumericTypes[M <: RegressionModel[_, M], T <: Regressor[_, _, M]](
      regressor: T, sqlContext: SQLContext)(check: (M, M) => Unit): Unit = {
    val dfs = genRegressionDFWithNumericLabelCol(sqlContext, "label", "features")
    val expected = regressor.fit(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => regressor.fit(dfs(t)))
    actuals.foreach(actual => check(expected, actual))
  }

  def checkAcceptAllNumericTypes[M <: ClassificationModel[_, M], T <: Classifier[_, _, M]](
      classifier: T, sqlContext: SQLContext)(check: (M, M) => Unit): Unit = {
    val dfs = genClassifDFWithNumericLabelCol(sqlContext, "label", "features")
    val expected = classifier.fit(dfs(DoubleType))
    val actuals = dfs.keys.filter(_ != DoubleType).map(t => classifier.fit(dfs(t)))
    actuals.foreach(actual => check(expected, actual))
  }
  def checkRejectNotNumericTypes(predictor: Predictor[_, _, _], sqlContext: SQLContext): Unit = {
    val dfWithStringLabels =
      MLTestingUtils.generateDFWithStringLabelCol(sqlContext, "label", "features")
    val thrown = intercept[IllegalArgumentException] {
      predictor.fit(dfWithStringLabels)
    }
    assert(thrown.getMessage contains
      "Column label must be of type NumericType but was actually of type StringType")
  }

  def genClassifDFWithNumericLabelCol(
      sqlContext: SQLContext,
      labelColName: String,
      featuresColName: String): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
      (0, Vectors.dense(0, 2, 3)),
      (1, Vectors.dense(0, 3, 1)),
      (0, Vectors.dense(0, 2, 2)),
      (1, Vectors.dense(0, 3, 9)),
      (0, Vectors.dense(0, 2, 6))
    )).toDF(labelColName, featuresColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map(t => t -> df.select(col(labelColName).cast(t), col(featuresColName))).toMap
  }

  def genRegressionDFWithNumericLabelCol(
      sqlContext: SQLContext,
      labelColName: String,
      featuresColName: String,
      censorColName: String = "censor"): Map[NumericType, DataFrame] = {
    val df = sqlContext.createDataFrame(Seq(
      (0, Vectors.dense(0), 0.0),
      (1, Vectors.dense(1), 1.0),
      (2, Vectors.dense(2), 0.0),
      (3, Vectors.dense(3), 1.0),
      (4, Vectors.dense(4), 0.0)
    )).toDF(labelColName, featuresColName, censorColName)

    val types =
      Seq(ShortType, LongType, IntegerType, FloatType, ByteType, DoubleType, DecimalType(10, 0))
    types.map(t =>
      t -> df.select(col(labelColName).cast(t), col(featuresColName), col(censorColName))).toMap
  }

  def generateDFWithStringLabelCol(
      sqlContext: SQLContext,
      labelColName: String,
      featuresColName: String,
      censorColName: String = "censor"): DataFrame =
    sqlContext.createDataFrame(Seq(
      ("0", Vectors.dense(0, 2, 3), 0.0),
      ("1", Vectors.dense(0, 3, 1), 1.0),
      ("0", Vectors.dense(0, 2, 2), 0.0),
      ("1", Vectors.dense(0, 3, 9), 1.0),
      ("0", Vectors.dense(0, 2, 6), 0.0)
    )).toDF(labelColName, featuresColName, censorColName)
}
