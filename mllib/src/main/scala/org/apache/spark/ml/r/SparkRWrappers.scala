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

package org.apache.spark.ml.api.r

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.ml.clustering.{KMeansModel, KMeans}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.attribute._
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{VectorAssembler, RFormula}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

object SparkRWrappers {
  def fitRModelFormula(
      value: String,
      df: DataFrame,
      family: String,
      lambda: Double,
      alpha: Double,
      standardize: Boolean,
      solver: String): PipelineModel = {
    val formula = new RFormula().setFormula(value)
    val estimator = family match {
      case "gaussian" => new LinearRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
        .setStandardization(standardize)
        .setSolver(solver)
      case "binomial" => new LogisticRegression()
        .setRegParam(lambda)
        .setElasticNetParam(alpha)
        .setFitIntercept(formula.hasIntercept)
        .setStandardization(standardize)
    }
    val pipeline = new Pipeline().setStages(Array(formula, estimator))
    pipeline.fit(df)
  }

  def fitKMeans(
      initMode: String,
      df: DataFrame,
      maxIter: Double,
      initSteps: Double,
      k: Double,
      columns: String): KMeansModel = {
    val assembler = new VectorAssembler().setInputCols(columns.split(",")).setOutputCol("features")
    val features = assembler.transform(df).select("features")
    // scalastyle:off println
    features.collect().foreach { case Row(v) => println(v) }
    // scalastyle:on println
    val kMeans = new KMeans()
      .setInitMode(initMode)
      .setMaxIter(maxIter.toInt)
      .setInitSteps(initSteps.toInt)
      .setK(k.toInt)
    kMeans.fit(features)
  }

  def getModelCoefficients(model: PipelineModel): Array[Double] = {
    model.stages.last match {
      case m: LinearRegressionModel => {
        val coefficientStandardErrorsR = Array(m.summary.coefficientStandardErrors.last) ++
          m.summary.coefficientStandardErrors.dropRight(1)
        val tValuesR = Array(m.summary.tValues.last) ++ m.summary.tValues.dropRight(1)
        val pValuesR = Array(m.summary.pValues.last) ++ m.summary.pValues.dropRight(1)
        if (m.getFitIntercept) {
          Array(m.intercept) ++ m.coefficients.toArray ++ coefficientStandardErrorsR ++
            tValuesR ++ pValuesR
        } else {
          m.coefficients.toArray ++ coefficientStandardErrorsR ++ tValuesR ++ pValuesR
        }
      }
      case m: LogisticRegressionModel => {
        if (m.getFitIntercept) {
          Array(m.intercept) ++ m.coefficients.toArray
        } else {
          m.coefficients.toArray
        }
      }
    }
  }

  def getModelDevianceResiduals(model: PipelineModel): Array[Double] = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        m.summary.devianceResiduals
      case m: LogisticRegressionModel =>
        throw new UnsupportedOperationException(
          "No deviance residuals available for LogisticRegressionModel")
    }
  }

  def getModelFeatures(model: PipelineModel): Array[String] = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        val attrs = AttributeGroup.fromStructField(
          m.summary.predictions.schema(m.summary.featuresCol))
        if (m.getFitIntercept) {
          Array("(Intercept)") ++ attrs.attributes.get.map(_.name.get)
        } else {
          attrs.attributes.get.map(_.name.get)
        }
      case m: LogisticRegressionModel =>
        val attrs = AttributeGroup.fromStructField(
          m.summary.predictions.schema(m.summary.featuresCol))
        if (m.getFitIntercept) {
          Array("(Intercept)") ++ attrs.attributes.get.map(_.name.get)
        } else {
          attrs.attributes.get.map(_.name.get)
        }
    }
  }

  def getModelName(model: PipelineModel): String = {
    model.stages.last match {
      case m: LinearRegressionModel =>
        "LinearRegressionModel"
      case m: LogisticRegressionModel =>
        "LogisticRegressionModel"
    }
  }

  // scalastyle:off println
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test-R").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)
    import sqlCtx.implicits._
    val initMode = "random"
    val columns = "Sepal_Length,Sepal_Width,Petal_Length,Petal_Width"
    val df = sc.textFile("iris.txt").map(_.split("\\s+"))
      .map(ary => (ary(1).toDouble, ary(2).toDouble, ary(3).toDouble, ary(4).toDouble))
      .toDF("Sepal_Length", "Sepal_Width", "Petal_Length", "Petal_Width")
    val model = fitKMeans(initMode, df, 10, 10, 3, columns)
    println(model)
  }
  // scalastyle:on println
}
