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

package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, Limits, ParamRandomBuilder}
import org.apache.spark.ml.tuning.RandomRanges._
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * A simple example demonstrating model selection using ParamRandomBuilder.
 *
 * Run with
 * {{{
 * bin/run-example ml.ModelSelectionViaRandomHyperparametersExample
 * }}}
 */
object ModelSelectionViaRandomHyperparametersExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ModelSelectionViaTrainValidationSplitExample")
      .getOrCreate()
    // scalastyle:off println
    // $example on$
    // Prepare training and test data.
    val data = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")

    val lr = new LinearRegression().setMaxIter(10)

    // We sample the regularization parameter logarithmically over the range [0.01, 1.0].
    // This means that values around 0.01, 0.1 and 1.0 are roughly equally likely.
    // Note that both parameters must be greater than zero as otherwise we'll get an infinity.
    // We sample the the ElasticNet mixing parameter uniformly over the range [0, 1]
    // Note that in real life, you'd choose more than the 5 samples we see below.
    val hyperparameters = new ParamRandomBuilder()
      .addLog10Random(lr.regParam, Limits(0.01, 1.0), 5)
      .addGrid(lr.fitIntercept)
      .addRandom(lr.elasticNetParam, Limits(0.0, 1.0), 5)
      .build()

    println(s"hyperparameters:\n${hyperparameters.mkString("\n")}")

    val cv: CrossValidator = new CrossValidator()
      .setEstimator(lr)
      .setEstimatorParamMaps(hyperparameters)
      .setEvaluator(new RegressionEvaluator)
      .setNumFolds(3)
    val cvModel: CrossValidatorModel = cv.fit(data)
    val parent: LinearRegression = cvModel.bestModel.parent.asInstanceOf[LinearRegression]

    println(s"""Optimal model has:
         |${lr.regParam}        = ${parent.getRegParam}
         |${lr.elasticNetParam} = ${parent.getElasticNetParam}
         |${lr.fitIntercept}    = ${parent.getFitIntercept}""".stripMargin)
    // $example off$

    spark.stop()
  }
  // scalastyle:on println
}
