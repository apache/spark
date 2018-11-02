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
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
// $example off$
import org.apache.spark.sql.SparkSession

/**
 * A simple example demonstrating model selection using TrainValidationSplit.
 *
 * Run with
 * {{{
 * bin/run-example ml.ModelSelectionViaTrainValidationSplitExample
 * }}}
 */
object ModelSelectionViaTrainValidationSplitExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("ModelSelectionViaTrainValidationSplitExample")
      .getOrCreate()

    // $example on$
    // Prepare training and test data.
    val data = spark.read.format("libsvm").load("data/mllib/sample_linear_regression_data.txt")
    val Array(training, test) = data.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LinearRegression()
        .setMaxIter(10)

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // TrainValidationSplit will try all combinations of values and determine best model using
    // the evaluator.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .addGrid(lr.fitIntercept)
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .build()

    // In this case the estimator is simply the linear regression.
    // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(lr)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      // 80% of the data will be used for training and the remaining 20% for validation.
      .setTrainRatio(0.8)
      // Evaluate up to 2 parameter settings in parallel
      .setParallelism(2)

    // Run train validation split, and choose the best set of parameters.
    val model = trainValidationSplit.fit(training)

    // Make predictions on test data. model is the model with combination of parameters
    // that performed best.
    model.transform(test)
      .select("features", "label", "prediction")
      .show()
    // $example off$

    spark.stop()
  }
}
