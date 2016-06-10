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

// scalastyle:off println
package org.apache.spark.examples.ml

// $example on$
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.MultilayerPerceptronRegressor
// $example off$
import org.apache.spark.sql.SparkSession


/**
 * An example for Multilayer Perceptron Regression.
 */
object MultilayerPerceptronRegressorExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
        .builder
        .appName("MultilayerPerceptronRegressorExample")
        .getOrCreate()

    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    val data = spark.read.format("libsvm")
        .load("data/mllib/sample_mlpr_data.txt")
    // Split the data into train and test
    val Array(train, test) = data.randomSplit(Array(0.7, 0.3))
    // Specify layers for the neural network:
    // Input layer that is the size of the number of features (12),
    // four hidden layers of size 20, 30, 40 and 50, and an output of size 1
    // (this will always be 1 for regression problems).
    val layers = Array[Int](12, 20, 30, 40, 50, 1)
    // Create the trainer and set its parameters
    val trainer = new MultilayerPerceptronRegressor()
      .setLayers(layers)
      .setSolver("l-bfgs")
      .setSeed(1234L)
    // Train the model
    val model = trainer.fit(train)
    // compute accuracy on the test set
    val result = model.transform(test)
    val predictionAndLabels = result.select("label", "prediction")
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictionAndLabels)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    // $example off$

  spark.stop()
  }
}

// scalastyle:on println
