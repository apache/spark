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

package org.apache.spark.examples.ml;

// $example on$
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.MinMaxScalerModel;
import org.apache.spark.ml.regression.FMRegressionModel;
import org.apache.spark.ml.regression.FMRegressor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
// $example off$

public class JavaFMRegressorExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaFMRegressorExample")
        .getOrCreate();

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");

    // Scale features.
    MinMaxScalerModel featureScaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")
        .fit(data);

    // Split the data into training and test sets (30% held out for testing).
    Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];

    // Train a FM model.
    FMRegressor fm = new FMRegressor()
        .setLabelCol("label")
        .setFeaturesCol("scaledFeatures")
        .setStepSize(0.001);

    // Create a Pipeline.
    Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {featureScaler, fm});

    // Train model.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> predictions = model.transform(testData);

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show(5);

    // Select (prediction, true label) and compute test error.
    RegressionEvaluator evaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictions);
    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    FMRegressionModel fmModel = (FMRegressionModel)(model.stages()[1]);
    System.out.println("Factors: " + fmModel.factors());
    System.out.println("Linear: " + fmModel.linear());
    System.out.println("Intercept: " + fmModel.intercept());
    // $example off$

    spark.stop();
  }
}
